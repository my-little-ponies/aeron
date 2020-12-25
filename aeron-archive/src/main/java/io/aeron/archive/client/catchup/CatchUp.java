package io.aeron.archive.client.catchup;

import static io.aeron.archive.client.util.AeronUtils.isConnectionBroken;
import static io.aeron.archive.client.util.AeronUtils.listRecordingsForUri;
import static io.aeron.CommonContext.NULL_SESSION_ID;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static org.agrona.CloseHelper.quietClose;

import io.aeron.archive.client.catchup.context.CatchUpContext;
import io.aeron.archive.client.catchup.context.EventStoreClientContext;
import io.aeron.archive.client.catchup.exception.CatchUpNotTheSameStreamsError;
import io.aeron.archive.client.catchup.exception.EventStoreTooManyLiveRecordingsException;
import io.aeron.archive.client.util.AeronUtils;
import io.aeron.archive.client.util.RateLimitedLogger;
import io.aeron.archive.client.util.RecordingDescriptor;
import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.AeronArchive.Context;
import io.aeron.archive.client.ArchiveException;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.agrona.DirectBuffer;
import org.agrona.collections.MutableReference;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.EpochClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main algorithm for the catch up process. It's implemented with finite state machine pattern.
 *
 */
public class CatchUp implements Agent, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(CatchUp.class);
    public static final long WARN_AFTER_CLOSE_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(5);

    private final EpochClock clock;
    private final Aeron aeron;

    private final String liveChannel;
    private final int liveStreamId;
    private final String replayChannel;
    private final int replayStreamId;
    private final boolean fragmentAssemblyRequired;
    private final boolean continuousReplay;
    private final long continuousReplayTimeoutMs;
    private final long logInfoTimeoutMs;

    private final FragmentHandler originalClientFragmentHandler;
    private final FragmentHandler clientFragmentHandler;
    private final MessageMatcher messageMatcher;

    private final BooleanSupplier isRunning;
    private final CaughtUpHook caughtUpHook;
    private final Consumer<RecordingDescriptor> nextRecordingHook;

    private final int fragmentLimit;
    private final int sessionId;
    private RecordingDescriptor lastRecording;
    private long lastPosition = NULL_POSITION;

    private AbstractPhase phase;
    private boolean closed = false;

    private long startTime;
    private long warnTimestamp = -1;
    private long logStatTimestamp = -1;

    private final AeronArchive archiveClient;

    public CatchUp(Aeron aeron, AeronArchive archiveClient, CatchUpContext context) {
        context.validated();

        this.aeron = aeron;
        this.archiveClient = archiveClient;
        this.clock = context.getClock();
        this.liveChannel = context.getLiveChannel();
        this.liveStreamId = context.getLiveStreamId();
        this.replayChannel = context.getReplayChannel();
        this.replayStreamId = context.getReplayStreamId();
        this.fragmentLimit = context.getFragmentLimit();
        this.sessionId = context.getSessionId();
        this.isRunning = context.getIsRunning();
        this.lastPosition = context.getLastPosition();
        this.originalClientFragmentHandler = context.getClientFragmentHandler();
        this.clientFragmentHandler =
                new FragmentAssembler(
                        new SavePositionFragmentHandler(context.getClientFragmentHandler()));
        this.messageMatcher = context.getMessageMatcher();
        this.caughtUpHook = new CaughtUpHook(context.getCaughtUpHook());
        this.fragmentAssemblyRequired = context.isFragmentAssemblyRequired();
        this.continuousReplay = context.isContinuousReplay();
        this.continuousReplayTimeoutMs = context.getContinuousReplayTimeoutMs();
        this.logInfoTimeoutMs = context.getLogInfoTimeoutMs();

        phase =
                new InitPhase(
                        context.getLastRecording()
                                .map(RecordingDescriptor::getRecordingId)
                                .orElse(0L),
                        context.getLastPosition());

        this.nextRecordingHook =
                rec -> {
                    lastRecording = rec;
                    lastPosition = rec.getStartPosition();
                    context.getNextRecordingHook().accept(rec);
                };



        LOG.info("Start a CatchUp for live channel {} stream id {}", liveChannel, liveStreamId);
    }

    public CatchUp(
            Aeron aeron, EventStoreClientContext archiveClientContext, CatchUpContext context) {
        this(aeron, connectToArchive(aeron, archiveClientContext), context);
    }

    private static AeronArchive connectToArchive(Aeron aeron, EventStoreClientContext context) {

        Context archiveCtx = new Context();
        try {
            archiveCtx
                    .aeron(aeron)
                    .controlRequestChannel(context.getControlRequestChannel())
                    .controlRequestStreamId(context.getControlRequestStreamId())
                    .controlResponseChannel(context.getControlResponseChannel())
                    .controlResponseStreamId(context.getControlResponseStreamId());
        } catch (Exception e) {
            archiveCtx.close();
            throw e;
        }

        AeronArchive aeronArchive = AeronArchive.connect(archiveCtx);
        LOG.info("Created aeronArchive with {}", aeronArchive.controlSessionId());
        return aeronArchive;
    }

    public boolean healthCheck() {
        return !phase.equals(AbstractPhase.TERMINAL_PHASE);
    }

    public boolean inProgress() {
        return isRunning.getAsBoolean() && !caughtUpHook.isTriggered();
    }

    public Optional<Subscription> getLiveSubscription() {
        if (phase instanceof OnlinePhase) {
            return Optional.of(((OnlinePhase) phase).getLiveSubscription());
        } else {
            return Optional.empty();
        }
    }

    /** For tests only */
    AeronArchive getArchiveClient() {
        return archiveClient;
    }

    /** For tests only */
    AbstractPhase getPhase() {
        return phase;
    }

    /** For tests only */
    void setPhase(AbstractPhase phase) {
        this.phase = phase;
    }

    @Override
    public int doWork() {
        if (!isRunning.getAsBoolean() || closed) {
            long now = clock.time();
            if (now - warnTimestamp >= WARN_AFTER_CLOSE_TIMEOUT_MS) {
                LOG.warn(
                        "invoked, but work is over... closed = {}, isRunning = {}",
                        closed,
                        isRunning.getAsBoolean());
                warnTimestamp = now;
            }
            return -1;
        }
        try {

            if (!phase.isFinished()) {
                long now = clock.time();
                if (now - logStatTimestamp >= logInfoTimeoutMs) {
                    phase.logStatistic();
                    logStatTimestamp = now;
                }
                return phase.doWork();
            } else {
                phase.logStatistic();
                LOG.info("Phase {} is finished", phase.getClass().getSimpleName());
                phase = phase.nextPhase();
                if (phase.equals(AbstractPhase.TERMINAL_PHASE)) {
                    LOG.warn("Go to TERMINAL_PHASE!");
                }
                return 1;
            }
        } catch (Exception e) {
            LOG.warn(
                    "Exception in phase {}. Go to TERMINAL_PHASE! Exception message: {}",
                    phase,
                    e.getMessage());
            LOG.debug("", e);
            phase.logStatistic();
            phase.close();
            phase = AbstractPhase.TERMINAL_PHASE;

            return 1;
        }
    }

    @Override
    public String roleName() {
        return "catch up";
    }

    @Override
    public void onClose() {
        if (closed) {
            return;
        }

        closed = true;
        phase.close();
        quietClose(archiveClient);
    }

    @Override
    public void close() {
        onClose();
    }

    /** Ensures that no more than one elements participates in the operation */
    private <T> BinaryOperator<T> noMoreThanOneRecording() {
        return (a, b) -> {
            throw new EventStoreTooManyLiveRecordingsException(liveChannel, liveStreamId);
        };
    }

    public CatchUpContext createContext() {
        return new CatchUpContext()
                .setLastRecording(lastRecording)
                .setLastPosition(lastPosition)
                .setFragmentAssemblyRequired(fragmentAssemblyRequired)
                .setLiveChannel(liveChannel)
                .setLiveStreamId(liveStreamId)
                .setReplayChannel(replayChannel)
                .setReplayStreamId(replayStreamId)
                .setClientFragmentHandler(originalClientFragmentHandler)
                .setMessageMatcher(messageMatcher)
                .setIsRunning(isRunning)
                .setContinuousReplay(continuousReplay)
                .setCaughtUpHook(caughtUpHook.isTriggered() ? () -> {} : caughtUpHook.hook);
    }

    private class SavePositionFragmentHandler implements FragmentHandler {
        private final FragmentHandler fragmentHandler;

        public SavePositionFragmentHandler(FragmentHandler clientFragmentHandler) {
            this.fragmentHandler = clientFragmentHandler;
        }

        @Override
        public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
            lastPosition = header.position();
            fragmentHandler.onFragment(buffer, offset, length, header);
        }
    }

    private class CaughtUpHook {
        private final Runnable hook;
        private boolean triggered = false;

        private CaughtUpHook(Runnable hook) {
            this.hook = hook;
        }

        public void triggerOnce() {
            if (!triggered) {
                LOG.info(
                        " CatchUp process before 'caught up' took {} ms", clock.time() - startTime);
                hook.run();
                triggered = true;
            }
        }

        public boolean isTriggered() {
            return triggered;
        }
    }

    abstract static class AbstractPhase {
        static AbstractPhase TERMINAL_PHASE =
                new AbstractPhase() {
                    @Override
                    AbstractPhase nextPhase() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    boolean isFinished() {
                        return false;
                    }

                    @Override
                    void close() {}

                    @Override
                    public String toString() {
                        return "TERMINAL_PHASE";
                    }

                    @Override
                    public void logStatistic() {
                        LOG.warn("CatchUp is in the TERMINAL_PHASE, no messages will be delivered");
                    }
                };

        AbstractPhase() {
            LOG.info("Start the next phase {}", this.getClass().getSimpleName());
        }

        boolean isFinished() {
            return true;
        }

        int doWork() {
            return 0;
        }

        abstract AbstractPhase nextPhase();

        abstract void close();

        @Override
        public String toString() {
            return getClass().getSimpleName() + " (finished " + isFinished() + ")";
        }

        public void logStatistic() {}
    }

    /**
     * Init phase just lists recordings in the event store and choose the next phase depending on
     * the results
     */
    class InitPhase extends AbstractPhase {
        private final Logger LOG = LoggerFactory.getLogger(this.getClass().getSimpleName());

        final long startRecordingId;
        final long startPosition;

        InitPhase(long startRecordingId, long startPosition) {
            this.startRecordingId = startRecordingId;
            this.startPosition = startPosition;
            LOG.info(
                    "Init catch up from recording id {} from position {}",
                    startRecordingId,
                    startPosition);
        }

        @Override
        AbstractPhase nextPhase() {
            startTime = clock.time();
            LOG.info(
                    "Send a list recording request to channel {} id {}",
                    archiveClient.context().controlRequestChannel(),
                    archiveClient.context().controlRequestStreamId());
            List<RecordingDescriptor> recordings =
                    listRecordingsForUri(
                            archiveClient, liveChannel, liveStreamId, startRecordingId);
            LOG.info(
                    "Recordings for channel {} stream id {} are {}",
                    liveChannel,
                    liveStreamId,
                    recordings);

            if (startPosition != NULL_POSITION) {
                recordings.stream()
                        .filter(descriptor -> descriptor.getRecordingId() == startRecordingId)
                        .findAny()
                        .ifPresent(
                                descriptor -> {
                                    lastRecording = descriptor;
                                    if (descriptor.getStopPosition() != NULL_POSITION
                                            && descriptor.getStopPosition() < startPosition) {
                                        throw new IllegalStateException(
                                                "wrong start position "
                                                        + startPosition
                                                        + " for recording "
                                                        + descriptor);
                                    }
                                    descriptor.setStartPosition(startPosition);
                                });
            }

            List<RecordingDescriptor> offlineRecordings =
                    recordings.stream()
                            .filter(RecordingDescriptor::isOffline)
                            .filter(rec -> rec.getRecordingId() >= startRecordingId)
                            .collect(Collectors.toList());

            if (!offlineRecordings.isEmpty()) {
                return new OfflinePhase(offlineRecordings);
            }

            LOG.info("There are no more offline recordings");
            Optional<RecordingDescriptor> liveRecording =
                    recordings.stream()
                            .filter(RecordingDescriptor::isLive)
                            .reduce(noMoreThanOneRecording());

            if (liveRecording.isPresent()) {
                if (continuousReplay) {
                    return new ContinuousReplayCatchUpPhase(liveRecording.get());
                } else {
                    return new PreMergePhase(liveRecording.get());
                }
            } else {
                if (continuousReplay) {
                    return new ContinuousReplayWaitPhase();
                } else {
                    return new PreMergePhase(null);
                }
            }
        }

        @Override
        void close() {
            // do nothing
        }
    }

    /** Offline phase replays the offline recordings (recordings with stop position != 0) */
    class OfflinePhase extends AbstractPhase {
        private final Logger LOG = LoggerFactory.getLogger(this.getClass().getSimpleName());

        private final Iterator<RecordingDescriptor> offlineRecordingsIterator;
        private RecordingDescriptor currentRecording;
        private final long offlineRecordingsCount;
        private long currentRecordingNumber;

        private OfflineFragmentHandler offlineFragmentHandler =
                new OfflineFragmentHandler(clientFragmentHandler, 0, 0);
        private ReplaySubscription replaySubscription;

        OfflinePhase(List<RecordingDescriptor> offlineRecordings) {
            offlineRecordingsIterator = offlineRecordings.iterator();
            offlineRecordingsCount = offlineRecordings.size();
            currentRecordingNumber = 0;

            replayNextRecording();
        }

        @Override
        boolean isFinished() {
            return !offlineRecordingsIterator.hasNext() && isCurrentRecordingOver();
        }

        private boolean isCurrentRecordingOver() {
            return offlineFragmentHandler.isStopPositionReached();
        }

        ReplaySubscription getReplaySubscription() {
            return replaySubscription;
        }

        @Override
        int doWork() {
            if (isFinished()) {
                return 0;
            }

            int workCount = 0;
            if (isCurrentRecordingOver()) {
                workCount += stopCurrentReplay();

                workCount += replayNextRecording();
            }

            if (replaySubscription != null) {
                workCount += replaySubscription.poll(offlineFragmentHandler, fragmentLimit);

                if (!isCurrentRecordingOver() && !replaySubscription.isProcessing()) {
                    throw new RuntimeException(
                            "Replay from current recording stopped! Last position = "
                                    + lastPosition
                                    + ". Recording: "
                                    + currentRecording);
                }
            }

            return workCount;
        }

        private int replayNextRecording() {
            if (!offlineRecordingsIterator.hasNext()) {
                return 1;
            }

            currentRecording = offlineRecordingsIterator.next();
            currentRecordingNumber++;
            nextRecordingHook.accept(currentRecording);

            LOG.info(
                    "Replay next offline recording ({} of {}) with ID {}, start position {}, "
                            + "length {} on channel {}, streamId {}",
                    currentRecordingNumber,
                    offlineRecordingsCount,
                    currentRecording.getRecordingId(),
                    currentRecording.getStartPosition(),
                    currentRecording.length(),
                    liveChannel,
                    liveStreamId);

            if (currentRecording.isEmpty()) {
                LOG.warn(
                        "Empty recording {} on channel {} stream {}",
                        currentRecording.getRecordingId(),
                        currentRecording.getOriginalChannel(),
                        currentRecording.getStreamId());

                return replayNextRecording();
            }

            replaySubscription =
                    new ReplaySubscription(
                            aeron, archiveClient, currentRecording, replayChannel, replayStreamId);
            offlineFragmentHandler =
                    new OfflineFragmentHandler(
                            clientFragmentHandler,
                            currentRecording.getStartPosition(),
                            currentRecording.getStopPosition());

            return 1;
        }

        @Override
        AbstractPhase nextPhase() {
            stopCurrentReplay();

            return new InitPhase(currentRecording.getRecordingId() + 1, NULL_POSITION);
        }

        private int stopCurrentReplay() {
            if (!offlineFragmentHandler.isStopPositionReached()) {
                LOG.warn(
                        "The replay for offline recording ({} of {}) with ID {} was interrupted",
                        currentRecordingNumber,
                        offlineRecordingsCount,
                        currentRecording.getRecordingId());
                replaySubscription.stopReplay();
            }

            quietClose(replaySubscription);
            return 1;
        }

        @Override
        public void logStatistic() {
            LOG.info(
                    "Reached position {} / {} for replaying offline recording ({} of {}) with ID = {}",
                    lastPosition,
                    currentRecording.getStopPosition(),
                    currentRecordingNumber,
                    offlineRecordingsCount,
                    currentRecording.getRecordingId());
        }

        @Override
        void close() {
            stopCurrentReplay();
        }

        private class OfflineFragmentHandler implements FragmentHandler {
            private static final int FRAGMENT_LOG_PERCENTAGE_STEP = 20;

            private final FragmentHandler clientFragmentHandler;
            private final long stopPosition;
            private boolean isStopPositionReached;

            private final long logStatisticStepSize;
            private long previousLoggedStatPosition;

            OfflineFragmentHandler(
                    FragmentHandler clientFragmentHandler, long startPosition, long stopPosition) {
                this.clientFragmentHandler = clientFragmentHandler;
                this.stopPosition = stopPosition;
                isStopPositionReached = stopPosition == startPosition;
                previousLoggedStatPosition = 0;
                logStatisticStepSize =
                        currentRecording == null
                                ? 0
                                : currentRecording.length() * FRAGMENT_LOG_PERCENTAGE_STEP / 100;
            }

            @Override
            public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
                logFragmentPosition(header.position());

                clientFragmentHandler.onFragment(buffer, offset, length, header);

                if (header.position() >= stopPosition) {
                    isStopPositionReached = true;
                }
            }

            boolean isStopPositionReached() {
                return isStopPositionReached;
            }

            private void logFragmentPosition(long fragmentPosition) {
                if (fragmentPosition - previousLoggedStatPosition >= logStatisticStepSize) {
                    previousLoggedStatPosition = fragmentPosition;

                    logStatistic();
                }
            }
        }
    }

    class PreMergePhase extends AbstractPhase {
        private final Logger LOG = LoggerFactory.getLogger(this.getClass().getSimpleName());

        private final RecordingDescriptor liveRecording;
        private final Subscription liveSubscription;
        private final MutableReference<Image> liveImageRef = new MutableReference<>();

        PreMergePhase(RecordingDescriptor liveRecording) {
            this.liveRecording = liveRecording;

            if (sessionId != NULL_SESSION_ID) {
                LOG.info("Thank you for providing the sessionId = {}", sessionId);
            } else {
                LOG.warn("You haven't provide a sessionId! We can't trust the connection");
            }

            if (liveRecording != null) {
                LOG.info("Should merge with the live recording {}", liveRecording);
                nextRecordingHook.accept(liveRecording);
            } else {
                if (sessionId != NULL_SESSION_ID) {
                    throw new IllegalStateException(
                            "You provided a sessionId, but there is no live recording was found");
                }
                LOG.info("There is no live recording... Wait for image and go to the online phase");
            }

            LOG.info("Start a live subscription and await connection (image)...");
            String liveChannelWithSession =
                    sessionId == NULL_SESSION_ID
                            ? liveChannel
                            : ChannelUri.addSessionId(liveChannel, sessionId);
            liveSubscription =
                    aeron.addSubscription(
                            liveChannelWithSession,
                            liveStreamId,
                            AeronUtils::availableImageHandler,
                            AeronUtils::unavailableImageHandler);

            LOG.info(
                    "Created subscription with registrationId {} for live channel {}, streamId {}",
                    liveSubscription.registrationId(),
                    liveChannelWithSession,
                    liveStreamId);
        }

        @Override
        AbstractPhase nextPhase() {
            LOG.info("The live subscription got an image");
            if (liveRecording == null) {
                return new OnlinePhase(liveSubscription, liveImageRef.get());
            } else {
                return new MergePhase(liveRecording, liveSubscription, liveImageRef.get());
            }
        }

        @Override
        boolean isFinished() {
            return liveImageRef.get() != null;
        }

        @Override
        int doWork() {
            if (liveSubscription.imageCount() > 1) {
                // todo test on this
                throw new IllegalStateException("more than 1 image!");
            }
            liveSubscription.forEachImage(liveImageRef::set);
            return isFinished() ? 1 : 0;
        }

        @Override
        public void logStatistic() {
            LOG.info(
                    "Waiting for a connection(image) on the live channel {}, streamId {}. With sessionId = {}",
                    liveChannel,
                    liveStreamId,
                    sessionId == NULL_SESSION_ID ? "NONE" : sessionId);
        }

        @Override
        void close() {
            quietClose(liveSubscription);
        }

        /** For test only */
        MutableReference<Image> getLiveImageRef() {
            return liveImageRef;
        }
    }

    /** Merge phase does seamless merge the live recording and the live stream */
    class MergePhase extends AbstractPhase {
        private final Logger LOG = LoggerFactory.getLogger(this.getClass().getSimpleName());

        private final Subscription liveSubscription;
        private final Image liveImage;
        private final LiveFragmentHandler liveFragmentHandler;
        private final RecordingDescriptor liveRecording;

        private final ReplayFragmentHandler replayFragmentHandler;
        private ReplaySubscription replaySubscription;

        private boolean merged = true;

        MergePhase(
                RecordingDescriptor liveRecording, Subscription liveSubscription, Image liveImage) {

            this.liveSubscription = liveSubscription;
            this.liveImage = liveImage;

            this.liveRecording = liveRecording;
            long recordingPosition =
                    archiveClient.getRecordingPosition(liveRecording.getRecordingId());
            long caughtUpPosition = Math.max(liveImage.joinPosition(), recordingPosition);

            liveFragmentHandler = new LiveFragmentHandler(liveImage.joinPosition());
            replayFragmentHandler =
                    new ReplayFragmentHandler(
                            liveRecording.getStartPosition(),
                            clientFragmentHandler,
                            caughtUpPosition);

            LOG.info(
                    "Live sub joined on position = {} and archive recorded up to position = {}",
                    liveImage.joinPosition(),
                    recordingPosition);
            LOG.info(
                    "Merge with live stream will be started when we reach position = {}",
                    caughtUpPosition);
            if (caughtUpPosition - liveRecording.getStartPosition() <= 0) {
                LOG.info("Live recording is empty. We caught up");
                return;
            }

            replaySubscription =
                    new ReplaySubscription(
                            aeron, archiveClient, liveRecording, replayChannel, replayStreamId);
        }

        @Override
        boolean isFinished() {
            return replayFragmentHandler.isCaughtUpPositionReached() && isMerged();
        }

        boolean isMerged() {
            return !liveFragmentHandler.wasThereLiveMessage() || merged;
        }

        Subscription getLiveSubscription() {
            return liveSubscription;
        }

        @Override
        int doWork() {
            int workCount = 0;
            workCount += replaySubscription.poll(replayFragmentHandler, 1);
            workCount += liveImage.poll(liveFragmentHandler, 1);
            if (liveImage.position() < replayFragmentHandler.getPosition()) {
                // todo test on this
                liveImage.position(replayFragmentHandler.getPosition());
            }

            if (!isMerged()) {
                merged = tryToMerge();
            }

            if (isConnectionBroken(liveImage)) {
                throw new RuntimeException(
                        "Connection is broken while merge! \nImage: " + liveImage.toString());
            }

            return workCount;
        }

        boolean tryToMerge() {
            boolean theSamePosition =
                    liveFragmentHandler.getPosition() == replayFragmentHandler.getPosition();
            boolean theSameLength =
                    liveFragmentHandler.getLength() == replayFragmentHandler.getLength();

            if (theSamePosition) {
                LOG.info(
                        "We've reached the same position {} in the live stream and in the live recording!",
                        liveFragmentHandler.getPosition());

                if (theSameLength) {
                    boolean userProof =
                            messageMatcher.match(
                                    liveFragmentHandler.getBuffer(),
                                    liveFragmentHandler.getOffset(),
                                    replayFragmentHandler.getBuffer(),
                                    replayFragmentHandler.getOffset(),
                                    replayFragmentHandler.getLength());
                    if (!userProof) {
                        throw new CatchUpNotTheSameStreamsError();
                    }

                    return userProof;
                } else {
                    // todo to log length and buffer
                    throw new CatchUpNotTheSameStreamsError();
                }
            }

            return false;
        }

        @Override
        AbstractPhase nextPhase() {
            stopReplay();
            LOG.info(
                    "Merged with the live stream. Next messages will be delivered from the image {}",
                    liveImage);
            return new OnlinePhase(liveSubscription, liveImage);
        }

        private void stopReplay() {
            quietClose(replaySubscription);
        }

        @Override
        public void logStatistic() {
            LOG.info(
                    " Merging with {}. \nCaughtUp will be after position {}. {} \nPositions: replay {} / live {}.",
                    liveRecording,
                    replayFragmentHandler.caughtUpPosition,
                    liveFragmentHandler.thereWasLiveMessage
                            ? "There was at least one message in the live stream"
                            : "No message in the live stream yet",
                    replayFragmentHandler.getPosition(),
                    liveImage.position());
        }

        @Override
        void close() {
            stopReplay();
            quietClose(liveSubscription);
        }

        private class MergeFragmentHandler implements FragmentHandler {
            private DirectBuffer buffer;
            private int offset;
            private int length;
            private long position;

            public MergeFragmentHandler(long initialPosition) {
                this.position = initialPosition;
            }

            @Override
            public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
                this.buffer = buffer;
                this.offset = offset;
                this.position = header.position();
                this.length = length;
            }

            DirectBuffer getBuffer() {
                return buffer;
            }

            int getOffset() {
                return offset;
            }

            long getPosition() {
                return position;
            }

            int getLength() {
                return length;
            }
        }

        private class LiveFragmentHandler extends MergeFragmentHandler {

            private final RateLimitedLogger log = new RateLimitedLogger(LOG);
            private boolean thereWasLiveMessage = false;

            public LiveFragmentHandler(long initialPosition) {
                super(initialPosition);
            }

            @Override
            public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
                super.onFragment(buffer, offset, length, header);

                if (!thereWasLiveMessage) {
                    merged = false;
                    LOG.info(
                            "The first message from live stream on position {} with length {}",
                            header.position(),
                            length);
                }

                thereWasLiveMessage = true;

                log.log(
                        "New live message on position {} with length {}",
                        header.position(),
                        length);
            }

            boolean wasThereLiveMessage() {
                return thereWasLiveMessage;
            }
        }

        private class ReplayFragmentHandler extends MergeFragmentHandler {
            private final FragmentHandler clientFragmentHandler;
            private final long caughtUpPosition;

            ReplayFragmentHandler(
                    long initialPosition,
                    FragmentHandler clientFragmentHandler,
                    long caughtUpPosition) {
                super(initialPosition);
                this.clientFragmentHandler = clientFragmentHandler;
                this.caughtUpPosition = caughtUpPosition;
            }

            @Override
            public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
                super.onFragment(buffer, offset, length, header);

                clientFragmentHandler.onFragment(buffer, offset, length, header);

                if (isCaughtUpPositionReached()) {
                    // wrap around original hook has defend from multiply call
                    caughtUpHook.triggerOnce();
                }
            }

            boolean isCaughtUpPositionReached() {
                return getPosition() >= caughtUpPosition;
            }
        }
    }

    /** Online phase is just usual polling for the live subscription */
    class OnlinePhase extends AbstractPhase {
        private final Logger LOG = LoggerFactory.getLogger(this.getClass().getSimpleName());

        private final Subscription liveSubscription;
        private final Image liveImage;
        private final FragmentHandler fragmentHandler;
        private long loggedPosition = NULL_POSITION;

        OnlinePhase(Subscription liveSubscription, Image liveImage) {
            this.liveImage = liveImage;
            caughtUpHook.triggerOnce();

            fragmentHandler = clientFragmentHandler;
            this.liveSubscription = liveSubscription;
            LOG.info(
                    " CatchUp process before merge with live stream took {} ms",
                    clock.time() - startTime);
        }

        Subscription getLiveSubscription() {
            return liveSubscription;
        }

        @Override
        boolean isFinished() {
            return isConnectionBroken(liveImage);
        }

        @Override
        int doWork() {
            return liveImage.poll(fragmentHandler, 1);
        }

        @Override
        AbstractPhase nextPhase() {
            close();
            return AbstractPhase.TERMINAL_PHASE;
        }

        @Override
        public void logStatistic() {
            if (loggedPosition != lastPosition) {
                loggedPosition = lastPosition;
                LOG.info("Live channel is on the position {}", lastPosition);
            }
        }

        @Override
        void close() {
            quietClose(liveSubscription);
        }
    }

    class ContinuousReplayWaitPhase extends AbstractPhase {
        private final Logger LOG = LoggerFactory.getLogger(this.getClass().getSimpleName());

        private long lastRequestTimestamp;
        private RecordingDescriptor liveRecording;

        ContinuousReplayWaitPhase() {
            LOG.info("There is no live recording. Should wait for a new one.");
            caughtUpHook.triggerOnce();
        }

        @Override
        boolean isFinished() {
            return liveRecording != null;
        }

        @Override
        int doWork() {
            long now = clock.time();
            if ((now - lastRequestTimestamp) >= continuousReplayTimeoutMs) {
                try {
                    listRecordingsForUri(archiveClient, liveChannel, liveStreamId)
                            .stream()
                            .filter(RecordingDescriptor::isLive)
                            .reduce(noMoreThanOneRecording())
                            .ifPresent(rec -> this.liveRecording = rec);
                } catch (ArchiveException e) {
                    LOG.warn(e.getMessage());
                }
                lastRequestTimestamp = now;
                return 1;
            } else {
                return 0;
            }
        }

        @Override
        AbstractPhase nextPhase() {
            return new ContinuousReplayCatchUpPhase(liveRecording);
        }

        @Override
        public void logStatistic() {
            LOG.info(
                    "Waiting for a live recording... The last position {}, the last recording {}",
                    lastPosition,
                    lastRecording);
        }

        @Override
        void close() {}
    }

    class ContinuousReplayCatchUpPhase extends AbstractPhase {
        private final Logger LOG = LoggerFactory.getLogger(this.getClass().getSimpleName());

        private final ReplaySubscription replaySubscription;
        private final FragmentHandler fragmentHandler;
        private final long caughtUpPosition;

        private boolean caughtUp = false;

        ContinuousReplayCatchUpPhase(RecordingDescriptor liveRecording) {
            LOG.info(
                    "Future messages will be received within continuous replay for recording {}",
                    liveRecording);
            nextRecordingHook.accept(liveRecording);

            replaySubscription =
                    new ReplaySubscription(
                            aeron, archiveClient, liveRecording, replayChannel, replayStreamId);

            caughtUpPosition = archiveClient.getRecordingPosition(liveRecording.getRecordingId());
            if (caughtUpPosition == liveRecording.getStartPosition()) {
                LOG.info("The live recording is empty");
                caughtUp();
            } else {
                LOG.info("The live recording is up to {} position", caughtUpPosition);
            }

            fragmentHandler = new ContinuousFragmentHandler(caughtUpPosition);
        }

        @Override
        boolean isFinished() {
            return caughtUp;
        }

        @Override
        int doWork() {
            if (!replaySubscription.isProcessing()) {
                throw new RuntimeException(
                        "Connection is broken on postion "
                                + lastPosition
                                + " while catching up with "
                                + replaySubscription.getRecording()
                                + " in continuous tailing mode");
            }
            return replaySubscription.poll(fragmentHandler, fragmentLimit);
        }

        @Override
        AbstractPhase nextPhase() {
            logStatistic();
            LOG.info("We passed through the catchUp position!");
            caughtUpHook.triggerOnce();
            return new ContinuousReplayPhase(replaySubscription);
        }

        @Override
        public void logStatistic() {
            LOG.info(
                    "Catching up in continuous replay mode with the recording {} with session {}.\n"
                            + "Now on the position {} / {}",
                    lastRecording,
                    replaySubscription.getReplaySessionId(),
                    lastPosition,
                    caughtUpPosition);
        }

        @Override
        void close() {
            quietClose(replaySubscription);
        }

        private void caughtUp() {
            caughtUp = true;
        }

        private class ContinuousFragmentHandler implements FragmentHandler {
            private final long catchUpPosition;

            private ContinuousFragmentHandler(long catchUpPosition) {
                this.catchUpPosition = catchUpPosition;
            }

            @Override
            public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
                clientFragmentHandler.onFragment(buffer, offset, length, header);
                if (header.position() >= catchUpPosition) {
                    caughtUp();
                }
            }
        }
    }

    class ContinuousReplayPhase extends AbstractPhase {
        private final Logger LOG = LoggerFactory.getLogger(this.getClass().getSimpleName());

        private final ReplaySubscription replaySubscription;
        private final FragmentHandler fragmentHandler;
        private long loggedPosition = NULL_POSITION;

        ContinuousReplayPhase(ReplaySubscription replaySubscription) {
            fragmentHandler = clientFragmentHandler;

            this.replaySubscription = replaySubscription;
        }

        ReplaySubscription getReplaySubscription() {
            return replaySubscription;
        }

        @Override
        boolean isFinished() {
            return !replaySubscription.isProcessing();
        }

        @Override
        int doWork() {
            return replaySubscription.poll(fragmentHandler, fragmentLimit);
        }

        @Override
        AbstractPhase nextPhase() {
            close();
            return TERMINAL_PHASE;
        }

        @Override
        public void logStatistic() {
            if (loggedPosition != lastPosition) {
                loggedPosition = lastPosition;
                LOG.info(
                        "Tailing the recording {} with session {}. Now on the position {}",
                        lastRecording,
                        replaySubscription.getReplaySessionId(),
                        loggedPosition);
            }
        }

        @Override
        void close() {
            quietClose(replaySubscription);
        }
    }
}
