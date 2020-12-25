package io.aeron.archive.client.catchup.context;

import static io.aeron.CommonContext.NULL_SESSION_ID;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static java.util.Objects.requireNonNull;

import io.aeron.archive.client.catchup.CatchUp;
import io.aeron.archive.client.catchup.MessageMatcher;
import io.aeron.archive.client.catchup.MessageMatcherBeginToEnd;
import io.aeron.archive.client.util.RecordingDescriptor;
import io.aeron.logbuffer.FragmentHandler;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.SystemEpochClock;

/** Context with all information which is required for {@link CatchUp} */
public class CatchUpContext {

    private String liveChannel;
    private int liveStreamId;
    private String replayChannel;
    private int replayStreamId;
    private FragmentHandler clientFragmentHandler;
    private MessageMatcher messageMatcher = new MessageMatcherBeginToEnd();
    private BooleanSupplier isRunning;
    private Runnable caughtUpHook = () -> {};
    private Consumer<RecordingDescriptor> nextRecordingHook = nextRecording -> {};
    private boolean fragmentAssemblyRequired = true;
    private boolean continuousReplay = false;
    private long continuousReplayTimeoutMs = 300L;
    private long logInfoTimeoutMs = TimeUnit.SECONDS.toMillis(5);
    private RecordingDescriptor lastRecording;
    private long lastPosition = NULL_POSITION;
    private int sessionId = NULL_SESSION_ID;
    private int fragmentLimit = 10;

    private EpochClock clock = new SystemEpochClock();

    public void validated() {
        requireNonNull(liveChannel, "liveChannel" + " was not provided");
        requireNonNull(replayChannel, "replayChannel" + " was not provided");
        requireNonNull(clientFragmentHandler, "clientFragmentHandler" + " was not provided");
        requireNonNull(messageMatcher, "messageMatcher" + " was not provided");
        requireNonNull(isRunning, "isRunning" + " was not provided");
        requireNonNull(caughtUpHook, "caughtUpHook" + " was not provided");
        requireNonNull(nextRecordingHook, "nextRecordingHook" + " was not provided");
    }

    public String getLiveChannel() {
        return liveChannel;
    }

    public int getLiveStreamId() {
        return liveStreamId;
    }

    public String getReplayChannel() {
        return replayChannel;
    }

    public int getReplayStreamId() {
        return replayStreamId;
    }

    public FragmentHandler getClientFragmentHandler() {
        return clientFragmentHandler;
    }

    public MessageMatcher getMessageMatcher() {
        return messageMatcher;
    }

    public BooleanSupplier getIsRunning() {
        return isRunning;
    }

    public Runnable getCaughtUpHook() {
        return caughtUpHook;
    }

    public Consumer<RecordingDescriptor> getNextRecordingHook() {
        return nextRecordingHook;
    }

    public CatchUpContext setLiveChannel(String liveChannel) {
        this.liveChannel = liveChannel;
        return this;
    }

    public CatchUpContext setLiveStreamId(int liveStreamId) {
        this.liveStreamId = liveStreamId;
        return this;
    }

    public CatchUpContext setReplayChannel(String replayChannel) {
        this.replayChannel = replayChannel;
        return this;
    }

    public CatchUpContext setReplayStreamId(int replayStreamId) {
        this.replayStreamId = replayStreamId;
        return this;
    }


    public CatchUpContext setClientFragmentHandler(FragmentHandler clientFragmentHandler) {
        this.clientFragmentHandler = clientFragmentHandler;
        return this;
    }

    public CatchUpContext setMessageMatcher(MessageMatcher messageMatcher) {
        this.messageMatcher = messageMatcher;
        return this;
    }

    public CatchUpContext setIsRunning(BooleanSupplier isRunning) {
        this.isRunning = isRunning;
        return this;
    }

    public CatchUpContext setCaughtUpHook(Runnable caughtUpHook) {
        this.caughtUpHook = caughtUpHook;
        return this;
    }

    public CatchUpContext setNextRecordingHook(Consumer<RecordingDescriptor> nextRecordingHook) {
        this.nextRecordingHook = nextRecordingHook;
        return this;
    }

    public boolean isFragmentAssemblyRequired() {
        return fragmentAssemblyRequired;
    }

    public CatchUpContext setFragmentAssemblyRequired(boolean fragmentAssemblyRequired) {
        this.fragmentAssemblyRequired = fragmentAssemblyRequired;
        return this;
    }

    public boolean isContinuousReplay() {
        return continuousReplay;
    }

    public CatchUpContext setContinuousReplay(boolean continuousReplay) {
        this.continuousReplay = continuousReplay;
        return this;
    }

    public long getContinuousReplayTimeoutMs() {
        return continuousReplayTimeoutMs;
    }

    public CatchUpContext setContinuousReplayTimeoutMs(long continuousReplayTimeoutMs) {
        this.continuousReplayTimeoutMs = continuousReplayTimeoutMs;
        return this;
    }

    public long getLogInfoTimeoutMs() {
        return logInfoTimeoutMs;
    }

    public CatchUpContext setLogInfoTimeoutMs(long logInfoTimeoutMs) {
        this.logInfoTimeoutMs = logInfoTimeoutMs;
        return this;
    }

    public CatchUpContext setLastRecording(RecordingDescriptor lastRecording) {
        this.lastRecording = lastRecording;
        return this;
    }

    public Optional<RecordingDescriptor> getLastRecording() {
        return Optional.ofNullable(lastRecording);
    }

    public CatchUpContext setLastPosition(long lastPosition) {
        this.lastPosition = lastPosition;
        return this;
    }

    public long getLastPosition() {
        return lastPosition;
    }

    public CatchUpContext setSessionId(int sessionId) {
        this.sessionId = sessionId;
        return this;
    }

    public int getSessionId() {
        return sessionId;
    }

    public CatchUpContext setFragmentLimit(int fragmentLimit) {
        this.fragmentLimit = fragmentLimit;
        return this;
    }

    public int getFragmentLimit() {
        return fragmentLimit;
    }

    public EpochClock getClock() {
        return clock;
    }

    public CatchUpContext setClock(EpochClock clock) {
        this.clock = clock;
        return this;
    }
}
