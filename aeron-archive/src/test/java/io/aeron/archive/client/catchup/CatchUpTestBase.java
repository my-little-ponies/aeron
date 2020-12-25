package io.aeron.archive.client.catchup;

import io.aeron.archive.client.catchup.CatchUp.AbstractPhase;
import io.aeron.archive.client.catchup.context.CatchUpContext;
import io.aeron.archive.client.util.RecordingDescriptor;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.IoUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static io.aeron.archive.client.util.AeronUtils.listRecordingsForUri;
import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.logbuffer.FrameDescriptor.BEGIN_FRAG_FLAG;
import static io.aeron.logbuffer.FrameDescriptor.UNFRAGMENTED;
import static java.time.Duration.ofSeconds;
import static org.agrona.CloseHelper.quietClose;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@SuppressWarnings({"RedundantThrows", "unchecked"})
public class CatchUpTestBase {

    public static final String LIVE_CHANNEL = IPC_CHANNEL;
    public static final int LIVE_STREAM = 123;
    public static final String REPLAY_CHANNEL = IPC_CHANNEL;
    public static final int REPLAY_STREAM = 234;
    public static final int TEST_MESSAGE_COUNT = 10;

    static MediaDriver mediaDriver;
    Aeron aeron;
    Archive archive;
    AeronArchive archiveClient;
    CatchUp catchUp;

    final List<Integer> verifyList = new ArrayList<>();
    final List<Integer> verifyListShouldBeBeforeCaughtUp = new ArrayList<>();
    final List<Integer> messagesWereBeforeCaughtUp = new ArrayList<>();
    final FragmentHandler testFragmentHandler = new ToVerifyListFragmentHandler();

    @SuppressWarnings("Convert2Lambda")
    final Runnable catchUpHook =
            spy(
                    new Runnable() {
                        @Override
                        public void run() {
                            messagesWereBeforeCaughtUp.addAll(verifyList);
                        }
                    });

    final MutableDirectBuffer buffer = new ExpandableArrayBuffer();
    final IdleStrategy idleStrategy = new SleepingMillisIdleStrategy(1);

    Subscription subscription;
    boolean isRunning = true;
    int testDataSource = 0;

    @BeforeAll
    public static void setUpClass() throws Exception {
        mediaDriver =
                MediaDriver.launch(
                        new Context()
                                .threadingMode(ThreadingMode.SHARED)
                                .sharedIdleStrategy(new BackoffIdleStrategy(100000, 10000, 1, 10))
                                .dirDeleteOnStart(true)
                                .imageLivenessTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                        // .spiesSimulateConnection(true)
                        );
    }

    @AfterAll
    public static void tearDownClass() throws Exception {
        quietClose(mediaDriver);
        IoUtil.delete(new File(mediaDriver.aeronDirectoryName()), true);
    }

    @BeforeEach
    public void init() {
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
        aeron =
                Aeron.connect(
                        new Aeron.Context()
                                .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                                .errorHandler(
                                        throwable -> {
                                            throwable.printStackTrace();
                                            fail(throwable.getMessage());
                                        }));
        archive = launchArchive();

        archiveClient = connectToArchive();

        catchUp = createCatchUp();
    }

    protected Archive.Context createArchiveContext() {
        return new Archive.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                .recordingEventsChannel(IPC_CHANNEL)
                .controlChannel(IPC_CHANNEL)
                .deleteArchiveOnStart(true)
                .archiveDir(new File(mediaDriver.aeronDirectoryName() + "/archive"))
                .errorHandler(
                        throwable -> {
                            throwable.printStackTrace();
                            fail(throwable.getMessage());
                        });
    }

    protected Archive launchArchive() {
        return Archive.launch(createArchiveContext());
    }

    protected AeronArchive.Context createAeronArchiveContext() {
        return new AeronArchive.Context()
                .messageTimeoutNs(TimeUnit.SECONDS.toNanos(30))
                .aeron(aeron)
                .ownsAeronClient(false)
                .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                .controlRequestChannel(IPC_CHANNEL)
                .controlResponseChannel(IPC_CHANNEL)
                .recordingEventsChannel(IPC_CHANNEL)
                .errorHandler(
                        throwable -> {
                            throwable.printStackTrace();
                            fail(throwable.getMessage());
                        });
    }

    protected AeronArchive connectToArchive() {
        return spy(AeronArchive.connect(createAeronArchiveContext()));
    }

    protected CatchUpContext createCatchUpContext() {
        return new CatchUpContext()
                .setFragmentLimit(10)
                .setFragmentAssemblyRequired(true)
                .setLiveChannel(LIVE_CHANNEL)
                .setLiveStreamId(LIVE_STREAM)
                .setReplayChannel(REPLAY_CHANNEL)
                .setReplayStreamId(REPLAY_STREAM)
                .setClientFragmentHandler(testFragmentHandler)
                .setMessageMatcher(new MessageMatcherBeginToEnd())
                .setIsRunning(() -> isRunning)
                .setCaughtUpHook(catchUpHook);
    }

    protected CatchUp createCatchUp() {
        return new CatchUp(aeron, connectToArchive(), createCatchUpContext());
    }

    @AfterEach
    public void tearDown() {
        quietClose(subscription);
        catchUp.close();
        archive.close();
        aeron.close();
    }

    void verifyOnline() {
        try (Publication publication = aeron.addPublication(LIVE_CHANNEL, LIVE_STREAM)) {
            verifyOnline(publication);
        }
    }

    void verifyOnline(Publication publication) {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
        while (catchUp.inProgress()) {
            catchUp.doWork();
            if (System.currentTimeMillis() > deadline) {
                fail(
                        "CatchUp didn't catch up in 5 seconds. "
                                + "Now it's in phase "
                                + catchUp.getPhase());
            }
        }

        int sizeBefore = verifyList.size();

        int data = new Random().nextInt();
        sendData(publication, data, catchUp::doWork);

        while (verifyList.size() == sizeBefore) {
            catchUp.doWork();
            if (subscription != null) {
                subscription.poll(this::nop, 100000);
            }
            assertTrue(catchUp.healthCheck());
        }

        verify(catchUpHook).run();
        assertEquals(data, verifyList.get(sizeBefore).intValue());
        verifySnapshotData();

        publication.close();
        waitForTerminalPhase();
        assertFalse(catchUp.getLiveSubscription().isPresent());
    }

    void fillArchiveWithRecords(int recordCount) {
        for (int i = 0; i < recordCount; i++) {
            createAndFillRecord();
        }

        while (areThereNonOfflineRecordings(archiveClient)) {
            idleStrategy.idle();
        }
    }

    void stopAndVerify(Class<? extends AbstractPhase> phaseClass) {
        isRunning = false;
        assertEquals(-1, catchUp.doWork());

        assertTrue(phaseClass.isInstance(catchUp.getPhase()));

        catchUp.close();

        assertEquals(-1, catchUp.doWork());
    }

    boolean areThereNonOfflineRecordings(AeronArchive archiveClient) {
        List<RecordingDescriptor> recordingDescriptors =
                listRecordingsForUri(archiveClient, LIVE_CHANNEL, LIVE_STREAM);
        return recordingDescriptors.stream()
                .anyMatch(descriptor -> descriptor.getStopPosition() == -1);
    }

    void createAndFillRecord() {
        createAndFillRecord(TEST_MESSAGE_COUNT);
    }

    void createAndFillRecord(int numberOfMessages) {
        final long publicationPosition;
        try (Subscription subscription = aeron.addSubscription(LIVE_CHANNEL, LIVE_STREAM);
                Publication publication = aeron.addPublication(LIVE_CHANNEL, LIVE_STREAM)) {

            for (int i = 0; i < numberOfMessages; i++) {
                sendData(publication);
                while (subscription.poll(this::nop, 1) == 0) {
                    idleStrategy.idle();
                }
            }
            publicationPosition = publication.position();
        }
        await().until(
                        () -> {
                            List<RecordingDescriptor> recordings =
                                    listRecordingsForUri(archiveClient, LIVE_CHANNEL, LIVE_STREAM);
                            return recordings.get(recordings.size() - 1).getStopPosition()
                                    == publicationPosition;
                        });
    }

    public Subscription openLiveSubscription() {
        if (subscription == null) {
            subscription = aeron.addSubscription(LIVE_CHANNEL, LIVE_STREAM);
        }
        return subscription;
    }

    public void waitForTerminalPhase() {
        waitForPhase(AbstractPhase.TERMINAL_PHASE.getClass());
    }

    public void waitForPhase(Class<? extends AbstractPhase> phaseClass) {
        waitForPhase(phaseClass, 10, TimeUnit.SECONDS);
    }

    public void waitForPhase(
            Class<? extends AbstractPhase> phaseClass, long timeout, TimeUnit timeUnit) {
        BackoffIdleStrategy idleStrategy =
                new BackoffIdleStrategy(1000, 1000, 100, TimeUnit.MILLISECONDS.toNanos(10));
        long deadline = System.currentTimeMillis() + timeUnit.toMillis(timeout);
        while (!phaseClass.isInstance(catchUp.getPhase())) {
            idleStrategy.idle(catchUp.doWork());
            if (System.currentTimeMillis() > deadline) {
                fail(
                        "CatchUp didn't catch the phase "
                                + (phaseClass.isAnonymousClass()
                                        ? AbstractPhase.TERMINAL_PHASE.toString()
                                        : phaseClass.getSimpleName())
                                + " within "
                                + timeout
                                + " "
                                + timeUnit
                                + ". Now it's in phase "
                                + catchUp.getPhase());
            }
        }
    }

    void sendSmallData(Publication publication) {
        sendData(publication, buffer, Integer.BYTES, testDataSource++, null);
    }

    void sendBigData(Publication publication, int numberOfFragments, Runnable callback) {
        sendData(
                publication,
                buffer,
                publication.maxPayloadLength() * numberOfFragments,
                testDataSource++,
                callback);
    }

    void sendData(Publication publication) {
        sendData(publication, testDataSource++);
    }

    void sendData(Publication publication, int data) {
        sendData(publication, data, null);
    }

    void sendData(Publication publication, int data, Runnable callback) {
        final int length;
        if (data % 2 == 0) {
            length = publication.maxPayloadLength() * ThreadLocalRandom.current().nextInt(2, 5);
        } else {
            length = Integer.BYTES;
        }
        sendData(publication, buffer, length, data, callback);
    }

    void sendData(Publication publication, MutableDirectBuffer buffer) {
        sendData(publication, buffer, buffer.capacity(), testDataSource++, null);
    }

    void sendData(Publication publication, MutableDirectBuffer buffer, Runnable callback) {
        sendData(publication, buffer, buffer.capacity(), testDataSource++, callback);
    }

    void sendData(
            Publication publication,
            MutableDirectBuffer buffer,
            int length,
            int data,
            Runnable callback) {
        if (buffer.capacity() < length) {
            buffer.setMemory(0, length, (byte) 'x');
        }
        buffer.putInt(0, data);
        AtomicLong offerResult = new AtomicLong(Long.MIN_VALUE);
        assertTimeoutPreemptively(
                ofSeconds(10),
                () -> {
                    do {
                        offerResult.set(publication.offer(buffer, 0, length));
                        idleStrategy.idle();
                        if (subscription != null) {
                            subscription.poll(this::nop, Integer.MAX_VALUE);
                        }
                        if (callback != null) {
                            callback.run();
                        }
                    } while (offerResult.get() < 0);
                },
                () -> "the last offerResult was " + offerResult.get());
    }

    void saveOfflineMessages() {
        for (int i = 0; i < testDataSource; i++) {
            verifyListShouldBeBeforeCaughtUp.add(i);
        }
    }

    void saveOfflineMessages(Publication publication) {
        saveOfflineMessages();

        await().until(() -> newSubsJoinAfterSentMessages(publication));
    }

    public void verifyReceivedData() {
        verifyReceivedData(testDataSource);
    }

    public void verifyReceivedData(int count) {
        await().untilAsserted(
                        () -> {
                            for (int i = 0; i < 1000; i++) {
                                catchUp.doWork();
                            }
                            assertThat(
                                    "expected: up to "
                                            + testDataSource
                                            + ", but result list: "
                                            + verifyList,
                                    verifyList.size(),
                                    greaterThanOrEqualTo(testDataSource));
                        });

        for (int i = 0; i < count; i++) {
            assertEquals(i, verifyList.get(i).intValue(), () -> "result list: " + verifyList);
        }
    }

    protected void verifySnapshotData() {
        verify(catchUpHook).run();
        assertThat(
                verifyListShouldBeBeforeCaughtUp.size(),
                lessThanOrEqualTo(messagesWereBeforeCaughtUp.size()));
        for (int i = 0; i < verifyListShouldBeBeforeCaughtUp.size(); i++) {
            assertEquals(
                    verifyListShouldBeBeforeCaughtUp.get(i), messagesWereBeforeCaughtUp.get(i));
        }
    }

    public int awaitPublisherIsRecording(Publication publication) {
        CountersReader counters = aeron.countersReader();
        MutableInteger result = new MutableInteger(0);
        await().until(
                        () -> {
                            int counterIdBySession =
                                    RecordingPos.findCounterIdBySession(
                                            counters, publication.sessionId());
                            result.set(counterIdBySession);
                            return NULL_VALUE != counterIdBySession;
                        });
        return result.get();
    }

    boolean newSubsJoinAfterSentMessages(Publication publication) {
        try (Subscription temp = aeron.addSubscription(LIVE_CHANNEL, LIVE_STREAM)) {
            while (temp.imageCount() == 0) {
                idleStrategy.idle();
            }
            return temp.imageAtIndex(0).joinPosition() >= publication.position();
        }
    }

    void findMiddlePositionOfSomeMessage(Subscription subscription, MutableLong position) {
        subscription.poll(
                (buf, offset, length, header) -> {
                    final byte flags = header.flags();
                    if ((flags & UNFRAGMENTED) != UNFRAGMENTED) {
                        if ((flags & BEGIN_FRAG_FLAG) == BEGIN_FRAG_FLAG) {
                            if (position.value == -1) {
                                position.set(header.position());
                            }
                        }
                    }
                },
                1000);
    }

    public void nop(DirectBuffer buffer, int offset, int length, Header header) {
        // empty fragment handler
    }

    class ToVerifyListFragmentHandler implements FragmentHandler {

        @Override
        public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
            int data = buffer.getInt(offset);
            verifyList.add(data);
        }
    }
}
