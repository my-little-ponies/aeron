package io.aeron.archive.client.catchup;

import io.aeron.archive.client.catchup.CatchUp.AbstractPhase;
import io.aeron.archive.client.catchup.CatchUp.MergePhase;
import io.aeron.archive.client.catchup.CatchUp.OfflinePhase;
import io.aeron.archive.client.catchup.CatchUp.OnlinePhase;
import io.aeron.archive.client.catchup.CatchUp.PreMergePhase;
import io.aeron.ChannelUri;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.driver.Configuration;
import org.agrona.collections.MutableLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("RedundantThrows")
public class CatchUpTestOnline extends CatchUpTestBase {

    @Test
    void shouldNotDoDoubleClose() {
        AbstractPhase phase = mock(AbstractPhase.class);
        catchUp.setPhase(phase);
        catchUp.close();
        catchUp.close();

        assertEquals(-1, catchUp.doWork());
        verify(phase).close();
    }

    @Test
    void shouldBeAbleToStopInOfflinePhase() {
        archiveClient.startRecording(LIVE_CHANNEL, LIVE_STREAM, SourceLocation.LOCAL);

        fillArchiveWithRecords(1);

        while (verifyList.isEmpty()) {
            idleStrategy.idle(catchUp.doWork());
        }
        stopAndVerify(OfflinePhase.class);
    }

    @Test
    public void shouldJumpToOnlinePhaseIfArchiveIsEmptyAndReceiveMessages() throws Exception {
        waitForPhase(PreMergePhase.class);

        try (Publication publication = aeron.addPublication(LIVE_CHANNEL, LIVE_STREAM)) {
            waitForPhase((OnlinePhase.class));

            OnlinePhase onlinePhase = (OnlinePhase) catchUp.getPhase();
            assertTrue(onlinePhase.getLiveSubscription().isConnected());

            verifyOnline(publication);
        }
    }

    @ParameterizedTest
    @CsvSource(
            value = {
                "true, 1",
                "false, 1",
                "true, 2",
                "false, 2",
            })
    void shouldReplayOfflineRecordingsStartedNotFromZeroPosition(
            boolean withData, int recordingsCount) {

        archiveClient.startRecording(LIVE_CHANNEL, LIVE_STREAM, SourceLocation.LOCAL);

        ChannelUri liveChanel = ChannelUri.parse(LIVE_CHANNEL);
        liveChanel.initialPosition(1600, 0, Configuration.termBufferLength());

        for (int i = 0; i < recordingsCount; i++) {
            try (Publication publication =
                    aeron.addExclusivePublication(liveChanel.toString(), LIVE_STREAM)) {
                awaitPublisherIsRecording(publication);
                if (withData) {
                    sendData(publication);
                }
            }
        }
        saveOfflineMessages();

        verifyReceivedData();
        verifyOnline();
    }

    @Test
    public void shouldStartOfflineReplayAndMoveToMergePhase() {
        archiveClient.startRecording(LIVE_CHANNEL, LIVE_STREAM, SourceLocation.LOCAL);

        // empty recording
        createAndFillRecord(0);

        fillArchiveWithRecords(2);

        try (Publication publication = aeron.addPublication(LIVE_CHANNEL, LIVE_STREAM)) {
            saveOfflineMessages(publication);

            catchUp.doWork();

            while (catchUp.getPhase() instanceof OfflinePhase) {
                idleStrategy.idle(catchUp.doWork());
            }
            catchUp.doWork();

            assertTrue(catchUp.getPhase() instanceof PreMergePhase);

            verifyReceivedData();
            verifyOnline(publication);
        }
    }

    @Test
    public void shouldReplayNewOfflineRecord() {
        archiveClient.startRecording(LIVE_CHANNEL, LIVE_STREAM, SourceLocation.LOCAL);

        fillArchiveWithRecords(2);
        // empty recording
        createAndFillRecord(0);

        catchUp.doWork();
        while (catchUp.getPhase() instanceof OfflinePhase) {
            idleStrategy.idle(catchUp.doWork());
        }

        fillArchiveWithRecords(1);
        saveOfflineMessages();

        catchUp.doWork();

        assertTrue(catchUp.getPhase() instanceof OfflinePhase);

        verifyReceivedData();
        verifyOnline();
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 16000})
    public void shouldMergeWithoutAnyMessage(long initPosition) {
        archiveClient.startRecording(LIVE_CHANNEL, LIVE_STREAM, SourceLocation.LOCAL);

        ChannelUri liveChanel = ChannelUri.parse(LIVE_CHANNEL);
        liveChanel.initialPosition(initPosition, 0, Configuration.termBufferLength());

        try (Publication publication =
                aeron.addExclusivePublication(liveChanel.toString(), LIVE_STREAM)) {

            while (catchUp.getPhase() instanceof PreMergePhase) {
                idleStrategy.idle(catchUp.doWork());
            }

            while (catchUp.getPhase() instanceof MergePhase) {
                MergePhase phase = (MergePhase) catchUp.getPhase();
                assertTrue(phase.isMerged());
                catchUp.doWork();
            }

            verifyReceivedData();
            verifyOnline(publication);
        }
    }

    @Test
    public void shouldMergeWithoutLiveMessageWhileMerging() {
        archiveClient.startRecording(LIVE_CHANNEL, LIVE_STREAM, SourceLocation.LOCAL);

        try (Publication publication = aeron.addPublication(LIVE_CHANNEL, LIVE_STREAM);
                Subscription subscription = aeron.addSubscription(LIVE_CHANNEL, LIVE_STREAM)) {

            for (int i = 0; i < TEST_MESSAGE_COUNT; i++) {
                sendData(publication);
                subscription.poll(this::nop, Integer.MAX_VALUE);
            }
            saveOfflineMessages(publication);

            while (catchUp.getPhase() instanceof PreMergePhase) {
                idleStrategy.idle(catchUp.doWork());
            }

            while (catchUp.getPhase() instanceof MergePhase) {
                MergePhase phase = (MergePhase) catchUp.getPhase();
                assertTrue(phase.isMerged());
                catchUp.doWork();
            }

            verifyReceivedData();
            verifyOnline(publication);
        }
    }

    @Test
    public void shouldMergeWithLiveStreamWithLiveEvents() {
        archiveClient.startRecording(LIVE_CHANNEL, LIVE_STREAM, SourceLocation.LOCAL);

        try (Publication publication = aeron.addPublication(LIVE_CHANNEL, LIVE_STREAM);
                Subscription subscription = aeron.addSubscription(LIVE_CHANNEL, LIVE_STREAM)) {

            int liveRecordingSize = 10 * TEST_MESSAGE_COUNT;
            for (int i = 0; i < liveRecordingSize; i++) {
                sendData(publication);
                subscription.poll(this::nop, Integer.MAX_VALUE);
            }
            saveOfflineMessages();

            while (verifyList.size() != 1) {
                catchUp.doWork();
            }

            assertEquals(catchUp.getPhase().getClass(), MergePhase.class);

            for (int i = 0; i < TEST_MESSAGE_COUNT; i++) {
                sendData(publication);
                subscription.poll(this::nop, Integer.MAX_VALUE);
                idleStrategy.idle(catchUp.doWork());
            }

            verifyReceivedData();
            verifyOnline(publication);
        }
    }

    @Test
    void shouldBeAbleToStopInMergePhase() {
        archiveClient.startRecording(LIVE_CHANNEL, LIVE_STREAM, SourceLocation.LOCAL);

        try (Publication publication = aeron.addPublication(LIVE_CHANNEL, LIVE_STREAM);
                Subscription subscription = aeron.addSubscription(LIVE_CHANNEL, LIVE_STREAM)) {

            for (int i = 0; i < TEST_MESSAGE_COUNT; i++) {
                sendData(publication);
                subscription.poll(this::nop, Integer.MAX_VALUE);
            }

            while (!(catchUp.getPhase() instanceof MergePhase)) {
                idleStrategy.idle(catchUp.doWork());
            }

            stopAndVerify(MergePhase.class);
        }
    }

    @Test
    void caughtUpInTheMiddleOfMessage() {
        archiveClient.startRecording(LIVE_CHANNEL, LIVE_STREAM, SourceLocation.LOCAL);

        try (Publication publication = aeron.addPublication(LIVE_CHANNEL, LIVE_STREAM);
                Subscription subscription = aeron.addSubscription(LIVE_CHANNEL, LIVE_STREAM); ) {

            MutableLong position = new MutableLong(-1);

            sendSmallData(publication);
            saveOfflineMessages();

            sendBigData(
                    publication, 30, () -> findMiddlePositionOfSomeMessage(subscription, position));
            while (position.value == -1) {
                findMiddlePositionOfSomeMessage(subscription, position);
            }
            // imitate that archive recorded up to the first message only
            when(catchUp.getArchiveClient().getRecordingPosition(anyLong()))
                    .thenReturn(position.get());

            waitForPhase(PreMergePhase.class);

            while (catchUp.getPhase() instanceof PreMergePhase) {
                PreMergePhase preMerge = (PreMergePhase) catchUp.getPhase();
                if (preMerge.getLiveImageRef().get() != null) {
                    Image liveImage = spy(preMerge.getLiveImageRef().get());
                    preMerge.getLiveImageRef().set(liveImage);

                    // imitate that we joined in the middle of a big message
                    when(liveImage.joinPosition()).thenReturn(position.get());
                }
                catchUp.doWork();
            }

            sendData(publication);
            sendData(publication);
            sendData(publication);

            verifyReceivedData();
            verifySnapshotData();
        }
    }
}
