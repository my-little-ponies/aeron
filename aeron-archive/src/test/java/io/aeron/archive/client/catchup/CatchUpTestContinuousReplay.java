package io.aeron.archive.client.catchup;

import io.aeron.archive.client.catchup.CatchUp.ContinuousReplayPhase;
import io.aeron.archive.client.catchup.context.CatchUpContext;
import io.aeron.ChannelUri;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.driver.Configuration;
import org.agrona.collections.MutableLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.when;

public class CatchUpTestContinuousReplay extends CatchUpTestBase {

    @Override
    protected CatchUpContext createCatchUpContext() {
        return super.createCatchUpContext()
                .setContinuousReplay(true)
                .setContinuousReplayTimeoutMs(100L);
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 16000})
    void shouldJoinContinuousReplayWithEmptyLiveRecording(long initPosition) {
        archiveClient.startRecording(LIVE_CHANNEL, LIVE_STREAM, SourceLocation.LOCAL);

        // empty recording
        createAndFillRecord(0);

        fillArchiveWithRecords(2);

        ChannelUri liveChanel = ChannelUri.parse(LIVE_CHANNEL);
        liveChanel.initialPosition(initPosition, 0, Configuration.termBufferLength());

        try (Publication publication =
                aeron.addExclusivePublication(liveChanel.toString(), LIVE_STREAM)) {
            saveOfflineMessages(publication);

            while (catchUp.inProgress()) {
                idleStrategy.idle(catchUp.doWork());
            }
            verifyReceivedData();
            verifyOnline(publication);
        }
    }

    @Test
    void shouldJoinContinuousReplayWithNonEmptyLiveRecording() {
        archiveClient.startRecording(LIVE_CHANNEL, LIVE_STREAM, SourceLocation.LOCAL);

        try (Publication publication = aeron.addPublication(LIVE_CHANNEL, LIVE_STREAM);
                Subscription subscription = aeron.addSubscription(LIVE_CHANNEL, LIVE_STREAM)) {

            for (int i = 0; i < TEST_MESSAGE_COUNT; i++) {
                sendData(publication);
                subscription.poll(this::nop, Integer.MAX_VALUE);
            }
            saveOfflineMessages(publication);

            while (catchUp.inProgress()) {
                idleStrategy.idle(catchUp.doWork());
            }

            verifyReceivedData();
            verifyOnline(publication);
        }
    }

    @Test
    void incomingMessagesShouldNotInterfereWithContinuousReplay() {
        archiveClient.startRecording(LIVE_CHANNEL, LIVE_STREAM, SourceLocation.LOCAL);

        try (Publication publication = aeron.addPublication(LIVE_CHANNEL, LIVE_STREAM);
                Subscription subscription = aeron.addSubscription(LIVE_CHANNEL, LIVE_STREAM)) {

            for (int i = 0; i < TEST_MESSAGE_COUNT; i++) {
                sendData(publication);
                subscription.poll(this::nop, Integer.MAX_VALUE);
            }
            saveOfflineMessages(publication);

            waitForPhase(ContinuousReplayPhase.class);

            for (int i = 0; i < TEST_MESSAGE_COUNT; i++) {
                sendData(publication);
                subscription.poll(this::nop, Integer.MAX_VALUE);
            }

            verifyReceivedData();
            verifyOnline(publication);
        }
    }

    @Test
    void shouldGoToTerminalPhaseIfArchiveIsClosed() {
        archiveClient.startRecording(LIVE_CHANNEL, LIVE_STREAM, SourceLocation.LOCAL);

        try (Subscription subscription = aeron.addSubscription(LIVE_CHANNEL, LIVE_STREAM);
                Publication publication = aeron.addPublication(LIVE_CHANNEL, LIVE_STREAM)) {

            for (int i = 0; i < TEST_MESSAGE_COUNT; i++) {
                sendData(publication);
                subscription.poll(this::nop, Integer.MAX_VALUE);
                catchUp.doWork();
            }

            verifyReceivedData();
            archive.close();
            waitForTerminalPhase();
        }
    }

    @Test
    void shouldWaitLiveRecording() {
        archiveClient.startRecording(LIVE_CHANNEL, LIVE_STREAM, SourceLocation.LOCAL);

        fillArchiveWithRecords(2);
        // empty recording
        createAndFillRecord(0);

        saveOfflineMessages();

        while (catchUp.inProgress()) {
            catchUp.doWork();
        }

        verifySnapshotData();
        verifyReceivedData();

        openLiveSubscription();
        verifyOnline();
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
                    publication, 20, () -> findMiddlePositionOfSomeMessage(subscription, position));
            while (position.value == -1) {
                findMiddlePositionOfSomeMessage(subscription, position);
            }

            sendData(publication);
            sendData(publication);
            sendData(publication);

            // imitate that archive recorded up to the first message only
            when(catchUp.getArchiveClient().getRecordingPosition(anyLong()))
                    .thenReturn(position.get());

            verifyReceivedData();
            verifySnapshotData();
        }
    }
}
