package io.aeron.archive.client.catchup;

import io.aeron.archive.client.catchup.CatchUp.MergePhase;
import io.aeron.archive.client.catchup.CatchUp.OfflinePhase;
import io.aeron.archive.client.catchup.CatchUp.OnlinePhase;
import io.aeron.archive.client.catchup.CatchUp.PreMergePhase;
import io.aeron.Publication;
import io.aeron.archive.codecs.SourceLocation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CatchUpTestGapFill extends CatchUpTestBase {

    private Publication publication;

    @Override
    protected CatchUp createCatchUp() {
        publication = aeron.addExclusivePublication(LIVE_CHANNEL, LIVE_STREAM);
        openLiveSubscription();

        return new CatchUp(
                aeron,
                connectToArchive(),
                createCatchUpContext().setSessionId(publication.sessionId()));
    }

    @AfterEach
    public void tearDown() {
        publication.close();
        super.tearDown();
    }

    @Test
    void shouldContinueFromPositionWhereItStoppedOffline() {
        publication.close();
        archiveClient.startRecording(LIVE_CHANNEL, LIVE_STREAM, SourceLocation.LOCAL);

        createAndFillRecord(100);

        catchUp = createCatchUp();

        waitForPhase(OfflinePhase.class);
        while (verifyList.isEmpty()) {
            catchUp.doWork();
        }
        OfflinePhase mergePhase = (OfflinePhase) catchUp.getPhase();
        mergePhase.getReplaySubscription().close();

        waitForDeathAndReborn();
    }

    @Test
    void shouldContinueFromPositionWhereItStoppedMerge() {
        archiveClient.startRecording(LIVE_CHANNEL, LIVE_STREAM, SourceLocation.LOCAL);

        sendData(publication);
        sendData(publication);
        sendData(publication);

        waitForPhase(MergePhase.class);
        MergePhase mergePhase = (MergePhase) catchUp.getPhase();
        mergePhase.getLiveSubscription().close();

        sendData(publication);

        publication.close();
        waitForDeathAndReborn();
    }

    @Test
    void shouldContinueFromPositionWhereItStoppedMergeTheSameRecording() {
        archiveClient.startRecording(LIVE_CHANNEL, LIVE_STREAM, SourceLocation.LOCAL);

        sendData(publication);

        waitForPhase(MergePhase.class);
        MergePhase mergePhase = (MergePhase) catchUp.getPhase();
        mergePhase.getLiveSubscription().close();

        for (int i = 0; i < 10; i++) {
            sendData(publication);
        }

        waitForDeathAndReborn();
    }

    @Test
    void shouldContinueFromPositionWhereItStoppedOnlineWithLostMessages() {
        archiveClient.startRecording(LIVE_CHANNEL, LIVE_STREAM, SourceLocation.LOCAL);

        sendData(publication);
        sendData(publication);
        sendData(publication);

        waitForPhase(OnlinePhase.class);

        sendData(publication);

        publication.close();
        waitForDeathAndReborn();
    }

    @Test
    void shouldContinueFromPositionWhereItStoppedOnlineWithoutLostMessages() {
        archiveClient.startRecording(LIVE_CHANNEL, LIVE_STREAM, SourceLocation.LOCAL);

        sendData(publication);

        waitForPhase(OnlinePhase.class);
        publication.close();

        waitForDeathAndReborn();
    }

    @Test
    void shouldTerminateIfThereIsNoLiveRecordingButSessionIsProvided() {
        archiveClient.startRecording(LIVE_CHANNEL, LIVE_STREAM, SourceLocation.LOCAL);
        publication.close();
        createAndFillRecord(1);

        waitForDeathAndReborn();
    }

    @Test
    void shouldContinueFromPositionOnEmptyLiveRecording() {
        archiveClient.startRecording(LIVE_CHANNEL, LIVE_STREAM, SourceLocation.LOCAL);
        sendData(publication);
        sendData(publication);
        sendData(publication);

        waitForPhase(PreMergePhase.class);
        publication.close();
        publication = aeron.addExclusivePublication(LIVE_CHANNEL, LIVE_STREAM);
        waitForPhase(MergePhase.class);

        publication.close();
        waitForTerminalPhase();
        catchUp.close();
        Assertions.assertFalse(catchUp.healthCheck());

        publication = aeron.addExclusivePublication(LIVE_CHANNEL, LIVE_STREAM);
        catchUp =
                new CatchUp(
                        aeron,
                        connectToArchive(),
                        catchUp.createContext().setSessionId(publication.sessionId()));
        waitForPhase(OnlinePhase.class);
        publication.close();

        waitForDeathAndReborn();
    }

    private void waitForDeathAndReborn() {
        waitForTerminalPhase();
        catchUp.close();
        Assertions.assertFalse(catchUp.healthCheck());

        if (publication.isClosed()) {
            publication = aeron.addExclusivePublication(LIVE_CHANNEL, LIVE_STREAM);
        }
        sendData(publication);

        catchUp =
                new CatchUp(
                        aeron,
                        connectToArchive(),
                        catchUp.createContext().setSessionId(publication.sessionId()));

        sendData(publication);

        verifyReceivedData();
        verifyOnline(publication);
    }
}
