package io.aeron.archive.client.catchup;

import io.aeron.archive.client.catchup.CatchUp.ContinuousReplayPhase;
import io.aeron.archive.client.catchup.CatchUp.ContinuousReplayWaitPhase;
import io.aeron.Publication;
import io.aeron.archive.codecs.SourceLocation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CatchUpTestGapFillContinuousReplay extends CatchUpTestBase {

    private Publication publication;

    @Override
    protected CatchUp createCatchUp() {
        publication = aeron.addExclusivePublication(LIVE_CHANNEL, LIVE_STREAM);
        openLiveSubscription();

        return new CatchUp(
                aeron,
                connectToArchive(),
                createCatchUpContext()
                        .setContinuousReplay(true)
                        .setSessionId(publication.sessionId()));
    }

    @AfterEach
    public void tearDown() {
        publication.close();
        super.tearDown();
    }

    @Test
    void shouldContinueFromPositionWhereItStoppedMerge() {
        archiveClient.startRecording(LIVE_CHANNEL, LIVE_STREAM, SourceLocation.LOCAL);

        sendData(publication);
        sendData(publication);
        sendData(publication);

        waitForPhase(ContinuousReplayPhase.class);

        while (verifyList.isEmpty()) {
            catchUp.doWork();
        }

        sendData(publication);
        sendData(publication);
        verifyReceivedData();

        ContinuousReplayPhase mergePhase = (ContinuousReplayPhase) catchUp.getPhase();
        mergePhase.getReplaySubscription().close();

        sendData(publication);
        sendData(publication);
        sendData(publication);

        publication.close();
        waitForTerminalPhase();
        catchUp.close();
        Assertions.assertFalse(catchUp.healthCheck());

        publication = aeron.addExclusivePublication(LIVE_CHANNEL, LIVE_STREAM);
        sendData(publication);

        catchUp =
                new CatchUp(
                        aeron,
                        connectToArchive(),
                        catchUp.createContext()
                                .setContinuousReplay(true)
                                .setSessionId(publication.sessionId()));
        sendData(publication);

        verifyReceivedData();

        publication.close();
        waitForTerminalPhase();
        catchUp.close();
        Assertions.assertFalse(catchUp.healthCheck());

        catchUp =
                new CatchUp(
                        aeron,
                        connectToArchive(),
                        catchUp.createContext()
                                .setContinuousReplay(true)
                                .setSessionId(publication.sessionId()));

        waitForPhase(ContinuousReplayWaitPhase.class);

        for (int i = 0; i < 10; i++) {
            catchUp.doWork();
        }

        publication = aeron.addExclusivePublication(LIVE_CHANNEL, LIVE_STREAM);
        sendData(publication);
        sendData(publication);

        verifyReceivedData();
        verifyOnline(publication);
    }

    @Test
    void shouldContinueFromPositionOnEmptyLiveRecording() {
        archiveClient.startRecording(LIVE_CHANNEL, LIVE_STREAM, SourceLocation.LOCAL);
        sendData(publication);
        sendData(publication);
        sendData(publication);

        publication.close();
        publication = aeron.addExclusivePublication(LIVE_CHANNEL, LIVE_STREAM);
        waitForPhase(ContinuousReplayPhase.class);

        publication.close();
        waitForTerminalPhase();
        catchUp.close();
        Assertions.assertFalse(catchUp.healthCheck());

        publication = aeron.addExclusivePublication(LIVE_CHANNEL, LIVE_STREAM);
        catchUp =
                new CatchUp(
                        aeron,
                        connectToArchive(),
                        catchUp.createContext()
                                .setContinuousReplay(true)
                                .setSessionId(publication.sessionId()));
        waitForPhase(ContinuousReplayPhase.class);
        for (int i = 0; i < 10; i++) {
            catchUp.doWork();
        }
        sendData(publication);
        sendData(publication);
        verifyReceivedData();
        verifyOnline(publication);
    }
}
