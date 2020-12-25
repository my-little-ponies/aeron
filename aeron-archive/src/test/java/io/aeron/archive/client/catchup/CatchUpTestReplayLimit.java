package io.aeron.archive.client.catchup;

import io.aeron.archive.client.catchup.CatchUp.OfflinePhase;
import io.aeron.archive.client.catchup.CatchUp.OnlinePhase;
import io.aeron.archive.client.catchup.CatchUp.PreMergePhase;
import io.aeron.archive.client.util.AeronUtils;
import io.aeron.archive.client.util.RecordingDescriptor;
import io.aeron.Publication;
import io.aeron.archive.Archive;
import io.aeron.archive.codecs.SourceLocation;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/** test for verify how catch up works with limited maxConcurrentReplays */
class CatchUpTestReplayLimit extends CatchUpTestBase {

    @Override
    protected Archive.Context createArchiveContext() {
        return super.createArchiveContext().maxConcurrentReplays(1);
    }

    @Test
    void shouldReconnectWhileCatchUpOffline() {
        archiveClient.startRecording(LIVE_CHANNEL, LIVE_STREAM, SourceLocation.LOCAL);

        createAndFillRecord(1);
        saveOfflineMessages();

        List<RecordingDescriptor> recordings =
                AeronUtils.listRecordingsForUri(archiveClient, LIVE_CHANNEL, LIVE_STREAM);

        long replaySessionId =
                archiveClient.startReplay(
                        recordings.get(0).getRecordingId(),
                        recordings.get(0).getStartPosition(),
                        Long.MAX_VALUE,
                        REPLAY_CHANNEL,
                        REPLAY_STREAM);

        for (int i = 0; i < 100; i++) {
            idleStrategy.idle(catchUp.doWork());
        }
        assertEquals(catchUp.getPhase().getClass(), OfflinePhase.class);

        archiveClient.stopReplay(replaySessionId);

        verifyReceivedData();
        verifyOnline();
    }

    @Test
    void shouldReconnectWhileCatchUpMerge() {
        archiveClient.startRecording(LIVE_CHANNEL, LIVE_STREAM, SourceLocation.LOCAL);

        try (Publication publication = aeron.addPublication(LIVE_CHANNEL, LIVE_STREAM)) {
            awaitPublisherIsRecording(publication);

            sendData(publication);

            List<RecordingDescriptor> recordings =
                    AeronUtils.listRecordingsForUri(archiveClient, LIVE_CHANNEL, LIVE_STREAM);
            long replaySessionId =
                    archiveClient.startReplay(
                            recordings.get(0).getRecordingId(),
                            recordings.get(0).getStartPosition(),
                            Long.MAX_VALUE,
                            REPLAY_CHANNEL,
                            REPLAY_STREAM);

            while (!(catchUp.getPhase() instanceof PreMergePhase)) {
                idleStrategy.idle(catchUp.doWork());
            }

            for (int i = 0; i < 100; i++) {
                idleStrategy.idle(catchUp.doWork());
                assertNotEquals(catchUp.getPhase().getClass(), OnlinePhase.class);
            }

            archiveClient.stopReplay(replaySessionId);

            verifyReceivedData();
            verifyOnline(publication);
        }
    }
}
