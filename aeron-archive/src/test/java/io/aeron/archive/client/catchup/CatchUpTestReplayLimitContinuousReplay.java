package io.aeron.archive.client.catchup;

import io.aeron.archive.client.catchup.context.CatchUpContext;
import io.aeron.archive.client.util.AeronUtils;
import io.aeron.archive.client.util.RecordingDescriptor;
import io.aeron.ChannelUri;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.collections.MutableBoolean;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** test for verify how catch up works with limited maxConcurrentReplays */
public class CatchUpTestReplayLimitContinuousReplay extends CatchUpTestBase {

    @Override
    protected CatchUpContext createCatchUpContext() {
        return super.createCatchUpContext()
                .setContinuousReplay(true)
                .setContinuousReplayTimeoutMs(100L);
    }

    @Override
    protected Archive.Context createArchiveContext() {
        return super.createArchiveContext().maxConcurrentReplays(1);
    }

    @Test
    void shouldReconnectWhileCatchUpContinuousReplay() {
        archiveClient.startRecording(LIVE_CHANNEL, LIVE_STREAM, SourceLocation.LOCAL);

        try (Publication publication = aeron.addPublication(LIVE_CHANNEL, LIVE_STREAM)) {
            awaitPublisherIsRecording(publication);

            for (int i = 0; i < 50; i++) {
                sendData(publication);
            }

            List<RecordingDescriptor> recordings =
                    AeronUtils.listRecordingsForUri(archiveClient, LIVE_CHANNEL, LIVE_STREAM);
            long replaySessionId =
                    archiveClient.startReplay(
                            recordings.get(0).getRecordingId(),
                            recordings.get(0).getStartPosition(),
                            Long.MAX_VALUE,
                            REPLAY_CHANNEL,
                            REPLAY_STREAM);

            try (Subscription replaySubscription =
                    aeron.addSubscription(
                            ChannelUri.addSessionId(REPLAY_CHANNEL, (int) replaySessionId),
                            REPLAY_STREAM); ) {
                while (!replaySubscription.isConnected()) {
                    idleStrategy.idle();
                    catchUp.doWork();
                    assertEquals(0, verifyList.size());
                }

                IdleStrategy idleStrategy = new SleepingMillisIdleStrategy(50);
                MutableBoolean finish = new MutableBoolean(false);
                FragmentHandler fragmentHandler =
                        (buffer, offset, length, header) -> {
                            if (buffer.getInt(offset) == testDataSource - 1) {
                                finish.set(true);
                            }
                        };
                while (!finish.get()) {
                    idleStrategy.idle(catchUp.doWork());
                    assertEquals(0, verifyList.size());
                    replaySubscription.poll(fragmentHandler, 1);
                }

                archiveClient.stopReplay(replaySessionId);
            }

            verifyReceivedData();
            verifyOnline(publication);
        }
    }
}
