package io.aeron.archive.client.catchup;

import static io.aeron.Aeron.NULL_VALUE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.agrona.CloseHelper.quietClose;

import io.aeron.archive.client.util.AeronUtils;
import io.aeron.archive.client.util.RecordingDescriptor;
import io.aeron.Aeron;
import io.aeron.AvailableImageHandler;
import io.aeron.ChannelUri;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.UnavailableImageHandler;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.collections.MutableReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplaySubscription implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(ReplaySubscription.class);
    public static final long TIMEOUT = SECONDS.toMillis(1);

    private final Aeron aeron;
    private final AeronArchive archiveClient;
    private final RecordingDescriptor recording;
    private final String replayChannel;
    private final int replayStreamId;
    private final AvailableImageHandler availableImageHandler;
    private final UnavailableImageHandler unavailableImageHandler;
    private final MutableReference<Image> replayImage = new MutableReference<>();

    private long replaySessionId = NULL_VALUE;
    private Subscription replaySubscription;
    private long lastConnectTimestamp = 0;

    public ReplaySubscription(
            Aeron aeron,
            AeronArchive archiveClient,
            RecordingDescriptor recording,
            String replayChannel,
            int replayStreamId) {
        this(
                aeron,
                archiveClient,
                recording,
                replayChannel,
                replayStreamId,
                AeronUtils::availableImageHandler,
                AeronUtils::unavailableImageHandler);
    }

    public ReplaySubscription(
            Aeron aeron,
            AeronArchive archiveClient,
            RecordingDescriptor recording,
            String replayChannel,
            int replayStreamId,
            AvailableImageHandler availableImageHandler,
            UnavailableImageHandler unavailableImageHandler) {
        this.aeron = aeron;
        this.archiveClient = archiveClient;
        this.recording = recording;
        this.replayChannel = replayChannel;
        this.replayStreamId = replayStreamId;
        this.availableImageHandler = availableImageHandler;
        this.unavailableImageHandler = unavailableImageHandler;

        connect();
    }

    private void connect() {
        LOG.info(
                "Start replay for recording ID = {} from position {}",
                recording.getRecordingId(),
                recording.getStartPosition());
        try {
            replaySessionId =
                    archiveClient.startReplay(
                            recording.getRecordingId(),
                            recording.getStartPosition(),
                            Long.MAX_VALUE,
                            replayChannel,
                            replayStreamId);
        } catch (ArchiveException e) {
            LOG.warn("Start replay call failed with an exception: {}", e.getMessage());
            return;
        }

        String replayChannelWithSession =
                ChannelUri.addSessionId(replayChannel, (int) replaySessionId);
        replaySubscription =
                aeron.addSubscription(
                        replayChannelWithSession,
                        replayStreamId,
                        availableImageHandler,
                        unavailableImageHandler);

        LOG.info(
                "Created subscription with registrationId {}, sessionId {} for replay channel {}, streamId {}",
                replaySubscription.registrationId(),
                replaySessionId,
                replayChannelWithSession,
                replayStreamId);
    }

    public int poll(FragmentHandler fragmentHandler, int fragmentLimit) {
        if (replaySubscription != null) {
            if (replayImage.ref == null) {
                if (replaySubscription.isClosed()) {
                    throw new IllegalStateException("Subscription is closed");
                }
                replaySubscription.forEachImage(replayImage::set);
            } else {
                return replayImage.ref.poll(fragmentHandler, fragmentLimit);
            }
        } else {
            long now = System.currentTimeMillis();
            if ((now - lastConnectTimestamp) >= TIMEOUT) {
                connect();
                lastConnectTimestamp = now;
            }
        }
        return 0;
    }

    public boolean isProcessing() {
        if (replaySubscription != null && replaySubscription.isClosed()) {
            return false;
        }
        if (replayImage.ref != null && AeronUtils.isConnectionBroken(replayImage.ref)) {
            return false;
        }
        return true;
    }

    public void stopReplay() {
        if (replaySessionId != Aeron.NULL_VALUE) {
            try {
                archiveClient.stopReplay(replaySessionId);
                replaySessionId = Aeron.NULL_VALUE;
            } catch (Exception e) {
                LOG.warn(e.getMessage());
            }
        }
    }

    public RecordingDescriptor getRecording() {
        return recording;
    }

    public long getReplaySessionId() {
        return replaySessionId;
    }

    @Override
    public void close() {
        quietClose(replaySubscription);
    }
}
