package io.aeron.archive.client.util;

import static java.util.Objects.requireNonNull;

import io.aeron.archive.client.catchup.context.EventStoreClientContext;
import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.agrona.LangUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Convenient methods for work with Aeron and Archive */
public class AeronUtils {
    private static final Logger LOG = LoggerFactory.getLogger("Connection");

    private AeronUtils() {}

    /**
     * Check that image is still alive
     *
     * @param image from a subscription
     * @return true if the image is closed or it's eos or it's not in the original subscription
     */
    public static boolean isConnectionBroken(Image image) {
        return image.subscription().imageBySessionId(image.sessionId()) == null
                || image.isClosed()
                || image.isEndOfStream();
    }

    /**
     * List of all {@link RecordingDescriptor} for the archive.
     *
     * @param archiveClient connection to the target archive from which we want to list recordings
     * @return list of descriptors
     */
    public static List<RecordingDescriptor> listRecordings(AeronArchive archiveClient) {
        return listRecordings(archiveClient, 0);
    }

    /**
     * List of all {@link RecordingDescriptor} for the archive.
     *
     * @param archiveClient connection to the target archive from which we want to list recordings
     * @param fromRecordingId at which to begin the listing
     * @return list of descriptors
     */
    public static List<RecordingDescriptor> listRecordings(
            AeronArchive archiveClient, long fromRecordingId) {
        List<RecordingDescriptor> result = new ArrayList<>();
        archiveClient.listRecordings(
                fromRecordingId, Integer.MAX_VALUE, new ToObjectRecordingConsumer(result::add));
        return result;
    }

    /**
     * List of all {@link RecordingDescriptor} for a given channel and stream id.
     *
     * @param archiveClient connection to the target archive from which we want to list recordings
     * @param channel stored in the archive
     * @param streamId stored in the archive
     * @return list of descriptors
     */
    public static List<RecordingDescriptor> listRecordingsForUri(
            AeronArchive archiveClient, String channel, int streamId) {
        return listRecordingsForUri(archiveClient, channel, streamId, 0);
    }

    /**
     * List of all {@link RecordingDescriptor} for a given channel and stream id.
     *
     * @param archiveClient connection to the target archive from which we want to list recordings
     * @param channel stored in the archive
     * @param streamId stored in the archive
     * @param fromRecordingId at which to begin the listing
     * @return list of descriptors
     */
    public static List<RecordingDescriptor> listRecordingsForUri(
            AeronArchive archiveClient, String channel, int streamId, long fromRecordingId) {
        List<RecordingDescriptor> result = new ArrayList<>();
        archiveClient.listRecordingsForUri(
                fromRecordingId,
                Integer.MAX_VALUE,
                channel,
                streamId,
                new ToObjectRecordingConsumer(result::add));
        return result;
    }

    /**
     * Default action on a notification of a new {@link Image} being available for polling.
     *
     * @param image that is now available
     */
    public static void availableImageHandler(Image image) {
        LOG.info("A new image became available {}", image);
    }

    /**
     * Default action on a notification that an {@link Image} is no longer available for polling.
     *
     * @param image that is no longer available for polling.
     */
    public static void unavailableImageHandler(Image image) {
        LOG.info("A image is gone (UNavaliable) {}", image);
    }

    public static AeronArchive createArchiveClient(Aeron aeron, EventStoreClientContext context) {
        return AeronArchive.connect(createArchiveClientContext(aeron, context));
    }

    /**
     * Convenient method to create native {@link AeronArchive.Context} from {@link
     * EventStoreClientContext}
     *
     * @param aeron connection to aeron
     * @param context original EventStoreClientContext
     * @return context for connection to the archive which underlies the event store from context
     *     above
     */
    public static AeronArchive.Context createArchiveClientContext(
            Aeron aeron, EventStoreClientContext context) {

        AeronArchive.Context archiveCtx = new AeronArchive.Context();
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

        return archiveCtx;
    }

    public static class ToObjectRecordingConsumer implements RecordingDescriptorConsumer {
        private final Consumer<RecordingDescriptor> recordingDescriptorConsumer;

        public ToObjectRecordingConsumer(
                Consumer<RecordingDescriptor> recordingDescriptorConsumer) {

            this.recordingDescriptorConsumer = recordingDescriptorConsumer;
        }

        public void onRecordingDescriptor(
                long controlSessionId,
                long correlationId,
                long recordingId,
                long startTimestamp,
                long stopTimestamp,
                long startPosition,
                long stopPosition,
                int initialTermId,
                int segmentFileLength,
                int termBufferLength,
                int mtuLength,
                int sessionId,
                int streamId,
                String strippedChannel,
                String originalChannel,
                String sourceIdentity) {
            recordingDescriptorConsumer.accept(
                    new RecordingDescriptor(
                            controlSessionId,
                            correlationId,
                            recordingId,
                            startTimestamp,
                            stopTimestamp,
                            startPosition,
                            stopPosition,
                            initialTermId,
                            segmentFileLength,
                            termBufferLength,
                            mtuLength,
                            sessionId,
                            streamId,
                            strippedChannel,
                            originalChannel,
                            sourceIdentity));
        }
    }
}
