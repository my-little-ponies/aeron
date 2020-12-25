package io.aeron.archive.client.util;

import io.aeron.archive.client.AeronArchive;

/**
 * Convenient class which contains all information from {@link
 * io.aeron.archive.client.RecordingDescriptorConsumer}
 *
 * <p>Used here {@link AeronUtils#listRecordingsForUri(AeronArchive, String, int)}
 */
public class RecordingDescriptor {
    private long controlSessionId;
    private long correlationId;
    private long recordingId;
    private long startTimestamp;
    private long stopTimestamp;
    private long startPosition;
    private long stopPosition;
    private int initialTermId;
    private int segmentFileLength;
    private int termBufferLength;
    private int mtuLength;
    private int sessionId;
    private int streamId;
    private String strippedChannel;
    private String originalChannel;
    private String sourceIdentity;

    public RecordingDescriptor(
            final long controlSessionId,
            final long correlationId,
            final long recordingId,
            final long startTimestamp,
            final long stopTimestamp,
            final long startPosition,
            final long stopPosition,
            final int initialTermId,
            final int segmentFileLength,
            final int termBufferLength,
            final int mtuLength,
            final int sessionId,
            final int streamId,
            final String strippedChannel,
            final String originalChannel,
            final String sourceIdentity) {
        super();
        this.controlSessionId = controlSessionId;
        this.correlationId = correlationId;
        this.recordingId = recordingId;
        this.startTimestamp = startTimestamp;
        this.stopTimestamp = stopTimestamp;
        this.startPosition = startPosition;
        this.stopPosition = stopPosition;
        this.initialTermId = initialTermId;
        this.segmentFileLength = segmentFileLength;
        this.termBufferLength = termBufferLength;
        this.mtuLength = mtuLength;
        this.sessionId = sessionId;
        this.streamId = streamId;
        this.strippedChannel = strippedChannel;
        this.originalChannel = originalChannel;
        this.sourceIdentity = sourceIdentity;
    }

    public long length() {
        if (isLive()) {
            return Long.MAX_VALUE;
        }
        return stopPosition - startPosition;
    }

    public boolean isEmpty() {
        return length() == 0;
    }

    public boolean isLive() {
        return getStopPosition() == AeronArchive.NULL_POSITION;
    }

    public boolean isOffline() {
        return !isLive();
    }

    public long getControlSessionId() {
        return controlSessionId;
    }

    public long getCorrelationId() {
        return correlationId;
    }

    public long getRecordingId() {
        return recordingId;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public long getStopTimestamp() {
        return stopTimestamp;
    }

    public long getStartPosition() {
        return startPosition;
    }

    public long getStopPosition() {
        return stopPosition;
    }

    public int getInitialTermId() {
        return initialTermId;
    }

    public int getSegmentFileLength() {
        return segmentFileLength;
    }

    public int getTermBufferLength() {
        return termBufferLength;
    }

    public int getMtuLength() {
        return mtuLength;
    }

    public int getSessionId() {
        return sessionId;
    }

    public int getStreamId() {
        return streamId;
    }

    public String getStrippedChannel() {
        return strippedChannel;
    }

    public String getOriginalChannel() {
        return originalChannel;
    }

    public String getSourceIdentity() {
        return sourceIdentity;
    }

    public RecordingDescriptor setControlSessionId(long controlSessionId) {
        this.controlSessionId = controlSessionId;
        return this;
    }

    public RecordingDescriptor setCorrelationId(long correlationId) {
        this.correlationId = correlationId;
        return this;
    }

    public RecordingDescriptor setRecordingId(long recordingId) {
        this.recordingId = recordingId;
        return this;
    }

    public RecordingDescriptor setStartTimestamp(long startTimestamp) {
        this.startTimestamp = startTimestamp;
        return this;
    }

    public RecordingDescriptor setStopTimestamp(long stopTimestamp) {
        this.stopTimestamp = stopTimestamp;
        return this;
    }

    public RecordingDescriptor setStartPosition(long startPosition) {
        this.startPosition = startPosition;
        return this;
    }

    public RecordingDescriptor setStopPosition(long stopPosition) {
        this.stopPosition = stopPosition;
        return this;
    }

    public RecordingDescriptor setInitialTermId(int initialTermId) {
        this.initialTermId = initialTermId;
        return this;
    }

    public RecordingDescriptor setSegmentFileLength(int segmentFileLength) {
        this.segmentFileLength = segmentFileLength;
        return this;
    }

    public RecordingDescriptor setTermBufferLength(int termBufferLength) {
        this.termBufferLength = termBufferLength;
        return this;
    }

    public RecordingDescriptor setMtuLength(int mtuLength) {
        this.mtuLength = mtuLength;
        return this;
    }

    public RecordingDescriptor setSessionId(int sessionId) {
        this.sessionId = sessionId;
        return this;
    }

    public RecordingDescriptor setStreamId(int streamId) {
        this.streamId = streamId;
        return this;
    }

    public RecordingDescriptor setStrippedChannel(String strippedChannel) {
        this.strippedChannel = strippedChannel;
        return this;
    }

    public RecordingDescriptor setOriginalChannel(String originalChannel) {
        this.originalChannel = originalChannel;
        return this;
    }

    public RecordingDescriptor setSourceIdentity(String sourceIdentity) {
        this.sourceIdentity = sourceIdentity;
        return this;
    }

    @Override
    public String toString() {
        return "RecordingDescriptor{"
                + "controlSessionId="
                + controlSessionId
                + ", sessionId="
                + sessionId
                + ", recordingId="
                + recordingId
                + ", startPosition="
                + startPosition
                + ", stopPosition="
                + stopPosition
                + ", streamId="
                + streamId
                + ", originalChannel='"
                + originalChannel
                + '\''
                + ", sourceIdentity='"
                + sourceIdentity
                + '\''
                + '}';
    }
}
