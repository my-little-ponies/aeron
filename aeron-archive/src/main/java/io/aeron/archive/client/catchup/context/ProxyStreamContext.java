package io.aeron.archive.client.catchup.context;

import io.aeron.CommonContext;

public class ProxyStreamContext {
    private final String channel;
    private final Integer streamId;

    public ProxyStreamContext(int originalStreamId, String channel, Integer streamId) {
        this.streamId = streamId == null ? calculateDefaultProxyStream(originalStreamId) : streamId;
        this.channel = channel == null ? CommonContext.IPC_CHANNEL : channel;
    }

    public String getChannel() {
        return channel;
    }

    public int getStreamId() {
        return streamId;
    }

    private static int calculateDefaultProxyStream(int streamId) {
        if (streamId <= 0) {
            throw new IllegalArgumentException("Stream id should be more that 0");
        }
        return Integer.MAX_VALUE - streamId;
    }
}
