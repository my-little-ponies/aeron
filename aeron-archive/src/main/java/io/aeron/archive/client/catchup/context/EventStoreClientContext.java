package io.aeron.archive.client.catchup.context;

import static io.aeron.archive.client.util.Util.minCheckedValue;
import static java.util.Objects.requireNonNull;


/**
 * Context for create a connection to an event store
 *
 * <ul>
 *   <li>Control request stream - the stream which is opened by event store and where it listens for
 *       requests
 *   <li>Control response stream - where we will receive a response on our request
 * </ul>
 */
public class EventStoreClientContext {


    private String controlRequestChannel;
    private int controlRequestStreamId;
    private String controlResponseChannel;
    private int controlResponseStreamId;

    @Override
    public String toString() {
        return "EventStoreClientContext [controlRequestChannel="
                + controlRequestChannel
                + ", controlRequestStreamId="
                + controlRequestStreamId
                + ", controlResponseChannel="
                + controlResponseChannel
                + ", controlResponseStreamId="
                + controlResponseStreamId
                + "]";
    }

    public String getControlRequestChannel() {
        return controlRequestChannel;
    }

    public void setControlRequestChannel(final String controlRequestChannel) {
        this.controlRequestChannel = controlRequestChannel;
    }

    public int getControlRequestStreamId() {
        return controlRequestStreamId;
    }

    public void setControlRequestStreamId(final int controlRequestStreamId) {
        this.controlRequestStreamId = controlRequestStreamId;
    }

    public String getControlResponseChannel() {
        return controlResponseChannel;
    }

    public void setControlResponseChannel(final String controlResponseChannel) {
        this.controlResponseChannel = controlResponseChannel;
    }

    public int getControlResponseStreamId() {
        return controlResponseStreamId;
    }

    public void setControlResponseStreamId(final int controlResponseStreamId) {
        this.controlResponseStreamId = controlResponseStreamId;
    }
}
