package io.aeron.archive.client.catchup.exception;

import static java.lang.String.format;

/**
 * When we start a catupUp process it must be only one live(active) recording in the event store. If
 * we have several ones the recording for catching up is undetermined and this exception will happen
 */
public class EventStoreTooManyLiveRecordingsException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public EventStoreTooManyLiveRecordingsException(String channel, int streamId) {
        super(
                format(
                        "More than one active recording exists for channel %s, streamId %d",
                        channel, streamId));
    }
}
