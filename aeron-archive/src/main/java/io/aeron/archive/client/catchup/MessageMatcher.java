package io.aeron.archive.client.catchup;

import org.agrona.DirectBuffer;

/**
 * Matcher interface to compare the last Live consumed message and the last Replayed from Event
 * Store. Used to switch from a replayed stream to a live stream.
 */
@FunctionalInterface
public interface MessageMatcher {

    /**
     * Matches live and replayed messages.
     *
     * @param liveMsgBuffer buffer of a live message
     * @param liveMsgOffset live message offset
     * @param replayMsgBuffer buffer of a replayed message
     * @param replayedMsgOffset replayed message offset
     * @param msgLength message length
     * @return true of live message matched the replayed one.
     */
    boolean match(
            DirectBuffer liveMsgBuffer,
            int liveMsgOffset,
            DirectBuffer replayMsgBuffer,
            int replayedMsgOffset,
            int msgLength);
}
