package io.aeron.archive.client.catchup;

import org.agrona.DirectBuffer;

/**
 * Generic implementation to compare 2 messages using their binary representation from the beginning
 * to the end.
 */
public final class MessageMatcherBeginToEnd implements MessageMatcher {

    @Override
    public boolean match(
            DirectBuffer liveMsgBuffer,
            int liveMsgOffset,
            DirectBuffer replayMsgBuffer,
            int replayedMsgOffset,
            int msgLength) {
        int liveIndex = liveMsgOffset;
        int replayIndex = replayedMsgOffset;

        for (int i = 0; i < msgLength; i++) {
            if (liveMsgBuffer.getByte(liveIndex) != replayMsgBuffer.getByte(replayIndex)) {
                return false;
            }
            ++liveIndex;
            ++replayIndex;
        }

        return true;
    }
}
