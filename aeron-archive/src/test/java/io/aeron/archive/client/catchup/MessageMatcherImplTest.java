package io.aeron.archive.client.catchup;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MessageMatcherImplTest {

    private final MessageMatcher matcher = new MessageMatcherBeginToEnd();

    @Test
    public void testEmptyMatches() {
        final UnsafeBuffer msg1 = new UnsafeBuffer(new byte[] {});
        final UnsafeBuffer msg2 = new UnsafeBuffer(new byte[] {});
        assertTrue(matcher.match(msg1, 0, msg2, 0, 0), "Messages should be equal");
    }

    @Test
    public void testNonEmptyWithOffsetMatches() {
        final UnsafeBuffer msg1 = new UnsafeBuffer(new byte[] {0, 1, 2, 3, 4, 5, 0, 0});
        final UnsafeBuffer msg2 = new UnsafeBuffer(new byte[] {0, 0, 1, 2, 3, 4, 5});
        assertTrue(matcher.match(msg1, 1, msg2, 2, 5), "Messages should be equal");
    }

    @Test
    public void testNonEmptyWithoutOffsetMatches() {
        final UnsafeBuffer msg1 = new UnsafeBuffer(new byte[] {1, 2, 3, 4, 5});
        final UnsafeBuffer msg2 = new UnsafeBuffer(new byte[] {1, 2, 3, 4, 5});
        assertTrue(matcher.match(msg1, 0, msg2, 0, 5), "Messages should be equal");
    }

    @Test
    public void testNonEmptyWithOffsetNotMatches() {
        final UnsafeBuffer msg1 = new UnsafeBuffer(new byte[] {0, 1, 2, 3, 4, 5, 0, 0});
        final UnsafeBuffer msg2 = new UnsafeBuffer(new byte[] {0, 0, 1, 2, 0, 4, 5});
        assertFalse(matcher.match(msg1, 1, msg2, 2, 5), "Messages should not be equal");
    }

    @Test
    public void testNonEmptyWithoutOffsetNotMatches() {
        final UnsafeBuffer msg1 = new UnsafeBuffer(new byte[] {1, 2, 3, 4, 5});
        final UnsafeBuffer msg2 = new UnsafeBuffer(new byte[] {1, 2, 9, 4, 5});
        assertFalse(matcher.match(msg1, 0, msg2, 0, 5), "Messages should not be equal");
    }

    @Test
    public void testOutOfBoundsNegative() {
        final UnsafeBuffer msg1 = new UnsafeBuffer(new byte[] {0, 1});
        final UnsafeBuffer msg2 = new UnsafeBuffer(new byte[] {0, 1});
        assertThrows(
                IndexOutOfBoundsException.class,
                () -> {
                    matcher.match(msg1, 1, msg2, 1, 2);
                });
    }
}
