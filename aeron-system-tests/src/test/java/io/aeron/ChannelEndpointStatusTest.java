/*
 * Copyright 2014-2020 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.exceptions.ChannelEndpointException;
import io.aeron.exceptions.RegistrationException;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.status.ChannelEndpointStatus;
import io.aeron.test.TestMediaDriver;
import io.aeron.test.Tests;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.SystemUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.Duration.ofSeconds;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.*;

public class ChannelEndpointStatusTest
{
    private static final String URI = "aeron:udp?endpoint=localhost:54326";
    private static final String URI_NO_CONFLICT = "aeron:udp?endpoint=localhost:54327";
    private static final String URI_WITH_INTERFACE_PORT =
        "aeron:udp?endpoint=localhost:54326|interface=localhost:34567";

    private static final int STREAM_ID = 1001;
    private static final ThreadingMode THREADING_MODE = ThreadingMode.DEDICATED;

    private static final int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int NUM_MESSAGES_PER_TERM = 64;
    private static final int MESSAGE_LENGTH =
        (TERM_BUFFER_LENGTH / NUM_MESSAGES_PER_TERM) - DataHeaderFlyweight.HEADER_LENGTH;
    private static final String ROOT_DIR =
        SystemUtil.tmpDirName() + "aeron-system-tests-" + UUID.randomUUID().toString() + File.separator;

    private Aeron clientA;
    private Aeron clientB;
    private Aeron clientC;
    private TestMediaDriver driverA;
    private TestMediaDriver driverB;

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[MESSAGE_LENGTH]);

    private final ErrorHandler errorHandlerClientA = mock(ErrorHandler.class);
    private final ErrorHandler errorHandlerClientB = mock(ErrorHandler.class);
    private final ErrorHandler errorHandlerClientC = mock(ErrorHandler.class);

    private final AtomicInteger errorCounter = new AtomicInteger();
    private final ErrorHandler countingErrorHandler = (ex) -> errorCounter.getAndIncrement();

    @BeforeEach
    public void before()
    {
        TestMediaDriver.notSupportedOnCMediaDriverYet(
            "Need an alternative way of getting the error counters for the C media driver");

        final String baseDirA = ROOT_DIR + "A";
        final String baseDirB = ROOT_DIR + "B";

        buffer.putInt(0, 1);

        final MediaDriver.Context driverAContext = new MediaDriver.Context()
            .publicationTermBufferLength(TERM_BUFFER_LENGTH)
            .aeronDirectoryName(baseDirA)
            .errorHandler(countingErrorHandler)
            .threadingMode(THREADING_MODE);

        final MediaDriver.Context driverBContext = new MediaDriver.Context()
            .publicationTermBufferLength(TERM_BUFFER_LENGTH)
            .aeronDirectoryName(baseDirB)
            .errorHandler(countingErrorHandler)
            .threadingMode(THREADING_MODE);

        driverA = TestMediaDriver.launch(driverAContext);
        driverB = TestMediaDriver.launch(driverBContext);

        clientA = Aeron.connect(
            new Aeron.Context()
                .aeronDirectoryName(driverAContext.aeronDirectoryName())
                .errorHandler(errorHandlerClientA));

        clientB = Aeron.connect(
            new Aeron.Context()
                .aeronDirectoryName(driverBContext.aeronDirectoryName())
                .errorHandler(errorHandlerClientB));

        clientC = Aeron.connect(
            new Aeron.Context()
                .aeronDirectoryName(driverBContext.aeronDirectoryName())
                .errorHandler(errorHandlerClientC));
    }

    @AfterEach
    public void after()
    {
        CloseHelper.quietCloseAll(clientC, clientB, clientA, driverB, driverA);
        IoUtil.delete(new File(ROOT_DIR), true);
    }

    @Test
    public void shouldErrorBadUri()
    {
        assertTimeoutPreemptively(ofSeconds(5), () ->
            assertThrows(RegistrationException.class, () -> clientA.addSubscription("bad uri", STREAM_ID)));
    }

    @Test
    public void shouldBeAbleToQueryChannelStatusForSubscription()
    {
        assertTimeoutPreemptively(ofSeconds(5), () ->
        {
            final Subscription subscription = clientA.addSubscription(URI, STREAM_ID);

            while (subscription.channelStatus() == ChannelEndpointStatus.INITIALIZING)
            {
                Thread.yield();
                Tests.checkInterruptedStatus();
            }

            assertThat(subscription.channelStatus(), is(ChannelEndpointStatus.ACTIVE));
        });
    }

    @Test
    public void shouldBeAbleToQueryChannelStatusForPublication()
    {
        assertTimeoutPreemptively(ofSeconds(5), () ->
        {
            final Publication publication = clientA.addPublication(URI, STREAM_ID);

            while (publication.channelStatus() == ChannelEndpointStatus.INITIALIZING)
            {
                Thread.yield();
                Tests.checkInterruptedStatus();
            }

            assertThat(publication.channelStatus(), is(ChannelEndpointStatus.ACTIVE));
        });
    }

    @Test
    public void shouldCatchErrorOnAddressAlreadyInUseForSubscriptions()
    {
        final Subscription subscriptionA = clientA.addSubscription(URI, STREAM_ID);

        while (subscriptionA.channelStatus() == ChannelEndpointStatus.INITIALIZING)
        {
            Thread.yield();
            Tests.checkInterruptedStatus();
        }

        assertThat(subscriptionA.channelStatus(), is(ChannelEndpointStatus.ACTIVE));

        final Subscription subscriptionB = clientB.addSubscription(URI, STREAM_ID);

        final ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(errorHandlerClientB, timeout(5000L)).onError(captor.capture());

        assertThat(captor.getValue(), instanceOf(ChannelEndpointException.class));

        final ChannelEndpointException channelEndpointException = (ChannelEndpointException)captor.getValue();
        final long status = clientB.countersReader().getCounterValue(channelEndpointException.statusIndicatorId());

        assertThat(status, is(ChannelEndpointStatus.ERRORED));
        assertThat(errorCounter.get(), greaterThan(0));

        assertThat(subscriptionB.channelStatusId(), is(channelEndpointException.statusIndicatorId()));
        assertThat(subscriptionA.channelStatus(), is(ChannelEndpointStatus.ACTIVE));
    }

    @Test
    public void shouldCatchErrorOnAddressAlreadyInUseForPublications()
    {
        final Publication publicationA = clientA.addPublication(URI_WITH_INTERFACE_PORT, STREAM_ID);

        while (publicationA.channelStatus() == ChannelEndpointStatus.INITIALIZING)
        {
            Thread.yield();
            Tests.checkInterruptedStatus();
        }

        assertThat(publicationA.channelStatus(), is(ChannelEndpointStatus.ACTIVE));

        final Publication publicationB = clientB.addPublication(URI_WITH_INTERFACE_PORT, STREAM_ID);

        final ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        verify(errorHandlerClientB, timeout(5000L)).onError(captor.capture());

        assertThat(captor.getValue(), instanceOf(ChannelEndpointException.class));

        final ChannelEndpointException channelEndpointException = (ChannelEndpointException)captor.getValue();
        final long status = clientB.countersReader().getCounterValue(channelEndpointException.statusIndicatorId());

        assertThat(status, is(ChannelEndpointStatus.ERRORED));
        assertThat(errorCounter.get(), greaterThan(0));

        assertThat(publicationB.channelStatusId(), is(channelEndpointException.statusIndicatorId()));
        assertTrue(publicationB.isClosed());
        assertThat(publicationA.channelStatus(), is(ChannelEndpointStatus.ACTIVE));
    }

    @Test
    public void shouldNotErrorOnAddressAlreadyInUseOnActiveChannelEndpointForSubscriptions()
    {
        final Subscription subscriptionA = clientA.addSubscription(URI, STREAM_ID);

        while (subscriptionA.channelStatus() == ChannelEndpointStatus.INITIALIZING)
        {
            Thread.yield();
            Tests.checkInterruptedStatus();
        }

        final Subscription subscriptionB = clientB.addSubscription(URI_NO_CONFLICT, STREAM_ID);
        final Subscription subscriptionC = clientC.addSubscription(URI, STREAM_ID);

        while (subscriptionB.channelStatus() == ChannelEndpointStatus.INITIALIZING ||
            subscriptionC.channelStatus() == ChannelEndpointStatus.INITIALIZING)
        {
            Thread.yield();
            Tests.checkInterruptedStatus();
        }

        verify(errorHandlerClientC, timeout(5000L)).onError(any(ChannelEndpointException.class));
        assertThat(errorCounter.get(), greaterThan(0));
        assertThat(subscriptionC.channelStatus(), is(ChannelEndpointStatus.ERRORED));
        assertTrue(subscriptionC.isClosed());

        assertThat(subscriptionA.channelStatus(), is(ChannelEndpointStatus.ACTIVE));
        assertThat(subscriptionB.channelStatus(), is(ChannelEndpointStatus.ACTIVE));
    }
}
