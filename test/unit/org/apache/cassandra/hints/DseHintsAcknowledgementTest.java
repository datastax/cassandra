/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.hints;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.HintsServiceMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MockMessagingService;
import org.apache.cassandra.net.MockMessagingSpy;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.NoSpamLogger;

import static org.apache.cassandra.hints.HintsTestUtil.writeAndFlushHints;
import static org.apache.cassandra.net.MessagingService.VERSION_3014;
import static org.apache.cassandra.net.MockMessagingService.verb;
import static org.apache.cassandra.net.Verb.HINT_REQ;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * DSE 6 compatibility mode: hints this node sends to a DSE 6 node (a peer at the pre-4.0 messaging version 11) wait for
 * DSE's acknowledgement, as between two nodes of this software. DSE 6.9.26 declares {@code HINTS.HINT} as an acked
 * request at every dialect: a hint it accepts is answered with a {@code REQUEST_RESPONSE} (verb id 4) with an empty
 * payload, matched to the hint by its message id; a hint it drops (unknown table, marshal error, range no longer owned
 * when configured to reject such hints) gets no answer (DSE 6.8 was not checked). A hint file is therefore removed
 * only once every hint in it was acknowledged; without an acknowledgement, or with a failure response, the file stays
 * and is sent again, and a WARN tells the operator that DSE may have dropped the hints.
 * <p>
 * The responses are built byte by byte in DSE's layout at version 11 (the Cassandra 3.0 header), read with the pre-4.0
 * codec ({@code Message.Serializer.deserialize} at version 11; the header-first path of the inbound handler reads the
 * same layout in {@code V11SerializationAuditTest.hintAcknowledgementAtVersion11}) and handed to the inbound sink,
 * which runs the response verb handler. The hints go through the real dispatch executor, at messaging version 11
 * because that is the version recorded for the target.
 */
public class DseHintsAcknowledgementTest
{
    private static final String KEYSPACE = "dse_hints_ack_test";
    private static final String TABLE = "table";
    private static final int HINTS = 20;

    /** The Cassandra 3.0 protocol magic that opens every message at version 11. */
    private static final int PROTOCOL_MAGIC = 0xCA552DFA;
    /** DSE's response verbs at the OSS dialect: REQUEST_RESPONSE and INTERNAL_RESPONSE. */
    private static final int DSE_REQUEST_RESPONSE = 4;
    private static final int DSE_INTERNAL_RESPONSE = 23;
    /** The id DSE maps {@code HINTS.HINT} to at the OSS dialect (an acked request). */
    private static final int DSE_HINT_VERB = 1;
    /** The WARN about a page a DSE 6 node did not acknowledge in full (HintsDispatcher): node, count, file, node. */
    private static final String WARNING = "DSE 6 compatibility mode: hints not acknowledged by DSE 6 node %s: %d in file %s; the " +
                                          "file stays and is sent again from that page. The node may be down or overloaded, it " +
                                          "may have answered with a failure, the hints may not have been sent (see the messaging " +
                                          "log), or it dropped them: DSE 6 does not acknowledge a hint it drops (unknown table, " +
                                          "invalid data, range no longer owned if configured to reject such hints), and such a " +
                                          "hint holds back this file and the later ones for that node until it expires (the " +
                                          "smallest gc_grace_seconds of its tables, capped by cassandra.maxHintTTL) or the node is " +
                                          "upgraded. If this repeats for the same file, nodetool truncatehints %s deletes all the " +
                                          "pending hints for that node, and the repair after the compatibility mode is dropped " +
                                          "brings their writes to it.";

    /**
     * How far the clock of the rate-limited WARN (once every ten minutes per node) is ahead: twenty more minutes for
     * each test, so that every test can see it.
     */
    private static final AtomicLong noSpamClockShift = new AtomicLong();

    private static TableMetadata metadata;
    private static InternodeCompatibilityMode modeBefore;
    private static DurationSpec.LongMillisecondsBound hintsTimeoutBefore;
    private static Integer targetVersionBefore;
    private static InetAddressAndPort target;
    private static UUID targetHostId;

    /** Checks made on the responder threads, rethrown by the test. */
    private final List<Throwable> responderErrors = new CopyOnWriteArrayList<>();
    private final Logger dispatcherLogger = (Logger) LoggerFactory.getLogger(HintsDispatcher.class);
    private final ListAppender<ILoggingEvent> warnings = new ListAppender<>();

    @BeforeClass
    public static void defineSchema()
    {
        SchemaLoader.prepareServer();
        StorageService.instance.initServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), SchemaLoader.standardCFMD(KEYSPACE, TABLE));
        metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        // the hints are addressed to this node itself: the mock messaging service intercepts them before they leave
        targetHostId = StorageService.instance.getLocalHostUUID();
        target = HintsEndpointProvider.instance.endpointForHost(targetHostId);
        targetVersionBefore = MessagingService.instance().versions.knows(target) ? MessagingService.instance().versions.getRaw(target) : null;
        hintsTimeoutBefore = DatabaseDescriptor.getRawConfig().hints_request_timeout;
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
    }

    @AfterClass
    public static void restore() throws Exception
    {
        HintsService.instance.shutdownBlocking();
        HintsService.instance.deleteAllHints();
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
        DatabaseDescriptor.getRawConfig().hints_request_timeout = hintsTimeoutBefore;
        if (targetVersionBefore == null)
            MessagingService.instance().versions.reset(target);
        else
            MessagingService.instance().versions.set(target, targetVersionBefore);
        NoSpamLogger.unsafeSetClock(Clock.Global::nanoTime);
    }

    @Before
    public void newHintsService() throws Exception
    {
        MessagingService.instance().inboundSink.clear();
        MessagingService.instance().outboundSink.clear();
        // the target speaks the pre-4.0 dialect: the executor dispatches to it at version 11
        MessagingService.instance().versions.set(target, VERSION_3014);
        // answered hints must not time out on a slow machine; the tests that expect timeouts shorten it
        hintsTimeout("20s");

        if (!HintsService.instance.isShutDown())
        {
            HintsService.instance.shutdownBlocking();
            HintsService.instance.deleteAllHints();
        }
        HintsService.instance = new HintsService(hostId -> true);
        // dispatch is started by hand, one run at a time, so that each run can be awaited
        HintsService.instance.resumeDispatch();

        noSpamClockShift.addAndGet(TimeUnit.MINUTES.toNanos(20));
        NoSpamLogger.unsafeSetClock(() -> Clock.Global.nanoTime() + noSpamClockShift.get());
        warnings.start();
        dispatcherLogger.addAppender(warnings);
    }

    @After
    public void cleanup()
    {
        dispatcherLogger.detachAppender(warnings);
        warnings.stop();
        MockMessagingService.cleanup();
        responderErrors.clear();
    }

    /** DSE acknowledges every hint: the hints count as delivered and the file is removed. */
    @Test
    public void acknowledgedHintsAreDeliveredAndTheFileRemoved() throws Throwable
    {
        HintsStore store = writeAndFlushHints(metadata, targetHostId, HINTS);
        assertTrue(store.hasFiles());
        long succeeded = HintsServiceMetrics.hintsSucceeded.getCount();
        long notDelivered = notDelivered();

        AtomicInteger acknowledged = new AtomicInteger();
        MockMessagingSpy spy = respondLikeDse(acknowledged, DseHintsAcknowledgementTest::dseAcknowledgement);
        dispatchAndWait(store);

        assertEquals(HINTS, spy.messagesIntercepted());
        awaitAnswers(acknowledged);
        assertEquals(succeeded + HINTS, HintsServiceMetrics.hintsSucceeded.getCount());
        assertEquals(notDelivered, notDelivered());
        assertFalse("the file is removed once every hint in it was acknowledged", store.hasFiles());
        assertEquals(0, hintsFilesOnDisk().size());
        assertEquals(0, dse6Warnings().size());
    }

    /**
     * DSE does not answer the hints (it dropped them, or the answer was lost): they time out, count as not delivered
     * and the file stays; the next dispatch sends them again and removes the file only once DSE acknowledges them.
     */
    @Test
    public void unacknowledgedHintsStayInTheFileAndAreSentAgain() throws Throwable
    {
        hintsTimeout("2s");
        HintsStore store = writeAndFlushHints(metadata, targetHostId, HINTS);
        String file = onlyHintsFile();
        long succeeded = HintsServiceMetrics.hintsSucceeded.getCount();
        long notDelivered = notDelivered();

        MockMessagingSpy silent = MockMessagingService.when(verb(HINT_REQ)).dontReply();
        dispatchAndWait(store);

        assertEquals(HINTS, silent.messagesIntercepted());
        assertEquals(succeeded, HintsServiceMetrics.hintsSucceeded.getCount());
        assertEquals(notDelivered + HINTS, notDelivered());
        assertTrue("a hint without an acknowledgement stays in its file", store.hasFiles());
        assertEquals(1, hintsFilesOnDisk().size());
        assertEquals(Collections.singletonList(warning(HINTS, file)), dse6Warnings());

        // a second failed dispatch within ten minutes does not repeat the WARN; one after ten minutes does
        dispatchAndWait(store);
        assertEquals(2 * HINTS, silent.messagesIntercepted());
        assertEquals(notDelivered + 2 * HINTS, notDelivered());
        assertEquals(1, dse6Warnings().size());
        noSpamClockShift.addAndGet(TimeUnit.MINUTES.toNanos(11));
        dispatchAndWait(store);
        assertEquals(3 * HINTS, silent.messagesIntercepted());
        assertEquals(Arrays.asList(warning(HINTS, file), warning(HINTS, file)), dse6Warnings());
        assertEquals(1, hintsFilesOnDisk().size());

        // the retry sends the same hints again, and this time DSE acknowledges them
        hintsTimeout("20s");
        MockMessagingService.cleanup();
        AtomicInteger acknowledged = new AtomicInteger();
        MockMessagingSpy spy = respondLikeDse(acknowledged, DseHintsAcknowledgementTest::dseAcknowledgement);
        dispatchAndWait(store);

        assertEquals(HINTS, spy.messagesIntercepted());
        awaitAnswers(acknowledged);
        assertEquals(succeeded + HINTS, HintsServiceMetrics.hintsSucceeded.getCount());
        assertFalse(store.hasFiles());
        assertEquals(0, hintsFilesOnDisk().size());
        assertEquals("no new warning", 2, dse6Warnings().size());
    }

    /**
     * DSE answers with a failure response in INTERNAL_RESPONSE (the hint asked for one): the hints count as failed and
     * the file stays.
     */
    @Test
    public void failureResponsesKeepTheFile() throws Throwable
    {
        failureResponsesKeepTheFile(DSE_INTERNAL_RESPONSE);
    }

    /** The same failure response in REQUEST_RESPONSE, the other verb DSE answers a failure with. */
    @Test
    public void failureResponsesInRequestResponseKeepTheFile() throws Throwable
    {
        failureResponsesKeepTheFile(DSE_REQUEST_RESPONSE);
    }

    private void failureResponsesKeepTheFile(int verbId) throws Throwable
    {
        HintsStore store = writeAndFlushHints(metadata, targetHostId, HINTS);
        String file = onlyHintsFile();
        long succeeded = HintsServiceMetrics.hintsSucceeded.getCount();
        long failed = HintsServiceMetrics.hintsFailed.getCount();

        AtomicInteger answered = new AtomicInteger();
        MockMessagingSpy spy = respondLikeDse(answered, dseFailureResponse(verbId));
        dispatchAndWait(store);

        assertEquals(HINTS, spy.messagesIntercepted());
        awaitAnswers(answered);
        assertEquals(succeeded, HintsServiceMetrics.hintsSucceeded.getCount());
        assertEquals(failed + HINTS, HintsServiceMetrics.hintsFailed.getCount());
        assertTrue("a hint answered with a failure stays in its file", store.hasFiles());
        assertEquals(1, hintsFilesOnDisk().size());
        assertEquals(Collections.singletonList(warning(HINTS, file)), dse6Warnings());
    }

    /**
     * DSE acknowledges half of the page: the WARN counts the other half, the file stays, and the next dispatch sends
     * the whole page again.
     */
    @Test
    public void partlyAcknowledgedPageIsSentAgainWhole() throws Throwable
    {
        hintsTimeout("5s");
        HintsStore store = writeAndFlushHints(metadata, targetHostId, HINTS);
        String file = onlyHintsFile();
        long succeeded = HintsServiceMetrics.hintsSucceeded.getCount();
        long notDelivered = notDelivered();

        AtomicInteger seen = new AtomicInteger();
        AtomicInteger answered = new AtomicInteger();
        MockMessagingSpy spy = MockMessagingService.when(verb(HINT_REQ)).respond((request, to) -> {
            try
            {
                // every other hint is acknowledged
                if (seen.getAndIncrement() % 2 == 0)
                {
                    long createdAtMillis = approxTime.translate().toMillisSinceEpoch(request.createdAtNanos());
                    deliver(dseAcknowledgement((int) request.id(), createdAtMillis, to), to);
                }
            }
            catch (Throwable t)
            {
                responderErrors.add(t);
            }
            finally
            {
                answered.incrementAndGet();
            }
            return null;
        });
        dispatchAndWait(store);

        assertEquals(HINTS, spy.messagesIntercepted());
        awaitAnswers(answered);
        assertEquals(succeeded + HINTS / 2, HintsServiceMetrics.hintsSucceeded.getCount());
        assertEquals(notDelivered + HINTS / 2, notDelivered());
        assertEquals(1, hintsFilesOnDisk().size());
        assertEquals(Collections.singletonList(warning(HINTS / 2, file)), dse6Warnings());

        hintsTimeout("20s");
        MockMessagingService.cleanup();
        AtomicInteger acknowledged = new AtomicInteger();
        MockMessagingSpy again = respondLikeDse(acknowledged, DseHintsAcknowledgementTest::dseAcknowledgement);
        dispatchAndWait(store);

        assertEquals(HINTS, again.messagesIntercepted());
        awaitAnswers(acknowledged);
        assertEquals("the whole page was sent again", succeeded + HINTS / 2 + HINTS, HintsServiceMetrics.hintsSucceeded.getCount());
        assertEquals(0, hintsFilesOnDisk().size());
    }

    /**
     * Acknowledgements that match no waiting hint deliver nothing and break nothing: one with an id nobody waits for,
     * one with the right id but from another node, and the right ones arriving once the callbacks of the page expired.
     * The next dispatch sends the whole page again.
     */
    @Test
    public void unmatchedAndLateAcknowledgementsDeliverNothing() throws Throwable
    {
        hintsTimeout("5s");
        HintsStore store = writeAndFlushHints(metadata, targetHostId, HINTS);
        long succeeded = HintsServiceMetrics.hintsSucceeded.getCount();
        long notDelivered = notDelivered();
        InetAddressAndPort otherNode = InetAddressAndPort.getByName("127.0.0.99");
        List<long[]> requests = new CopyOnWriteArrayList<>();

        AtomicInteger answered = new AtomicInteger();
        MockMessagingSpy spy = MockMessagingService.when(verb(HINT_REQ)).respond((request, to) -> {
            try
            {
                long createdAtMillis = approxTime.translate().toMillisSinceEpoch(request.createdAtNanos());
                requests.add(new long[]{ request.id(), createdAtMillis });
                // the unknown id from the target and the hint's id from another node must complete no callback: not
                // the hint's, nor the ones registered for the unknown id from the other node and for the hint's id from
                // a third node
                int unknownId = (int) request.id() + 1_000_000;
                InetAddressAndPort thirdNode = InetAddressAndPort.getByName("127.0.0.98");
                AtomicInteger sentinelRuns = new AtomicInteger();
                Message<NoPayload> unknownIdOtherNode = Message.builder(HINT_REQ, NoPayload.noPayload).withId(unknownId).build();
                Message<NoPayload> sameIdThirdNode = Message.builder(HINT_REQ, NoPayload.noPayload).withId(request.id()).build();
                MessagingService.instance().callbacks.addWithExpiration(message -> sentinelRuns.incrementAndGet(), unknownIdOtherNode, otherNode);
                MessagingService.instance().callbacks.addWithExpiration(message -> sentinelRuns.incrementAndGet(), sameIdThirdNode, thirdNode);
                try
                {
                    handle(read(dseAcknowledgement(unknownId, createdAtMillis, to), to));
                    handle(read(dseAcknowledgement((int) request.id(), createdAtMillis, otherNode), otherNode));
                    assertEquals("no other callback ran", 0, sentinelRuns.get());
                    assertNotNull(MessagingService.instance().callbacks.get(unknownId, otherNode));
                    assertNotNull(MessagingService.instance().callbacks.get(request.id(), thirdNode));
                    assertNotNull("the hint still waits for its acknowledgement", MessagingService.instance().callbacks.get(request.id(), to));
                }
                finally
                {
                    MessagingService.instance().callbacks.remove(unknownId, otherNode);
                    MessagingService.instance().callbacks.remove(request.id(), thirdNode);
                }
            }
            catch (Throwable t)
            {
                responderErrors.add(t);
            }
            finally
            {
                answered.incrementAndGet();
            }
            return null;
        });
        dispatchAndWait(store);

        assertEquals(HINTS, spy.messagesIntercepted());
        awaitAnswers(answered);
        assertEquals(succeeded, HintsServiceMetrics.hintsSucceeded.getCount());
        assertEquals(notDelivered + HINTS, notDelivered());
        assertEquals(1, hintsFilesOnDisk().size());

        // the right acknowledgements, once no callback waits for them any more: read, then dropped by the response
        // verb handler without touching the callbacks that are registered, for the same ids from another node and for
        // another id from the target
        assertEquals(HINTS, requests.size());
        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            for (long[] request : requests)
                assertNull(MessagingService.instance().callbacks.get(request[0], target));
        });
        hintsTimeout("20s");
        AtomicInteger sentinelRuns = new AtomicInteger();
        for (long[] request : requests)
        {
            Message<NoPayload> sameIdOtherNode = Message.builder(HINT_REQ, NoPayload.noPayload).withId(request[0]).build();
            MessagingService.instance().callbacks.addWithExpiration(message -> sentinelRuns.incrementAndGet(), sameIdOtherNode, otherNode);
        }
        Message<NoPayload> otherId = Message.out(HINT_REQ, NoPayload.noPayload);
        MessagingService.instance().callbacks.addWithExpiration(message -> sentinelRuns.incrementAndGet(), otherId, target);
        try
        {
            for (long[] request : requests)
            {
                Message<?> late = read(dseAcknowledgement((int) request[0], request[1], target), target);
                assertSame(Verb.REQUEST_RSP, late.verb());
                assertNull(late.payload);
                handle(late);
            }
            assertEquals("no other callback ran", 0, sentinelRuns.get());
            for (long[] request : requests)
                assertNotNull(MessagingService.instance().callbacks.get(request[0], otherNode));
            assertNotNull(MessagingService.instance().callbacks.get(otherId.id(), target));
        }
        finally
        {
            for (long[] request : requests)
                MessagingService.instance().callbacks.remove(request[0], otherNode);
            MessagingService.instance().callbacks.remove(otherId.id(), target);
        }
        assertEquals(succeeded, HintsServiceMetrics.hintsSucceeded.getCount());
        assertEquals(1, hintsFilesOnDisk().size());

        // the next dispatch sends all the hints of the page again, and DSE acknowledges them this time
        MockMessagingService.cleanup();
        AtomicInteger acknowledged = new AtomicInteger();
        MockMessagingSpy again = respondLikeDse(acknowledged, DseHintsAcknowledgementTest::dseAcknowledgement);
        dispatchAndWait(store);

        assertEquals(HINTS, again.messagesIntercepted());
        awaitAnswers(acknowledged);
        assertEquals(succeeded + HINTS, HintsServiceMetrics.hintsSucceeded.getCount());
        assertEquals(0, hintsFilesOnDisk().size());
    }

    /** No WARN with the mode off: a peer at a pre-4.0 version is not a DSE 6 node then. */
    @Test
    public void noDse6WarningWithTheModeOff() throws Throwable
    {
        hintsTimeout("2s");
        HintsStore store = writeAndFlushHints(metadata, targetHostId, HINTS);
        long notDelivered = notDelivered();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        try
        {
            MockMessagingSpy silent = MockMessagingService.when(verb(HINT_REQ)).dontReply();
            dispatchAndWait(store);

            assertEquals(HINTS, silent.messagesIntercepted());
            assertEquals(notDelivered + HINTS, notDelivered());
            assertEquals(1, hintsFilesOnDisk().size());
            assertEquals(0, dse6Warnings().size());
        }
        finally
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        }
    }

    /** The WARN is about DSE 6 nodes only: a failure from a peer at 4.0 or later keeps the file as before, silently. */
    @Test
    public void noDse6WarningForAPeerAt40OrLater() throws Throwable
    {
        MessagingService.instance().versions.set(target, MessagingService.current_version);
        HintsStore store = writeAndFlushHints(metadata, targetHostId, HINTS);
        long failed = HintsServiceMetrics.hintsFailed.getCount();

        AtomicInteger answered = new AtomicInteger();
        MockMessagingSpy spy = MockMessagingService.when(verb(HINT_REQ)).respond((request, to) -> {
            try
            {
                MessagingService.instance().inboundSink.accept(request.failureResponse(RequestFailureReason.UNKNOWN));
            }
            catch (Throwable t)
            {
                responderErrors.add(t);
            }
            finally
            {
                answered.incrementAndGet();
            }
            return null;
        });
        dispatchAndWait(store);

        assertEquals(HINTS, spy.messagesIntercepted());
        awaitAnswers(answered);
        assertEquals(failed + HINTS, HintsServiceMetrics.hintsFailed.getCount());
        assertEquals(1, hintsFilesOnDisk().size());
        assertEquals(0, dse6Warnings().size());
    }

    private interface DseResponse
    {
        byte[] bytes(int id, long createdAtMillis, InetAddressAndPort from) throws IOException;
    }

    /**
     * Answers each hint as a DSE node does: checks the hint as it would travel at version 11 (DSE's HINT verb id, the
     * failure-callback flag DSE's own dispatcher sets too), checks that a callback waits for its id, then reads the DSE
     * response bytes through the pre-4.0 codec and hands the message to the inbound sink. {@code answered} counts the
     * hints handled, whether or not their checks passed.
     */
    private MockMessagingSpy respondLikeDse(AtomicInteger answered, DseResponse dse)
    {
        return MockMessagingService.when(verb(HINT_REQ)).respond((request, to) -> {
            try
            {
                assertHintAtVersion11(request);
                assertNotNull("a callback waits for the hint's acknowledgement", MessagingService.instance().callbacks.get(request.id(), to));
                long createdAtMillis = approxTime.translate().toMillisSinceEpoch(request.createdAtNanos());
                Message<?> response = deliver(dse.bytes((int) request.id(), createdAtMillis, to), to);
                assertEquals(request.id(), response.id());
                assertNull("the response consumed the hint's callback", MessagingService.instance().callbacks.get(request.id(), to));
            }
            catch (Throwable t)
            {
                responderErrors.add(t);
            }
            finally
            {
                answered.incrementAndGet();
            }
            return null;
        });
    }

    /** Reads a message sent by {@code from} at version 11 and hands it to the inbound sink, as a connection does. */
    private static Message<?> deliver(byte[] bytes, InetAddressAndPort from) throws IOException
    {
        Message<?> message = read(bytes, from);
        MessagingService.instance().inboundSink.accept(message);
        return message;
    }

    /** Reads a message sent by {@code from} at version 11. */
    private static Message<?> read(byte[] bytes, InetAddressAndPort from) throws IOException
    {
        try (DataInputBuffer in = new DataInputBuffer(bytes))
        {
            Message<?> message = Message.serializer.deserialize(in, from, VERSION_3014);
            assertEquals(0, in.available());
            return message;
        }
    }

    /** Runs the handler of a message's verb directly, so that an exception reaches the test. */
    @SuppressWarnings("unchecked")
    private static void handle(Message<?> message) throws IOException
    {
        message.verb().<Object>handler().doVerb((Message<Object>) message);
    }

    /** DSE's acknowledgement of an accepted hint: REQUEST_RESPONSE, no params, an empty payload. */
    private static byte[] dseAcknowledgement(int id, long createdAtMillis, InetAddressAndPort from) throws IOException
    {
        byte[] bytes = dseResponse(id, createdAtMillis, from, DSE_REQUEST_RESPONSE, 0, out -> {});
        try (DataInputBuffer in = new DataInputBuffer(bytes))
        {
            Message<?> message = Message.serializer.deserialize(in, from, VERSION_3014);
            assertSame(Verb.REQUEST_RSP, message.verb());
            assertNull("an empty payload", message.payload);
        }
        return bytes;
    }

    /**
     * A DSE failure response: the FAIL flag and a FAIL_REASON code, an empty payload, with the verb id given (DSE
     * answers a failure with INTERNAL_RESPONSE, or with REQUEST_RESPONSE for the verbs outside SCHEMA, REPAIR and
     * OPERATIONS; the codec recognises the failure from the FAIL param alone).
     */
    private static DseResponse dseFailureResponse(int verbId)
    {
        return (id, createdAtMillis, from) -> dseFailureResponse(id, createdAtMillis, from, verbId);
    }

    private static byte[] dseFailureResponse(int id, long createdAtMillis, InetAddressAndPort from, int verbId) throws IOException
    {
        byte[] bytes = dseResponse(id, createdAtMillis, from, verbId, 2, out -> {
            out.writeUTF("FAIL");
            out.writeInt(1);
            out.writeByte(0);
            out.writeUTF("FAIL_REASON");
            out.writeInt(2);
            out.writeShort(0x0000); // UNKNOWN
        });
        try (DataInputBuffer in = new DataInputBuffer(bytes))
        {
            Message<?> message = Message.serializer.deserialize(in, from, VERSION_3014);
            assertSame(Verb.FAILURE_RSP, message.verb());
            assertSame(RequestFailureReason.UNKNOWN, message.payload);
        }
        return bytes;
    }

    private interface ParamsWriter
    {
        void write(DataOutputBuffer out) throws IOException;
    }

    /**
     * A message in the Cassandra 3.0 layout DSE uses at version 11: protocol magic, int id, int creation time (DSE
     * echoes the request's), the sender's address with a 1-byte length and no port, int verb id, int param count, the
     * params, int payload size (0) and no payload.
     */
    private static byte[] dseResponse(int id, long createdAtMillis, InetAddressAndPort from, int verbId, int paramCount, ParamsWriter params) throws IOException
    {
        byte[] address = from.getAddress().getAddress();
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.writeInt(PROTOCOL_MAGIC);
            out.writeInt(id);
            out.writeInt((int) createdAtMillis);
            out.writeByte(address.length);
            out.write(address);
            out.writeInt(verbId);
            out.writeInt(paramCount);
            params.write(out);
            out.writeInt(0);
            return out.toByteArray();
        }
    }

    /**
     * The hint as it leaves for a DSE node: DSE's HINT verb id, its message id, the failure-callback flag as its only
     * param (a 1-byte value, 0), then the payload with its size.
     */
    private static void assertHintAtVersion11(Message<?> request) throws IOException
    {
        byte[] bytes;
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            Message.serializer.serialize(request, out, VERSION_3014);
            bytes = out.toByteArray();
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        assertEquals(PROTOCOL_MAGIC, buffer.getInt());
        assertEquals(request.id(), buffer.getInt());
        buffer.getInt(); // creation time
        int addressLength = buffer.get();
        buffer.position(buffer.position() + addressLength); // sender address
        assertEquals("DSE's HINT verb id", DSE_HINT_VERB, buffer.getInt());
        assertEquals("one param", 1, buffer.getInt());
        try (DataInputBuffer in = new DataInputBuffer(buffer, false))
        {
            assertEquals("the failure-callback flag", "CAL_BAC", in.readUTF());
            assertEquals("a 1-byte flag value", 1, in.readInt());
            assertEquals(0, in.readByte());
            int payloadSize = in.readInt();
            assertTrue(payloadSize > 0);
            assertEquals("the payload ends the message", payloadSize, in.available());
        }
    }

    /** Waits until the responder threads handled every hint, then rethrows the first failed check. */
    private void awaitAnswers(AtomicInteger answered) throws Throwable
    {
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> assertEquals(HINTS, answered.get()));
        if (!responderErrors.isEmpty())
            throw responderErrors.get(0);
    }

    /**
     * The hints that timed out or failed. A hint nobody answers counts as timed out when the dispatcher stops waiting,
     * or as failed if the expiry of its callback, which reports a timeout failure, comes first.
     */
    private static long notDelivered()
    {
        return HintsServiceMetrics.hintsTimedOut.getCount() + HintsServiceMetrics.hintsFailed.getCount();
    }

    private static void hintsTimeout(String duration)
    {
        DatabaseDescriptor.getRawConfig().hints_request_timeout = new DurationSpec.LongMillisecondsBound(duration);
    }

    private static void dispatchAndWait(HintsStore store) throws Exception
    {
        HintsService.instance.dispatcherExecutor().dispatch(store).get(60, TimeUnit.SECONDS);
    }

    /** The names of the hints files of the target on disk. */
    private static List<String> hintsFilesOnDisk()
    {
        String prefix = targetHostId + "-";
        File[] files = DatabaseDescriptor.getHintsDirectory().tryList(f -> f.name().startsWith(prefix) && HintsDescriptor.isHintFileName(f.toPath()));
        return Arrays.stream(files).map(File::name).collect(Collectors.toList());
    }

    private static String onlyHintsFile()
    {
        List<String> files = hintsFilesOnDisk();
        assertEquals(files.toString(), 1, files.size());
        return files.get(0);
    }

    private static String warning(int unacknowledged, String file)
    {
        return String.format(WARNING, target, unacknowledged, file, target.getHostAddress(false));
    }

    /** The DSE 6 WARNs the dispatcher logged during the test. */
    private List<String> dse6Warnings()
    {
        return warnings.list.stream()
                            .filter(e -> e.getLevel() == Level.WARN && e.getFormattedMessage().startsWith("DSE 6 compatibility mode: "))
                            .map(ILoggingEvent::getFormattedMessage)
                            .collect(Collectors.toList());
    }
}
