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

package org.apache.cassandra.gms;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.dsecompat.DseFixtures.DseVersion;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.DsePeers;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.RequestCallbacks;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.GOSSIP_DISABLE_THREAD_VALIDATION;
import static org.apache.cassandra.net.MessagingService.VERSION_3014;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The gossiper marks a peer up when it answers an echo request, and sends one request per endpoint state of the peer. In
 * DSE 6 compatibility mode, a request a peer leaves unanswered is sent again once its timeout has passed, and one that
 * failed once a gossip interval has passed, the next time gossip brings the peer's state ({@link DseEchoRetries}),
 * whether the peer is a DSE 6 node or not (an HCD node); every peer outside the mode still gets one
 * request per state.
 * <p>
 * The echo requests are captured and dropped before they reach the network; an answer or a failure response is
 * delivered to the callback the gossiper registered, removed first as the response handler does. The requests are timed
 * with a clock the test moves; the callbacks are registered with the real clock and the request timeout of 10 s, which
 * no test comes near, so none of them expires on its own. The storage service is not running: the peer is identified
 * as the storage service does when it reads the NET_VERSION of the peer's state ({@link DsePeers#fromGossipedNetVersion}).
 */
public class DseEchoRetryTest
{
    private static final List<Token> TOKENS = Collections.singletonList(Murmur3Partitioner.instance.getToken(ByteBufferUtil.bytes("k")));
    private static final long REQUEST_TIMEOUT_MILLIS = 10_000;
    /** What DSE 6.8 gossips as its NET_VERSION: its relative messaging version shifted by 8 bits. */
    private static final int DSE_6_8_NET_VERSION = DseVersion.DSE_6_8_65.relativeMessagingVersion << 8;
    /** What the debug line of a retry says of a request answered with a failure response (UNKNOWN). */
    private static final String FAILED = "answered with a failure (UNKNOWN)";
    /** What the debug line of a retry says of a request whose callback learnt of a TIMEOUT. */
    private static final String DROPPED = "did not answer (the request expired, its answer came after the request expired, " +
                                          "or this node dropped the request)";

    private static InetAddressAndPort peer;

    private final VersionedValue.VersionedValueFactory factory = new VersionedValue.VersionedValueFactory(Murmur3Partitioner.instance);
    /** The echo requests sent to the peer, each with the callback registered for it, in order. */
    private final List<Sent> echoes = new CopyOnWriteArrayList<>();
    private final BiPredicate<Message<?>, InetAddressAndPort> captureToPeer = this::capture;
    private final Logger retriesLogger = (Logger) LoggerFactory.getLogger(DseEchoRetries.class);
    private final ListAppender<ILoggingEvent> logged = new ListAppender<>();
    private final AtomicLong clockNanos = new AtomicLong(TimeUnit.DAYS.toNanos(1));

    private InternodeCompatibilityMode modeBefore;
    private long requestTimeoutBefore;
    private Level levelBefore;
    private int generation;
    private int heartbeat;

    private static final class Sent
    {
        final Message<?> message;
        final RequestCallbacks.CallbackInfo callback;

        Sent(Message<?> message, RequestCallbacks.CallbackInfo callback)
        {
            this.message = message;
            this.callback = callback;
        }
    }

    @BeforeClass
    public static void startup() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
        peer = InetAddressAndPort.getByName("127.0.0.221");
        // the gossiper checks that state changes run on the gossip stage; the property is read when the class initialises
        try (WithProperties properties = new WithProperties().set(GOSSIP_DISABLE_THREAD_VALIDATION, true))
        {
            Gossiper.instance.getClass();
        }
    }

    @Before
    public void setUp()
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        requestTimeoutBefore = DatabaseDescriptor.getRpcTimeout(TimeUnit.MILLISECONDS);
        DatabaseDescriptor.setRpcTimeout(REQUEST_TIMEOUT_MILLIS);
        DsePeers.reset();
        Gossiper.instance.unsafeAnnulEndpoint(peer);
        Gossiper.instance.dseEchoRetries.setClock(clockNanos::get);
        MessagingService.instance().outboundSink.add(captureToPeer);
        levelBefore = retriesLogger.getLevel();
        retriesLogger.setLevel(Level.DEBUG);
        logged.start();
        retriesLogger.addAppender(logged);
        generation = (int) FBUtilities.now().getEpochSecond();
        heartbeat = 1;
    }

    @After
    public void tearDown()
    {
        retriesLogger.detachAppender(logged);
        logged.stop();
        retriesLogger.setLevel(levelBefore);
        MessagingService.instance().outboundSink.remove(captureToPeer);
        for (Sent echo : echoes)
            MessagingService.instance().callbacks.remove(echo.message.id(), peer);
        Gossiper.instance.dseEchoRetries.setClock(Clock.Global::nanoTime);
        Gossiper.instance.unsafeAnnulEndpoint(peer);
        DsePeers.reset();
        DatabaseDescriptor.setRpcTimeout(requestTimeoutBefore);
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
    }

    /** Captures and drops every message to the peer; the callback of a request is registered before it is sent. */
    private boolean capture(Message<?> message, InetAddressAndPort to)
    {
        if (!peer.equals(to))
            return true;
        if (message.verb() == Verb.ECHO_REQ)
            echoes.add(new Sent(message, MessagingService.instance().callbacks.get(message.id(), to)));
        return false;
    }

    private static void mode(InternodeCompatibilityMode mode)
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(mode);
    }

    /** Gossip brings the peer's state with the given status, the current generation and a new heartbeat. */
    private void gossip(VersionedValue status)
    {
        EndpointState state = new EndpointState(new HeartBeatState(generation, heartbeat++));
        state.addApplicationState(ApplicationState.STATUS_WITH_PORT, status);
        Gossiper.instance.applyStateLocally(ImmutableMap.of(peer, state));
    }

    private void gossipNormal()
    {
        gossip(factory.normal(TOKENS));
    }

    /**
     * The peer restarts: a new generation, first gossiped with a dead state (hibernating here), as observed with DSE 6.8.65,
     * where this node logged "Not marking ... alive due to dead state" for the first state of the restarted DSE node.
     */
    private void restartedInADeadState()
    {
        generation++;
        gossip(factory.hibernate(true));
    }

    private void advanceMillis(long millis)
    {
        clockNanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(millis));
    }

    /** Moves the clock just short of the request timeout: the last request is not due. */
    private void justBeforeTheRequestTimeout()
    {
        advanceMillis(REQUEST_TIMEOUT_MILLIS - 1);
    }

    /** Moves the clock past the request timeout of the last request. */
    private void afterTheRequestTimeout()
    {
        advanceMillis(REQUEST_TIMEOUT_MILLIS + 1);
    }

    private void afterAGossipInterval()
    {
        advanceMillis(Gossiper.intervalInMillis + 1);
    }

    /** The peer answers the request: the response handler removes its callback and passes the answer on. */
    private static void answer(Sent echo)
    {
        assertThat(echo.callback).as("callback of the echo request").isNotNull();
        MessagingService.instance().callbacks.remove(echo.message.id(), peer);
        echo.callback.callback.onResponse(echo.message.emptyResponse());
    }

    /**
     * The peer refuses the request, as DSE does while it is not ready to answer echo requests: the response handler
     * removes its callback and passes the failure on.
     */
    private static void refuse(Sent echo)
    {
        assertThat(echo.callback).as("callback of the echo request").isNotNull();
        MessagingService.instance().callbacks.remove(echo.message.id(), peer);
        echo.callback.callback.onFailure(peer, RequestFailureReason.UNKNOWN);
    }

    /**
     * True when the echo request asks for a failure response: its callback handles failures and the message carries the
     * failure-callback flag (bit 0 of the flags, {@code MessageFlag.CALL_BACK_ON_FAILURE}) and, in DSE 6 compatibility
     * mode, serialized at messaging version 11, the failure-callback param ({@code CAL_BAC} as a flag param: the UTF-8 key,
     * an int length 1 and the byte 0); false when none of them is the case.
     */
    private static boolean asksForAFailureResponse(Sent echo) throws Exception
    {
        boolean invokeOnFailure = echo.callback.callback.invokeOnFailure();
        assertThat((echo.message.header.flags() & 1) != 0).as("the flag of the message matches the callback")
                                                          .isEqualTo(invokeOnFailure);
        if (!DatabaseDescriptor.isDse6CompatibilityMode())
            return invokeOnFailure; // the pre-4.0 layout exists only in the mode
        byte[] param;
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.writeUTF("CAL_BAC");
            out.writeInt(1);
            out.writeByte(0);
            param = out.toByteArray();
        }
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            Message.serializer.serialize(echo.message, out, VERSION_3014);
            assertThat(Bytes.indexOf(out.toByteArray(), param) >= 0).as("the param on the wire matches the callback")
                                                                    .isEqualTo(invokeOnFailure);
        }
        return invokeOnFailure;
    }

    /** The peer is identified as a DSE node by the NET_VERSION of its state, as the storage service does. */
    private static void identifiedByItsNetVersion()
    {
        DsePeers.fromGossipedNetVersion(peer, DSE_6_8_NET_VERSION);
        assertThat(DsePeers.isDse(peer)).isTrue();
    }

    private static boolean isUp()
    {
        return Gossiper.instance.isAlive(peer);
    }

    /** The attempt number of the last request recorded for the peer, 0 when none is. */
    private static int recordedAttempt()
    {
        return Gossiper.instance.dseEchoRetries.recordedAttempt(peer);
    }

    private List<String> lines(Level level)
    {
        return logged.list.stream()
                          .filter(event -> event.getLevel() == level)
                          .map(ILoggingEvent::getFormattedMessage)
                          .collect(Collectors.toList());
    }

    private static String retryLine(String what, long millisAgo, int attempt)
    {
        return "DSE 6 compatibility mode: peer " + peer + ' ' + what + " the echo request sent " + millisAgo +
               " ms ago; sending it again (attempt " + attempt + "), since it stays down until one is answered";
    }

    private static String answeredLine(int attempt)
    {
        return "DSE 6 compatibility mode: peer " + peer + " answered the echo request sent again (attempt " + attempt +
               "); marking it up";
    }

    /**
     * The peer gossips the NET_VERSION this software gossips in the mode, 12, as an HCD node does. {@link DsePeers} keeps
     * no record of a peer that is not a DSE node, so this only clears an identification or a presumption, of which the
     * tests calling it have none: the HCD tests document the intent, they do not take another code path than a peer of
     * which nothing is known.
     */
    private static void anHcdNodeByItsNetVersion()
    {
        DsePeers.fromGossipedNetVersion(peer, MessagingService.VERSION_40);
        assertThat(DsePeers.isDse(peer)).isFalse();
    }

    /**
     * The sequence observed with DSE 6.8.65: a DSE node restarts, gossips a dead state under the new generation ("Not marking alive due to dead
     * state"), then NORMAL under the same generation; the echo request sent then gets no answer. It is sent again once the
     * request timeout has passed, not before, and the answer marks the node up, with one info line; no further request
     * follows, and the next state counts its attempts from 1 again. Every request asks for a failure response, the first
     * one too, sent before the node is identified.
     */
    @Test
    public void testUnansweredEchoToDseNodeIsSentAgainAndMarksItUp() throws Exception
    {
        mode(InternodeCompatibilityMode.DSE_6);
        gossipNormal();
        assertThat(asksForAFailureResponse(echoes.get(0))).as("asks for a failure response before the node is identified").isTrue();
        identifiedByItsNetVersion();
        answer(echoes.get(0));
        assertThat(isUp()).isTrue();

        restartedInADeadState();
        assertThat(isUp()).isFalse();
        assertThat(echoes).hasSize(1);
        assertThat(recordedAttempt()).isZero();

        gossipNormal();
        assertThat(echoes).as("the echo request on NORMAL").hasSize(2);
        assertThat(asksForAFailureResponse(echoes.get(1))).as("asks for a failure response once the node is identified").isTrue();
        assertThat(recordedAttempt()).isEqualTo(1);
        justBeforeTheRequestTimeout();
        gossipNormal();
        assertThat(echoes).as("not sent again before the request timeout").hasSize(2);

        advanceMillis(2);
        gossipNormal();
        assertThat(echoes).as("sent again after the request timeout").hasSize(3);
        assertThat(recordedAttempt()).isEqualTo(2);
        assertThat(isUp()).isFalse();
        assertThat(lines(Level.DEBUG)).containsExactly(retryLine("did not answer", REQUEST_TIMEOUT_MILLIS + 1, 2));
        assertThat(lines(Level.INFO)).isEmpty();

        answer(echoes.get(2));
        assertThat(isUp()).isTrue();
        assertThat(recordedAttempt()).isZero();
        assertThat(lines(Level.INFO)).containsExactly(answeredLine(2));

        afterTheRequestTimeout();
        gossipNormal();
        assertThat(echoes).as("no request to a node that is up").hasSize(3);

        restartedInADeadState();
        gossipNormal();
        assertThat(echoes).as("the echo request for the next state").hasSize(4);
        assertThat(recordedAttempt()).isEqualTo(1);
    }

    /**
     * The timeout that counts is the one the request was sent with: a request sent with a short timeout is due after it
     * even once the request timeout was raised, and one sent with the raised timeout is not due before that.
     */
    @Test
    public void testTheTimeoutOfTheRequestCounts()
    {
        mode(InternodeCompatibilityMode.DSE_6);
        identifiedByItsNetVersion();
        gossipNormal();
        DatabaseDescriptor.setRpcTimeout(REQUEST_TIMEOUT_MILLIS * 6);
        afterTheRequestTimeout();
        gossipNormal();
        assertThat(echoes).as("due after the timeout it was sent with").hasSize(2);

        advanceMillis(REQUEST_TIMEOUT_MILLIS * 6 - 1);
        gossipNormal();
        assertThat(echoes).as("sent with the raised timeout: not due just before it").hasSize(2);

        advanceMillis(2);
        gossipNormal();
        assertThat(echoes).as("due after the raised timeout").hasSize(3);
    }

    /**
     * A DSE node refusing the request with a failure response (DSE does while it is not ready to answer echo requests) gets it
     * again a gossip interval after it was sent, without waiting for the request timeout; a late refusal of an earlier
     * request does not make the last one due.
     */
    @Test
    public void testRefusedEchoToDseNodeIsSentAgainAfterAGossipInterval()
    {
        mode(InternodeCompatibilityMode.DSE_6);
        identifiedByItsNetVersion();
        gossipNormal();
        refuse(echoes.get(0));
        advanceMillis(Gossiper.intervalInMillis - 1);
        gossipNormal();
        assertThat(echoes).as("not sent again within a gossip interval").hasSize(1);

        advanceMillis(2);
        gossipNormal();
        assertThat(echoes).as("sent again after a gossip interval").hasSize(2);
        assertThat(recordedAttempt()).isEqualTo(2);
        assertThat(lines(Level.DEBUG)).containsExactly(retryLine(FAILED, Gossiper.intervalInMillis + 1, 2));

        echoes.get(0).callback.callback.onFailure(peer, RequestFailureReason.UNKNOWN);
        afterAGossipInterval();
        gossipNormal();
        assertThat(echoes).as("the last request was not refused").hasSize(2);

        answer(echoes.get(1));
        assertThat(isUp()).isTrue();
    }

    /**
     * The failure response a DSE node sends at messaging version 11 (the FAIL flag and the 2-byte FAIL_REASON param, no
     * payload) is decoded as a failure and reaches the callback of the echo request with its reason, which makes it due
     * after a gossip interval. DSE sends it as INTERNAL_RESPONSE, as its code reads; REQUEST_RESPONSE is decoded the same
     * way. The layout comes from DSE's code, not from a capture.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testDseFailureResponseAtTheDialectReachesTheCallback() throws Exception
    {
        mode(InternodeCompatibilityMode.DSE_6);
        identifiedByItsNetVersion();
        gossipNormal();
        for (Verb responseVerb : new Verb[]{ Verb.INTERNAL_RSP, Verb.REQUEST_RSP })
        {
            Sent echo = echoes.get(echoes.size() - 1);
            Message<?> failure = deserializeAtTheDialect(dseFailureResponse(echo.message.id(), responseVerb.id));
            assertThat(failure.verb()).as(responseVerb.toString()).isEqualTo(Verb.FAILURE_RSP);
            // dispatched as the inbound path does: the handler of FAILURE_RSP is the response handler
            ((IVerbHandler<Object>) failure.verb().handler()).doVerb((Message<Object>) failure);
            assertThat(MessagingService.instance().callbacks.get(echo.message.id(), peer)).as("callback used").isNull();

            int before = echoes.size();
            afterAGossipInterval();
            gossipNormal();
            assertThat(echoes).as("sent again after the failure response as " + responseVerb).hasSize(before + 1);
            assertThat(lines(Level.DEBUG)).last()
                                          .isEqualTo(retryLine(FAILED, Gossiper.intervalInMillis + 1, before + 1));
        }
    }

    /** A DSE failure response at messaging version 11 to the request {@code id}, laid out as DSE sends it. */
    private static byte[] dseFailureResponse(long id, int verbId) throws Exception
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.writeInt(0xCA552DFA);
            out.writeInt((int) id);
            out.writeInt((int) FBUtilities.now().toEpochMilli());
            byte[] address = peer.getAddress().getAddress();
            out.writeByte(address.length);
            out.write(address);
            out.writeInt(verbId);
            out.writeInt(2);
            out.writeUTF("FAIL");
            out.writeInt(1);
            out.writeByte(0);
            out.writeUTF("FAIL_REASON");
            out.writeInt(2);
            out.writeShort(RequestFailureReason.UNKNOWN.code);
            out.writeInt(0);
            return out.toByteArray();
        }
    }

    private static Message<?> deserializeAtTheDialect(byte[] bytes) throws Exception
    {
        try (DataInputBuffer in = new DataInputBuffer(bytes))
        {
            Message<?> message = Message.serializer.deserialize(in, peer, VERSION_3014);
            assertThat(in.available()).isZero();
            return message;
        }
    }

    /**
     * The debug line of the retries is logged for attempts 2 and 3, then every tenth, while a DSE node keeps refusing the
     * requests.
     */
    @Test
    public void testRetriesAreLoggedForTheFirstAttemptsThenEveryTenth()
    {
        mode(InternodeCompatibilityMode.DSE_6);
        identifiedByItsNetVersion();
        gossipNormal();
        for (int attempt = 2; attempt <= 21; attempt++)
        {
            refuse(echoes.get(echoes.size() - 1));
            afterAGossipInterval();
            gossipNormal();
        }
        assertThat(echoes).hasSize(21);
        assertThat(recordedAttempt()).isEqualTo(21);
        long ago = Gossiper.intervalInMillis + 1;
        assertThat(lines(Level.DEBUG)).containsExactly(retryLine(FAILED, ago, 2), retryLine(FAILED, ago, 3),
                                                       retryLine(FAILED, ago, 10), retryLine(FAILED, ago, 20));
    }

    /**
     * A request to an identified DSE node that this node drops (its connection closed with the request queued) or that
     * expires in the outbound queue reaches the callback as TIMEOUT, through the request callbacks, and is sent again
     * after a gossip interval, with its own debug line.
     */
    @Test
    public void testRequestDroppedOrExpiredOnThisNode()
    {
        mode(InternodeCompatibilityMode.DSE_6);
        identifiedByItsNetVersion();
        gossipNormal();
        MessagingService.instance().callbacks.onDiscardOnClose(echoes.get(0).message, peer);
        awaitTheFailureOfTheLastRequest();
        afterAGossipInterval();
        gossipNormal();
        assertThat(echoes).as("sent again after its drop").hasSize(2);

        MessagingService.instance().callbacks.onExpired(echoes.get(1).message, peer);
        awaitTheFailureOfTheLastRequest();
        afterAGossipInterval();
        gossipNormal();
        assertThat(echoes).as("sent again after its expiry").hasSize(3);

        long ago = Gossiper.intervalInMillis + 1;
        assertThat(lines(Level.DEBUG)).containsExactly(retryLine(DROPPED, ago, 2), retryLine(DROPPED, ago, 3));
    }

    /** The request callbacks report the failure of the last request (asynchronously) as TIMEOUT. */
    private static void awaitTheFailureOfTheLastRequest()
    {
        Util.spinAssertEquals(RequestFailureReason.TIMEOUT, () -> Gossiper.instance.dseEchoRetries.recordedFailure(peer), 5);
    }

    /**
     * A late answer to the first request, in the window where its callback expired but was not reaped yet, marks the node
     * up too, without the info line; the one sent again finds nothing to do.
     */
    @Test
    public void testLateAnswerToTheFirstRequest()
    {
        mode(InternodeCompatibilityMode.DSE_6);
        identifiedByItsNetVersion();
        gossipNormal();
        afterTheRequestTimeout();
        gossipNormal();
        assertThat(echoes).hasSize(2);

        answer(echoes.get(0));
        assertThat(isUp()).isTrue();
        assertThat(recordedAttempt()).isZero();
        answer(echoes.get(1));
        assertThat(isUp()).isTrue();
        assertThat(lines(Level.INFO)).isEmpty();
    }

    /**
     * Outside the mode the request is sent once per state, as upstream, without asking for a failure response, also after
     * the request expired on this node or the request timeout passed, and for a peer identified as a DSE node while the
     * mode was on (an HCD node takes the same path: the identification is not read outside the mode); nothing is
     * recorded or logged.
     */
    @Test
    public void testNoneModeSendsOneRequestPerState() throws Exception
    {
        mode(InternodeCompatibilityMode.DSE_6);
        identifiedByItsNetVersion();
        mode(InternodeCompatibilityMode.NONE);
        gossipNormal();
        assertThat(asksForAFailureResponse(echoes.get(0))).isFalse();
        // the request callbacks drop the callback of an expired request; without the flag no failure is reported
        MessagingService.instance().callbacks.onExpired(echoes.get(0).message, peer);
        assertThat(MessagingService.instance().callbacks.get(echoes.get(0).message.id(), peer)).isNull();
        afterAGossipInterval();
        afterTheRequestTimeout();
        gossipNormal();
        assertThat(echoes).hasSize(1);
        assertThat(isUp()).isFalse();
        assertThat(recordedAttempt()).isZero();

        // a new state gets its own request, as upstream
        restartedInADeadState();
        gossipNormal();
        assertThat(echoes).hasSize(2);
        answer(echoes.get(1));
        assertThat(isUp()).isTrue();
        assertThat(recordedAttempt()).isZero();
        assertThat(logged.list).isEmpty();
    }

    /**
     * In the mode, a peer known not to be a DSE node (an HCD node) whose echo request goes unanswered gets it again once
     * the request timeout has passed, not before, even when its callback learnt of no failure (as observed: most
     * likely an answer dropped on arrival here); the answer to the request sent again marks it up, with the info line,
     * and no further request follows. Its requests ask for a failure response too.
     */
    @Test
    public void testUnansweredEchoToHcdNodeIsSentAgainAndMarksItUp() throws Exception
    {
        mode(InternodeCompatibilityMode.DSE_6);
        anHcdNodeByItsNetVersion();
        gossipNormal();
        assertThat(echoes).hasSize(1);
        assertThat(asksForAFailureResponse(echoes.get(0))).isTrue();
        justBeforeTheRequestTimeout();
        gossipNormal();
        assertThat(echoes).as("not sent again before the request timeout").hasSize(1);

        advanceMillis(2);
        gossipNormal();
        assertThat(echoes).as("sent again after the request timeout").hasSize(2);
        assertThat(asksForAFailureResponse(echoes.get(1))).isTrue();
        assertThat(recordedAttempt()).isEqualTo(2);
        assertThat(isUp()).isFalse();
        assertThat(lines(Level.DEBUG)).containsExactly(retryLine("did not answer", REQUEST_TIMEOUT_MILLIS + 1, 2));

        answer(echoes.get(1));
        assertThat(isUp()).isTrue();
        assertThat(recordedAttempt()).isZero();
        assertThat(lines(Level.INFO)).containsExactly(answeredLine(2));

        afterTheRequestTimeout();
        gossipNormal();
        assertThat(echoes).as("no request to a node that is up").hasSize(2);
    }

    /**
     * In the mode, an echo request to an HCD node that failed gets it again a gossip interval after it was sent, not
     * before: one that expired or was dropped on this node (TIMEOUT, through the request callbacks), and one the node
     * refused with a failure response. The failure leaves the pending echo request in place, so gossip within the
     * interval sends nothing; the answer to the last request marks the node up.
     */
    @Test
    public void testFailedEchoToHcdNodeIsSentAgainAfterAGossipInterval() throws Exception
    {
        mode(InternodeCompatibilityMode.DSE_6);
        anHcdNodeByItsNetVersion();
        gossipNormal();
        MessagingService.instance().callbacks.onExpired(echoes.get(0).message, peer);
        awaitTheFailureOfTheLastRequest();
        advanceMillis(Gossiper.intervalInMillis - 1);
        gossipNormal();
        assertThat(echoes).as("not sent again within a gossip interval of the expiry").hasSize(1);

        advanceMillis(2);
        gossipNormal();
        assertThat(echoes).as("sent again after a gossip interval").hasSize(2);
        assertThat(asksForAFailureResponse(echoes.get(1))).isTrue();

        refuse(echoes.get(1));
        advanceMillis(Gossiper.intervalInMillis - 1);
        gossipNormal();
        assertThat(echoes).as("not sent again within a gossip interval of the refusal").hasSize(2);

        advanceMillis(2);
        gossipNormal();
        assertThat(echoes).as("sent again after the refusal").hasSize(3);
        assertThat(asksForAFailureResponse(echoes.get(2))).isTrue();
        assertThat(recordedAttempt()).isEqualTo(3);
        long ago = Gossiper.intervalInMillis + 1;
        assertThat(lines(Level.DEBUG)).containsExactly(retryLine(DROPPED, ago, 2), retryLine(FAILED, ago, 3));

        answer(echoes.get(2));
        assertThat(isUp()).isTrue();
        assertThat(lines(Level.INFO)).containsExactly(answeredLine(3));
    }

    /**
     * A failure response of a 4.0 or later node (HCD 1.2, this software), at messaging version 12 and at the current
     * version, which two nodes of this software connect at: the echo request carries the failure-callback flag on the
     * wire; the failure response {@code InboundSink.fail} would build for it, when the node cannot process it, is decoded
     * as a failure, reaches the callback of the request through the response handler, and makes the request due after a
     * gossip interval.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testFailureResponseOfA40NodeReachesTheCallback() throws Exception
    {
        mode(InternodeCompatibilityMode.DSE_6);
        anHcdNodeByItsNetVersion();
        gossipNormal();
        Sent echo = echoes.get(0);
        Message<?> failure = null;
        for (int version : new int[]{ MessagingService.VERSION_40, MessagingService.current_version })
        {
            Message<?> received = roundTrip(echo.message, FBUtilities.getBroadcastAddressAndPort(), version);
            assertThat(received.header.flags() & 1).as("the failure-callback flag at messaging version " + version)
                                                   .isEqualTo(1);
            Message<?> response = received.failureResponse(RequestFailureReason.forException(new RuntimeException("refused")));
            failure = roundTrip(response.withFrom(peer), peer, version);
            assertThat(failure.verb()).as("version " + version).isEqualTo(Verb.FAILURE_RSP);
            assertThat(failure.isFailureResponse()).as("version " + version).isTrue();
        }
        ((IVerbHandler<Object>) failure.verb().handler()).doVerb((Message<Object>) failure);
        assertThat(MessagingService.instance().callbacks.get(echo.message.id(), peer)).as("callback used").isNull();
        assertThat(Gossiper.instance.dseEchoRetries.recordedFailure(peer)).isEqualTo(RequestFailureReason.UNKNOWN);

        advanceMillis(Gossiper.intervalInMillis - 1);
        gossipNormal();
        assertThat(echoes).as("not sent again within a gossip interval").hasSize(1);
        advanceMillis(2);
        gossipNormal();
        assertThat(echoes).as("sent again after a gossip interval").hasSize(2);
        assertThat(lines(Level.DEBUG)).containsExactly(retryLine(FAILED, Gossiper.intervalInMillis + 1, 2));
    }

    /** {@code message} serialized at messaging version {@code version} and deserialized as received from {@code from}. */
    private static Message<?> roundTrip(Message<?> message, InetAddressAndPort from, int version) throws Exception
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            Message.serializer.serialize(message, out, version);
            try (DataInputBuffer in = new DataInputBuffer(out.toByteArray()))
            {
                Message<?> deserialized = Message.serializer.deserialize(in, from, version);
                assertThat(in.available()).isZero();
                return deserialized;
            }
        }
    }

    /**
     * A peer of which nothing is known (no NET_VERSION read yet, a node first heard of) gets the request again too, with
     * the failure-callback flag, once the request timeout has passed and not before.
     */
    @Test
    public void testUnknownPeerGetsTheRequestAgain() throws Exception
    {
        mode(InternodeCompatibilityMode.DSE_6);
        gossipNormal();
        assertThat(DsePeers.isDse(peer)).isFalse();
        assertThat(DsePeers.isPresumed(peer)).isFalse();
        assertThat(asksForAFailureResponse(echoes.get(0))).isTrue();
        justBeforeTheRequestTimeout();
        gossipNormal();
        assertThat(echoes).as("not sent again before the request timeout").hasSize(1);

        advanceMillis(2);
        gossipNormal();
        assertThat(echoes).as("sent again after the request timeout").hasSize(2);
        assertThat(asksForAFailureResponse(echoes.get(1))).isTrue();
        assertThat(lines(Level.DEBUG)).containsExactly(retryLine("did not answer", REQUEST_TIMEOUT_MILLIS + 1, 2));
        answer(echoes.get(1));
        assertThat(isUp()).isTrue();
    }

    /**
     * A peer only presumed to be a DSE node from its stored release gets the request again after a failure too, a gossip
     * interval after it was sent.
     */
    @Test
    public void testPresumedPeerGetsTheRequestAgainAfterAFailure() throws Exception
    {
        mode(InternodeCompatibilityMode.DSE_6);
        DsePeers.presume(peer, "stored release 4.0.0.6865");
        assertThat(DsePeers.isPresumed(peer)).isTrue();
        assertThat(DsePeers.isDse(peer)).isFalse();
        gossipNormal();
        assertThat(asksForAFailureResponse(echoes.get(0))).isTrue();
        refuse(echoes.get(0));
        advanceMillis(Gossiper.intervalInMillis - 1);
        gossipNormal();
        assertThat(echoes).as("not sent again within a gossip interval").hasSize(1);

        advanceMillis(2);
        gossipNormal();
        assertThat(echoes).as("sent again after a gossip interval").hasSize(2);
        assertThat(lines(Level.DEBUG)).containsExactly(retryLine(FAILED, Gossiper.intervalInMillis + 1, 2));
        answer(echoes.get(1));
        assertThat(isUp()).isTrue();
    }

    /** A dead state stops the requests: the peer is marked down, its record dropped, and nothing is sent again. */
    @Test
    public void testDeadStateStopsTheRequests()
    {
        mode(InternodeCompatibilityMode.DSE_6);
        identifiedByItsNetVersion();
        gossipNormal();
        assertThat(recordedAttempt()).isEqualTo(1);

        restartedInADeadState();
        assertThat(recordedAttempt()).isZero();
        afterTheRequestTimeout();
        gossip(factory.hibernate(true));
        assertThat(echoes).hasSize(1);
        assertThat(isUp()).isFalse();
    }

    /**
     * A removed node loses its record, which is what keeps a request from being sent again for a node that left (the
     * identification as a DSE node no longer counts); the node is forgotten by {@link DsePeers} too.
     */
    @Test
    public void testRemovedNodeDropsTheRecord()
    {
        mode(InternodeCompatibilityMode.DSE_6);
        identifiedByItsNetVersion();
        gossipNormal();
        assertThat(recordedAttempt()).isEqualTo(1);
        EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(peer);

        Gossiper.instance.removeEndpoint(peer);
        assertThat(recordedAttempt()).isZero();
        afterTheRequestTimeout();
        assertThat(Gossiper.instance.dseEchoRetries.isDue(peer, state)).as("not due once removed").isFalse();
        assertThat(DsePeers.isDse(peer)).as("forgotten by DsePeers").isFalse();
    }

    /** An evicted node loses its record too, and is forgotten by {@link DsePeers}. */
    @Test
    public void testEvictedNodeDropsTheRecord()
    {
        mode(InternodeCompatibilityMode.DSE_6);
        identifiedByItsNetVersion();
        gossipNormal();
        assertThat(recordedAttempt()).isEqualTo(1);
        EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(peer);

        Gossiper.instance.evictFromMembership(peer);
        assertThat(recordedAttempt()).isZero();
        afterTheRequestTimeout();
        assertThat(Gossiper.instance.dseEchoRetries.isDue(peer, state)).as("not due once evicted").isFalse();
        assertThat(DsePeers.isDse(peer)).as("forgotten by DsePeers").isFalse();
        assertThat(Gossiper.instance.getEndpointStateMap()).doesNotContainKey(peer);
    }

    /** The test cleanup of the gossiper drops the records. */
    @Test
    public void testClearUnsafeDropsTheRecords()
    {
        mode(InternodeCompatibilityMode.DSE_6);
        identifiedByItsNetVersion();
        gossipNormal();
        assertThat(recordedAttempt()).isEqualTo(1);

        Gossiper.instance.clearUnsafe();
        assertThat(recordedAttempt()).isZero();
    }
}
