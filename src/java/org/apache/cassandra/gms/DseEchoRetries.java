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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.Clock;

/**
 * The echo requests the gossiper sends to mark a peer up, in DSE 6 compatibility mode, so that one a peer (a DSE 6 node
 * or a node of this software) leaves unanswered, or answers with a failure, is sent again. Every method but
 * {@link #clear} (test cleanup) and the test accessors is only called in that mode.
 * <p>
 * The gossiper sends one echo request per endpoint state of a peer it does not consider alive, and marks the peer up
 * when the answer arrives; upstream's callback ignores failures, so while the peer keeps the same state (the same
 * gossip generation) a request that is lost or refused, or whose answer is lost, is never sent again and the peer stays
 * down until one of the two nodes restarts (a restart of the peer brings a new generation). That was seen twice in test
 * clusters. First with DSE 6.8.65 (about one DSE restart in three): no answer to the echo request this node sent when a
 * restarted DSE node reached {@code NORMAL}, and the DSE node shown down on this node indefinitely while it gossiped
 * normally. Why the request went unanswered is not established. Two explanations fit: DSE refuses echo requests while
 * it is not yet ready to answer them (DSE's {@code isSafeToReplyEchos}), with a failure response
 * ({@code UNKNOWN}) to a peer whose release it takes to support the serial echo, as it does ours, and silently to
 * others; or the request or its answer is lost while DSE 6.8.65 sets up its connections to this node (its flagged first
 * handshake message, the socket closed once answered with the dialect, a reconnection at 11 about 120 ms later). Which
 * one holds, and whether DSE sends the failure response, is not established: this node logs the retries at debug level
 * with the reason of a refusal. Then between two nodes of this software, both in the mode: one convicted the other
 * (why is not established), reconnected, and then dropped an {@code ECHO_RSP} as expired (on
 * arrival or before the gossip stage ran it), 14.5 s after the sender created it (the only {@code ECHO_RSP} the
 * dropped-messages report of that 5 s interval counts), most likely the peer's answer to the echo request this node
 * sent it (not proved: the logs do not name the sender); it sent no other request and kept the peer
 * down for about four hours, still down when the diagnostics ended, while the peer's gossip state, same generation and
 * an advancing heartbeat, kept arriving.
 * <p>
 * So the gossiper, which calls {@code markAlive} for such a peer whenever gossip brings its state, sends the request
 * again, whatever the peer:
 * <ul>
 *   <li>once the timeout the last request for the same state was sent with (the request timeout of {@code ECHO_REQ}
 *       then) has passed since it was sent: no answer at all, or an answer this node dropped as expired;</li>
 *   <li>once the request failed and a gossip interval has passed since it was sent. Every request carries the
 *       failure-callback flag, as HCD 1.2's echo requests (to every peer) and DSE's own do: a pre-4.0 node sends a
 *       failure response only to a request that asks for it, and a 4.0 or later node (HCD 1.2, this software) sends one
 *       only when it cannot process the request (it arrived corrupt, its connection closed before the whole request
 *       arrived, it could not be deserialized, or the echo handler threw, which it does not do: it answers, or ignores
 *       the request while it shuts down). With the flag the callback learns of two kinds of failure: the peer's failure
 *       response (from a DSE node, a refusal; from any other peer, a request it could not process), and, as
 *       {@code TIMEOUT}, a request that expired, whether its answer never came or came after the request expired, or
 *       that this node dropped before it was sent (its connection closed with the request still queued, or the request
 *       could not be serialized), which can be well before the timeout. The debug line of the retry tells a failure
 *       response from a {@code TIMEOUT}, and both from a request whose callback learnt of nothing (yet). A failure only
 *       records its reason: the gossiper keeps its pending echo request, so that the next {@code markAlive} finds the
 *       request due.</li>
 * </ul>
 * HCD 1.2 drops its pending echo on any failure, so that its next {@code markAlive} sends a request at once; waiting
 * for a gossip interval since the last request instead bounds the requests to a peer that keeps refusing them (a DSE
 * node while it starts) to one per interval, where {@code markAlive} runs for every gossip message carrying the peer's
 * state. Every peer outside the mode keeps the upstream behaviour: one request per state, without the flag.
 * <p>
 * There is no timer: a request is only sent again from {@code markAlive}, which the gossiper never calls for a peer in
 * a dead state (shut down, left, removed) and which stops once the peer is up. The record of a peer is dropped wherever
 * the gossiper drops its pending echo request (the answer, the peer marked down or removed) and when it is evicted, so
 * no request is sent again for a node that left. A peer this node cannot reach while its gossip state still arrives
 * through other nodes (a partition between the two only) is not in a dead state: it gets a request every request
 * timeout, or every gossip interval when the requests fail sooner, for as long as that lasts, where upstream sends one;
 * the debug line of the retries is logged for the first attempts and then every {@value #LOG_EVERY}th.
 */
final class DseEchoRetries
{
    private static final Logger logger = LoggerFactory.getLogger(DseEchoRetries.class);

    /** The retries logged at debug level: the first ones, then one in this many. */
    static final int LOG_EVERY = 10;
    private static final int LOGGED_FIRST = 3;
    private static final long GOSSIP_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(Gossiper.intervalInMillis);

    /** The last echo request sent to a peer: for which state, when, with which timeout, its attempt number, and how it failed. */
    private static final class Sent
    {
        final EndpointState state;
        final long atNanos;
        final long timeoutNanos;
        final int attempt;
        final RequestFailureReason failure;

        Sent(EndpointState state, long atNanos, long timeoutNanos, int attempt, RequestFailureReason failure)
        {
            this.state = state;
            this.atNanos = atNanos;
            this.timeoutNanos = timeoutNanos;
            this.attempt = attempt;
            this.failure = failure;
        }
    }

    private final Map<InetAddressAndPort, Sent> sent = new ConcurrentHashMap<>();
    private volatile LongSupplier clock = Clock.Global::nanoTime;

    /**
     * True when the echo request for {@code state}, which the gossiper has pending for {@code peer} already, is to be sent
     * again now, whatever the peer: the last request for that very state was sent at least its timeout before, or failed
     * and was sent at least a gossip interval before.
     */
    boolean isDue(InetAddressAndPort peer, EndpointState state)
    {
        Sent last = sent.get(peer);
        if (last == null || last.state != state)
            return false;
        long elapsed = clock.getAsLong() - last.atNanos;
        return elapsed >= last.timeoutNanos || (last.failure != null && elapsed >= GOSSIP_INTERVAL_NANOS);
    }

    /**
     * Records the echo request about to be sent to {@code peer} for {@code state} with the timeout {@code timeoutNanos},
     * and returns its attempt number for that state: 1 for the first, more for a request sent again, which is logged.
     */
    int sending(InetAddressAndPort peer, EndpointState state, long timeoutNanos)
    {
        long now = clock.getAsLong();
        Sent[] previous = new Sent[1];
        Sent recorded = sent.compute(peer, (p, last) -> {
            previous[0] = last;
            return new Sent(state, now, timeoutNanos, last != null && last.state == state ? last.attempt + 1 : 1, null);
        });
        Sent last = previous[0];
        int attempt = recorded.attempt;
        if (attempt > 1 && (attempt <= LOGGED_FIRST || attempt % LOG_EVERY == 0))
            logger.debug("DSE 6 compatibility mode: peer {} {} the echo request sent {} ms ago; sending it again (attempt {}), " +
                         "since it stays down until one is answered",
                         peer, outcome(last.failure),
                         TimeUnit.NANOSECONDS.toMillis(now - last.atNanos), attempt);
        return attempt;
    }

    /** What happened to a request, for the debug line of its retry, from the failure its callback learnt of, if any. */
    private static String outcome(RequestFailureReason failure)
    {
        if (failure == null)
            return "did not answer";
        if (failure == RequestFailureReason.TIMEOUT)
            return "did not answer (the request expired, its answer came after the request expired, or this node dropped the request)";
        return "answered with a failure (" + failure + ')';
    }

    /**
     * Called when the echo request of attempt number {@code attempt} sent to {@code peer} for {@code state} failed: a
     * failure response from the peer, or ({@code TIMEOUT}) its expiry or its drop on this node. It becomes due a gossip
     * interval after it was sent ({@link #isDue}), if it is still the last one.
     */
    void failed(InetAddressAndPort peer, EndpointState state, int attempt, RequestFailureReason reason)
    {
        sent.computeIfPresent(peer, (p, last) -> last.state == state && last.attempt == attempt
                                                 ? new Sent(last.state, last.atNanos, last.timeoutNanos, last.attempt, reason)
                                                 : last);
    }

    /**
     * Called when {@code peer} answered the echo request of attempt number {@code attempt} and is about to be marked up:
     * drops its record, and logs once when the answer is to a request sent again.
     */
    void answered(InetAddressAndPort peer, int attempt)
    {
        sent.remove(peer);
        if (attempt > 1)
            logger.info("DSE 6 compatibility mode: peer {} answered the echo request sent again (attempt {}); marking it up",
                        peer, attempt);
    }

    /** Drops the record of {@code peer}: the gossiper dropped its pending echo request, or evicted the peer. */
    void forget(InetAddressAndPort peer)
    {
        sent.remove(peer);
    }

    void clear()
    {
        sent.clear();
    }

    /** The attempt number of the last echo request recorded for {@code peer}, 0 when none is. */
    @VisibleForTesting
    int recordedAttempt(InetAddressAndPort peer)
    {
        Sent last = sent.get(peer);
        return last == null ? 0 : last.attempt;
    }

    /** How the last echo request recorded for {@code peer} failed, null when it did not or none is recorded. */
    @VisibleForTesting
    RequestFailureReason recordedFailure(InetAddressAndPort peer)
    {
        Sent last = sent.get(peer);
        return last == null ? null : last.failure;
    }

    /** Replaces the clock the requests are timed with, in nanoseconds. */
    @VisibleForTesting
    void setClock(LongSupplier clock)
    {
        this.clock = clock;
    }
}
