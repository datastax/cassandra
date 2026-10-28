/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.HintsServiceMetrics;
import org.apache.cassandra.net.DseCompat;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.concurrent.Condition;


import static org.apache.cassandra.hints.HintsDispatcher.Callback.Outcome.*;
import static org.apache.cassandra.metrics.HintsServiceMetrics.updateDelayMetrics;
import static org.apache.cassandra.net.Verb.HINT_REQ;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;
import static org.apache.cassandra.utils.concurrent.Condition.newOneTimeCondition;

/**
 * Dispatches a single hints file to a specified node in a batched manner.
 *
 * For hints stored at the current messaging version, endpoint writability is checked by peeking at the
 * first few bytes of the buffer to extract the keyspace (via {@link Hint.Serializer#getKeyspaceFromBuffer})
 * rather than fully deserializing the hint. Full deserialization is only performed when the file's messaging
 * version differs from the current one and re-encoding is required.
 */
final class HintsDispatcher implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(HintsDispatcher.class);

    private enum Action { CONTINUE, ABORT }

    private final HintsReader reader;
    final UUID hostId;
    final InetAddressAndPort address;
    private final int messagingVersion;
    private final BooleanSupplier abortRequested;

    private InputPosition currentPagePosition;

    private HintsDispatcher(HintsReader reader, UUID hostId, InetAddressAndPort address, int messagingVersion, BooleanSupplier abortRequested)
    {
        currentPagePosition = null;

        this.reader = reader;
        this.hostId = hostId;
        this.address = address;
        this.messagingVersion = messagingVersion;
        this.abortRequested = abortRequested;
    }

    static HintsDispatcher create(File file,
                                  RateLimiter rateLimiter,
                                  InetAddressAndPort address,
                                  UUID hostId,
                                  int messagingVersion,
                                  BooleanSupplier abortRequested)
    {
        HintsDispatcher dispatcher = new HintsDispatcher(HintsReader.open(file, rateLimiter), hostId, address, messagingVersion, abortRequested);
        HintDiagnostics.dispatcherCreated(dispatcher);
        return dispatcher;
    }

    public void close()
    {
        HintDiagnostics.dispatcherClosed(this);
        reader.close();
    }

    void seek(InputPosition position)
    {
        reader.seek(position);
    }

    /**
     * @return whether or not dispatch completed entirely and successfully
     */
    boolean dispatch()
    {
        for (HintsReader.Page page : reader)
        {
            currentPagePosition = page.position;
            if (dispatch(page) != Action.CONTINUE)
                return false;
        }

        return true;
    }

    /**
     * @return offset of the first non-delivered page
     */
    InputPosition dispatchPosition()
    {
        return currentPagePosition;
    }


    // retry in case of a timeout; stop in case of a failure, host going down, or delivery paused
    private Action dispatch(HintsReader.Page page)
    {
        HintDiagnostics.dispatchPage(this);
        return sendHintsAndAwait(page);
    }

    private Action sendHintsAndAwait(HintsReader.Page page)
    {
        Collection<Callback> callbacks = new ArrayList<>();

        Action action = sendHints(page.buffersIterator(), callbacks);

        if (action == Action.ABORT)
            return action;

        long success = 0, failures = 0, timeouts = 0;
        for (Callback cb : callbacks)
        {
            Callback.Outcome outcome = cb.await();
            if (outcome == Callback.Outcome.SUCCESS) success++;
            else if (outcome == Callback.Outcome.FAILURE) failures++;
            else if (outcome == Callback.Outcome.TIMEOUT) timeouts++;
        }

        updateMetrics(success, failures, timeouts);

        if (failures > 0 || timeouts > 0)
        {
            HintDiagnostics.pageFailureResult(this, success, failures, timeouts);
            if (DseCompat.isPre40Layout(messagingVersion))
                warnUnacknowledgedByDse6(failures + timeouts);
            return Action.ABORT;
        }
        else
        {
            HintDiagnostics.pageSuccessResult(this, success, failures, timeouts);
            return Action.CONTINUE;
        }
    }

    /**
     * DSE 6 compatibility mode, for a page sent to a DSE 6 node (at a pre-4.0 messaging version,
     * {@link DseCompat#isPre40Layout}): DSE 6.9 acknowledges every hint it applies, but not one it drops (a table it
     * does not know, data it cannot validate, a range it no longer owns when configured to reject such hints); DSE 6.8
     * is taken to do the same, so the WARN names DSE 6. A dropped hint is dropped again at every retry: the page is
     * sent again at every dispatch, and the rest of the file and the later files for that node wait until the hint
     * expires (the smallest gc_grace_seconds of its tables, capped by cassandra.maxHintTTL) or the node is upgraded
     * (this software acknowledges the hints it drops). A node that is down or overloaded leaves hints unacknowledged
     * too, and a hint that could not be sent (outbound queue full or expired, a serialization failure, the connection
     * closed) fails its callback. The WARN gives the total of the timeouts and failures: no split between them is
     * reliable, since a failure is a failure response from the node or one of those local failures, and the expiry of
     * the hint's callback is reported as a timeout failure when it comes before the dispatcher's own deadline. Once
     * every ten minutes per node at most.
     */
    private void warnUnacknowledgedByDse6(long unacknowledged)
    {
        NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, "dse6-unacknowledged-hints-" + address, 10, TimeUnit.MINUTES,
                         "DSE 6 compatibility mode: hints not acknowledged by DSE 6 node {}: {} in file {}; the file stays and is " +
                         "sent again from that page. The node may be down or overloaded, it may have answered with a failure, the " +
                         "hints may not have been sent (see the messaging log), or it dropped them: DSE 6 does not acknowledge a " +
                         "hint it drops (unknown table, invalid data, range no longer owned if configured to reject such hints), " +
                         "and such a hint holds back this file and the later ones for that node until it expires (the smallest " +
                         "gc_grace_seconds of its tables, capped by cassandra.maxHintTTL) or the node is upgraded. If this repeats " +
                         "for the same file, nodetool truncatehints {} deletes all the pending hints for that node, and the repair " +
                         "after the compatibility mode is dropped brings their writes to it.",
                         address, unacknowledged, reader.descriptor().fileName(), address.getHostAddress(false));
    }

    private void updateMetrics(long success, long failures, long timeouts)
    {
        HintsServiceMetrics.hintsSucceeded.mark(success);
        HintsServiceMetrics.hintsFailed.mark(failures);
        HintsServiceMetrics.hintsTimedOut.mark(timeouts);
    }

    private Action sendHints(Iterator<ByteBuffer> encodedHints, Collection<Callback> callbacks)
    {
        List<ByteBuffer> buffersToSend = new ArrayList<>();
        List<Hint> hintsToSend = new ArrayList<>();
        int fileVersion = reader.descriptor().messagingVersion();
        boolean sameVersion = fileVersion == messagingVersion;

        while (encodedHints.hasNext())
        {
            if (abortRequested.getAsBoolean())
            {
                HintDiagnostics.abortRequested(this);
                return Action.ABORT;
            }

            ByteBuffer buffer = encodedHints.next();

            if (sameVersion)
            {
                // Peek at the buffer to get the keyspace without deserializing the hint.
                String keyspace = Hint.serializer.getKeyspaceFromBuffer(buffer, fileVersion);
                if (keyspace != null && !isWritable(keyspace))
                    return Action.ABORT;

                buffersToSend.add(buffer);
            }
            else
            {
                // Cross-version path: must deserialize to re-encode for the target messaging version.
                Hint hint;
                try (DataInputBuffer in = new DataInputBuffer(buffer, true))
                {
                    hint = Hint.serializer.deserialize(in, fileVersion);
                }
                catch (UnknownTableException e)
                {
                    logger.warn("Failed to read a hint for {}: {} - table with id {} is unknown in file {}",
                                HintsEndpointProvider.instance.endpointForHost(reader.descriptor().hostId),
                                reader.descriptor().hostId,
                                e.id,
                                reader.descriptor().fileName());
                    // Dropped table — skip; no affinity to enforce.
                    continue;
                }
                catch (IOException e)
                {
                    throw new FSReadError(e, reader.descriptor().fileName());
                }

                if (!isWritable(hint.mutation().getKeyspaceName()))
                    return Action.ABORT;

                hintsToSend.add(hint);
            }
        }

        int count = sameVersion ? buffersToSend.size() : hintsToSend.size();
        for (int i = 0; i < count; i++)
        {
            if (abortRequested.getAsBoolean())
            {
                HintDiagnostics.abortRequested(this);
                return Action.ABORT;
            }

            callbacks.add(sameVersion
                          ? sendEncodedHint(buffersToSend.get(i))
                          : sendHint(hintsToSend.get(i)));
        }
        return Action.CONTINUE;
    }

    private boolean isWritable(String keyspace)
    {
        return DatabaseDescriptor.getEndpointSnitch().filterByAffinityForWrites(keyspace).test(address);
    }

    private Callback sendHint(Hint hint)
    {
        Callback callback = new Callback(hint.creationTime);
        Message<?> message = Message.out(HINT_REQ, new HintMessage(hostId, hint));
        MessagingService.instance().sendWithCallback(message, address, callback);
        return callback;
    }

    private Callback sendEncodedHint(ByteBuffer hint)
    {
        HintMessage.Encoded message = new HintMessage.Encoded(hostId, hint, messagingVersion);
        Callback callback = new Callback(message.getHintCreationTime());
        MessagingService.instance().sendWithCallback(Message.out(HINT_REQ, message), address, callback);
        return callback;
    }

    static final class Callback implements RequestCallback
    {
        enum Outcome { SUCCESS, TIMEOUT, FAILURE, INTERRUPTED }

        private final long start = approxTime.now();
        private final Condition condition = newOneTimeCondition();
        private volatile Outcome outcome;
        private final long hintCreationNanoTime;

        private Callback(long hintCreationTimeMillisSinceEpoch)
        {
            this.hintCreationNanoTime = approxTime.translate().fromMillisSinceEpoch(hintCreationTimeMillisSinceEpoch);
        }

        Outcome await()
        {
            boolean timedOut;
            try
            {
                timedOut = !condition.awaitUntil(HINT_REQ.expiresAtNanos(start));
            }
            catch (InterruptedException e)
            {
                logger.warn("Hint dispatch was interrupted", e);
                return INTERRUPTED;
            }

            return timedOut ? TIMEOUT : outcome;
        }

        @Override
        public boolean invokeOnFailure()
        {
            return true;
        }

        @Override
        public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
        {
            outcome = FAILURE;
            condition.signalAll();
        }

        @Override
        public void onResponse(Message msg)
        {
            updateDelayMetrics(msg.from(), approxTime.now() - this.hintCreationNanoTime);
            outcome = SUCCESS;
            condition.signalAll();
        }
    }
}
