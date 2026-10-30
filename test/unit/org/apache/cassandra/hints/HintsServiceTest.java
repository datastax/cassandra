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

import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.utils.MoreFutures;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.locator.AbstractNetworkTopologySnitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.metrics.HintsServiceMetrics;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MockMessagingService;
import org.apache.cassandra.net.MockMessagingSpy;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.hints.HintsTestUtil.*;
import static org.junit.Assert.assertEquals;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;

import static org.apache.cassandra.Util.dk;
import static org.apache.cassandra.config.CassandraRelevantProperties.SKIP_REWRITING_HINTS_ON_HOST_LEFT;
import static org.apache.cassandra.net.Verb.HINT_REQ;
import static org.apache.cassandra.net.Verb.HINT_RSP;
import static org.apache.cassandra.net.MockMessagingService.verb;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class HintsServiceTest
{
    private static final String KEYSPACE = "hints_service_test";
    private static final String TABLE = "table";
    private final MockFailureDetector failureDetector = new MockFailureDetector();
    private static TableMetadata metadata;
    private final AtomicBoolean isAlive = new AtomicBoolean(true);
    private final List<InetAddressAndPort> members = new ArrayList<>();

    @BeforeClass
    public static void defineSchema()
    {
        SKIP_REWRITING_HINTS_ON_HOST_LEFT.setBoolean(true);
        SchemaLoader.prepareServer();
        StorageService.instance.initServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                KeyspaceParams.simple(1),
                SchemaLoader.standardCFMD(KEYSPACE, TABLE));
        metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
    }

    @AfterClass
    public static void tearDown()
    {
        System.clearProperty(SKIP_REWRITING_HINTS_ON_HOST_LEFT.getKey());
    }

    @After
    public void cleanup()
    {
        MockMessagingService.cleanup();
        members.forEach(StorageService.instance.getTokenMetadata()::removeEndpoint);
        members.clear();
    }

    @Before
    public void reinstanciateService() throws Throwable
    {
        MessagingService.instance().inboundSink.clear();
        MessagingService.instance().outboundSink.clear();

        // Hint service has to know the endpoint version to be able to send the hints. Otherwise,
        // it wouldn't be able to make a decision whether to decode (rewrap) or not.
        MessagingService.instance().versions.set(
            HintsEndpointProvider.instance.endpointForHost(StorageService.instance.getLocalHostUUID()),
            MessagingService.current_version);

        if (!HintsService.instance.isShutDown())
        {
            HintsService.instance.shutdownBlocking();
            HintsService.instance.deleteAllHints();
        }

        isAlive.set(true);

        HintsService.instance = new HintsService(e -> isAlive.get());

        HintsService.instance.startDispatch();
    }

    @Test
    public void testHintsDroppedForUnknownHost()
    {
        // pause the scheduled dispatch before writing hints
        HintsService.instance.pauseDispatch();

        long totalHints = StorageMetrics.totalHints.getCount();

        // write 100 hints on disk for host that is not part of cluster
        UUID randomHost = UUID.randomUUID();
        int numHints = 100;

        HintsStore store = writeAndFlushHints(metadata, randomHost, numHints);
        assertTrue(store.hasFiles());

        // metrics should have been updated with number of create hints
        assertEquals(totalHints + numHints, StorageMetrics.totalHints.getCount());

        // re-enable dispatching
        HintsService.instance.resumeDispatch();

        // trigger a manual dispatching on host that is not part of cluster: hints should be dropped
        HintsService.instance.dispatcherExecutor().dispatch(store);
        ConditionFactory hints_dropped = Awaitility.await("Hints dropped");
        hints_dropped.atMost(30, TimeUnit.SECONDS);
        hints_dropped.untilAsserted(() ->
                                    assertEquals(totalHints + numHints,
                                                 StorageMetrics.totalHints.getCount())
        );
    }

    @Test
    public void testCountingCorruptedHints()
    {
        // pause the scheduled dispatch before writing hints
        HintsService.instance.pauseDispatch();
        try
        {
            long hintsOnDisk0 = HintsServiceMetrics.hintsOnDisk.getCount();
            long corruptedHintsOnDisk0 = HintsServiceMetrics.corruptedHintsOnDisk.getCount();

            UUID randomHost1 = UUID.randomUUID();
            UUID randomHost2 = UUID.randomUUID();
            int numHints = 10;

            HintsStore store1 = writeAndFlushHints(metadata, randomHost1, numHints);
            HintsStore store2 = writeAndFlushHints(metadata, randomHost2, numHints);

            HintsDescriptor desc1 = store1.poll();
            store1.markCorrupted(desc1);

            assertThat(HintsServiceMetrics.hintsOnDisk.getCount()).isEqualTo(hintsOnDisk0 + 20);
            assertThat(HintsServiceMetrics.corruptedHintsOnDisk.getCount()).isEqualTo(corruptedHintsOnDisk0 + 10);

            store1.deleteAllHints();
            store2.deleteAllHints();

            assertThat(HintsServiceMetrics.hintsOnDisk.getCount()).isEqualTo(hintsOnDisk0);
            assertThat(HintsServiceMetrics.corruptedHintsOnDisk.getCount()).isEqualTo(corruptedHintsOnDisk0);
        }
        finally
        {
            // re-enable dispatching
            HintsService.instance.resumeDispatch();
        }

    }

    @Test
    public void testDispatchHints() throws InterruptedException, ExecutionException
    {
        long cnt = StorageMetrics.totalHints.getCount();

        // create spy for hint messages
        MockMessagingSpy spy = sendHintsAndResponses(metadata, 100, -1);

        // metrics should have been updated with number of create hints
        assertEquals(cnt + 100, StorageMetrics.totalHints.getCount());

        // wait until hints have been send
        spy.interceptMessageOut(100).get();
        spy.interceptNoMsg(500, TimeUnit.MILLISECONDS).get();
    }

    @Test
    public void testTransferHintsForDownHost() throws Exception
    {
        // only the transfer target is alive, the host the hints were written for is down
        UUID target = StorageService.instance.getLocalHostUUID();
        UUID downHost = addMember("127.0.0.2");
        restartWithLiveHosts(target);

        MockMessagingSpy spy = MockMessagingService.when(verb(HINT_REQ))
                                                   .respond(Message.internalResponse(HINT_RSP, NoPayload.noPayload));
        HintsStore store = writeAndFlushHints(downHost, 100);
        assertTrue(store.hasFiles());

        // the transfer must not be aborted because the host the hints are for is down (HCD-473)
        HintsService.instance.transferHints(() -> target).get(30, TimeUnit.SECONDS);

        spy.interceptMessageOut(100).get(30, TimeUnit.SECONDS);
        assertFalse(store.hasFiles());
    }

    @Test
    public void testTransferHintsWrittenDuringTransfer() throws Exception
    {
        UUID target = StorageService.instance.getLocalHostUUID();
        UUID downHost = addMember("127.0.0.2");
        restartWithLiveHosts(target);

        // this node keeps coordinating writes, and so writing hints, while the transfer runs: write some
        // while the first hint is in flight, into the buffer the transfer has to flush and close
        AtomicBoolean written = new AtomicBoolean();
        AtomicInteger delivered = new AtomicInteger();
        MockMessagingService.when(verb(HINT_REQ)).respond((message, to) -> {
            if (written.compareAndSet(false, true))
                writeHints(downHost, 50);
            delivered.incrementAndGet();
            return Message.internalResponse(HINT_RSP, NoPayload.noPayload);
        });
        HintsStore store = writeAndFlushHints(downHost, 100);

        // the transfer completes only once every hint it sent was acknowledged, so the count is final here
        HintsService.instance.transferHints(() -> target).get(30, TimeUnit.SECONDS);

        assertEquals(150, delivered.get());
        assertFalse(store.hasFiles());
        assertFalse(store.isWriting());
    }

    @Test
    public void testTransferNotFailedByHintsFilesClosedDuringTransfer() throws Exception
    {
        UUID target = StorageService.instance.getLocalHostUUID();
        UUID downHost1 = addMember("127.0.0.2");
        UUID downHost2 = addMember("127.0.0.3");
        restartWithLiveHosts(target);

        // Whenever the transfer moves on to a store, give the other one a new file, as the dispatch trigger does
        // when it closes the writer of a live host's store: whichever store goes first, it has a file again once
        // the round is over. Do it twice per store, so that the old success criterion fails the first attempt
        // and the retry too.
        Map<UUID, AtomicInteger> newFiles = ImmutableMap.of(downHost1, new AtomicInteger(), downHost2, new AtomicInteger());
        AtomicReference<UUID> current = new AtomicReference<>();
        AtomicInteger delivered = new AtomicInteger();
        MockMessagingService.when(verb(HINT_REQ)).respond((message, to) -> {
            UUID hinted = hostIdOf(message);
            if (!hinted.equals(current.getAndSet(hinted)))
            {
                UUID other = hinted.equals(downHost1) ? downHost2 : downHost1;
                if (newFiles.get(other).getAndIncrement() < 2)
                    writeAndFlushHints(other, 10);
            }
            delivered.incrementAndGet();
            return Message.internalResponse(HINT_RSP, NoPayload.noPayload);
        });
        writeAndFlushHints(downHost1, 10);
        writeAndFlushHints(downHost2, 10);

        HintsService.instance.transferHints(() -> target).get(30, TimeUnit.SECONDS);

        assertEquals(60, delivered.get());
        assertFalse(HintsService.instance.getCatalog().hasFiles());
    }

    @Test
    public void testTransferWaitsForDispatchInFlight() throws Exception
    {
        UUID target = StorageService.instance.getLocalHostUUID();
        UUID downHost = addMember("127.0.0.2");
        restartWithLiveHosts(target);
        HintsDispatchExecutor executor = HintsService.instance.dispatcherExecutor();
        // the held dispatch needs a delivery thread of its own next to the transfer's
        assertTrue(executor.getDispatcherCorePoolSize() >= 2);

        // Hold the dispatch of the target's own hints in flight: the transfer must wait for it before delivering
        // anything, or that dispatch could put a file it failed to deliver back after the transfer had found the
        // store empty. The held hints must not time out meanwhile, which would end that dispatch by itself.
        DurationSpec.LongMillisecondsBound savedTimeout = DatabaseDescriptor.getRawConfig().hints_request_timeout;
        DatabaseDescriptor.getRawConfig().hints_request_timeout = new DurationSpec.LongMillisecondsBound("60s");
        HintsStore targetStore = writeAndFlushHints(target, 10);
        writeAndFlushHints(downHost, 10);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger targetDelivered = new AtomicInteger();
        AtomicBoolean transferWaited = new AtomicBoolean(true);
        MockMessagingService.when(verb(HINT_REQ)).respond((message, to) -> {
            if (hostIdOf(message).equals(target))
            {
                Uninterruptibles.awaitUninterruptibly(release);
                targetDelivered.incrementAndGet();
            }
            else if (targetDelivered.get() != 10 || executor.isScheduled(targetStore))
                transferWaited.set(false);
            return Message.internalResponse(HINT_RSP, NoPayload.noPayload);
        });

        ExecutorService thread = Executors.newSingleThreadExecutor();
        try
        {
            executor.dispatch(targetStore);
            // transferHints() waits for the held dispatch on its caller's thread, once the transfer is registered
            java.util.concurrent.Future<Future> transfer = thread.submit(() -> HintsService.instance.transferHints(() -> target));
            Awaitility.await().atMost(30, TimeUnit.SECONDS).until(executor::isTransferring);
            release.countDown();
            transfer.get(30, TimeUnit.SECONDS).get(30, TimeUnit.SECONDS);
        }
        finally
        {
            release.countDown();
            thread.shutdownNow();
            DatabaseDescriptor.getRawConfig().hints_request_timeout = savedTimeout;
        }

        assertTrue(transferWaited.get());
        assertEquals(10, targetDelivered.get());
        assertFalse(HintsService.instance.getCatalog().hasFiles());
        assertFalse(executor.isTransferring());
    }

    @Test
    public void testNoDispatchScheduledDuringTransfer() throws Exception
    {
        UUID target = StorageService.instance.getLocalHostUUID();
        UUID downHost = addMember("127.0.0.2");
        restartWithLiveHosts(target);
        HintsDispatchExecutor executor = HintsService.instance.dispatcherExecutor();

        // While the transfer runs, hints for the target itself are written and their dispatch is asked for, as the
        // trigger does for a live host: nothing is scheduled, the transfer picks them up in its next round.
        AtomicInteger delivered = new AtomicInteger();
        AtomicBoolean asked = new AtomicBoolean();
        AtomicBoolean scheduled = new AtomicBoolean();
        MockMessagingService.when(verb(HINT_REQ)).respond((message, to) -> {
            if (asked.compareAndSet(false, true))
            {
                HintsStore targetStore = writeAndFlushHints(target, 10);
                executor.dispatch(targetStore);
                scheduled.set(executor.isScheduled(targetStore));
            }
            delivered.incrementAndGet();
            return Message.internalResponse(HINT_RSP, NoPayload.noPayload);
        });
        writeAndFlushHints(downHost, 10);

        HintsService.instance.transferHints(() -> target).get(30, TimeUnit.SECONDS);

        assertTrue(asked.get());
        assertFalse(scheduled.get());
        assertEquals(20, delivered.get());
        assertFalse(HintsService.instance.getCatalog().hasFiles());
    }

    @Test
    public void testTransferSucceedsWhileHintsKeepComing() throws Exception
    {
        UUID target = StorageService.instance.getLocalHostUUID();
        UUID downHost = addMember("127.0.0.2");
        restartWithLiveHosts(target);

        // A coordinator under load never stops writing hints for a down replica: every round finds one more.
        // Write one during the first round, then one per delivery, so that no round ever finds the store empty.
        AtomicInteger delivered = new AtomicInteger();
        MockMessagingService.when(verb(HINT_REQ)).respond((message, to) -> {
            int n = delivered.incrementAndGet();
            if (n == 1 || n > 100)
                writeHints(downHost, 1);
            return Message.internalResponse(HINT_RSP, NoPayload.noPayload);
        });
        HintsStore store = writeAndFlushHints(downHost, 100);

        // the transfer must not fail because of them: it stops after its rounds, with every hint it found delivered
        HintsService.instance.transferHints(() -> target).get(30, TimeUnit.SECONDS);

        assertEquals(100 + HintsDispatchExecutor.MAX_TRANSFER_ROUNDS - 1, delivered.get());
        assertTrue(store.hasFiles());
    }

    @Test
    public void testTransferRetriesWithAnotherTargetWhenTargetLeft() throws Exception
    {
        UUID target = StorageService.instance.getLocalHostUUID();
        UUID leftTarget = UUID.randomUUID();
        UUID downHost = addMember("127.0.0.2");
        restartWithLiveHosts(target, leftTarget);

        AtomicInteger delivered = new AtomicInteger();
        MockMessagingService.when(verb(HINT_REQ)).respond((message, to) -> {
            delivered.incrementAndGet();
            return Message.internalResponse(HINT_RSP, NoPayload.noPayload);
        });
        HintsStore store = writeAndFlushHints(downHost, 100);

        // the first target cannot be resolved any more: the hints must neither be dropped nor rewritten for it,
        // the transfer retries with the next target (after its 10 seconds delay)
        AtomicInteger targets = new AtomicInteger();
        HintsService.instance.transferHints(() -> targets.getAndIncrement() == 0 ? leftTarget : target).get(60, TimeUnit.SECONDS);

        assertEquals(2, targets.get());
        assertEquals(100, delivered.get());
        assertFalse(store.hasFiles());
        assertFalse(HintsService.instance.dispatcherExecutor().isTransferring());
    }

    @Test
    public void testTransferRetriesWhenTargetVersionIsUnknown() throws Exception
    {
        UUID target = StorageService.instance.getLocalHostUUID();
        UUID unknownTarget = addMember("127.0.0.3");
        UUID downHost = addMember("127.0.0.2");
        restartWithLiveHosts(target, unknownTarget);
        MessagingService.instance().versions.reset(HintsEndpointProvider.instance.endpointForHost(unknownTarget));

        AtomicInteger delivered = new AtomicInteger();
        MockMessagingService.when(verb(HINT_REQ)).respond((message, to) -> {
            delivered.incrementAndGet();
            return Message.internalResponse(HINT_RSP, NoPayload.noPayload);
        });
        HintsStore store = writeAndFlushHints(downHost, 100);

        // the messaging version of the first target is not known: a failure, and the file must go back to the
        // store, so that the retry with the next target finds it
        AtomicInteger targets = new AtomicInteger();
        HintsService.instance.transferHints(() -> targets.getAndIncrement() == 0 ? unknownTarget : target).get(60, TimeUnit.SECONDS);

        assertEquals(2, targets.get());
        assertEquals(100, delivered.get());
        assertFalse(store.hasFiles());
    }

    @Test
    public void testTransferSkipsCorruptedFile() throws Exception
    {
        UUID target = StorageService.instance.getLocalHostUUID();
        UUID downHost = addMember("127.0.0.2");
        restartWithLiveHosts(target);

        AtomicInteger delivered = new AtomicInteger();
        MockMessagingService.when(verb(HINT_REQ)).respond((message, to) -> {
            delivered.incrementAndGet();
            return Message.internalResponse(HINT_RSP, NoPayload.noPayload);
        });
        HintsStore store = writeAndFlushHints(downHost, 10);
        HintsDescriptor corrupted = store.poll();
        store.offerFirst(corrupted);
        writeAndFlushHints(downHost, 10);
        File file = corrupted.file(DatabaseDescriptor.getHintsDirectory());
        Files.write(file.toPath(), new byte[(int) file.length()]);

        // a corrupted file can be transferred by no one: it is set aside, and the transfer carries on with the next
        // file within the same attempt (a retry asks the supplier again)
        AtomicInteger targets = new AtomicInteger();
        HintsService.instance.transferHints(() -> { targets.incrementAndGet(); return target; }).get(30, TimeUnit.SECONDS);

        assertEquals(1, targets.get());
        assertEquals(10, delivered.get());
        assertFalse(store.hasFiles());
        assertEquals(1, store.getCorruptedFilesNum());
    }

    @Test
    public void testTransferWaitsForRunningTransfer() throws Exception
    {
        UUID target = StorageService.instance.getLocalHostUUID();
        UUID downHost = addMember("127.0.0.2");
        restartWithLiveHosts(target);

        // hold the first transfer at its first deliveries
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger delivered = new AtomicInteger();
        MockMessagingService.when(verb(HINT_REQ)).respond((message, to) -> {
            Uninterruptibles.awaitUninterruptibly(release);
            delivered.incrementAndGet();
            return Message.internalResponse(HINT_RSP, NoPayload.noPayload);
        });
        HintsStore store = writeAndFlushHints(downHost, 10);
        Future first = HintsService.instance.transferHints(() -> target);

        // A decommission retried while the transfer of the previous attempt still runs waits for it, rather than
        // running a second transfer alongside or refusing. The wait on the first transfer is the only wait on the
        // second call's way, hence the thread state check.
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread second = new Thread(() -> {
            try
            {
                HintsService.instance.transferHints(() -> target).get(30, TimeUnit.SECONDS);
            }
            catch (Throwable t)
            {
                failure.set(t);
            }
        });
        try
        {
            second.start();
            Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> second.getState() == Thread.State.WAITING);
            assertEquals(0, delivered.get());
            release.countDown();
            first.get(30, TimeUnit.SECONDS);
            second.join(TimeUnit.SECONDS.toMillis(30));
        }
        finally
        {
            release.countDown();
        }

        assertFalse(second.isAlive());
        assertNull(failure.get());
        assertEquals(10, delivered.get());
        assertFalse(store.hasFiles());
        assertFalse(HintsService.instance.dispatcherExecutor().isTransferring());
    }

    @Test
    public void testTransferDropsHintsForLeftHost() throws Exception
    {
        UUID target = StorageService.instance.getLocalHostUUID();
        UUID downHost = addMember("127.0.0.2");
        UUID leftHost = UUID.randomUUID();
        restartWithLiveHosts(target);

        AtomicInteger delivered = new AtomicInteger();
        MockMessagingService.when(verb(HINT_REQ)).respond((message, to) -> {
            delivered.incrementAndGet();
            return Message.internalResponse(HINT_RSP, NoPayload.noPayload);
        });
        HintsStore downStore = writeAndFlushHints(downHost, 10);
        HintsStore leftStore = writeAndFlushHints(leftHost, 100);

        // hints for a host that left are not handed to the target, which could never dispatch them: with
        // SKIP_REWRITING_HINTS_ON_HOST_LEFT set they are dropped, as their own dispatch would
        HintsService.instance.transferHints(() -> target).get(30, TimeUnit.SECONDS);

        assertEquals(10, delivered.get());
        assertFalse(downStore.hasFiles());
        assertFalse(leftStore.hasFiles());
    }

    @Test
    public void testTransferWithNoHints() throws Exception
    {
        UUID target = StorageService.instance.getLocalHostUUID();
        restartWithLiveHosts(target);

        // nothing to transfer: done at once, no target is even looked up
        HintsService.instance.transferHints(() -> { throw new AssertionError("no target needed"); }).get(5, TimeUnit.SECONDS);
        assertFalse(HintsService.instance.dispatcherExecutor().isTransferring());
    }

    /**
     * Registers a member of the cluster hints can be written for: a host the endpoint provider resolves, unlike
     * a random host id, which is a host that left. Whether it is alive is up to the liveness predicate.
     */
    private UUID addMember(String address) throws UnknownHostException
    {
        InetAddressAndPort endpoint = InetAddressAndPort.getByName(address);
        UUID hostId = UUID.randomUUID();
        TokenMetadata tokenMetadata = StorageService.instance.getTokenMetadata();
        tokenMetadata.updateHostId(hostId, endpoint);
        tokenMetadata.updateNormalToken(DatabaseDescriptor.getPartitioner().getRandomToken(), endpoint);
        members.add(endpoint);
        return hostId;
    }

    private void restartWithLiveHosts(UUID... liveHosts) throws Exception
    {
        Set<UUID> live = ImmutableSet.copyOf(liveHosts);
        HintsService.instance.shutdownBlocking();
        HintsService.instance = new HintsService(live::contains);
        HintsService.instance.startDispatch();
    }

    private static UUID hostIdOf(Message<?> message)
    {
        return message.payload instanceof HintMessage ? ((HintMessage) message.payload).hostId
                                                      : ((HintMessage.Encoded) message.payload).hostId();
    }

    @Test
    public void testPauseAndResume() throws InterruptedException, ExecutionException
    {
        HintsService.instance.pauseDispatch();

        // create spy for hint messages
        MockMessagingSpy spy = sendHintsAndResponses(metadata, 100, -1);

        // we should not send any hints while paused
        ListenableFuture<Boolean> noMessagesWhilePaused = spy.interceptNoMsg(15, TimeUnit.SECONDS);
        Futures.addCallback(noMessagesWhilePaused, new MoreFutures.SuccessCallback<Boolean>()
        {
            public void onSuccess(@Nullable Boolean aBoolean)
            {
                HintsService.instance.resumeDispatch();
            }
        }, MoreExecutors.directExecutor());

        Futures.allAsList(
                noMessagesWhilePaused,
                spy.interceptMessageOut(100),
                spy.interceptNoMsg(200, TimeUnit.MILLISECONDS)
        ).get();
    }

    @Test
    public void testPageRetry() throws InterruptedException, ExecutionException, TimeoutException
    {
        // create spy for hint messages, but only create responses for 5 hints
        MockMessagingSpy spy = sendHintsAndResponses(metadata, 20, 5);

        Futures.allAsList(
                // the dispatcher will always send all hints within the current page
                // and only wait for the acks before going to the next page
                spy.interceptMessageOut(20),
                spy.interceptNoMsg(200, TimeUnit.MILLISECONDS),

                // next tick will trigger a retry of the same page as we only replied with 5/20 acks
                spy.interceptMessageOut(20)
        ).get();

        // marking the destination node as dead should stop sending hints
        isAlive.set(false);
        spy.interceptNoMsg(20, TimeUnit.SECONDS).get();
    }

    @Test
    public void testPageSeek() throws InterruptedException, ExecutionException
    {
        // create spy for hint messages, stop replying after 12k (should be on 3rd page)
        MockMessagingSpy spy = sendHintsAndResponses(metadata, 20000, 12000);

        // At this point the dispatcher will constantly retry the page we stopped acking,
        // thus we receive the same hints from the page multiple times and in total more than
        // all written hints. Lets just consume them for a while and then pause the dispatcher.
        spy.interceptMessageOut(22000).get();
        HintsService.instance.pauseDispatch();
        Thread.sleep(1000);

        // verify that we have a dispatch offset set for the page we're currently stuck at
        HintsStore store = HintsService.instance.getCatalog().get(StorageService.instance.getLocalHostUUID());
        HintsDescriptor descriptor = store.poll();
        store.offerFirst(descriptor); // add again for cleanup during re-instanciation
        InputPosition dispatchOffset = store.getDispatchOffset(descriptor);
        assertTrue(dispatchOffset != null);
        assertTrue(((ChecksummedDataInput.Position) dispatchOffset).sourcePosition > 0);
    }

    @Test
    public void testDeleteHintsForEndpoint() throws UnknownHostException
    {
        int numHints = 10;
        TokenMetadata tokenMeta = StorageService.instance.getTokenMetadata();
        InetAddressAndPort endpointToDeleteHints = InetAddressAndPort.getByName("1.1.1.1");
        UUID hostIdToDeleteHints = UUID.randomUUID();
        tokenMeta.updateHostId(hostIdToDeleteHints, endpointToDeleteHints);
        InetAddressAndPort anotherEndpoint = InetAddressAndPort.getByName("1.1.1.2");
        UUID anotherHostId = UUID.randomUUID();
        tokenMeta.updateHostId(anotherHostId, anotherEndpoint);

        HintsStore storeToDeleteHints = writeAndFlushHints(metadata, hostIdToDeleteHints, numHints);
        assertTrue(storeToDeleteHints.hasFiles());
        HintsStore anotherStore = writeAndFlushHints(metadata, anotherHostId, numHints);
        assertTrue(anotherStore.hasFiles());
        assertThat(HintsService.instance.getTotalFilesNum()).isEqualTo(2);
        assertThat(HintsService.instance.getCorruptedFilesNum()).isEqualTo(0);
        assertThat(HintsServiceMetrics.hintsOnDisk.getCount()).isEqualTo(20);
        assertThat(HintsServiceMetrics.corruptedHintsOnDisk.getCount()).isZero();

        HintsService.instance.deleteAllHintsForEndpoint(endpointToDeleteHints);
        assertFalse(storeToDeleteHints.hasFiles());
        assertTrue(anotherStore.hasFiles());
        assertTrue(HintsService.instance.getCatalog().hasFiles());
        assertThat(HintsServiceMetrics.hintsOnDisk.getCount()).isEqualTo(10);
        assertThat(HintsServiceMetrics.corruptedHintsOnDisk.getCount()).isZero();

        HintsService.instance.deleteAllHints();
        assertEquals(0, HintsService.instance.getTotalHintsSize());
        assertFalse(anotherStore.hasFiles());
        assertThat(HintsServiceMetrics.hintsOnDisk.getCount()).isEqualTo(0);
        assertThat(HintsServiceMetrics.corruptedHintsOnDisk.getCount()).isZero();
    }

    @Test
    public void testReplayWaitsForWriteAffinity() throws Exception
    {
        UUID hostId = StorageService.instance.getLocalHostUUID();
        InetAddressAndPort endpoint = HintsEndpointProvider.instance.endpointForHost(hostId);
        IEndpointSnitch savedSnitch = DatabaseDescriptor.getEndpointSnitch();

        try
        {
            DatabaseDescriptor.setEndpointSnitch(new WriteAffinityExcludingSnitch(endpoint));
            HintsStore store = writeAndFlushHints(hostId, 1);
            MockMessagingSpy spy = MockMessagingService.when(verb(HINT_REQ))
                                                       .respond(Message.internalResponse(HINT_RSP, NoPayload.noPayload));

            HintsService.instance.dispatcherExecutor().dispatch(store).get();
            spy.interceptNoMsg(500, TimeUnit.MILLISECONDS).get();
            assertTrue(store.hasFiles());

            DatabaseDescriptor.setEndpointSnitch(savedSnitch);
            HintsService.instance.dispatcherExecutor().dispatch(store).get();
            spy.interceptMessageOut(1).get();
            assertFalse(store.hasFiles());
        }
        finally
        {
            DatabaseDescriptor.setEndpointSnitch(savedSnitch);
        }
    }

    private static class WriteAffinityExcludingSnitch extends AbstractNetworkTopologySnitch
    {
        private final InetAddressAndPort excluded;

        WriteAffinityExcludingSnitch(InetAddressAndPort excluded)
        {
            this.excluded = excluded;
        }

        public String getRack(InetAddressAndPort endpoint)
        {
            return "rack1";
        }

        public String getDatacenter(InetAddressAndPort endpoint)
        {
            return "datacenter1";
        }

        @Override
        public Predicate<InetAddressAndPort> filterByAffinityForWrites(String keyspace)
        {
            return endpoint -> !endpoint.equals(excluded);
        }
    }

    private MockMessagingSpy sendHintsAndResponses(TableMetadata ignored, int noOfHints, int noOfResponses)
    {
        return sendHintsAndResponses(noOfHints, noOfResponses);
    }

    private MockMessagingSpy sendHintsAndResponses(int noOfHints, int noOfResponses)
    {
        // create spy for hint messages, but only create responses for noOfResponses hints
        Message<NoPayload> message = Message.internalResponse(HINT_RSP, NoPayload.noPayload);

        MockMessagingSpy spy;
        if (noOfResponses != -1)
        {
            spy = MockMessagingService.when(verb(HINT_REQ)).respondN(message, noOfResponses);
        }
        else
        {
            spy = MockMessagingService.when(verb(HINT_REQ)).respond(message);
        }

        writeHints(StorageService.instance.getLocalHostUUID(), noOfHints);
        return spy;
    }

    private HintsStore writeAndFlushHints(TableMetadata ignored, UUID hostId, int noOfHints)
    {
        return writeAndFlushHints(hostId, noOfHints);
    }

    private HintsStore writeAndFlushHints(UUID hostId, int noOfHints)
    {
        writeHints(hostId, noOfHints);
        HintsService.instance.flushAndFsyncBlockingly(Collections.singleton(hostId));

        // close the write so hints are available for dispatching
        HintsStore store = HintsService.instance.getCatalog().get(hostId);
        store.closeWriter();

        return store;
    }

    private void writeHints(UUID hostId, int noOfHints)
    {
        // create and write noOfHints using service
        for (int i = 0; i < noOfHints; i++)
        {
            long now = System.currentTimeMillis();
            DecoratedKey dkey = dk(String.valueOf(i));
            TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
            PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, dkey).timestamp(now);
            builder.row("column0").add("val", "value0");
            Hint hint = Hint.create(builder.buildAsMutation(), now);
            HintsService.instance.write(hostId, hint);
        }
    }
}
