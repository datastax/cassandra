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

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.StorageCompatibilityMode;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.Promise;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

import static org.apache.cassandra.config.CassandraRelevantProperties.SKIP_REWRITING_HINTS_ON_HOST_LEFT;

/**
 * A multi-threaded (by default) executor for dispatching hints.
 *
 * Most of dispatch is triggered by {@link HintsDispatchTrigger} running every ~10 seconds.
 */
final class HintsDispatchExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(HintsDispatchExecutor.class);

    private final File hintsDirectory;
    private final ExecutorPlus executor;
    private final AtomicBoolean isPaused;
    private final Predicate<UUID> isAlive;
    private final Map<UUID, Future> scheduledDispatches;

    /*
     * The running transfer, if any: a transfer handles every store itself, so no dispatch is scheduled while it
     * runs, and a transfer starts only once the dispatches already scheduled have completed. The lock orders
     * the scheduling of a dispatch against the start of a transfer.
     */
    private final Object transferLock = new Object();
    private Promise<Void> transfer; // guarded by transferLock

    /*
     * Bounds the rounds of a transfer attempt. Hints may keep being written for as long as this node coordinates,
     * and every round can find some; see TransferHintsTask.transfer for what happens when the rounds are used up.
     */
    static final int MAX_TRANSFER_ROUNDS = 10;

    HintsDispatchExecutor(File hintsDirectory, int maxThreads, AtomicBoolean isPaused, Predicate<UUID> isAlive)
    {
        this.hintsDirectory = hintsDirectory;
        this.isPaused = isPaused;
        this.isAlive = isAlive;

        scheduledDispatches = new ConcurrentHashMap<>();
        executor = executorFactory()
                .withJmxInternal()
                .configurePooled("HintsDispatcher", maxThreads)
                .withThreadPriority(Thread.MIN_PRIORITY)
                .build();
    }

    /*
     * It's safe to terminate dispatch in process and to deschedule dispatch.
     */
    void shutdownBlocking()
    {
        scheduledDispatches.clear();
        executor.shutdownNow();
        try
        {
            executor.awaitTermination(1, TimeUnit.MINUTES);
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
    }

    boolean isScheduled(HintsStore store)
    {
        return scheduledDispatches.containsKey(store.hostId);
    }

    Future dispatch(HintsStore store)
    {
        return dispatch(store, store.hostId);
    }

    /**
     * Schedules the dispatch of the store's hints, unless one is scheduled already.
     *
     * @return the future of the dispatch of this store: the one scheduled now or before, or the running transfer,
     *         which handles every store itself; nothing is scheduled while a transfer runs, since a dispatch it
     *         did not run could put a file it failed to deliver back after the transfer had found the store empty
     */
    Future dispatch(HintsStore store, UUID hostId)
    {
        /*
         * It is safe to perform dispatch for the same host id concurrently in two or more threads,
         * however there is nothing to win from it - so we don't.
         *
         * Additionally, having just one dispatch task per host id ensures that we'll never violate our per-destination
         * rate limit, without having to share a ratelimiter between threads.
         *
         * It also simplifies reasoning about dispatch sessions.
         */
        synchronized (transferLock)
        {
            if (transfer != null && !transfer.isDone())
                return transfer;
            return scheduledDispatches.computeIfAbsent(hostId, uuid -> executor.submit(new DispatchHintsTask(store, hostId)));
        }
    }

    /**
     * Transfers all the hints in the catalog to the host supplied by hostIdSupplier.
     *
     * Waits for the transfer already running, if any (a decommission retried while the transfer of the previous
     * attempt still runs), then for the dispatches already scheduled; no dispatch is scheduled from then on
     * until the transfer is done, see {@link #dispatch(HintsStore, UUID)}.
     *
     * @param closeWriters flushes the hints written so far and closes their writers, so that they are in files
     *                     the next round can transfer
     * @return a future that succeeds once every hint present when a round started has been delivered (see
     *         {@link TransferHintsTask#transfer(UUID, InetAddressAndPort)}), at once if there is nothing to
     *         transfer, and fails if a delivery failed on two targets in a row, or if no target could be supplied.
     *         Cancelling it does nothing: the transfer runs until done.
     * @throws RuntimeException if closing the writers fails, or the executor refuses the task (shut down)
     */
    Future transfer(HintsCatalog catalog, Supplier<UUID> hostIdSupplier, Runnable closeWriters)
    {
        Promise<Void> transfer = AsyncPromise.uncancellable();
        for (Future previous = register(transfer); previous != null; previous = register(transfer))
        {
            logger.info("Waiting for the hints transfer already running before starting a new one");
            awaitQuietly(previous);
        }

        try
        {
            // wait for the dispatches already scheduled, then make a file of every hint written so far
            scheduledDispatches.values().forEach(this::awaitQuietly);
            closeWriters.run();
            if (!catalog.hasFiles())
            {
                logger.info("No hints to transfer");
                transfer.setSuccess(null);
                return transfer;
            }
            executor.submit(new TransferHintsTask(catalog, hostIdSupplier, closeWriters, transfer));
        }
        catch (Throwable t)
        {
            transfer.tryFailure(t);
            throw t;
        }
        return transfer;
    }

    /**
     * @return null if the transfer is now the running one, else the transfer still running, to wait for first
     */
    private Future register(Promise<Void> transfer)
    {
        synchronized (transferLock)
        {
            if (this.transfer != null && !this.transfer.isDone())
                return this.transfer;
            this.transfer = transfer;
            return null;
        }
    }

    boolean isTransferring()
    {
        synchronized (transferLock)
        {
            return transfer != null && !transfer.isDone();
        }
    }

    /**
     * Waits for the future, whatever its outcome: a failed dispatch has logged its failure already, and must
     * not fail the transfer waiting for it to finish.
     */
    private void awaitQuietly(Future future)
    {
        try
        {
            future.get();
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
        catch (ExecutionException | CancellationException e)
        {
            logger.debug("A hints dispatch the transfer waited for did not complete: {}", e.toString());
        }
    }

    void completeDispatchBlockingly(HintsStore store)
    {
        Future future = scheduledDispatches.get(store.hostId);
        try
        {
            if (future != null)
                future.get();
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    void interruptDispatch(UUID hostId)
    {
        Future future = scheduledDispatches.remove(hostId);

        if (null != future)
            future.cancel(true);
    }

    private final class TransferHintsTask implements Runnable
    {
        private final HintsCatalog catalog;
        private final Runnable closeWriters;
        private final Promise<Void> result;

        /*
         * Supplies target hosts to stream to. Generally returns the one the DynamicSnitch thinks is closest.
         * We use a supplier here to be able to get a new host if the current one dies during streaming.
         */
        private final Supplier<UUID> hostIdSupplier;

        private TransferHintsTask(HintsCatalog catalog, Supplier<UUID> hostIdSupplier, Runnable closeWriters, Promise<Void> result)
        {
            this.catalog = catalog;
            this.hostIdSupplier = hostIdSupplier;
            this.closeWriters = closeWriters;
            this.result = result;
        }

        @Override
        public void run()
        {
            try
            {
                transfer();
                result.setSuccess(null);
            }
            catch (Throwable t)
            {
                result.setFailure(t);
            }
        }

        private void transfer()
        {
            UUID hostId = hostIdSupplier.get();
            InetAddressAndPort address = HintsEndpointProvider.instance.endpointForHost(hostId);
            logger.info("Transferring all hints to {}: {}", address, hostId);
            if (transfer(hostId, address))
            {
                logger.info("Finished transferring hints to {}: {}", address, hostId);
                return;
            }

            logger.warn("Failed to transfer all hints to {}: {}; will retry in {} seconds", address, hostId, 10);

            try
            {
                TimeUnit.SECONDS.sleep(10);
            }
            catch (InterruptedException e)
            {
                throw new UncheckedInterruptedException(e);
            }

            hostId = hostIdSupplier.get();
            address = HintsEndpointProvider.instance.endpointForHost(hostId);
            logger.info("Transferring all hints to {}: {}", address, hostId);
            if (!transfer(hostId, address))
            {
                logger.error("Failed to transfer all hints to {}: {}", address, hostId);
                throw new RuntimeException("Failed to transfer all hints to " + address + ": " + hostId);
            }
            logger.info("Finished transferring hints to {}: {}", address, hostId);
        }

        /*
         * This node keeps coordinating writes, and so writing hints, while the transfer runs: into the buffer
         * pool and the open writers, which a round never sees. Close the writers after each round and go round
         * again, so that the next round picks up what was written during the previous one; a round that finds
         * nothing ends the transfer.
         *
         * Hints may keep coming for as long as the node coordinates, so the rounds are bounded, and using them up
         * is still a success: every hint present when a round started was delivered, and only a failed delivery
         * fails the transfer. What was written since is left for a later transfer; decommission runs one more
         * once its client servers are shut down and nothing coordinates writes any more.
         */
        private boolean transfer(UUID hostId, InetAddressAndPort address)
        {
            for (int round = 0; round < MAX_TRANSFER_ROUNDS; round++)
            {
                // dispatchAllForTransfer(), not run(): these tasks are not scheduled, and run() would remove the
                // scheduled dispatch of the transfer target's own hints, were there one. allMatch stops at the
                // first store that could not be delivered, as intended: the retry, with another target, starts
                // over from every store.
                boolean delivered = catalog.stores()
                                           .map(store -> new DispatchHintsTask(store, hostId, true))
                                           .allMatch(DispatchHintsTask::dispatchAllForTransfer);
                if (!delivered)
                    return false;

                closeWriters.run();
                if (!catalog.hasFiles())
                    return true;
            }

            logger.warn("Hints kept being written while transferring them to {}: {}; stopping after {} rounds, " +
                        "the hints written since are left on this node for a later transfer", address, hostId, MAX_TRANSFER_ROUNDS);
            return true;
        }
    }

    private final class DispatchHintsTask implements Runnable
    {
        private final HintsStore store;
        private final UUID hostId;
        private final RateLimiter rateLimiter;
        private final boolean isTransfer;

        DispatchHintsTask(HintsStore store, UUID hostId, boolean isTransfer)
        {
            this.store = store;
            this.hostId = hostId;
            this.rateLimiter = HintsRateLimiterFactory.instance.create(hostId, isTransfer);
            this.isTransfer = isTransfer;
        }

        DispatchHintsTask(HintsStore store, UUID hostId)
        {
            this(store, hostId, false);
        }

        public void run()
        {
            try
            {
                dispatchAll();
            }
            finally
            {
                scheduledDispatches.remove(hostId);
            }
        }

        /*
         * @return true if every file of the store was dispatched, false if dispatch was paused or failed.
         */
        private boolean dispatchAll()
        {
            while (true)
            {
                if (isPaused.get())
                    return false;

                HintsDescriptor descriptor = store.poll();
                if (descriptor == null)
                    return true;

                try
                {
                    if (!dispatch(descriptor))
                        return false;
                }
                catch (FSReadError e)
                {
                    logger.error(String.format("Failed to dispatch hints file %s: file is corrupted", descriptor.fileName()), e);
                    store.cleanUp(descriptor);
                    store.markCorrupted(descriptor);
                    throw e;
                }
            }
        }

        /*
         * dispatchAll() for a transfer: a corrupted file has been set aside by the time its FSReadError gets
         * here, so carry on with the next one instead of failing the transfer.
         */
        private boolean dispatchAllForTransfer()
        {
            while (true)
            {
                try
                {
                    return dispatchAll();
                }
                catch (FSReadError e)
                {
                    // logged, cleaned up and marked corrupted by dispatchAll()
                }
            }
        }

        /*
         * Will return true if dispatch was successful, false if we hit a failure (destination node went down, for example).
         */
        private boolean dispatch(HintsDescriptor descriptor)
        {
            logger.debug("Dispatching hints file {}", descriptor.fileName());

            InetAddressAndPort address = HintsEndpointProvider.instance.endpointForHost(hostId);
            if (address == null)
            {
                if (isTransfer)
                {
                    // the transfer target left the cluster: a failure, so that the transfer retries with another one
                    logger.warn("Hints transfer target {} is no longer a member of the cluster", hostId);
                    store.offerFirst(descriptor);
                    return false;
                }

                // the host the hints are for no longer exists; find a new home for each hint entry
                dropOrConvert(descriptor);
                return true;
            }

            // Hints for a host that left the cluster are not transferred: the target would keep them as orphan
            // files, since it never dispatches to a host it cannot resolve. Give them a new home here instead,
            // as their own dispatch would.
            if (isTransfer && HintsEndpointProvider.instance.endpointForHost(descriptor.hostId) == null)
            {
                dropOrConvert(descriptor);
                return true;
            }

            return deliver(descriptor, address);
        }

        private void dropOrConvert(HintsDescriptor descriptor)
        {
            if (SKIP_REWRITING_HINTS_ON_HOST_LEFT.getBoolean())
            {
                logger.debug("Host {} is no longer a member of cluster, dropping hints", descriptor.hostId);
                store.cleanUp(descriptor);
                store.delete(descriptor);
            }
            else
                convert(descriptor);
        }

        private boolean deliver(HintsDescriptor descriptor, InetAddressAndPort address)
        {
            File file = descriptor.file(hintsDirectory);
            InputPosition offset = store.getDispatchOffset(descriptor);

            // Abort when the node we deliver to goes down. On a transfer (decommission) that is the transfer
            // target, not descriptor.hostId: the node the hints were written for is typically down, which is
            // why the hints exist, and testing it would abort every transfer of such hints.
            BooleanSupplier shouldAbort = () -> !isAlive.test(hostId) || isPaused.get();

            Optional<Integer> optVersion = HintsEndpointProvider.instance.versionForEndpoint(address);
            if (optVersion.isEmpty())
            {
                logger.debug("Cannot deliver handoff to endpoint {}: its version is unknown. This should be temporary.", address);
                store.offerFirst(descriptor);
                return false;
            }

            // Use the minimum of peer's messaging version and storage compatibility mode's version.
            // This ensures hints written with the storage-compatible version can be dispatched using the
            // encoded path without deserialization, as long as the peer supports that version.
            int dispatchVersion = Math.min(optVersion.get(), StorageCompatibilityMode.current().storageMessagingVersion());

            try (HintsDispatcher dispatcher = HintsDispatcher.create(file, rateLimiter, address, descriptor.hostId, dispatchVersion, shouldAbort))
            {
                if (offset != null)
                    dispatcher.seek(offset);

                try
                {
                    if (dispatcher.dispatch())
                    {
                        store.delete(descriptor);
                        store.cleanUp(descriptor);
                        logger.info("Finished hinted handoff of file {} to endpoint {}: {}", descriptor.fileName(), address, hostId);
                        return true;
                    }
                    else
                    {
                        handleDispatchFailure(dispatcher, descriptor, address);
                        return false;
                    }
                }
                // we wrap InterruptedException in UncheckedInterruptedException
                // without that catch, undispatched HintsDescriptor won't be added back to the store and cleaned
                // up by HintsStore.delete in tests
                catch (UncheckedInterruptedException e)
                {
                    handleDispatchFailure(dispatcher, descriptor, address);
                    throw e;
                }
            }
        }

        private void handleDispatchFailure(HintsDispatcher dispatcher, HintsDescriptor descriptor, InetAddressAndPort address)
        {
            store.markDispatchOffset(descriptor, dispatcher.dispatchPosition());
            store.offerFirst(descriptor);
            logger.info("Finished hinted handoff of file {} to endpoint {}: {}, partially", descriptor.fileName(), address, hostId);
        }

        // for each hint in the hints file for a node that isn't part of the ring anymore, write RF hints for each replica
        private void convert(HintsDescriptor descriptor)
        {
            File file = descriptor.file(hintsDirectory);

            try (HintsReader reader = HintsReader.open(file, rateLimiter))
            {
                reader.forEach(page -> page.hintsIterator().forEachRemaining(HintsService.instance::writeForAllReplicas));
                store.delete(descriptor);
                store.cleanUp(descriptor);
                logger.info("Finished converting hints file {}", descriptor.fileName());
            }
        }
    }

    public boolean isPaused()
    {
        return isPaused.get();
    }

    public boolean hasScheduledDispatches()
    {
        return !scheduledDispatches.isEmpty();
    }

    public void updateDispatcherConcurrency(int concurrency)
    {
        logger.info("updating HintsDispatchExecutor with new concurrency = {} (current value = {})", concurrency, executor.getCorePoolSize());
        if (concurrency > executor.getCorePoolSize())
        {
            // we are increasing the value
            executor.setMaximumPoolSize(concurrency);
            executor.setCorePoolSize(concurrency);
        }
        else if (concurrency < executor.getCorePoolSize())
        {
            // we are reducing the value
            executor.setCorePoolSize(concurrency);
            executor.setMaximumPoolSize(concurrency);
        }
    }

    public int getDispatcherCorePoolSize()
    {
        return executor.getCorePoolSize();
    }

    public int getDispatcherMaxPoolSize()
    {
        return executor.getMaximumPoolSize();
    }
}
