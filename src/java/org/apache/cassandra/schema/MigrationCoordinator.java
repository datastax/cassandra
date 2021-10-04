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

package org.apache.cassandra.schema;

import java.lang.management.ManagementFactory;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static org.apache.cassandra.net.Verb.SCHEMA_PUSH_REQ;

public class MigrationCoordinator
{
    private static final Logger logger = LoggerFactory.getLogger(MigrationCoordinator.class);
    private static final Future<Void> FINISHED_FUTURE = Futures.immediateFuture(null);

    private static LongSupplier getUptimeFn = () -> ManagementFactory.getRuntimeMXBean().getUptime();

    @VisibleForTesting
    public static void setUptimeFn(LongSupplier supplier)
    {
        getUptimeFn = supplier;
    }

    private static final int MIGRATION_DELAY_IN_MS = 60000;
    private static final int MAX_OUTSTANDING_VERSION_REQUESTS = 3;

    public static final String IGNORED_VERSIONS_PROP = "cassandra.skip_schema_check_for_versions";
    public static final String IGNORED_ENDPOINTS_PROP = "cassandra.skip_schema_check_for_endpoints";

    private final MessagingService messagingService;

    private final AtomicReference<ScheduledFuture<?>> periodicPullTask = new AtomicReference<>();

    private static ImmutableSet<UUID> getIgnoredVersions()
    {
        String s = System.getProperty(IGNORED_VERSIONS_PROP);
        if (s == null || s.isEmpty())
            return ImmutableSet.of();

        ImmutableSet.Builder<UUID> versions = ImmutableSet.builder();
        for (String version : s.split(","))
        {
            versions.add(UUID.fromString(version));
        }

        return versions.build();
    }

    private static final Set<UUID> IGNORED_VERSIONS = getIgnoredVersions();

    private static Set<InetAddressAndPort> getIgnoredEndpoints()
    {
        Set<InetAddressAndPort> endpoints = new HashSet<>();

        String s = System.getProperty(IGNORED_ENDPOINTS_PROP);
        if (s == null || s.isEmpty())
            return endpoints;

        for (String endpoint : s.split(","))
        {
            try
            {
                endpoints.add(InetAddressAndPort.getByName(endpoint));
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
        }

        return endpoints;
    }

    static class VersionInfo
    {
        final UUID version;

        final Set<InetAddressAndPort> endpoints = Sets.newConcurrentHashSet();
        final Set<InetAddressAndPort> outstandingRequests = Sets.newConcurrentHashSet();
        final Deque<InetAddressAndPort> requestQueue = new ArrayDeque<>();

        private final WaitQueue waitQueue = new WaitQueue();

        volatile boolean receivedSchema;

        VersionInfo(UUID version)
        {
            this.version = version;
        }

        WaitQueue.Signal register()
        {
            return waitQueue.register();
        }

        void markReceived()
        {
            if (receivedSchema)
                return;

            receivedSchema = true;
            waitQueue.signalAll();
        }

        boolean wasReceived()
        {
            return receivedSchema;
        }
    }

    private final Map<UUID, VersionInfo> versionInfo = new HashMap<>();
    private final Map<InetAddressAndPort, UUID> endpointVersions = new HashMap<>();
    private final Set<InetAddressAndPort> ignoredEndpoints = getIgnoredEndpoints();
    private final BiConsumer<InetAddressAndPort, Collection<Mutation>> schemaUpdateCallback;

    public MigrationCoordinator(MessagingService messagingService, BiConsumer<InetAddressAndPort, Collection<Mutation>> schemaUpdateCallback)
    {
        this.messagingService = messagingService;
        this.schemaUpdateCallback = schemaUpdateCallback;
    }

    public void start()
    {
        periodicPullTask.updateAndGet(curTask -> curTask == null
                                                 ? ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(this::pullUnreceivedSchemaVersions, 1, 1, TimeUnit.MINUTES)
                                                 : curTask);
    }

    @VisibleForTesting
    synchronized List<Future<Void>> pullUnreceivedSchemaVersions()
    {
        List<Future<Void>> futures = new ArrayList<>();
        for (VersionInfo info : versionInfo.values())
        {
            if (info.wasReceived() || info.outstandingRequests.size() > 0)
                continue;

            Future<Void> future = maybePullSchema(info);
            if (future != null && future != FINISHED_FUTURE)
                futures.add(future);
        }

        return futures;
    }

    private synchronized Future<Void> maybePullSchema(VersionInfo info)
    {
        if (info.endpoints.isEmpty() || info.wasReceived() || !shouldPullSchema(info.version))
            return FINISHED_FUTURE;

        if (info.outstandingRequests.size() >= getMaxOutstandingVersionRequests())
            return FINISHED_FUTURE;

        for (int i = 0, isize = info.requestQueue.size(); i < isize; i++)
        {
            InetAddressAndPort endpoint = info.requestQueue.remove();
            if (!info.endpoints.contains(endpoint))
                continue;

            if (shouldPullFromEndpoint(endpoint) && info.outstandingRequests.add(endpoint))
            {
                return scheduleSchemaPull(endpoint, info);
            }
            else
            {
                // return to queue
                info.requestQueue.offer(endpoint);
            }
        }

        // no suitable endpoints were found, check again in a minute, the periodic task will pick it up
        return null;
    }

    // used only in log message
    public synchronized Map<UUID, Set<InetAddressAndPort>> outstandingVersions()
    {
        HashMap<UUID, Set<InetAddressAndPort>> map = new HashMap<>();
        for (VersionInfo info : versionInfo.values())
            if (!info.wasReceived())
                map.put(info.version, ImmutableSet.copyOf(info.endpoints));
        return map;
    }

    @VisibleForTesting
    protected VersionInfo getVersionInfoUnsafe(UUID version)
    {
        return versionInfo.get(version);
    }

    @VisibleForTesting
    protected int getMaxOutstandingVersionRequests()
    {
        return MAX_OUTSTANDING_VERSION_REQUESTS;
    }

    @VisibleForTesting
    protected boolean isAlive(InetAddressAndPort endpoint)
    {
        return IFailureDetector.instance.isAlive(endpoint);
    }

    @VisibleForTesting
    protected boolean shouldPullSchema(UUID version)
    {
        Schema schema = SchemaManager.instance.schema();

        if (Objects.equals(schema.getVersion(), version))
        {
            logger.debug("Not pulling schema for version {}, because schema versions match: " +
                         "local={}, remote={}",
                         version,
                         Schema.schemaVersionToString(schema.getVersion()),
                         Schema.schemaVersionToString(version));
            return false;
        }
        return true;
    }

    @VisibleForTesting
    protected boolean shouldPullFromEndpoint(InetAddressAndPort endpoint)
    {
        if (endpoint.equals(FBUtilities.getBroadcastAddressAndPort()))
            return false;

        EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        if (state == null)
            return false;

        final String releaseVersion = state.getApplicationState(ApplicationState.RELEASE_VERSION).value;
        final String ourMajorVersion = FBUtilities.getReleaseVersionMajor();

        if (!releaseVersion.startsWith(ourMajorVersion))
        {
            logger.debug("Not pulling schema from {} because release version in Gossip is not major version {}, it is {}",
                         endpoint, ourMajorVersion, releaseVersion);
            return false;
        }

        if (!messagingService.versions.knows(endpoint))
        {
            logger.debug("Not pulling schema from {} because their messaging version is unknown", endpoint);
            return false;
        }

        if (messagingService.versions.getRaw(endpoint) != MessagingService.current_version)
        {
            logger.debug("Not pulling schema from {} because their schema format is incompatible", endpoint);
            return false;
        }

        if (Gossiper.instance.isGossipOnlyMember(endpoint))
        {
            logger.debug("Not pulling schema from {} because it's a gossip only member", endpoint);
            return false;
        }
        return true;
    }

    @VisibleForTesting
    protected boolean shouldPullImmediately(InetAddressAndPort endpoint, UUID version)
    {
        Schema schema = SchemaManager.instance.schema();
        if (schema.isEmpty() || getUptimeFn.getAsLong() < MIGRATION_DELAY_IN_MS)
        {
            // If we think we may be bootstrapping or have recently started, submit MigrationTask immediately
            logger.debug("Immediately submitting migration task for {}, " +
                         "schema versions: local={}, remote={}",
                         endpoint,
                         Schema.schemaVersionToString(schema.getVersion()),
                         Schema.schemaVersionToString(version));
            return true;
        }
        return false;
    }

    @VisibleForTesting
    protected boolean isLocalVersion(UUID version)
    {
        return SchemaManager.instance.schema().getVersion().equals(version);
    }

    /**
     * If a previous schema update brought our version the same as the incoming schema, don't apply it
     */
    private synchronized boolean shouldApplySchemaFor(VersionInfo info)
    {
        if (info.wasReceived())
            return false;
        return !isLocalVersion(info.version);
    }

    private synchronized Future<Void> reportEndpointVersionInternal(InetAddressAndPort endpoint, UUID version)
    {
        if (ignoredEndpoints.contains(endpoint) || IGNORED_VERSIONS.contains(version))
        {
            endpointVersions.remove(endpoint);
            removeEndpointFromVersion(endpoint, null);
            return FINISHED_FUTURE;
        }

        UUID current = endpointVersions.put(endpoint, version);
        if (current != null && current.equals(version))
            return FINISHED_FUTURE;

        VersionInfo info = versionInfo.computeIfAbsent(version, VersionInfo::new);
        if (isLocalVersion(version))
            info.markReceived();
        info.endpoints.add(endpoint);
        info.requestQueue.addFirst(endpoint);

        // disassociate this endpoint from its (now) previous schema version
        removeEndpointFromVersion(endpoint, current);

        return maybePullSchema(info);
    }

    public void reportEndpointVersion(InetAddressAndPort endpoint, UUID version)
    {
        reportEndpointVersionInternal(endpoint, version);
    }

    public void reportEndpointVersion(InetAddressAndPort endpoint, UUID version, boolean waitForPull)
    {
        Future<Void> result = reportEndpointVersionInternal(endpoint, version);
        if (result != null && waitForPull)
            FBUtilities.waitOnFuture(result, Duration.ofMinutes(10));
    }

    public void reportEndpointVersion(InetAddressAndPort endpoint, EndpointState state, boolean waitForPull)
    {
        if (state != null)
        {
            UUID version = state.getSchemaVersion();
            if (version != null)
            {
                Future<Void> result = reportEndpointVersionInternal(endpoint, version);
                if (result != null && waitForPull)
                    FBUtilities.waitOnFuture(result, Duration.ofMinutes(10));
            }
        }
    }

    private synchronized void removeEndpointFromVersion(InetAddressAndPort endpoint, UUID version)
    {
        if (version == null)
            return;

        VersionInfo info = versionInfo.get(version);

        if (info == null)
            return;

        info.endpoints.remove(endpoint);
        if (info.endpoints.isEmpty())
        {
            info.waitQueue.signalAll();
            versionInfo.remove(version);
        }
    }

    public synchronized void removeAndIgnoreEndpoint(InetAddressAndPort endpoint)
    {
        Preconditions.checkArgument(endpoint != null);
        ignoredEndpoints.add(endpoint);
        Set<UUID> versions = ImmutableSet.copyOf(versionInfo.keySet());
        for (UUID version : versions)
        {
            removeEndpointFromVersion(endpoint, version);
        }
    }

    private Future<Void> scheduleSchemaPull(InetAddressAndPort endpoint, VersionInfo info)
    {
        FutureTask<Void> task = new FutureTask<>(() -> pullSchema(endpoint, new Callback(endpoint, info)), null);
        if (shouldPullImmediately(endpoint, info.version))
        {
            submitToMigrationIfNotShutdown(task);
        }
        else
        {
            ScheduledExecutors.nonPeriodicTasks.schedule(() -> submitToMigrationIfNotShutdown(task), MIGRATION_DELAY_IN_MS, TimeUnit.MILLISECONDS);
        }
        return task;
    }

    public CompletableFuture<Collection<Mutation>> pullSchemaFrom(InetAddressAndPort endpoint)
    {
        CompletableFuture<Collection<Mutation>> result = new CompletableFuture<>();
        FutureTask<Void> task = new FutureTask<>(() -> pullSchema(endpoint, new RequestCallback<Collection<Mutation>>()
        {
            @Override
            public void onResponse(Message<Collection<Mutation>> msg)
            {
                result.complete(msg.payload);
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
            {
                result.completeExceptionally(new RuntimeException("Failed to get schema from " + from + ". The failure reason was: " + failureReason));
            }

            @Override
            public boolean invokeOnFailure()
            {
                return true;
            }
        }), null);

        return submitToMigrationIfNotShutdown(task).thenCompose(ignored -> result);
    }

    private static CompletableFuture<Void> submitToMigrationIfNotShutdown(Runnable task)
    {
        if (Stage.MIGRATION.executor().isShutdown() || Stage.MIGRATION.executor().isTerminated())
        {
            logger.info("Skipped scheduled pulling schema from other nodes: the MIGRATION executor service has been shutdown.");
            return CompletableFuture.completedFuture(null);
        }
        else
        {
            return Stage.MIGRATION.submit(task);
        }
    }

    @VisibleForTesting
    protected void mergeSchemaFrom(InetAddressAndPort endpoint, Collection<Mutation> mutations)
    {
        schemaUpdateCallback.accept(endpoint, mutations);
    }

    @VisibleForTesting
    class Callback implements RequestCallback<Collection<Mutation>>
    {
        final InetAddressAndPort endpoint;
        final VersionInfo info;

        public Callback(InetAddressAndPort endpoint, VersionInfo info)
        {
            this.endpoint = endpoint;
            this.info = info;
        }

        public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
        {
            fail();
        }

        Future<Void> fail()
        {
            return pullComplete(endpoint, info, false);
        }

        public void onResponse(Message<Collection<Mutation>> message)
        {
            response(message.payload);
        }

        Future<Void> response(Collection<Mutation> mutations)
        {
            synchronized (info)
            {
                if (shouldApplySchemaFor(info))
                {
                    try
                    {
                        mergeSchemaFrom(endpoint, mutations);
                    }
                    catch (Exception e)
                    {
                        logger.error(String.format("Unable to merge schema from %s", endpoint), e);
                        return fail();
                    }
                }
                return pullComplete(endpoint, info, true);
            }
        }

        public boolean isLatencyForSnitch()
        {
            return false;
        }
    }

    private void pullSchema(InetAddressAndPort endpoint, RequestCallback<?> callback)
    {
        if (!isAlive(endpoint))
        {
            logger.warn("Can't send schema pull request: node {} is down.", endpoint);
            callback.onFailure(endpoint, RequestFailureReason.UNKNOWN);
            return;
        }

        // There is a chance that quite some time could have passed between now and the MM#maybeScheduleSchemaPull(),
        // potentially enough for the endpoint node to restart - which is an issue if it does restart upgraded, with
        // a higher major.
        if (!shouldPullFromEndpoint(endpoint))
        {
            logger.info("Skipped sending a migration request: node {} has a higher major version now.", endpoint);
            callback.onFailure(endpoint, RequestFailureReason.UNKNOWN);
            return;
        }

        logger.debug("Requesting schema from {}", endpoint);
        sendMigrationMessage(endpoint, callback);
    }

    private void sendMigrationMessage(InetAddressAndPort endpoint, RequestCallback<?> callback)
    {
        Message<?> message = Message.out(Verb.SCHEMA_PULL_REQ, NoPayload.noPayload);
        logger.info("Sending schema pull request to {}", endpoint);
        messagingService.sendWithCallback(message, endpoint, callback);
    }

    private synchronized Future<Void> pullComplete(InetAddressAndPort endpoint, VersionInfo info, boolean wasSuccessful)
    {
        if (wasSuccessful)
            info.markReceived();

        info.outstandingRequests.remove(endpoint);
        info.requestQueue.add(endpoint);
        return maybePullSchema(info);
    }

    /**
     * Wait until we've received schema responses for all versions we're aware of
     *
     * @param waitMillis
     * @return true if response for all schemas were received, false if we timed out waiting
     */
    @VisibleForTesting
    public boolean awaitSchemaRequests(long waitMillis)
    {
        if (!FBUtilities.getBroadcastAddressAndPort().equals(InetAddressAndPort.getLoopbackAddress()))
            Gossiper.waitToSettle();

        if (versionInfo.isEmpty())
            logger.debug("Nothing in versionInfo - so no schemas to wait for");

        WaitQueue.Signal signal = null;
        try
        {
            synchronized (this)
            {
                List<WaitQueue.Signal> signalList = new ArrayList<>(versionInfo.size());
                for (VersionInfo version : versionInfo.values())
                {
                    if (version.wasReceived())
                        continue;

                    signalList.add(version.register());
                }

                if (signalList.isEmpty())
                    return true;

                WaitQueue.Signal[] signals = new WaitQueue.Signal[signalList.size()];
                signalList.toArray(signals);
                signal = WaitQueue.all(signals);
            }

            return signal.awaitUntil(System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(waitMillis));
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            if (signal != null)
                signal.cancel();
        }
    }

    public void pushSchemaMutations(Collection<Mutation> schemaMutations)
    {
        Set<InetAddressAndPort> schemaDestinationEndpoints = new HashSet<>();
        Set<InetAddressAndPort> schemaEndpointsIgnored = new HashSet<>();
        Message<Collection<Mutation>> message = Message.out(SCHEMA_PUSH_REQ, schemaMutations);
        for (InetAddressAndPort endpoint : Gossiper.instance.getLiveMembers())
        {
            if (shouldPushSchemaTo(endpoint))
            {
                MessagingService.instance().send(message, endpoint);
                schemaDestinationEndpoints.add(endpoint);
            }
            else
            {
                schemaEndpointsIgnored.add(endpoint);
            }
        }

        SchemaAnnouncementDiagnostics.schemaTransformationAnnounced(schemaDestinationEndpoints,
                                                                    schemaEndpointsIgnored,
                                                                    null);
    }

    @VisibleForTesting
    public boolean shouldPushSchemaTo(InetAddressAndPort endpoint)
    {
        // only push schema to nodes with known and equal versions
        return !endpoint.equals(FBUtilities.getBroadcastAddressAndPort())
               && MessagingService.instance().versions.knows(endpoint)
               && MessagingService.instance().versions.getRaw(endpoint) == MessagingService.current_version;
    }

}