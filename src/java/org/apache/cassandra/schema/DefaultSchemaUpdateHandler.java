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

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.DseCompat;
import org.apache.cassandra.net.DseCompatRefusals;
import org.apache.cassandra.net.DseDialectVersions;
import org.apache.cassandra.net.DsePeers;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.SchemaTransformation.SchemaTransformationResult;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Awaitable;

import static org.apache.cassandra.schema.MigrationCoordinator.MAX_OUTSTANDING_VERSION_REQUESTS;

public class DefaultSchemaUpdateHandler implements SchemaUpdateHandler, IEndpointStateChangeSubscriber
{
    private static final Logger logger = LoggerFactory.getLogger(DefaultSchemaUpdateHandler.class);

    /** The reason under which, in the DSE 6 compatibility mode, the schema pulls answered with no schema are counted. */
    public static final String SCHEMA_PULL_WITHOUT_SCHEMA = "schema pull (answered with no schema)";

    /** The reason under which, in the DSE 6 compatibility mode, the ignored schema pushes are counted. */
    public static final String SCHEMA_PUSH_IGNORED = "schema push (ignored)";

    @VisibleForTesting
    final MigrationCoordinator migrationCoordinator;

    /** The messaging service whose peer versions tell a DSE 6 peer from a node of this software; read in the mode only. */
    private final MessagingService messagingService;

    private final boolean requireSchemas;
    private final BiConsumer<SchemaTransformationResult, Boolean> updateCallback;
    private volatile DistributedSchema schema = DistributedSchema.EMPTY;

    private volatile AsyncPromise<Void> requestedReset;

    private MigrationCoordinator createMigrationCoordinator(MessagingService messagingService)
    {
        return new MigrationCoordinator(messagingService,
                                        Stage.MIGRATION,
                                        ScheduledExecutors.scheduledTasks,
                                        MAX_OUTSTANDING_VERSION_REQUESTS,
                                        Gossiper.instance,
                                        this::getSchemaVersionForCoordinator,
                                        this::applyMutationsFromCoordinator);
    }

    public DefaultSchemaUpdateHandler(BiConsumer<SchemaTransformationResult, Boolean> updateCallback)
    {
        this(null, MessagingService.instance(), !CassandraRelevantProperties.BOOTSTRAP_SKIP_SCHEMA_CHECK.getBoolean(), updateCallback);
    }

    public DefaultSchemaUpdateHandler(MigrationCoordinator migrationCoordinator,
                                      MessagingService messagingService,
                                      boolean requireSchemas,
                                      BiConsumer<SchemaTransformationResult, Boolean> updateCallback)
    {
        this.messagingService = messagingService;
        this.requireSchemas = requireSchemas;
        this.updateCallback = updateCallback;
        this.migrationCoordinator = migrationCoordinator == null ? createMigrationCoordinator(messagingService) : migrationCoordinator;
        Gossiper.instance.register(this);
        SchemaPushVerbHandler.instance.register(msg -> {
            if (DseCompat.enabled() && !this.migrationCoordinator.exchangesSchemaWith(msg.from()))
            {
                ignoreSchemaPush(msg.from());
                return;
            }
            synchronized (this)
            {
                if (requestedReset == null)
                    applyMutations(msg.payload);
            }
        });
        SchemaPullVerbHandler.instance.register(msg -> {
            try
            {
                Collection<Mutation> answer = schemaMutationsForPull(msg.from());
                if (answer == null)
                    messagingService.send(msg.failureResponse(RequestFailureReason.UNKNOWN), msg.from());
                else
                    messagingService.send(msg.responseWith(answer), msg.from());
            }
            catch (RuntimeException ex)
            {
                logger.error("Failed to send schema mutations to " + msg.from(), ex);
            }
        });
    }

    public synchronized void start()
    {
        if (StorageService.instance.isReplacing())
            onRemove(DatabaseDescriptor.getReplaceAddress());

        SchemaKeyspace.saveSystemKeyspacesSchema();

        migrationCoordinator.start();
    }

    @Override
    public boolean waitUntilReady(Duration timeout)
    {
        logger.debug("Waiting for schema to be ready (max {})", timeout);
        boolean schemasReceived = migrationCoordinator.awaitSchemaRequests(timeout.toMillis());

        if (schemasReceived)
            return true;

        logger.warn("There are nodes in the cluster with a different schema version than us, from which we did not merge schemas: " +
                    "our version: ({}), outstanding versions -> endpoints: {}. Use -D{}=true to ignore this, " +
                    "-D{}=<ep1[,epN]> to skip specific endpoints, or -D{}=<ver1[,verN]> to skip specific schema versions",
                    Schema.instance.getVersion(),
                    migrationCoordinator.outstandingVersions(),
                    CassandraRelevantProperties.BOOTSTRAP_SKIP_SCHEMA_CHECK.getKey(),
                    CassandraRelevantProperties.IGNORED_SCHEMA_CHECK_ENDPOINTS.getKey(),
                    CassandraRelevantProperties.IGNORED_SCHEMA_CHECK_VERSIONS.getKey());

        if (requireSchemas)
        {
            logger.error("Didn't receive schemas for all known versions within the {}. Use -D{}=true to skip this check.",
                         timeout, CassandraRelevantProperties.BOOTSTRAP_SKIP_SCHEMA_CHECK.getKey());

            return false;
        }

        return true;
    }

    @Override
    public void onRemove(InetAddressAndPort endpoint)
    {
        migrationCoordinator.removeAndIgnoreEndpoint(endpoint);
    }

    @Override
    public void onChange(InetAddressAndPort endpoint, ApplicationState state, VersionedValue value)
    {
        if (state == ApplicationState.SCHEMA)
        {
            EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
            if (epState != null && !Gossiper.instance.isDeadState(epState) && StorageService.instance.getTokenMetadata().isMember(endpoint))
            {
                migrationCoordinator.reportEndpointVersion(endpoint, UUID.fromString(value.value));
            }
        }
    }

    @Override
    public void onJoin(InetAddressAndPort endpoint, EndpointState epState)
    {
        // no-op
    }

    @Override
    public void beforeChange(InetAddressAndPort endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue)
    {
        // no-op
    }

    @Override
    public void onAlive(InetAddressAndPort endpoint, EndpointState state)
    {
        // no-op
    }

    @Override
    public void onDead(InetAddressAndPort endpoint, EndpointState state)
    {
        // no-op
    }

    @Override
    public void onRestart(InetAddressAndPort endpoint, EndpointState state)
    {
        // no-op
    }

    @VisibleForTesting
    synchronized SchemaTransformationResult applyMutations(Collection<Mutation> schemaMutations)
    {
        // fetch the current state of schema for the affected keyspaces only
        DistributedSchema before = schema;

        // apply the schema mutations
        SchemaKeyspace.applyChanges(schemaMutations);

        // only compare the keyspaces affected by this set of schema mutations
        Set<String> affectedKeyspaces = SchemaKeyspace.affectedKeyspaces(schemaMutations);

        // apply the schema mutations and fetch the new versions of the altered keyspaces
        Keyspaces updatedKeyspaces = SchemaKeyspace.fetchKeyspaces(affectedKeyspaces);
        Set<String> removedKeyspaces = affectedKeyspaces.stream().filter(ks -> !updatedKeyspaces.containsKeyspace(ks)).collect(Collectors.toSet());
        Keyspaces afterKeyspaces = before.getKeyspaces().withAddedOrUpdated(updatedKeyspaces).without(removedKeyspaces);

        Keyspaces.KeyspacesDiff diff = Keyspaces.diff(before.getKeyspaces(), afterKeyspaces);
        UUID version = SchemaKeyspace.calculateSchemaDigest();
        DistributedSchema after = new DistributedSchema(afterKeyspaces, version);
        SchemaTransformationResult update = new SchemaTransformationResult(before, after, diff);

        logger.info("Applying schema change due to received mutations: {}", update);
        updateSchema(update, false);
        return update;
    }

    @Override
    public synchronized SchemaTransformationResult apply(SchemaTransformation transformation, boolean local)
    {
        DistributedSchema before = schema;
        Keyspaces afterKeyspaces = transformation.apply(before.getKeyspaces());
        Keyspaces.KeyspacesDiff diff = Keyspaces.diff(before.getKeyspaces(), afterKeyspaces);

        if (diff.isEmpty())
            return new SchemaTransformationResult(before, before, diff);

        Collection<Mutation> mutations = SchemaKeyspace.convertSchemaDiffToMutations(diff, transformation.fixedTimestampMicros().orElse(FBUtilities.timestampMicros()));
        SchemaKeyspace.applyChanges(mutations);

        DistributedSchema after = new DistributedSchema(afterKeyspaces, SchemaKeyspace.calculateSchemaDigest());
        SchemaTransformationResult update = new SchemaTransformationResult(before, after, diff);

        updateSchema(update, local);
        if (!local)
        {
            migrationCoordinator.executor.submit(() -> {
                Pair<Set<InetAddressAndPort>, Set<InetAddressAndPort>> endpoints = migrationCoordinator.pushSchemaMutations(mutations);
                SchemaAnnouncementDiagnostics.schemaTransformationAnnounced(endpoints.left(), endpoints.right(), transformation);
            });
        }

        return update;
    }

    private void updateSchema(SchemaTransformationResult update, boolean local)
    {
        if (!update.diff.isEmpty() || !update.after.getVersion().equals(schema.getVersion()))
        {
            this.schema = update.after;
            logger.debug("Schema updated: {}", update);
            updateCallback.accept(update, true);
            if (!local)
            {
                migrationCoordinator.announce(update.after.getVersion());
            }
        }
        else
        {
            logger.debug("Schema update is empty - skipping");
        }
    }

    private synchronized void reload()
    {
        DistributedSchema before = this.schema;
        DistributedSchema after = new DistributedSchema(SchemaKeyspace.fetchNonSystemKeyspaces(), SchemaKeyspace.calculateSchemaDigest());
        Keyspaces.KeyspacesDiff diff = Keyspaces.diff(before.getKeyspaces(), after.getKeyspaces());
        SchemaTransformationResult update = new SchemaTransformationResult(before, after, diff);

        updateSchema(update, false);
    }

    @Override
    public void reset(boolean local)
    {
        if (local)
        {
            reload();
        }
        else
        {
            migrationCoordinator.reset();
            if (!migrationCoordinator.awaitSchemaRequests(CassandraRelevantProperties.MIGRATION_DELAY.getLong()))
            {
                logger.error("Timeout exceeded when waiting for schema from other nodes");
            }
        }
    }

    /**
     * When clear is called the update handler will flag that the clear was requested. It means that migration
     * coordinator will think that we have empty schema version and will apply whatever it receives from other nodes.
     * When a first attempt to apply mutations from other node is called, it will first clear the schema and apply
     * the mutations on a truncated table. The flag is then reset.
     * <p>
     * This way the clear is postponed until we really fetch any schema we can use as a replacement. Otherwise, nothing
     * will happen. We will simply reset the flag after the timeout and throw exceptions to the caller.
     *
     * @return
     */
    @Override
    public Awaitable clear()
    {
        synchronized (this)
        {
            if (requestedReset == null)
            {
                requestedReset = new AsyncPromise<>();
                migrationCoordinator.reset();
            }
            return requestedReset;
        }
    }

    private UUID getSchemaVersionForCoordinator()
    {
        if (requestedReset != null)
            return SchemaConstants.emptyVersion;
        else
            return schema.getVersion();
    }

    private synchronized void applyMutationsFromCoordinator(InetAddressAndPort from, Collection<Mutation> mutations)
    {
        if (requestedReset != null && !mutations.isEmpty())
        {
            schema = DistributedSchema.EMPTY;
            SchemaKeyspace.truncate();
            requestedReset.setSuccess(null);
            requestedReset = null;
        }
        applyMutations(mutations);
    }

    /**
     * The answer to a schema pull from the peer: this node's schema, or, in the DSE 6 compatibility mode, for a peer this
     * node does not exchange its schema with ({@link MigrationCoordinator#exchangesSchemaWith}):
     * <ul>
     * <li>no schema for a DSE 6 peer (known at the pre-4.0 dialect, or identified as DSE), counted in
     *     {@code nodetool compatstatus} and logged at most once every ten minutes;</li>
     * <li>null for any other peer (its messaging version unknown, or another 4.0 or later version than this node's), to
     *     be answered with a failure: the requester takes an empty answer as the schema received and would not pull that
     *     schema version again, while a failure makes it retry, possibly from another node.</li>
     * </ul>
     */
    @VisibleForTesting
    @Nullable
    Collection<Mutation> schemaMutationsForPull(InetAddressAndPort from)
    {
        if (DseCompat.enabled() && !migrationCoordinator.exchangesSchemaWith(from))
        {
            if (!DseDialectVersions.isPre40(messagingService.versions, from) && !DsePeers.isDse(from))
            {
                NoSpamLogger.log(logger, NoSpamLogger.Level.INFO, 10, TimeUnit.MINUTES,
                                 "Answering with a failure the schema pulls of peers whose messaging version is neither this node's " +
                                 "nor DSE 6's (for example {}): in internode_compatibility_mode DSE_6 this node's schema is sent " +
                                 "only to peers speaking its messaging version; the peer retries", from);
                return null;
            }
            DseCompatRefusals.refused(SCHEMA_PULL_WITHOUT_SCHEMA);
            NoSpamLogger.log(logger, NoSpamLogger.Level.INFO, 10, TimeUnit.MINUTES,
                             "Answering with no schema the schema pulls of peers that do not speak this node's messaging version, " +
                             "DSE 6 nodes in the first place (for example {}): in internode_compatibility_mode DSE_6 the schema is " +
                             "frozen and this node's schema must not reach the DSE 6 nodes", from);
            return Collections.emptyList();
        }
        return getSchemaMutations();
    }

    /**
     * In the DSE 6 compatibility mode, drops a schema push from a peer this node does not exchange its schema with
     * ({@link MigrationCoordinator#exchangesSchemaWith}): the schema is frozen during the upgrade, so a schema change
     * made on a DSE 6 node does not reach this node. Counted in {@code nodetool compatstatus} and logged at most once
     * every ten minutes. A push from a node of this software whose messaging version this node does not know at that
     * moment is dropped too: its schema change reaches this node by the pull that follows its gossiped schema version,
     * once the version is known.
     */
    private static void ignoreSchemaPush(InetAddressAndPort from)
    {
        DseCompatRefusals.refused(SCHEMA_PUSH_IGNORED);
        NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 10, TimeUnit.MINUTES,
                         "Ignoring the schema pushes of peers that do not speak this node's messaging version, DSE 6 nodes in the " +
                         "first place (for example {}): in internode_compatibility_mode DSE_6 the schema is frozen, and a schema " +
                         "change made on a DSE 6 node during the upgrade does not reach this node", from);
    }

    private synchronized Collection<Mutation> getSchemaMutations()
    {
        if (requestedReset != null)
            return Collections.emptyList();
        else
            return SchemaKeyspace.convertSchemaToMutations();
    }

    public Map<UUID, Set<InetAddressAndPort>> getOutstandingSchemaVersions()
    {
        return migrationCoordinator.outstandingVersions();
    }
}
