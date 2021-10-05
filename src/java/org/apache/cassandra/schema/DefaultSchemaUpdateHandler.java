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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.SchemaTransformation.SchemaTransformationResult;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

@NotThreadSafe
public class DefaultSchemaUpdateHandler implements SchemaUpdateHandler.GossipAware, IEndpointStateChangeSubscriber
{
    private final static Logger logger = LoggerFactory.getLogger(DefaultSchemaUpdateHandler.class);

    @VisibleForTesting
    final MigrationCoordinator migrationCoordinator;

    private final Clock clock;
    private final boolean requireSchemas;
    private final Executor executor;
    private final Consumer<SchemaTransformationResult> preUpdateCallback;
    private final Consumer<SchemaTransformationResult> postUpdateCallback;
    private volatile Schema schema;

    public DefaultSchemaUpdateHandler(Executor executor, Consumer<SchemaTransformationResult> preUpdateCallback, Consumer<SchemaTransformationResult> postUpdateCallback)
    {
        this(null,
             !CassandraRelevantProperties.BOOTSTRAP_SKIP_SCHEMA_CHECK.getBoolean(),
             Clock.systemDefaultZone(),
             executor,
             preUpdateCallback,
             postUpdateCallback);
    }

    private MigrationCoordinator createMigrationCoordinator()
    {
        return new MigrationCoordinator(MessagingService.instance(), this::applyReceivedSchemaMutations, this::schema, ScheduledExecutors.scheduledTasks);
    }

    public DefaultSchemaUpdateHandler(MigrationCoordinator migrationCoordinator,
                                      boolean requireSchemas,
                                      Clock clock,
                                      Executor executor,
                                      Consumer<SchemaTransformationResult> preUpdateCallback,
                                      Consumer<SchemaTransformationResult> postUpdateCallback)
    {
        this.requireSchemas = requireSchemas;
        this.clock = clock;
        this.schema = new Schema(Keyspaces.none(), SchemaConstants.emptyVersion);
        this.executor = executor;
        this.preUpdateCallback = preUpdateCallback;
        this.postUpdateCallback = postUpdateCallback;
        this.migrationCoordinator = migrationCoordinator == null ? createMigrationCoordinator() : migrationCoordinator;
        Gossiper.instance.register(this);
    }

    private void setSchema(Schema schema)
    {
        this.schema = schema;
    }

    @Override
    public void start()
    {
        migrationCoordinator.start();
    }

    @Override
    public @Nonnull
    Schema schema()
    {
        return schema;
    }

    @Override
    public boolean waitUntilReady(Duration timeout)
    {
        logger.debug("Waiting for schema to be ready (max {})", timeout);
        Instant deadline = clock.instant().plus(timeout);

        while (schema().isEmpty() && clock.instant().isBefore(deadline))
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

        boolean schemasReceived = migrationCoordinator.awaitSchemaRequests(Math.max(0, Duration.between(clock.instant(), deadline).toMillis()));

        if (schemasReceived)
            return true;

        logger.warn("There are nodes in the cluster with a different schema version than us, from which we did not merge schemas: " +
                    "our version: ({}), outstanding versions -> endpoints: {}. Use -D{}}=true to ignore this, " +
                    "-D{}=<ep1[,epN]> to skip specific endpoints, or -D{}=<ver1[,verN]> to skip specific schema versions",
                    schema().getVersion(),
                    migrationCoordinator.outstandingVersions(),
                    CassandraRelevantProperties.BOOTSTRAP_SKIP_SCHEMA_CHECK.getKey(),
                    MigrationCoordinator.IGNORED_ENDPOINTS_PROP, MigrationCoordinator.IGNORED_VERSIONS_PROP);

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
    public CompletableFuture<SchemaTransformationResult> applyReceivedSchemaMutations(InetAddressAndPort pushRequestFrom, Collection<Mutation> schemaMutations)
    {
        return CompletableFuture.supplyAsync(() -> applyReceivedSchemaMutationsInternal(pushRequestFrom, schemaMutations), executor);
    }

    private SchemaTransformationResult applyReceivedSchemaMutationsInternal(InetAddressAndPort pushRequestFrom, Collection<Mutation> schemaMutations)
    {
        Schema before = schema();

        // apply the schema mutations
        SchemaKeyspace.applyChanges(schemaMutations);

        // only compare the keyspaces affected by this set of schema mutations
        Set<String> affectedKeyspaces = SchemaKeyspace.affectedKeyspaces(schemaMutations);

        // apply the schema mutations and fetch the new versions of the altered keyspaces
        Keyspaces updatedKeyspaces = SchemaKeyspace.fetchKeyspaces(affectedKeyspaces);
        Set<String> removedKeyspaces = affectedKeyspaces.stream().filter(ks -> !updatedKeyspaces.containsKeyspace(ks)).collect(Collectors.toSet());
        Keyspaces afterKeyspaces = before.getKeyspaces().withAddedOrReplaced(updatedKeyspaces).without(removedKeyspaces);

        Keyspaces.KeyspacesDiff diff = Keyspaces.diff(before.getKeyspaces(), afterKeyspaces);
        UUID version = SchemaKeyspace.calculateSchemaDigest();
        Schema after = new Schema(afterKeyspaces, version);
        SchemaTransformationResult update = new SchemaTransformationResult(before, after, diff);

        preUpdateCallback.accept(update);
        updateSchema(update);
        postUpdateCallback.accept(update);

        return update;
    }

    @Override
    public CompletableFuture<SchemaTransformationResult> apply(SchemaTransformation transformation, boolean locally)
    {
        return CompletableFuture.supplyAsync(() -> applyInternal(transformation, locally), executor);
    }

    private SchemaTransformationResult applyInternal(SchemaTransformation transformation, boolean locally)
    {
        Schema before = schema();
        Keyspaces afterKeyspaces = transformation.apply(before.getKeyspaces());
        Keyspaces.KeyspacesDiff diff = Keyspaces.diff(before.getKeyspaces(), afterKeyspaces);

        if (diff.isEmpty())
            return new SchemaTransformationResult(before, before, diff);

        Collection<Mutation> mutations = SchemaKeyspace.convertSchemaDiffToMutations(diff, transformation.fixedTimestampMicros().orElse(FBUtilities.timestampMicros()));
        SchemaKeyspace.applyChanges(mutations);
        Schema after = new Schema(afterKeyspaces, SchemaKeyspace.calculateSchemaDigest());
        SchemaTransformationResult update = new SchemaTransformationResult(before, after, diff);

        preUpdateCallback.accept(update);
        updateSchema(update);
        postUpdateCallback.accept(update);

        if (!locally)
            migrationCoordinator.pushSchemaMutations(mutations); // this was not there in OSS, but it is there in DSE

        return update;
    }


    @Override
    public CompletableFuture<Collection<Mutation>> prepareRequestedSchemaMutations(InetAddressAndPort pullRequestFrom)
    {
        return CompletableFuture.supplyAsync(SchemaKeyspace::convertSchemaToMutations, executor);
    }

    /**
     * Load schema definitions from disk.
     */
    @Override
    public CompletableFuture<SchemaTransformationResult> initializeSchemaFromDisk()
    {
        return CompletableFuture.supplyAsync(() -> {
            Schema before = schema();
            Keyspaces keyspaces = SchemaKeyspace.fetchNonSystemKeyspaces();
            UUID version = SchemaKeyspace.calculateSchemaDigest();
            Schema after = new Schema(keyspaces, version);
            setSchema(after);
            return new SchemaTransformationResult(before, after, Keyspaces.diff(before.getKeyspaces(), after.getKeyspaces()));
        }, executor);
    }

    /*
     * Reload schema from local disk. Useful if a user made changes to schema tables by hand, or has suspicion that
     * in-memory representation got out of sync somehow with what's on disk.
     */
    @Override
    public CompletableFuture<SchemaTransformationResult> reloadSchemaFromDisk()
    {
        return CompletableFuture.supplyAsync(() -> {
            Keyspaces after = SchemaKeyspace.fetchNonSystemKeyspaces();
            return applyInternal(existing -> after, false);
        }, executor);
    }

    private void updateSchema(SchemaTransformationResult update)
    {
        assert schema == update.before;

        if (update.diff.isEmpty())
            return;

        setSchema(update.after);
    }

    @Override
    public CompletableFuture<SchemaTransformationResult> clearUnsafe()
    {
        Optional<InetAddressAndPort> endpoint = Gossiper.instance.getLiveMembers()
                                                                 .stream()
                                                                 .filter(migrationCoordinator::shouldPullFromEndpoint)
                                                                 .findFirst();

        if (endpoint.isPresent())
        {
            return migrationCoordinator.pullSchemaFrom(endpoint.get())
                                       .thenApplyAsync(pulledSchema -> {
                                           SchemaKeyspace.truncate();
                                           setSchema(new Schema(Keyspaces.none(), SchemaConstants.emptyVersion));
                                           return applyReceivedSchemaMutationsInternal(endpoint.get(), pulledSchema);
                                       }, executor);
        }
        else
        {
            logger.warn("There is no endpoint to pull schema from");
            return CompletableFuture.supplyAsync(() -> {
                SchemaKeyspace.truncate();
                setSchema(new Schema(Keyspaces.none(), SchemaConstants.emptyVersion));
                return applyReceivedSchemaMutationsInternal(null, Collections.emptyList());
            }, executor);
        }
    }
}
