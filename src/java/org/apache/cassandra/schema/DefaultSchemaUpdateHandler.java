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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

@NotThreadSafe
public class DefaultSchemaUpdateHandler implements SchemaUpdateHandler.GossipAware, IEndpointStateChangeSubscriber
{
    private final static Logger logger = LoggerFactory.getLogger(DefaultSchemaUpdateHandler.class);

    private final MigrationCoordinator migrationCoordinator;
    private final Clock clock;
    private final boolean requireSchemas;
    private volatile Schema schema;

    public DefaultSchemaUpdateHandler()
    {
        this(MigrationCoordinator.instance,
             !CassandraRelevantProperties.BOOTSTRAP_SKIP_SCHEMA_CHECK.getBoolean(),
             Clock.systemDefaultZone());
    }

    public DefaultSchemaUpdateHandler(MigrationCoordinator migrationCoordinator, boolean requireSchemas, Clock clock)
    {
        this.migrationCoordinator = migrationCoordinator;
        this.requireSchemas = requireSchemas;
        this.clock = clock;
        this.schema = new Schema(Keyspaces.none(), SchemaConstants.emptyVersion);
        Gossiper.instance.register(this);
    }

    @Override
    public void start()
    {
        migrationCoordinator.start(this);
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
    public void addOrUpdate(KeyspaceMetadata ksm)
    {
        schema = new Schema(schema.getKeyspaces().withAddedOrUpdated(ksm), schema.getVersion());
    }

    @Override
    public void remove(String ksName)
    {
        schema = new Schema(schema.getKeyspaces().without(ksName), schema.getVersion());
    }

    @Override
    public void updateVersion(UUID version)
    {
        schema = new Schema(schema.getKeyspaces(), version);
    }

    @Override
    public void reset()
    {
        Set<InetAddressAndPort> liveEndpoints = Gossiper.instance.getLiveMembers();
        liveEndpoints.remove(FBUtilities.getBroadcastAddressAndPort());

        // force migration if there are nodes around
        for (InetAddressAndPort node : liveEndpoints)
        {
            EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(node);
            migrationCoordinator.reportEndpointVersion(node, state, true);
        }
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
    public void applyReceivedSchemaMutations(InetAddressAndPort pushRequestFrom, Collection<Mutation> schemaMutations)
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
        SchemaTransformation.SchemaTransformationResult update = new SchemaTransformation.SchemaTransformationResult(before, after, diff);

        updateSchema(update);
        announceVersionUpdate(after.getVersion());
    }

    @Override
    public SchemaTransformation.SchemaTransformationResult apply(SchemaTransformation transformation, boolean locally, boolean preserveExistingSettings)
    {
        Schema before = schema();
        Keyspaces afterKeyspaces = transformation.apply(before.getKeyspaces());
        Keyspaces.KeyspacesDiff diff = Keyspaces.diff(before.getKeyspaces(), afterKeyspaces);

        if (diff.isEmpty())
            return new SchemaTransformation.SchemaTransformationResult(before, before, diff);

        Collection<Mutation> mutations = SchemaKeyspace.convertSchemaDiffToMutations(diff, preserveExistingSettings ? 0 : FBUtilities.timestampMicros());
        SchemaKeyspace.applyChanges(mutations);
        Schema after = new Schema(afterKeyspaces, SchemaKeyspace.calculateSchemaDigest());
        SchemaTransformation.SchemaTransformationResult update = new SchemaTransformation.SchemaTransformationResult(before, after, diff);

        updateSchema(update);
        if (!locally)
        {
            migrationCoordinator.pushSchemaMutations(mutations); // this was not there in OSS, but it is there in DSE
            announceVersionUpdate(after.getVersion());
        }

        return update;
    }

    @Override
    public Collection<Mutation> prepareRequestedSchemaMutations(InetAddressAndPort pullRequestFrom)
    {
        return SchemaKeyspace.convertSchemaToMutations();
    }

    /**
     * Load schema definitions from disk.
     */
    @Override
    public void initializeSchemaFromDisk()
    {
        SchemaDiagnostics.schemataLoading(schema());

        Keyspaces keyspaces = SchemaKeyspace.fetchNonSystemKeyspaces();
        UUID version = SchemaKeyspace.calculateSchemaDigest();
        schema = new Schema(keyspaces, version);
        SchemaDiagnostics.versionUpdated(SchemaManager.instance.schema());
        SchemaManager.instance.updateRefs(Keyspaces.diff(Keyspaces.none(), keyspaces));
        if (!keyspaces.isEmpty())
            announceVersionUpdate(schema.getVersion());

        SchemaDiagnostics.schemataLoaded(SchemaManager.instance.schema());
    }

    public void announceVersionUpdate(UUID version)
    {
        SystemKeyspace.updateSchemaVersion(version);
        if (Gossiper.instance.isEnabled())
            Gossiper.instance.addLocalApplicationState(ApplicationState.SCHEMA, StorageService.instance.valueFactory.schema(version));
        SchemaDiagnostics.versionAnnounced(schema);
    }

    /*
     * Reload schema from local disk. Useful if a user made changes to schema tables by hand, or has suspicion that
     * in-memory representation got out of sync somehow with what's on disk.
     */
    @Override
    public void reloadSchemaFromDisk()
    {
        Keyspaces after = SchemaKeyspace.fetchNonSystemKeyspaces();
        apply(existing -> after, false, false);
    }


    public void updateSchema(SchemaTransformation.SchemaTransformationResult update)
    {
        assert schema == update.before;

        if (update.diff.isEmpty())
            return;

        // TODO notifyPreChanges(diff)
        schema = update.after;
        SchemaManager.instance.updateRefs(update.diff);
        SchemaManager.instance.applyChangesLocally(update.diff);
    }
}
