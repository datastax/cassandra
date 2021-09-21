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

import java.net.UnknownHostException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.ObjectUtils;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.functions.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.schema.KeyspaceMetadata.KeyspaceDiff;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Throwables;

import static com.google.common.collect.Iterables.size;
import static java.lang.String.format;

/**
 * Manages keyspace instances. It provides methods to query schema, but it does not provide any methods to modify the
 * schema. All schema modification are managed by the implementation of {@link SchemaUpdateHandler}. Once the schema is
 * updated, {@link SchemaUpdateHandler} applies the changes to the keyspace instances stored by {@link SchemaManager}.
 */
public final class SchemaManager implements SchemaProvider, IEndpointStateChangeSubscriber
{
    private final static Logger logger = LoggerFactory.getLogger(SchemaManager.class);

    public static final SchemaManager instance = new SchemaManager();

    private final LocalKeyspaces localKeyspaces;

    private final SchemaRefCache schemaRefCache = new SchemaRefCache();

    // Keyspace objects, one per keyspace. Only one instance should ever exist for any given keyspace.
    private final ConcurrentMap<String, CompletableFuture<Keyspace>> keyspaceInstances = new NonBlockingHashMap<>();

    private final SchemaChangeNotifier schemaChangeNotifier = new SchemaChangeNotifier();

    SchemaUpdateHandler updateHandler = SchemaUpdateHandlerFactory.instance.getSchemaUpdateHandler();

    /**
     * Initialize empty schema object and load the hardcoded system tables
     */
    private SchemaManager()
    {
        boolean init = DatabaseDescriptor.isDaemonInitialized() || DatabaseDescriptor.isToolInitialized();
        localKeyspaces = new LocalKeyspaces(init);
        localKeyspaces.getAll().forEach(this::addNewRefs);
    }

    public void startSync()
    {
        updateHandler.start();
    }

    public boolean waitUntilReady(Duration timeout)
    {
        return updateHandler.waitUntilReady(timeout);
    }

    public void initializeSchemaFromDisk()
    {
        updateHandler.initializeSchemaFromDisk();
    }

    public void reloadSchemaFromDisk()
    {
        updateHandler.reloadSchemaFromDisk();
    }

    public SchemaTransformation.SchemaTransformationResult apply(SchemaTransformation transformation, boolean locally, boolean preserveExistingUserSettings)
    {
        return updateHandler.apply(transformation, locally, preserveExistingUserSettings);
    }

    /**
     * Update (or insert) new keyspace definition
     *
     * @param ksm The metadata about keyspace
     */
    synchronized public void load(KeyspaceMetadata ksm)
    {
        KeyspaceMetadata previous = updateHandler.schema().getKeyspaces().getNullable(ksm.name);

        if (previous == null)
            addNewRefs(ksm);
        else
            updateRefs(previous, ksm);

        updateHandler.addOrUpdate(ksm);
    }

    /**
     * Should be called after a keyspace is added to the local schema. It updates the table and index references cache.
     */
    void addNewRefs(KeyspaceMetadata ksm)
    {
        schemaRefCache.addNewRefs(ksm);
        SchemaDiagnostics.metadataInitialized(schema(), ksm);
    }

    /**
     * Should be called once a keyspace metadata is changed (tables, views, indexes). It updates the table and index
     * references cache.
     */
    void updateRefs(KeyspaceMetadata previous, KeyspaceMetadata updated)
    {
        Keyspace keyspace = getKeyspaceInstance(updated.name);
        if (null != keyspace)
            keyspace.setMetadata(updated);

        schemaRefCache.updateRefs(previous, updated);
//      TODO  SchemaDiagnostics.metadataReloaded(schema.get(), previous, updated, tablesDiff, viewsDiff, indexesDiff);
    }

    /**
     * Should be called once a keyspace is removed from local schema. It updates the table and index reference cache.
     */
    synchronized void removeRefs(KeyspaceMetadata ksm)
    {
        schemaRefCache.removeRefs(ksm);
        SchemaDiagnostics.metadataRemoved(schema(), ksm);
    }

    /**
     * Update/create/drop the {@link TableMetadataRef} in {@link SchemaManager}.
     */
    void updateRefs(KeyspacesDiff diff)
    {
        diff.dropped.forEach(this::removeRefs);
        diff.created.forEach(this::addNewRefs);
        diff.altered.forEach(delta -> this.updateRefs(delta.before, delta.after));
    }

    public void registerListener(SchemaChangeListener listener)
    {
        schemaChangeNotifier.registerListener(listener);
    }

    @SuppressWarnings("unused")
    public void unregisterListener(SchemaChangeListener listener)
    {
        schemaChangeNotifier.unregisterListener(listener);
    }

    /**
     * Get keyspace instance by name
     *
     * @param keyspaceName The name of the keyspace
     *
     * @return Keyspace object or null if keyspace was not found, or if the keyspace has not completed construction yet
     */
    @Override
    public Keyspace getKeyspaceInstance(String keyspaceName)
    {
        CompletableFuture<Keyspace> future = keyspaceInstances.get(keyspaceName);
        if (future != null && future.isDone())
            return future.join();
        else
            return null;
    }

    public ColumnFamilyStore getColumnFamilyStoreInstance(TableId id)
    {
        TableMetadata metadata = getTableMetadata(id);
        if (metadata == null)
            return null;

        Keyspace instance = getKeyspaceInstance(metadata.keyspace);
        if (instance == null)
            return null;

        return instance.hasColumnFamilyStore(metadata.id)
               ? instance.getColumnFamilyStore(metadata.id)
               : null;
    }

    /**
     * Store given Keyspace instance to the schema
     *
     * @param keyspace The Keyspace instance to store
     *
     * @throws IllegalArgumentException if Keyspace is already stored
     */
    // todo perhaps there should be two interfaces implemented by SchemaManager - one for querying and one for updating
    @VisibleForTesting
    public void storeKeyspaceInstance(Keyspace keyspace)
    {
        CompletableFuture<Keyspace> future = CompletableFuture.completedFuture(keyspace);
        CompletableFuture<Keyspace> existing = keyspaceInstances.putIfAbsent(keyspace.getName(), future);
        if (existing != null)
            throw new IllegalArgumentException(String.format("Keyspace %s was already initialized.", keyspace.getName()));
    }

    /**
     * Remove keyspace from schema. This puts a temporary entry in the map that throws an exception when queried.
     * When the metadata is also deleted, that temporary entry must also be deleted using clearKeyspaceInstance below.
     *
     * @param keyspaceName The name of the keyspace to remove
     *
     * @return removed keyspace instance or null if it wasn't found
     */
    // todo perhaps there should be two interfaces implemented by SchemaManager - one for querying and one for updating
    public Keyspace removeKeyspaceInstance(String keyspaceName, Consumer<Keyspace> unloadFunction)
    {
        CompletableFuture<Keyspace> droppedFuture = new CompletableFuture<>();
        droppedFuture.completeExceptionally(new KeyspaceNotDefinedException(keyspaceName));

        CompletableFuture<Keyspace> existingFuture = keyspaceInstances.put(keyspaceName, droppedFuture);
        if (existingFuture == null || existingFuture.isCompletedExceptionally())
            return null;

        Keyspace instance = existingFuture.join();
        unloadFunction.accept(instance);

        CompletableFuture<Keyspace> future = keyspaceInstances.remove(keyspaceName);
        assert future == droppedFuture;

        return instance;
    }

    @Override
    public Keyspace getOrCreateKeyspaceInstance(String keyspaceName, Supplier<Keyspace> loadFunction)
    {
        CompletableFuture<Keyspace> future = keyspaceInstances.get(keyspaceName);
        if (future == null)
        {
            CompletableFuture<Keyspace> empty = new CompletableFuture<>();
            future = keyspaceInstances.putIfAbsent(keyspaceName, empty);
            if (future == null)
            {
                // We managed to create an entry for the keyspace. Now initialize it.
                future = empty;
                try
                {
                    empty.complete(loadFunction.get());
                }
                catch (Throwable t)
                {
                    empty.completeExceptionally(t);
                    // Remove future so that construction can be retried later
                    keyspaceInstances.remove(keyspaceName, future);
                }
            }
            // Else some other thread beat us to it, but we now have the reference to the future which we can wait for.
        }

        // Most of the time the keyspace will be ready and this will complete immediately. If it is being created
        // concurrently, wait for that process to complete.
        try
        {
            return future.join();
        }
        catch (CompletionException e)
        {
            // Unwrap exception so that caller can process it correctly.
            throw Throwables.cleaned(e);
        }
    }

    // todo maybe there should be a universal method which accepts flags determining which sets of keyspaces (user, system, local, virtual) should be returned
    public Keyspaces snapshot()
    {
        return Keyspaces.builder().add(localKeyspaces.getAll()).add(updateHandler.schema().getKeyspaces()).build();
    }


    // todo maybe there should be a universal method which accepts flags determining which sets of keyspaces (user, system, local, virtual) should be returned
    public int getNumberOfTables()
    {
        return updateHandler.schema().getKeyspaces().stream().mapToInt(k -> size(k.tablesAndViews())).sum() + localKeyspaces.getAllTablesAndViewsCount();
    }

    public ViewMetadata getView(String keyspaceName, String viewName)
    {
        Preconditions.checkNotNull(keyspaceName);
        KeyspaceMetadata ksm = ObjectUtils.getFirstNonNull(() -> updateHandler.schema().getKeyspaces().getNullable(keyspaceName),
                                                           () -> localKeyspaces.get(keyspaceName));
        return (ksm == null) ? null : ksm.views.getNullable(viewName);
    }

    /**
     * Get metadata about keyspace by its name
     *
     * @param keyspaceName The name of the keyspace
     *
     * @return The keyspace metadata or null if it wasn't found
     */
    @Override
    public KeyspaceMetadata getKeyspaceMetadata(String keyspaceName)
    {
        Preconditions.checkNotNull(keyspaceName);
        return ObjectUtils.getFirstNonNull(() -> updateHandler.schema().getKeyspaces().getNullable(keyspaceName),
                                           () -> localKeyspaces.get(keyspaceName),
                                           () -> VirtualKeyspaceRegistry.instance.getKeyspaceMetadataNullable(keyspaceName));
    }

    /**
     * @return collection of the non-system keyspaces (note that this count as system only the
     * non replicated keyspaces, so keyspace like system_traces which are replicated are actually
     * returned. See getUserKeyspace() below if you don't want those)
     */
    public ImmutableList<String> getNonSystemKeyspaces()
    {
        return ImmutableList.copyOf(updateHandler.schema().getKeyspaces().names());
    }

    /**
     * @return a collection of keyspaces that do not use LocalStrategy for replication
     */
    public List<String> getNonLocalStrategyKeyspaces()
    {
        return updateHandler.schema().getKeyspaces().stream()
                                           .filter(keyspace -> keyspace.params.replication.klass != LocalStrategy.class)
                                           .map(keyspace -> keyspace.name)
                                           .collect(Collectors.toList());
    }

    /**
     * @return a collection of keyspaces that partition data across the ring
     */
    public List<String> getPartitionedKeyspaces()
    {
        return updateHandler.schema().getKeyspaces().stream()
                                           .filter(keyspace -> Keyspace.open(keyspace.name).getReplicationStrategy().isPartitioned())
                                           .map(keyspace -> keyspace.name)
                                           .collect(Collectors.toList());
    }

    /**
     * @return collection of the user defined keyspaces
     */
    public List<String> getUserKeyspaces()
    {
        return ImmutableList.copyOf(Sets.difference(updateHandler.schema().getKeyspaces().names(), SchemaConstants.REPLICATED_SYSTEM_KEYSPACE_NAMES));
    }

    /**
     * @return collection of the user defined keyspaces, excluding DSE internal keyspaces.
     */
    public List<String> getNonInternalKeyspaces()
    {
        return getUserKeyspaces().stream().filter(ks -> !SchemaConstants.isInternalKeyspace(ks)).collect(Collectors.toList());
    }

    /**
     * Get metadata about keyspace inner ColumnFamilies
     *
     * @param keyspaceName The name of the keyspace
     *
     * @return metadata about ColumnFamilies the belong to the given keyspace
     */
    public Iterable<TableMetadata> getTablesAndViews(String keyspaceName)
    {
        Preconditions.checkNotNull(keyspaceName);
        KeyspaceMetadata ksm = ObjectUtils.getFirstNonNull(() -> updateHandler.schema().getKeyspaces().getNullable(keyspaceName),
                                                           () -> localKeyspaces.get(keyspaceName));
        Preconditions.checkNotNull(ksm, "Keyspace %s not found", keyspaceName);
        return ksm.tablesAndViews();
    }

    /**
     * @return collection of the all keyspace names registered in the system (system and non-system)
     */
    public Set<String> getKeyspaces()
    {
        return new ImmutableSet.Builder<String>().addAll(updateHandler.schema().getKeyspaces().names())
                                                 .addAll(localKeyspaces.getAllNames())
                                                 .build();
    }

    /* TableMetadata/Ref query/control methods */

    /**
     * Given a keyspace name and table/view name, get the table metadata
     * reference. If the keyspace name or table/view name is not present
     * this method returns null.
     *
     * @return TableMetadataRef object or null if it wasn't found
     */
    @Override
    public TableMetadataRef getTableMetadataRef(String keyspace, String table)
    {
        TableMetadata tm = getTableMetadata(keyspace, table);
        return tm == null
               ? null
               : schemaRefCache.getTableMetadataRef(tm.id);
    }

    public TableMetadataRef getIndexTableMetadataRef(String keyspace, String index)
    {
        return schemaRefCache.getIndexTableMetadataRef(keyspace, index);
    }

    /**
     * Get Table metadata by its identifier
     *
     * @param id table or view identifier
     *
     * @return metadata about Table or View
     */
    @Override
    public TableMetadataRef getTableMetadataRef(TableId id)
    {
        TableMetadataRef ref = schemaRefCache.getTableMetadataRef(id);
        return getTableMetadata(ref.keyspace, ref.name) == null ? null : ref;
    }

    @Override
    public TableMetadataRef getTableMetadataRef(Descriptor descriptor)
    {
        return getTableMetadataRef(descriptor.ksname, descriptor.cfname);
    }

    /**
     * Given a keyspace name and table name, get the table
     * meta data. If the keyspace name or table name is not valid
     * this function returns null.
     *
     * @param keyspace The keyspace name
     * @param table The table name
     *
     * @return TableMetadata object or null if it wasn't found
     */
    public TableMetadata getTableMetadata(String keyspace, String table)
    {
        assert keyspace != null;
        assert table != null;

        KeyspaceMetadata ksm = getKeyspaceMetadata(keyspace);
        return ksm == null
               ? null
               : ksm.getTableOrViewNullable(table);
    }

    @Override
    public TableMetadata getTableMetadata(TableId id)
    {
        return ObjectUtils.getFirstNonNull(() -> updateHandler.schema().getKeyspaces().getTableOrViewNullable(id),
                                           () -> localKeyspaces.getTableOrView(id),
                                           () -> VirtualKeyspaceRegistry.instance.getTableMetadataNullable(id));
    }

    public TableMetadata validateTable(String keyspaceName, String tableName)
    {
        if (tableName.isEmpty())
            throw new InvalidRequestException("non-empty table is required");

        KeyspaceMetadata keyspace = getKeyspaceMetadata(keyspaceName);
        if (keyspace == null)
            throw new KeyspaceNotDefinedException(format("keyspace %s does not exist", keyspaceName));

        TableMetadata metadata = keyspace.getTableOrViewNullable(tableName);
        if (metadata == null)
            throw new InvalidRequestException(format("table %s does not exist", tableName));

        return metadata;
    }

    public TableMetadata getTableMetadata(Descriptor descriptor)
    {
        return getTableMetadata(descriptor.ksname, descriptor.cfname);
    }

    /* Function helpers */

    /**
     * Get all function overloads with the specified name
     *
     * @param name fully qualified function name
     * @return an empty list if the keyspace or the function name are not found;
     *         a non-empty collection of {@link Function} otherwise
     */
    public Collection<Function> getFunctions(FunctionName name)
    {
        if (!name.hasKeyspace())
            throw new IllegalArgumentException(String.format("Function name must be fully qualified: got %s", name));

        KeyspaceMetadata ksm = getKeyspaceMetadata(name.keyspace);
        return ksm == null
               ? Collections.emptyList()
               : ksm.functions.get(name);
    }

    /**
     * Find the function with the specified name
     *
     * @param name fully qualified function name
     * @param argTypes function argument types
     * @return an empty {@link Optional} if the keyspace or the function name are not found;
     *         a non-empty optional of {@link Function} otherwise
     */
    public Optional<Function> findFunction(FunctionName name, List<AbstractType<?>> argTypes)
    {
        if (!name.hasKeyspace())
            throw new IllegalArgumentException(String.format("Function name must be fully quallified: got %s", name));

        KeyspaceMetadata ksm = getKeyspaceMetadata(name.keyspace);
        return ksm == null
               ? Optional.empty()
               : ksm.functions.find(name, argTypes);
    }

    /* Version control */


    /**
     * Read schema from system keyspace and calculate MD5 digest of every row, resulting digest
     * will be converted into UUID which would act as content-based version of the schema.
     */
    public void updateVersion()
    {
        UUID version = SchemaKeyspace.calculateSchemaDigest();
        updateHandler.updateVersion(version);
        SystemKeyspace.updateSchemaVersion(version);
        SchemaDiagnostics.versionUpdated(schema());
    }

    /*
     * Like updateVersion, but also announces via gossip
     */
    public void updateVersionAndAnnounce()
    {
        updateVersion();
        passiveAnnounceVersion();
    }

    @Override
    public void onRemove(InetAddressAndPort endpoint)
    {
        if (updateHandler instanceof IEndpointStateChangeSubscriber)
            ((IEndpointStateChangeSubscriber) updateHandler).onRemove(endpoint);
    }

    /**
     * Announce my version passively over gossip.
     * Used to notify nodes as they arrive in the cluster.
     */
    private void passiveAnnounceVersion()
    {
        Schema schema = schema();
        Gossiper.instance.addLocalApplicationState(ApplicationState.SCHEMA, StorageService.instance.valueFactory.schema(schema.getVersion()));
        SchemaDiagnostics.versionAnnounced(schema());
    }

    /**
     * Clear all KS/CF metadata and reset version.
     */
    public synchronized void clear()
    {
        getNonSystemKeyspaces().forEach(k -> removeRefs(getKeyspaceMetadata(k)));
        updateVersionAndAnnounce();
        SchemaDiagnostics.schemataCleared(schema());
    }

    public void applyReceivedSchemaMutationsOrThrow(InetAddressAndPort from, Collection<Mutation> payload)
    {
        updateHandler.asGossipAwareTrackerOrThrow("Received schema push request from " + from).applyReceivedSchemaMutations(from, payload);
    }

    public Collection<Mutation> prepareRequestedSchemaMutationsOrThrow(InetAddressAndPort from)
    {
        return updateHandler.asGossipAwareTrackerOrThrow("Received schema pull request from " + from).prepareRequestedSchemaMutations(from);
    }

    public Schema schema()
    {
        return updateHandler.schema();
    }

    public void applyChangesLocally(KeyspacesDiff diff)
    {
        diff.dropped.forEach(this::dropKeyspace);
        diff.created.forEach(this::createKeyspace);
        diff.altered.forEach(this::alterKeyspace);
    }

    private void alterKeyspace(KeyspaceDiff delta)
    {
        SchemaDiagnostics.keyspaceAltering(schema(), delta);

        // drop tables and views
        delta.views.dropped.forEach(this::dropView);
        delta.tables.dropped.forEach(this::dropTable);

        // add tables and views
        delta.tables.created.forEach(this::createTable);
        delta.views.created.forEach(this::createView);

        // update tables and views
        delta.tables.altered.forEach(diff -> alterTable(diff.after));
        delta.views.altered.forEach(diff -> alterView(diff.after));

        // deal with all added, and altered views
        Keyspace.open(delta.after.name).viewManager.reload(true);

        schemaChangeNotifier.notifyKeyspaceAltered(delta);
    }

    private void createKeyspace(KeyspaceMetadata keyspace)
    {
        SchemaDiagnostics.keyspaceCreating(schema(), keyspace);
        Keyspace.open(keyspace.name);
        schemaChangeNotifier.notifyKeyspaceCreated(keyspace);
    }

    private void dropKeyspace(KeyspaceMetadata keyspace)
    {
        SchemaDiagnostics.keyspaceDroping(schema(), keyspace);
        keyspace.views.forEach(this::dropView);
        keyspace.tables.forEach(this::dropTable);

        // remove the keyspace from the static instances.
        removeKeyspaceInstance(keyspace.name, Keyspace::unload);
        Keyspace.writeOrder.awaitNewBarrier();
        schemaChangeNotifier.notifyKeyspaceDropped(keyspace);
    }

    private void dropView(ViewMetadata metadata)
    {
        Keyspace.open(metadata.keyspace()).viewManager.dropView(metadata.name());
        dropTable(metadata.metadata);
    }

    private void dropTable(TableMetadata metadata)
    {
        SchemaDiagnostics.tableDropping(schema(), metadata);
        ColumnFamilyStore cfs = Keyspace.open(metadata.keyspace).getColumnFamilyStore(metadata.name);
        assert cfs != null;
        // make sure all the indexes are dropped, or else.
        cfs.indexManager.markAllIndexesRemoved();
        CompactionManager.instance.interruptCompactionFor(Collections.singleton(metadata), (sstable) -> true, true);
        if (DatabaseDescriptor.isAutoSnapshot())
            cfs.snapshot(Keyspace.getTimestampedSnapshotNameWithPrefix(cfs.name, ColumnFamilyStore.SNAPSHOT_DROP_PREFIX));
        CommitLog.instance.forceRecycleAllSegments(Collections.singleton(metadata.id));
        Keyspace.open(metadata.keyspace).dropCf(metadata.id);
        SchemaDiagnostics.tableDropped(schema(), metadata);
    }

    private void createTable(TableMetadata table)
    {
        SchemaDiagnostics.tableCreating(schema(), table);
        Keyspace.open(table.keyspace).initCf(schemaRefCache.getTableMetadataRef(table.id), true);
        SchemaDiagnostics.tableCreated(schema(), table);
    }

    private void createView(ViewMetadata view)
    {
        Keyspace.open(view.keyspace()).initCf(schemaRefCache.getTableMetadataRef(view.metadata.id), true);
    }

    private void alterTable(TableMetadata updated)
    {
        SchemaDiagnostics.tableAltering(schema(), updated);
        Keyspace.open(updated.keyspace).getColumnFamilyStore(updated.name).reload();
        SchemaDiagnostics.tableAltered(schema(), updated);
    }

    private void alterView(ViewMetadata updated)
    {
        Keyspace.open(updated.keyspace()).getColumnFamilyStore(updated.name()).reload();
    }

    /**
     * Clear all locally stored schema information and reset schema to initial state.
     * Called by user (via JMX) who wants to get rid of schema disagreement.
     */
    public void resetLocalSchema()
    {
        logger.info("Starting local schema reset...");

        SchemaMigrationDiagnostics.resetLocalSchema();
        SchemaKeyspace.truncate();
        clear();
        updateHandler.reset();

        logger.info("Local schema reset is complete.");
    }

    /**
     * We have a set of non-local, distributed system keyspaces, e.g. system_traces, system_auth, etc.
     * (see {@link SchemaConstants#REPLICATED_SYSTEM_KEYSPACE_NAMES}), that need to be created on cluster initialisation,
     * and later evolved on major upgrades (sometimes minor too). This method compares the current known definitions
     * of the tables (if the keyspace exists) to the expected, most modern ones expected by the running version of C*;
     * if any changes have been detected, a schema Mutation will be created which, when applied, should make
     * cluster's view of that keyspace aligned with the expected modern definition.
     *
     * @param keyspace   the expected modern definition of the keyspace
     * @param generation timestamp to use for the table changes in the schema mutation
     *
     * @return empty Optional if the current definition is up to date, or an Optional with the Mutation that would
     *         bring the schema in line with the expected definition.
     */
    public Optional<Mutation> evolveSystemKeyspace(KeyspaceMetadata keyspace, long generation)
    {
        Mutation.SimpleBuilder builder = null;

        KeyspaceMetadata definedKeyspace = instance.getKeyspaceMetadata(keyspace.name);
        Tables definedTables = null == definedKeyspace ? Tables.none() : definedKeyspace.tables;

        for (TableMetadata table : keyspace.tables)
        {
            if (table.equals(definedTables.getNullable(table.name)))
                continue;

            if (null == builder)
            {
                // for the keyspace definition itself (name, replication, durability) always use generation 0;
                // this ensures that any changes made to replication by the user will never be overwritten.
                builder = SchemaKeyspace.makeCreateKeyspaceMutation(keyspace.name, keyspace.params, 0);

                // now set the timestamp to generation, so the tables have the expected timestamp
                builder.timestamp(generation);
            }

            // for table definitions always use the provided generation; these tables, unlike their containing
            // keyspaces, are *NOT* meant to be altered by the user; if their definitions need to change,
            // the schema must be updated in code, and the appropriate generation must be bumped.
            SchemaKeyspace.addTableToSchemaMutation(table, true, builder);
        }

        return builder == null ? Optional.empty() : Optional.of(builder.build());
    }

}
