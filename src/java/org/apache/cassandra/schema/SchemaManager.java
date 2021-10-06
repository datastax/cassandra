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
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.KeyspaceNotDefinedException;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
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
import org.apache.cassandra.schema.SchemaTransformation.SchemaTransformationResult;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;

import static com.google.common.collect.Iterables.size;
import static java.lang.String.format;
import static org.apache.cassandra.config.DatabaseDescriptor.isDaemonInitialized;
import static org.apache.cassandra.config.DatabaseDescriptor.isToolInitialized;

/**
 * Manages keyspace instances. It provides methods to query schema, but it does not provide any methods to modify the
 * schema. All schema modification are managed by the implementation of {@link SchemaUpdateHandler}. Once the schema is
 * updated, {@link SchemaUpdateHandler} applies the changes to the keyspace instances stored by {@link SchemaManager}.
 */
public final class SchemaManager implements SchemaProvider, IEndpointStateChangeSubscriber
{
    private final static Logger logger = LoggerFactory.getLogger(SchemaManager.class);

    public static final String FORCE_LOAD_LOCAL_KEYSPACES_PROP = "cassandra.schema.force_load_local_keyspaces";
    private final static boolean FORCE_LOAD_LOCAL_KEYSPACES = Boolean.getBoolean(FORCE_LOAD_LOCAL_KEYSPACES_PROP);

    public static final String FORCE_OFFLINE_MODE_PROP = "cassandra.schema.force_offline_mode";
    private final static boolean FORCE_OFFLINE_MODE = Boolean.getBoolean(FORCE_OFFLINE_MODE_PROP);

    public static final String SCHEMA_UPDATE_TIMEOUT_PROP = "cassandra.schema.update_timeout_seconds";
    private final static Duration SCHEMA_UPDATE_TIMEOUT = Duration.ofSeconds(Integer.getInteger(SCHEMA_UPDATE_TIMEOUT_PROP, 300));

    public static final SchemaManager instance = new SchemaManager();

    private final LocalKeyspaces localKeyspaces;

    private final SchemaRefCache schemaRefCache = new SchemaRefCache();

    // Keyspace objects, one per keyspace. Only one instance should ever exist for any given keyspace.
    private final ConcurrentMap<String, CompletableFuture<Keyspace>> keyspaceInstances = new NonBlockingHashMap<>();

    private final SchemaChangeNotifier schemaChangeNotifier = new SchemaChangeNotifier();

    private final ThreadLocal<SchemaManager> threadContext = new ThreadLocal<>();

    @VisibleForTesting
    final SchemaUpdateHandler updateHandler;

    private final Executor executor;

    private final boolean updateInstances;

    private final boolean online;

    /**
     * Initialize empty schema object and load the hardcoded system tables
     */
    private SchemaManager()
    {
        this(new LocalKeyspaces(FORCE_LOAD_LOCAL_KEYSPACES || isDaemonInitialized() || isToolInitialized()),
             SchemaUpdateHandlerFactoryProvider.instance.get(),
             Stage.SCHEMA_UPDATE.executor(),
             !FORCE_OFFLINE_MODE && (isDaemonInitialized() || isToolInitialized()),
             !FORCE_OFFLINE_MODE && isDaemonInitialized());
    }

    public SchemaManager(LocalKeyspaces localKeyspaces, SchemaUpdateHandlerFactory updateHandlerFactory, Executor executor, boolean updateInstances, boolean online)
    {
        this.updateInstances = updateInstances;
        this.online = online;
        this.executor = createExecutor(executor);
        this.updateHandler = updateHandlerFactory.getSchemaUpdateHandler(online, this.executor, schemaChangeNotifier::notifyPreChanges, this::onSchemaChanged);
        this.localKeyspaces = localKeyspaces;
        this.localKeyspaces.getAll().forEach(ksm -> {
            schemaRefCache.addNewRefs(ksm);
            SchemaDiagnostics.metadataInitialized(this.updateHandler.schema(), ksm);
        });
    }

    private Executor createExecutor(Executor underlying)
    {
        return runnable -> {
            if (threadContext.get() == this)
            {
                runnable.run();
            }
            else
            {
                underlying.execute(() -> {
                    assert threadContext.get() == null;
                    threadContext.set(this);
                    try
                    {
                        runnable.run();
                    }
                    finally
                    {
                        threadContext.remove();
                    }
                });
            }
        };
    }

    private void onSchemaChanged(SchemaTransformationResult update)
    {
        logger.debug("Schema changed: {}", update);
        assert threadContext.get() == this;
        updateRefs(update.diff);
        applyChangesLocally(update.diff);
        announceVersionUpdate(update.after);
    }

    private void announceVersionUpdate(Schema schema)
    {
        if (online)
        {
            SystemKeyspace.updateSchemaVersion(schema.getVersion());
            SchemaDiagnostics.versionUpdated(schema);
        }
        if (online && Gossiper.instance.isEnabled())
        {
            Gossiper.instance.addLocalApplicationState(ApplicationState.SCHEMA, StorageService.instance.valueFactory.schema(schema.getVersion()));
            SchemaDiagnostics.versionAnnounced(schema);
        }
    }

    public void startSync()
    {
        logger.debug("Starting update handler");
        updateHandler.start();
    }

    public boolean waitUntilReady(Duration timeout)
    {
        logger.debug("Waiting for update handler to be ready...");
        return updateHandler.waitUntilReady(timeout);
    }

    public void initializeSchemaFromDisk()
    {
        logger.debug("Initializing schema from disk");
        SchemaDiagnostics.schemaLoading(schema());
        FBUtilities.waitOnFuture(CompletableFuture.supplyAsync(() -> {
            assert threadContext.get() == this;
            SchemaTransformationResult update = updateHandler.initializeSchemaFromDisk();
            updateRefs(update.diff);
            announceVersionUpdate(update.after);
            logger.debug("Initialized schema from disk: {}", update);
            SchemaDiagnostics.schemaLoaded(update.after);
            return update;
        }, executor), SCHEMA_UPDATE_TIMEOUT);
    }

    public void reloadSchemaFromDisk()
    {
        logger.debug("Reloading schema from disk");
        SchemaDiagnostics.schemaLoading(schema());
        FBUtilities.waitOnFuture(CompletableFuture.supplyAsync(() -> {
            SchemaTransformationResult update = updateHandler.reloadSchemaFromDisk();
            SchemaDiagnostics.schemaLoaded(schema());
            return update;
        }, executor), SCHEMA_UPDATE_TIMEOUT);
    }

    public SchemaTransformationResult apply(SchemaTransformation transformation, boolean locally)
    {
        logger.debug("Applying schema transformation{}: {}", locally ? " locally" : "", transformation);
        return FBUtilities.waitOnFuture(CompletableFuture.supplyAsync(() -> updateHandler.apply(transformation, locally), executor), SCHEMA_UPDATE_TIMEOUT);
    }

    public void clearUnsafe()
    {
        logger.debug("Clearing schema");
        FBUtilities.waitOnFuture(CompletableFuture.runAsync(() -> {
            updateRefs(Keyspaces.diff(schema().getKeyspaces(), Keyspaces.none()));
            SchemaTransformationResult update = gossipAwareSchemaUpdateHandlerOrThrow(null).clearUnsafe();
            SchemaDiagnostics.schemaCleared(schema());
        }, executor), SCHEMA_UPDATE_TIMEOUT);
    }

    /**
     * Update/create/drop the {@link TableMetadataRef} in {@link SchemaManager}.
     */
    private void updateRefs(KeyspacesDiff diff)
    {
        Schema schema = updateHandler.schema();
        diff.dropped.forEach(ksm -> {
            schemaRefCache.removeRefs(ksm);
            SchemaDiagnostics.metadataRemoved(schema, ksm);
        });
        diff.created.forEach(ksm -> {
            schemaRefCache.addNewRefs(ksm);
            SchemaDiagnostics.metadataInitialized(schema, ksm);
        });
        diff.altered.forEach(delta -> {
            schemaRefCache.updateRefs(delta.before, delta.after);
            SchemaDiagnostics.metadataReloaded(schema, delta.before, delta.after, delta.tables, delta.views, delta.before.tables.indexesDiff(delta.after.tables));
        });
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

        Keyspace instance = Keyspace.open(metadata.keyspace);
        if (instance == null)
            return null;

        return instance.hasColumnFamilyStore(metadata.id)
               ? instance.getColumnFamilyStore(metadata.id)
               : null;
    }

    /**
     * Remove keyspace from schema. This puts a temporary entry in the map that throws an exception when queried.
     * When the metadata is also deleted, that temporary entry must also be deleted using clearKeyspaceInstance below.
     *
     * @param keyspaceName The name of the keyspace to remove
     */
    private void removeKeyspaceInstance(String keyspaceName, Consumer<Keyspace> unloadFunction)
    {
        CompletableFuture<Keyspace> droppedFuture = new CompletableFuture<>();
        droppedFuture.completeExceptionally(new KeyspaceNotDefinedException(keyspaceName));

        CompletableFuture<Keyspace> existingFuture = keyspaceInstances.put(keyspaceName, droppedFuture);
        if (existingFuture == null || existingFuture.isCompletedExceptionally())
            return;

        Keyspace instance = existingFuture.join();
        unloadFunction.accept(instance);

        CompletableFuture<Keyspace> future = keyspaceInstances.remove(keyspaceName);
        assert future == droppedFuture;
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
                    empty.complete(FBUtilities.waitOnFuture(CompletableFuture.supplyAsync(loadFunction, executor), SCHEMA_UPDATE_TIMEOUT));
                }
                catch (Throwable t)
                {
                    empty.completeExceptionally(new Throwable(t));
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

    /**
     * Returns the number of tables in all local and non-local keyspaces. It does not count tables in virtual keyspaces.
     */
    public int getNumberOfTables()
    {
        return schema().getKeyspaces().stream().mapToInt(k -> size(k.tablesAndViews())).sum() + localKeyspaces.getAllTablesAndViewsCount();
    }

    /**
     * Finds a view metadata by keyspace and view names. Does not look into virtual keyspaces.
     *
     * @param keyspaceName local or non-local keyspace, but not a virtual keyspace
     * @param viewName     view name
     * @return view metadata or {@code null} if view wasn't found
     */
    public ViewMetadata getView(String keyspaceName, String viewName)
    {
        Preconditions.checkNotNull(keyspaceName);
        KeyspaceMetadata ksm = ObjectUtils.getFirstNonNull(() -> schema().getKeyspaces().getNullable(keyspaceName),
                                                           () -> localKeyspaces.get(keyspaceName));
        return (ksm == null) ? null : ksm.views.getNullable(viewName);
    }

    /**
     * Finds a keyspace metadata by name.
     *
     * @param keyspaceName local, non-local or virtual keyspace name.
     * @return keyspace metadata or {@code null} if keyspace wasn't found
     */
    @Override
    public KeyspaceMetadata getKeyspaceMetadata(String keyspaceName)
    {
        Preconditions.checkNotNull(keyspaceName);
        return ObjectUtils.getFirstNonNull(() -> schema().getKeyspaces().getNullable(keyspaceName),
                                           () -> localKeyspaces.get(keyspaceName),
                                           () -> VirtualKeyspaceRegistry.instance.getKeyspaceMetadataNullable(keyspaceName));
    }

    /**
     * Returns all non-local keyspaces, that is, all but {@link SchemaConstants#LOCAL_SYSTEM_KEYSPACE_NAMES}
     * or virtual keyspaces.
     */
    public Keyspaces getNonSystemKeyspaces()
    {
        return schema().getKeyspaces();
    }

    /**
     * Returns all non-local keyspaces whose replication strategy is not {@link LocalStrategy}.
     */
    public Keyspaces getNonLocalStrategyKeyspaces()
    {
        return schema().getKeyspaces().filter(keyspace -> keyspace.params.replication.klass != LocalStrategy.class);
    }

    /**
     * Returns keyspaces that partition data across the ring.
     */
    public Keyspaces getPartitionedKeyspaces()
    {
        return schema().getKeyspaces().filter(keyspace -> Keyspace.open(keyspace.name).getReplicationStrategy().isPartitioned());
    }

    /**
     * Returns user keyspaces, that is all but {@link SchemaConstants#LOCAL_SYSTEM_KEYSPACE_NAMES},
     * {@link SchemaConstants#REPLICATED_SYSTEM_KEYSPACE_NAMES} or virtual keyspaces.
     */
    public Keyspaces getUserKeyspaces()
    {
        return schema().getKeyspaces().without(SchemaConstants.REPLICATED_SYSTEM_KEYSPACE_NAMES);
    }

    /**
     * Returns non-internal keyspaces
     */
    public Keyspaces getNonInternalKeyspaces()
    {
        return getUserKeyspaces().filter(ks -> !SchemaConstants.isInternalKeyspace(ks.name));
    }

    /**
     * Get metadata about keyspace inner ColumnFamilies
     *
     * @param keyspaceName The name of the keyspace
     * @return metadata about ColumnFamilies the belong to the given keyspace
     */
    public Iterable<TableMetadata> getTablesAndViews(String keyspaceName)
    {
        Preconditions.checkNotNull(keyspaceName);
        KeyspaceMetadata ksm = ObjectUtils.getFirstNonNull(() -> schema().getKeyspaces().getNullable(keyspaceName),
                                                           () -> localKeyspaces.get(keyspaceName));
        Preconditions.checkNotNull(ksm, "Keyspace %s not found", keyspaceName);
        return ksm.tablesAndViews();
    }

    /**
     * @return collection of the all keyspace names registered in the system (system and non-system)
     */
    public Set<String> getKeyspaces()
    {
        return new ImmutableSet.Builder<String>().addAll(schema().getKeyspaces().names())
                                                 .addAll(localKeyspaces.getAllNames())
                                                 .build();
    }

    public Keyspaces getLocalKeyspaces()
    {
        return Keyspaces.builder().add(localKeyspaces.getAll()).build();
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
     * @return metadata about Table or View
     */
    @Override
    public TableMetadataRef getTableMetadataRef(TableId id)
    {
        return schemaRefCache.getTableMetadataRef(id);
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
     * @param table    The table name
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
        return ObjectUtils.getFirstNonNull(() -> schema().getKeyspaces().getTableOrViewNullable(id),
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
     * a non-empty collection of {@link Function} otherwise
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
     * @param name     fully qualified function name
     * @param argTypes function argument types
     * @return an empty {@link Optional} if the keyspace or the function name are not found;
     * a non-empty optional of {@link Function} otherwise
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

    public void applyReceivedSchemaMutationsOrThrow(InetAddressAndPort from, Collection<Mutation> payload)
    {
        logger.debug("Applying schema mutations from {}: {}", from, payload);
        FBUtilities.waitOnFuture(CompletableFuture.supplyAsync(() -> gossipAwareSchemaUpdateHandlerOrThrow("Received schema push request from " + from).applyReceivedSchemaMutations(from, payload), executor), SCHEMA_UPDATE_TIMEOUT);
    }

    public Collection<Mutation> prepareRequestedSchemaMutationsOrThrow(InetAddressAndPort from)
    {
        logger.debug("Preparing schema mutations for {}", from);
        return FBUtilities.waitOnFuture(CompletableFuture.supplyAsync(() -> gossipAwareSchemaUpdateHandlerOrThrow("Received schema pull request from " + from).prepareRequestedSchemaMutations(from), executor), SCHEMA_UPDATE_TIMEOUT);
    }

    public Schema schema()
    {
        return updateHandler.schema();
    }

    private void applyChangesLocally(KeyspacesDiff diff)
    {
        if (updateInstances)
        {
            diff.dropped.forEach(this::dropKeyspace);
            diff.created.forEach(this::createKeyspace);
            diff.altered.forEach(this::alterKeyspace);
        }
    }

    private void alterKeyspace(KeyspaceDiff delta)
    {
        SchemaDiagnostics.keyspaceAltering(schema(), delta);

        // update keyspace metadata
        Keyspace keyspace = getKeyspaceInstance(delta.before.name);
        assert keyspace != null;

        assert delta.before.name.equals(delta.after.name);

        // drop tables and views
        delta.tables.dropped.forEach(t -> dropTable(keyspace, t));
        delta.views.dropped.forEach(v -> dropView(keyspace, v));

        // add tables and views
        delta.tables.created.forEach(t -> createTable(keyspace, t));
        delta.views.created.forEach(v -> createView(keyspace, v));

        // update tables and views
        delta.tables.altered.forEach(diff -> alterTable(keyspace, diff.after));
        delta.views.altered.forEach(diff -> alterView(keyspace, diff.after));

        // update keyspace metadata in keyspace instance
        keyspace.setMetadata(delta.after);

        // deal with all added, and altered views
        keyspace.viewManager.reload(true);
        schemaChangeNotifier.notifyKeyspaceAltered(delta);

        SchemaDiagnostics.keyspaceAltered(schema(), delta);
    }

    private void createKeyspace(KeyspaceMetadata keyspace)
    {
        SchemaDiagnostics.keyspaceCreating(schema(), keyspace);
        Keyspace.open(keyspace.name);
        schemaChangeNotifier.notifyKeyspaceCreated(keyspace);
        SchemaDiagnostics.keyspaceCreated(schema(), keyspace);
    }

    private void dropKeyspace(KeyspaceMetadata keyspace)
    {
        SchemaDiagnostics.keyspaceDropping(schema(), keyspace);

        // remove the keyspace from the static instances
        removeKeyspaceInstance(keyspace.name, ks -> {
            keyspace.views.forEach(v -> dropView(ks, v));
            keyspace.tables.forEach(t -> dropTable(ks, t));
            ks.unload();
        });

        Keyspace.writeOrder.awaitNewBarrier();
        schemaChangeNotifier.notifyKeyspaceDropped(keyspace);
        SchemaDiagnostics.keyspaceDropped(schema(), keyspace);
    }

    private void dropView(Keyspace keyspace, ViewMetadata metadata)
    {
        keyspace.viewManager.dropView(metadata.name());
        dropTable(keyspace, metadata.metadata);
    }

    private void dropTable(Keyspace keyspace, TableMetadata metadata)
    {
        SchemaDiagnostics.tableDropping(schema(), metadata);
        keyspace.dropCf(metadata.id);
        SchemaDiagnostics.tableDropped(schema(), metadata);
    }

    private void createTable(Keyspace keyspace, TableMetadata table)
    {
        SchemaDiagnostics.tableCreating(schema(), table);
        keyspace.initCf(schemaRefCache.getTableMetadataRef(table.id), true);
        SchemaDiagnostics.tableCreated(schema(), table);
    }

    private void createView(Keyspace keyspace, ViewMetadata view)
    {
        SchemaDiagnostics.tableCreating(schema(), view.metadata);
        keyspace.initCf(schemaRefCache.getTableMetadataRef(view.metadata.id), true);
        SchemaDiagnostics.tableCreated(schema(), view.metadata);
    }

    private void alterTable(Keyspace keyspace, TableMetadata updated)
    {
        SchemaDiagnostics.tableAltering(schema(), updated);
        keyspace.getColumnFamilyStore(updated.name).reload();
        SchemaDiagnostics.tableAltered(schema(), updated);
    }

    private void alterView(Keyspace keyspace, ViewMetadata updated)
    {
        SchemaDiagnostics.tableAltering(schema(), updated.metadata);
        keyspace.getColumnFamilyStore(updated.name()).reload();
        SchemaDiagnostics.tableAltered(schema(), updated.metadata);
    }

    private Optional<SchemaUpdateHandler.GossipAware> gossipAwareSchemaUpdateHandler()
    {
        return updateHandler instanceof SchemaUpdateHandler.GossipAware
               ? Optional.of((SchemaUpdateHandler.GossipAware) updateHandler)
               : Optional.empty();
    }

    private SchemaUpdateHandler.GossipAware gossipAwareSchemaUpdateHandlerOrThrow(String msg)
    {
        String format = "The current schema tracker (%s) does not implement GossipAware. %s";
        return gossipAwareSchemaUpdateHandler().orElseThrow(() -> new UnsupportedOperationException(String.format(format, this.getClass().getName(), StringUtils.trimToEmpty(msg))));
    }
}
