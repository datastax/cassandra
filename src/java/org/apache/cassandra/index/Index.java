/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.index;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.IndexHints;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.internal.CollatedViewIndexBuilder;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ReducingKeyIterator;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.lucene.index.CorruptIndexException;


/**
 * Consisting of a top level Index interface and two sub-interfaces which handle read and write operations,
 * Searcher and Indexer respectively, this defines a secondary index implementation.
 * Instantiation is done via reflection and implementations must provide a constructor which takes the base
 * table's ColumnFamilyStore and the IndexMetadata which defines the Index as arguments. e.g:
 *  {@code MyCustomIndex( ColumnFamilyStore baseCfs, IndexMetadata indexDef )}
 *
 * The main interface defines methods for index management, index selection at both write and query time,
 * as well as validation of values that will ultimately be indexed.
 * Two sub-interfaces are also defined, which represent single use helpers for short lived tasks at read and write time.
 * Indexer: an event listener which receives notifications at particular points during an update of a single partition
 *          in the base table.
 * Searcher: performs queries against the index based on a predicate defined in a RowFilter. An instance
 *          is expected to be single use, being involved in the execution of a single ReadCommand.
 * </p>
 * The main interface includes factory methods for obtaining instances of both of the sub-interfaces;
 * </p>
 * The methods defined in the top level interface can be grouped into 3 categories:
 * </p>
 * Management Tasks:
 * This group of methods is primarily concerned with maintenance of secondary indexes are mainly called from
 * SecondaryIndexManager. It includes methods for registering and un-registering an index, performing maintenance
 * tasks such as (re)building an index from SSTable data, flushing, invalidating and so forth, as well as some to
 * retrieve general metadata about the index (index name, any internal tables used for persistence etc.).
 * Several of these maintenance functions have a return type of {@code Callable<?>}; the expectation for these methods is
 * that any work required to be performed by the method be done inside the Callable so that the responsibility for
 * scheduling its execution can rest with SecondaryIndexManager. For instance, a task like reloading index metadata
 * following potential updates caused by modifications to the base table may be performed in a blocking way. In
 * contrast, adding a new index may require it to be built from existing SSTable data, a potentially expensive task
 * which should be performed asynchronously.
 * </p>
 * Index Selection:
 * There are two facets to index selection, write time and read time selection. The former is concerned with
 * identifying whether an index should be informed about a particular write operation. The latter is about providing
 * means to use the index for search during query execution.
 * </p>
 * Validation:
 * Values that may be written to an index are checked as part of input validation, prior to an update or insert
 * operation being accepted.
 * </p>
 *
 * Sub-interfaces:
 * </p>
 * Update processing:
 * Indexes are subscribed to the stream of events generated by modifications to the base table. Subscription is
 * done via first registering the Index with the base table's SecondaryIndexManager. For each partition update, the set
 * of registered indexes are then filtered based on the properties of the update using the selection methods on the main
 * interface described above. Each of the indexes in the filtered set then provides an event listener to receive
 * notifications about the update as it is processed. As such then, a event handler instance is scoped to a single
 * partition update; SecondaryIndexManager obtains a new handler for every update it processes (via a call to the
 * factory method, indexerFor. That handler will then receive all events for the update, before being
 * discarded by the SecondaryIndexManager. Indexer instances are never re-used by SecondaryIndexManager and the
 * expectation is that each call to indexerFor should return a unique instance, or at least if instances can
 * be recycled, that a given instance is only used to process a single partition update at a time.
 * </p>
 * Search:
 * Each query (i.e. a single ReadCommand) that uses indexes will use a single instance of Index.Searcher. As with
 * processing of updates, an Index must be registered with the primary table's SecondaryIndexManager to be able to
 * support queries. During the processing of a ReadCommand, the Expressions in its RowFilter are examined to determine
 * whether any of them are supported by a registered Index. supportsExpression is used to filter out Indexes which
 * cannot support a given Expression. After filtering, the set of candidate indexes are ranked according to the result
 * of getEstimatedResultRows and the most selective (i.e. the one expected to return the smallest number of results) is
 * chosen. A Searcher instance is then obtained from the searcherFor method and used to perform the actual Index lookup.
 * Finally, Indexes can define a post processing step to be performed on the coordinator, after results (partitions from
 * the primary table) have been received from replicas and reconciled. This post processing is defined as a
 * {@code java.util.functions.BiFunction<PartitionIterator, RowFilter, PartitionIterator>}, that is a function which takes as
 * arguments a PartitionIterator (containing the reconciled result rows) and a RowFilter (from the ReadCommand being
 * executed) and returns another iterator of partitions, possibly having transformed the initial results in some way.
 * The post-processing function is obtained from the Index's postProcessorFor method; the built-in indexes which ship
 * with Cassandra return a no-op function here.
 * </p>
 * An optional static method may be provided to validate custom index options (two variants are supported):
 *
 * <pre>{@code public static Map<String, String> validateOptions(Map<String, String> options);}</pre>
 *
 * The input is the map of index options supplied in the WITH clause of a CREATE INDEX statement.
 *
 * <pre>{@code public static Map<String, String> validateOptions(Map<String, String> options, TableMetadata metadata);}</pre>
 *
 * In this version, the base table's metadata is also supplied as an argument.
 * If both overloaded methods are provided, only the one including the base table's metadata will be invoked.
 * </p>
 * The validation method should return a map containing any of the supplied options which are not valid for the
 * implementation. If the returned map is not empty, validation is considered failed and an error is raised.
 * Alternatively, the implementation may choose to throw an org.apache.cassandra.exceptions.ConfigurationException
 * if invalid options are encountered.
 */
public interface Index
{
    /**
     * Supported loads. An index could be badly initialized and support only reads i.e.
     */
    enum LoadType
    {
        READ, WRITE, ALL, NOOP;

        public boolean supportsWrites()
        {
            return this == ALL || this == WRITE;
        }

        public boolean supportsReads()
        {
            return this == ALL || this == READ;
        }
    }

    /*
     * Helpers for building indexes from SSTable data
     */

    /**
     * Provider of {@code SecondaryIndexBuilder} instances. See {@code getBuildTaskSupport} and
     * {@code SecondaryIndexManager} for more detail.
     */
    interface IndexBuildingSupport
    {
        SecondaryIndexBuilder getIndexBuildTask(ColumnFamilyStore cfs, Set<Index> indexes, Collection<SSTableReader> sstables, boolean isFullRebuild);

        default List<SecondaryIndexBuilder> getParallelIndexBuildTasks(ColumnFamilyStore cfs, Set<Index> indexes, Collection<SSTableReader> sstables, boolean isFullRebuild)
        {
            return Collections.singletonList(getIndexBuildTask(cfs, indexes, sstables, isFullRebuild));
        }
    }

    /**
     * Default implementation of {@code IndexBuildingSupport} which uses a {@code ReducingKeyIterator} to obtain a
     * collated view of the data in the SSTables.
     */
    class CollatedViewIndexBuildingSupport implements IndexBuildingSupport
    {
        @SuppressWarnings("resource")
        public SecondaryIndexBuilder getIndexBuildTask(ColumnFamilyStore cfs, Set<Index> indexes, Collection<SSTableReader> sstables, boolean isFullRebuild)
        {
            return new CollatedViewIndexBuilder(cfs, indexes, new ReducingKeyIterator(sstables), sstables);
        }
    }

    /**
     * Singleton instance of {@code CollatedViewIndexBuildingSupport}, which may be used by any {@code Index}
     * implementation.
     */
    CollatedViewIndexBuildingSupport INDEX_BUILDER_SUPPORT = new CollatedViewIndexBuildingSupport();

    /*
     * Management functions
     */

    /**
     * Get an instance of a helper to provide tasks for building the index from a set of SSTable data.
     * When processing a number of indexes to be rebuilt, {@code SecondaryIndexManager.buildIndexesBlocking} groups
     * those with the same {@code IndexBuildingSupport} instance, allowing multiple indexes to be built with a
     * single pass through the data. The singleton instance returned from the default method implementation builds
     * indexes using a {@code ReducingKeyIterator} to provide a collated view of the SSTable data.
     *
     * @return an instance of the index build task helper. Index implementations which return <b>the same instance</b>
     * will be built using a single task.
     */
    default IndexBuildingSupport getBuildTaskSupport()
    {
        return INDEX_BUILDER_SUPPORT;
    }
    /**
     * Same as {@code getBuildTaskSupport} but can be overloaded with a specific 'recover' logic different than the index building one
     */
    default IndexBuildingSupport getRecoveryTaskSupport()
    {
        return getBuildTaskSupport();
    }
    
    /**
     * Returns the type of operations supported by the index in case its building has failed and it's needing recovery.
     *
     * @param isInitialBuild {@code true} if the failure is for the initial build task on index creation, {@code false}
     * if the failure is for a full rebuild or recovery.
     */
    default LoadType getSupportedLoadTypeOnFailure(boolean isInitialBuild)
    {
        return isInitialBuild ? LoadType.WRITE : LoadType.ALL;
    }

    /**
     * @return Period in millis to trigger a flush for indexes in the group. Non-positive value to disable it.
     */
    default int getFlushPeriodInMs()
    {
        return -1;
    }

    /**
     * Returns true if index initialization should be skipped, false if it should run
     * (via {@link #getInitializationTask()}); defaults to skipping based on {@link IndexBuildDecider#onInitialBuild()}
     * decision.
     */
    default boolean shouldSkipInitialization()
    {
        return IndexBuildDecider.instance.onInitialBuild().skipped();
    }

    /**
     * Return a task to perform any initialization work when a new index instance is created.
     * This may involve costly operations such as (re)building the index, and is performed asynchronously
     * by SecondaryIndexManager
     * @return a task to perform any necessary initialization work
     */
    Callable<?> getInitializationTask();

    /**
     * Returns the IndexMetadata which configures and defines the index instance. This should be the same
     * object passed as the argument to setIndexMetadata.
     * @return the index's metadata
     */
    IndexMetadata getIndexMetadata();

    /**
     * Return a task to reload the internal metadata of an index.
     * Called when the base table metadata is modified or when the configuration of the Index is updated
     * Implementations should return a task which performs any necessary work to be done due to
     * updating the configuration(s) such as (re)building etc. This task is performed asynchronously
     * by SecondaryIndexManager
     * @return task to be executed by the index manager during a reload
     */
    Callable<?> getMetadataReloadTask(IndexMetadata indexMetadata);

    /**
     * An index must be registered in order to be able to either subscribe to update events on the base
     * table and/or to provide Searcher functionality for reads. The double dispatch involved here, where
     * the Index actually performs its own registration by calling back to the supplied IndexRegistry's
     * own registerIndex method, is to make the decision as to whether or not to register an index belong
     * to the implementation, not the manager.
     * @param registry the index registry to register the instance with
     */
    void register(IndexRegistry registry);

    /**
     * Unregister current index when it's removed from system
     *
     * @param registry the index registry to unregister the instance with
     */
    default void unregister(IndexRegistry registry)
    {
        // for singleton index, the group key is the index itself
        registry.unregisterIndex(this, new Index.Group.Key(this));
    }

    /**
     * If the index implementation uses a local table to store its index data this method should return a
     * handle to it. If not, an empty Optional should be returned. Typically, this is useful for the built-in
     * Index implementations.
     * @return an Optional referencing the Index's backing storage table if it has one, or Optional.empty() if not.
     */
    Optional<ColumnFamilyStore> getBackingTable();

    /**
     * Return a task which performs a blocking flush of the index's data to persistent storage.
     * @return task to be executed by the index manager to perform the flush.
     */
    Callable<?> getBlockingFlushTask();

    /**
     * Return a task which invalidates the index, indicating it should no longer be considered usable.
     * This should include an clean up and releasing of resources required when dropping an index.
     * @return task to be executed by the index manager to invalidate the index.
     */
    Callable<?> getInvalidateTask();

    /**
     * Return a task which unload the index, indicating it should no longer be considered usable.
     * This should include a cleanup and releasing of resources required without removing files.
     *
     * @return task to be executed by the index manager to invalidate the index.
     */
    default Callable<?> getUnloadTask()
    {
        return () -> null;
    }

    /**
     * Return a task to truncate the index with the specified truncation timestamp.
     * Called when the base table is truncated.
     *
     * @param truncatedAt timestamp of the truncation operation. This will be the same timestamp used
     *                    in the truncation of the base table.
     * @return task to be executed by the index manager when the base table is truncated.
     */
    Callable<?> getTruncateTask(long truncatedAt);

    /**
     * Return a task to be executed before the node enters NORMAL state and finally joins the ring.
     *
     * @param hadBootstrap If the node had bootstrap before joining.
     * @return task to be executed by the index manager before joining the ring.
     */
    default Callable<?> getPreJoinTask(boolean hadBootstrap)
    {
        return null;
    }

    /**
     * Return true if this index can be built or rebuilt when the index manager determines it is necessary. Returning
     * false enables the index implementation (or some other component) to control if and when SSTable data is
     * incorporated into the index.
     * </p>
     * This is called by SecondaryIndexManager in buildIndexBlocking, buildAllIndexesBlocking and rebuildIndexesBlocking
     * where a return value of false causes the index to be exluded from the set of those which will process the
     * SSTable data.
     *
     * @return if the index should be included in the set which processes SSTable data, false otherwise.
     */
    boolean shouldBuildBlocking();

    /**
     * Get flush observer to observe partition/cell events generated by flushing SSTable (memtable flush or compaction).
     *
     * @param descriptor The descriptor of the sstable observer is requested for.
     * @param tracker The {@link LifecycleNewTracker} associated with the SSTable being written
     * @return SSTable flush observer.
     */
    default SSTableFlushObserver getFlushObserver(Descriptor descriptor, LifecycleNewTracker tracker)
    {
        return null;
    }

    /**
     * @param indexes the indexes to join
     * @return a comma-separated list of alphabetically sorted unqualified index names
     */
    static String joinNames(Iterable<Index> indexes)
    {
        return IndexMetadata.joinNames(getMetadata(indexes));
    }

    /**
     * @param indexes the indexes to get the metadata from
     * @return the list of index metadata
     */
    static List<IndexMetadata> getMetadata(Iterable<Index> indexes)
    {
        List<IndexMetadata> metadata = new ArrayList<>();
        for (Index index : indexes)
            metadata.add(index.getIndexMetadata());
        return metadata;
    }

    /*
     * Index selection
     */

    /**
     * Called to determine whether this index targets a specific column.
     * Used during schema operations such as when dropping or renaming a column, to check if
     * the index will be affected by the change. Typically, if an index answers that it does
     * depend upon a column, then schema operations on that column are not permitted until the index
     * is dropped or altered.
     *
     * @param column the column definition to check
     * @return true if the index depends on the supplied column being present; false if the column may be
     *              safely dropped or modified without adversely affecting the index
     */
    boolean dependsOn(ColumnMetadata column);

    /**
     * Called to determine whether this index can provide a searcher to execute a query on the
     * supplied expression. This forms part of the query validation done before a CQL select
     * statement is executed.
     *
     * @param expression a search query predicate
     * @return true if this index is capable of supporting such expressions, false otherwise
     */
    default boolean supportsExpression(RowFilter.Expression expression)
    {
        return supportsExpression(expression.column(), expression.operator());
    }

    /**
     * Called to determine whether this index can provide a searcher to execute a query on the
     * supplied column using the specified operator. This forms part of the query validation done
     * before a CQL select statement is executed.
     *
     * @param column the target column of a search query predicate
     * @param operator the operator of a search query predicate
     * @return true if this index is capable of supporting such expressions, false otherwise
     */
    boolean supportsExpression(ColumnMetadata column, Operator operator);

    /**
     * If the index supports custom search expressions using the
     * {@code}SELECT * FROM table WHERE expr(index_name, expression){@code} syntax, this
     * method should return the expected type of the expression argument.
     * For example, if the index supports custom expressions as Strings, calls to this
     * method should return {@code}UTF8Type.instance{@code}.
     * If the index implementation does not support custom expressions, then it should
     * return null.
     *
     * @return the type of custom index expressions supported by this index, or an
     *         null if custom expressions are not supported.
     */
    AbstractType<?> customExpressionValueType();

    /**
     * If the index supports custom search expressions using the
     * {@code SELECT * FROM table WHERE expr(index_name, expression)} syntax, this method should return a new
     * {@link RowFilter.CustomExpression} for the specified expression value. Index implementations may provide their
     * own implementations using method {@link RowFilter.CustomExpression#isSatisfiedBy(TableMetadata, DecoratedKey, Row)}
     * to filter reconciled rows in the coordinator. Otherwise, the default implementation will accept all rows.
     * See DB-2185 and DSP-16537 for further details.
     *
     * @param metadata the indexed table metadata
     * @param value the custom expression value
     * @return a custom index expression for the specified value
     */
    default RowFilter.CustomExpression customExpressionFor(TableMetadata metadata, ByteBuffer value)
    {
        return new RowFilter.CustomExpression(metadata, getIndexMetadata(), value);
    }

    /**
     * Returns whether this index transforms the indexed column values.
     *
     * @return {@code true} if this index transforms the indexed column values, {@code false} otherwise
     */
    default boolean isAnalyzed()
    {
        return false;
    }

    /**
     * Returns the {@link Analyzer} for this index, if any. If the index doesn't transform the column values,
     * this method will return an empty optional.
     *
     * @return the transforming column value analyzer for the index, if any
     */
    default Optional<Analyzer> getAnalyzer(ByteBuffer queriedValue)
    {
        return Optional.empty();
    }

    /**
     * Class representing a transformation of the indexed values done by the index.
     * </p>
     * This is used by the CQL operators when a filtering expression supported by an index is evaluated outside the
     * index. It can be used to perform the same transformation on values that the index does when indexing. That way,
     * the CQL operator can replicate the index behaviour when filtering results.
     */
    interface Analyzer
    {
        List<ByteBuffer> indexedTokens(ByteBuffer indexedValue);

        List<ByteBuffer> queriedTokens();
    }

    /**
     * Transform an initial RowFilter into the filter that will still need to applied
     * to a set of Rows after the index has performed it's initial scan.
     * Used in ReadCommand#executeLocal to reduce the amount of filtering performed on the
     * results of the index query.
     *
     * @param filter the intial filter belonging to a ReadCommand
     * @return the (hopefully) reduced filter that would still need to be applied after
     *         the index was used to narrow the initial result set
     */
    RowFilter getPostIndexQueryFilter(RowFilter filter);

    /**
     * Return an estimate of the number of results this index is expected to return for any given
     * query that it can be used to answer. Used in conjunction with indexes() and supportsExpression()
     * to determine the most selective index for a given ReadCommand. Additionally, this is also used
     * by StorageProxy.estimateResultsPerRange to calculate the initial concurrency factor for range requests
     *
     * @return the estimated average number of results a Searcher may return for any given query
     */
    long getEstimatedResultRows();

    /**
     * Check if current index is queryable based on the index status.
     *
     * @param status current status of the index
     * @return true if index should be queryable, false if index should be non-queryable
     */
    default boolean isQueryable(Status status)
    {
        return true;
    }

    /*
     * Input validation
     */

    /**
     * Called at write time to ensure that values present in the update
     * are valid according to the rules of all registered indexes which
     * will process it. The partition key as well as the clustering and
     * cell values for each row in the update may be checked by index
     * implementations
     *
     * @param update PartitionUpdate containing the values to be validated by registered Index implementations
     * @throws InvalidRequestException if the update isn't valid according to the rules of all registered indexes which
     * will process it
     */
    void validate(PartitionUpdate update) throws InvalidRequestException;

    /*
     * Update processing
     */

    /**
     * Creates a new {@code Indexer} object for updates to a given partition.
     *
     * @param key key of the partition being modified
     * @param columns the regular and static columns the created indexer will have to deal with.
     * This can be empty as an update might only contain partition, range and row deletions, but
     * the indexer is guaranteed to not get any cells for a column that is not part of {@code columns}.
     * @param nowInSec current time of the update operation
     * @param ctx WriteContext spanning the update operation
     * @param transactionType indicates what kind of update is being performed on the base data
     *                        i.e. a write time insert/update/delete or the result of compaction
     * @param memtable The current memtable that is the source of the updates
     * @return the newly created indexer or {@code null} if the index is not interested by the update
     * (this could be because the index doesn't care about that particular partition, doesn't care about
     * that type of transaction, ...).
     */
    Indexer indexerFor(DecoratedKey key,
                       RegularAndStaticColumns columns,
                       int nowInSec,
                       WriteContext ctx,
                       IndexTransaction.Type transactionType,
                       Memtable memtable);

    /**
     * Listener for processing events emitted during a single partition update.
     * Instances of this are responsible for applying modifications to the index in response to a single update
     * operation on a particular partition of the base table.
     * </p>
     * That update may be generated by the normal write path, by iterating SSTables during streaming operations or when
     * building or rebuilding an index from source. Updates also occur during compaction when multiple versions of a
     * source partition from different SSTables are merged.
     * </p>
     * Implementations should not make assumptions about resolution or filtering of the partition update being
     * processed. That is to say that it is possible for an Indexer instance to receive notification of a
     * PartitionDelete or RangeTombstones which shadow a Row it then receives via insertRow/updateRow.
     * </p>
     * It is important to note that the only ordering guarantee made for the methods here is that the first call will
     * be to begin() and the last call to finish(). The other methods may be called to process update events in any
     * order. This can also include duplicate calls, in cases where a memtable partition is under contention from
     * several updates. In that scenario, the same set of events may be delivered to the Indexer as memtable update
     * which failed due to contention is re-applied.
     */
    interface Indexer
    {
        /**
         * Notification of the start of a partition update.
         * This event always occurs before any other during the update.
         */
        void begin();

        /**
         * Notification of a top level partition delete.
         *
         * @param deletionTime the deletion time of the deleted partition
         */
        void partitionDelete(DeletionTime deletionTime);

        /**
         * Notification of a RangeTombstone.
         * An update of a single partition may contain multiple RangeTombstones,
         * and a notification will be passed for each of them.
         *
         * @param tombstone the RangeTombstone being applied to the base table
         */
        void rangeTombstone(RangeTombstone tombstone);

        /**
         * Notification that a new row was inserted into the Memtable holding the partition.
         * This only implies that the inserted row was not already present in the Memtable,
         * it *does not* guarantee that the row does not exist in an SSTable, potentially with
         * additional column data.
         *
         * @param row the Row being inserted into the base table's Memtable.
         */
        void insertRow(Row row);

        /**
         * Notification of a modification to a row in the base table's Memtable.
         * This is allow an Index implementation to clean up entries for base data which is
         * never flushed to disk (and so will not be purged during compaction).
         * It's important to note that the old and new rows supplied here may not represent
         * the totality of the data for the Row with this particular Clustering. There may be
         * additional column data in SSTables which is not present in either the old or new row,
         * so implementations should be aware of that.
         * The supplied rows contain only column data which has actually been updated.
         * oldRowData contains only the columns which have been removed from the Row's
         * representation in the Memtable, while newRowData includes only new columns
         * which were not previously present. Any column data which is unchanged by
         * the update is not included.
         *
         * @param oldRowData data that was present in existing row and which has been removed from
         *                   the base table's Memtable
         * @param newRowData data that was not present in the existing row and is being inserted
         *                   into the base table's Memtable
         */
        void updateRow(Row oldRowData, Row newRowData);

        /**
         * Notification that a row was removed from the partition.
         * Note that this is only called as part of either a compaction or a cleanup.
         * This context is indicated by the TransactionType supplied to the indexerFor method.
         * </p>
         * As with updateRow, it cannot be guaranteed that all data belonging to the Clustering
         * of the supplied Row has been removed (although in the case of a cleanup, that is the
         * ultimate intention).
         * There may be data for the same row in other SSTables, so in this case Indexer implementations
         * should *not* assume that all traces of the row have been removed. In particular,
         * it is not safe to assert that all values associated with the Row's Clustering
         * have been deleted, so implementations which index primary key columns should not
         * purge those entries from their indexes.
         *
         * @param row data being removed from the base table
         */
        void removeRow(Row row);

        /**
         * Notification of the end of the partition update.
         * This event always occurs after all others for the particular update.
         */
        void finish();
    }

    /*
     * Querying
     */

    /**
     * Used to validate the various parameters of a supplied {@code}ReadCommand{@code},
     * this is called prior to execution.
     * </p>
     * Custom index implementations should perform any validation of query expressions here and throw a meaningful
     * InvalidRequestException when any expression or other parameter is invalid.
     *
     * @param command a ReadCommand whose parameters are to be verified
     * @throws InvalidRequestException if the details of the command fail to meet the
     *         index's validation rules
     */
    default void validate(ReadCommand command) throws InvalidRequestException
    {
    }

    /**
     * Tells whether this index supports replica fitering protection or not.
     * </p>
     * Replica filtering protection might need to run the query row filter in the coordinator to detect stale results.
     * An index implementation will be compatible with this protection mechanism if it returns the same results for the
     * row filter as CQL will return with {@code ALLOW FILTERING} and without using the index. This means that index
     * implementations using custom query syntax or applying transformations to the indexed data won't support it.
     * See CASSANDRA-8272 for further details.
     *
     * @param rowFilter rowFilter of query to decide if it supports replica filtering protection or not
     * @return true if this index supports replica filtering protection, false otherwise
     */
    default boolean supportsReplicaFilteringProtection(RowFilter rowFilter)
    {
        return true;
    }

    /**
     * Factory method for query time search helper.
     *
     * @param command the read command being executed
     * @return a Searcher with which to perform the supplied command
     */
    Searcher searcherFor(ReadCommand command);

    /**
     * Performs the actual index lookup during execution of a ReadCommand.
     * An instance performs its query according to the RowFilter.Expression it was created for (see searcherFor)
     * An Expression is a predicate of the form [column] [operator] [value].
     */
    interface Searcher
    {
        /**
         * Returns the {@link ReadCommand} for which this searcher has been created.
         *
         * @return the base read command
         */
        ReadCommand command();

        /**
         * @param executionController the collection of OpOrder.Groups which the ReadCommand is being performed under.
         * @return partitions from the base table matching the criteria of the search.
         */
        UnfilteredPartitionIterator search(ReadExecutionController executionController);
    }

    /**
     * Class providing grouped operations for indexes that communicate with each other.
     * </p>
     * Index implementations should provide a {@code Group} implementation calling to
     * {@link IndexRegistry#registerIndex(Index, Key, Supplier)}
     * at {@link #register(IndexRegistry)} method and provide {@code groupKey} calling to
     * {@link IndexRegistry#unregisterIndex(Index, Key)} during index unregistering
     * at {@link #unregister(IndexRegistry)} method
     */
    interface Group
    {
        /**
         * Group key is used to uniquely identify a {@link Group} within a table
         */
        class Key
        {
            private final Object object;

            public Key(Object object)
            {
                this.object = object;
            }

            @Override
            public boolean equals(Object o)
            {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Key key = (Key) o;
                return Objects.equals(object, key.object);
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(object);
            }
        }

        /**
         * Returns the indexes that are members of this group.
         *
         * @return the indexes that are members of this group
         */
        Set<? extends Index> getIndexes();

        /**
         * Returns the indexes that are members of this group that are not excluded by the hints.
         *
         * @param hints the index hints with the indexes to exclude.
         * @return the indexes that are members of this group that are not excluded by the hints.
         */
        default Set<? extends Index> getNotExcludedIndexes(IndexHints hints)
        {
            return hints.notExcluded(getIndexes());
        }

        /**
         * Adds the specified {@link Index} as a member of this group.
         *
         * @param index the index to be added
         */
        void addIndex(Index index);

        /**
         * Removes the specified {@link Index} from the members of this group.
         *
         * @param index the index to be removed
         */
        void removeIndex(Index index);

        /**
         * Returns if this group contains the specified {@link Index}.
         *
         * @param index the index to be removed
         * @return {@code true} if this group contains {@code index}, {@code false} otherwise
         */
        boolean containsIndex(Index index);

        /**
         * Creates an new {@code Indexer} object for updates to a given partition.
         *
         * @param indexSelector a predicate selecting the targeted members
         * @param key key of the partition being modified
         * @param columns the regular and static columns the created indexer will have to deal with.
         * This can be empty as an update might only contain partition, range and row deletions, but
         * the indexer is guaranteed to not get any cells for a column that is not part of {@code columns}.
         * @param nowInSec current time of the update operation
         * @param ctx WriteContext spanning the update operation
         * @param transactionType indicates what kind of update is being performed on the base data
         *                        i.e. a write time insert/update/delete or the result of compaction
         * @param memtable the {@link Memtable} to which the updates are being applied or {@code null}
         *                 if the source of the updates is an existing {@link SSTable}
         *
         * @return the newly created indexer or {@code null} if the index is not interested by the update
         * (this could be because the index doesn't care about that particular partition, doesn't care about
         * that type of transaction, ...).
         */
        Indexer indexerFor(Predicate<Index> indexSelector,
                           DecoratedKey key,
                           RegularAndStaticColumns columns,
                           int nowInSec,
                           WriteContext ctx,
                           IndexTransaction.Type transactionType,
                           Memtable memtable);

        /**
         * Returns a new {@link QueryPlan} for the specified {@link RowFilter}, or {@code null} if none of the indexes in
         * this group supports the expression in the row filter.
         *
         * @param rowFilter a row filter
         * @return a new query plan for the specified {@link RowFilter} if it's supported, {@code null} otherwise
         */
        @Nullable
        QueryPlan queryPlanFor(RowFilter rowFilter);

        /**
         * Get flush observer to observe partition/cell events generated by flushing SSTable (memtable flush or compaction).
         *
         * @param descriptor    The descriptor of the sstable observer is requested for.
         * @param tracker       The {@link LifecycleNewTracker} associated with the SSTable being written
         * @param tableMetadata The immutable metadata of the table at the moment the SSTable is flushed
         * @param keyCount      The estimated number of keys in the sstable being flushed
         * @return SSTable flush observer.
         */
        SSTableFlushObserver getFlushObserver(Descriptor descriptor, LifecycleNewTracker tracker, TableMetadata tableMetadata, long keyCount);

        /**
         * @param type index transaction type
         * @return true if index will be able to handle given index transaction type
         */
        default boolean handles(IndexTransaction.Type type)
        {
            return true;
        }

        /**
         * Called when the table associated with this group has been invalidated or all indexes in the group are removed.
         * Implementations should dispose of any resources tied to the lifecycle of the {@link Group}.
         */
        default void invalidate() { }

        /**
         * Called when the table associated with this group has been unloaded. Implementations
         * should dispose of any resources tied to the lifecycle of the {@link Group} without removing index files.
         */
        default void unload() { }

        /**
         * Returns the set of sstable-attached components that this group will create for a newly flushed sstable.
         * </p>
         * Note that the result of this method is only valid for newly flushed/written sstables as the components
         * returned will assume a version of {@link Version#current()} and a generation of 0. SSTables for which some
         * index have been rebuild may have index components that do not match what this method return in particular.
         */
        Set<Component> componentsForNewSSTable();

        /**
         * Return the set of sstable-attached components belonging to the group that are currently "active" for the
         * provided sstable.
         * <p>
         * The "active" components are the components that are currently in use, meaning that if a given component
         * of the sstable exists with multiple versions or generation on disk, only the most recent version/generation
         * is the active one.
         *
         * @param sstable the sstable to get components for.
         * @return the set of the sstable-attached components of the provided sstable for this group.
         */
        Set<Component> activeComponents(SSTableReader sstable);

        /**
         * Validate the sstable-attached components belonging to the group that are currently "active" for the
         * provided sstable. Method is side effect free.
         * <p>
         * The "active" components are those returned by {@link #activeComponents}.
         *
         * @param sstable          the sstable to validate components for.
         * @param validateChecksum if {@code true}, the checksum of the components will be validated. Otherwise, only
         *                         basic checks on the header and footers will be performed.
         * @throws CorruptIndexException if the validation fails.
         */
        void validateComponents(SSTableReader sstable, boolean validateChecksum) throws CorruptIndexException;

        /**
         * @return true if this index group is capable of supporting multiple contains restrictions, false otherwise
         */
        default boolean supportsMultipleContains()
        {
            return false;
        }

        /**
         * @return true is this index group supports disjunction queries of "a = 1 OR a = 2" or "a IN (1, 2)"
         */
        default boolean supportsDisjunction()
        {
            return false;
        }
    }

    /**
     * Specifies a set of compatible indexes to be used with a query according to its {@link RowFilter}, ignoring data
     * ranges, limits, etc. All the indexes in it should belong to the same {@link Group}.
     * <p>
     * It's created by {@link Group#queryPlanFor} from the {@link RowFilter} that is common to all the subcommands of a
     * user query's {@link ReadCommand}, so it can be reused by those subcommands along the cluster nodes. The
     * {@link #searcherFor(ReadCommand)} method provides the {@link Searcher} object to read the index for each
     * particular (sub)command.
     */
    interface QueryPlan extends Comparable<QueryPlan>
    {
        /**
         * Returns the indexes selected by this query plan, all of them belonging to the same {@link Group}.
         * It should never be empty.
         *
         * @return the indexes selected by this query plan, which is never empty
         */
        Set<Index> getIndexes();

        /**
         * Returns the first index in this plan.
         *
         * @return the first index
         */
        @Nonnull
        default Index getFirst()
        {
            return getIndexes().iterator().next();
        }

        /**
         * Return an estimate of the number of results this plan is expected to return for any given {@link ReadCommand}
         * that it can be used to answer. Used by  {@link SecondaryIndexManager#getBestIndexQueryPlanFor(RowFilter)}
         * to determine the {@link Group} with the most selective plan for a given {@link RowFilter}.
         * Additionally, this is also used by StorageProxy.estimateResultsPerRange to calculate the initial concurrency
         * factor for range requests.
         * </p>
         * Please note that some index implementations (SASI and SAI) will always return -1 for that method to
         * prioritize themselves. Third party implementations can also return similar fixed values. See CNDB-14764 for
         * details.
         *
         * @return the estimated average number of results a Searcher may return for any given command
         */
        default long getEstimatedResultRows()
        {
            // CQL only supports AND expressions, so the estimated number of results for multiple indexes will be the
            // lowest of the estimates for each index
            return getIndexes().stream()
                               .mapToLong(Index::getEstimatedResultRows)
                               .min()
                               .orElseThrow(AssertionError::new); // registered groups are never empty
        }

        /**
         * Used to determine whether to estimate initial concurrency during remote range reads. Default is true, each
         * implementation must override this method if they choose a different strategy (e.g. StorageAttachedIndexQueryPlan).
         *
         * @return true if the {@link QueryPlan} should estimate initial concurrency, false otherwise
         */
        default boolean shouldEstimateInitialConcurrency()
        {
            return true;
        }

        @Override
        default int compareTo(QueryPlan other)
        {
            // initially, we prefer the plan with less estimated results
            int results = Long.compare(getEstimatedResultRows(), other.getEstimatedResultRows());
            if (results != 0)
                return results;

            // In case of having the same number of estimated results, we favour the plan that involves more indexes.
            // This way, we honour the possible absence of ALLOW FILTERING in the CQL query. Also, this criteria should
            // not break the transitivity of this method because the estimated number of results for a plan is the
            // minimum of the estimates of its members.
            return Integer.compare(getIndexes().size(), other.getIndexes().size());
        }

        /**
         * Used to validate the various parameters of a supplied {@link ReadCommand} against the indexes in this plan.
         *
         * @param command a ReadCommand whose parameters are to be verified
         * @throws InvalidRequestException if the details of the command fail to meet the validation rules of the
         * indexes in the query plan
         */
        default void validate(ReadCommand command) throws InvalidRequestException
        {
            getIndexes().forEach(i -> i.validate(command));
        }

        /**
         * Factory method for query time search helper.
         *
         * @param command the read command being executed
         * @return an Searcher with which to perform the supplied command
         */
        Searcher searcherFor(ReadCommand command);

        /**
         * Return a function which performs post-processing on the results of a partition range read command.
         * In the future, this may be used as a generalized mechanism for transforming results on the coordinator prior
         * to returning them to the caller.
         * </p>
         * This is used on the coordinator during execution of a range command to perform post-processing of merged
         * results obtained from the necessary replicas. This is the only way in which results are
         * transformed in this way but this may change over time as usage is generalized.
         * See CASSANDRA-8717 for further discussion.
         * </p>
         * The function takes a PartitionIterator of the results from the replicas which has already been collated
         * and reconciled, along with the command being executed. It returns another PartitionIterator containing the
         * results of the transformation (which may be the same as the input if the transformation is a no-op).
         *
         * @param command the read command being executed
         */
        default Function<PartitionIterator, PartitionIterator> postProcessor(ReadCommand command)
        {
            return partitions -> partitions;
        }

        /**
         * Transform an initial {@link RowFilter} into the filter that will still need to applied to a set of Rows after
         * the index has performed its initial scan.
         * </p>
         * Used in {@link ReadCommand#executeLocally(ReadExecutionController)} to reduce the amount of filtering
         * performed on the results of the index query.
         *
         * @return the (hopefully) reduced filter that would still need to be applied after
         *         the index was used to narrow the initial result set
         */
        RowFilter postIndexQueryFilter();

        /**
         * @return true if the indexes in this plan support querying multiple vnode ranges at once.
         */
        default boolean supportsMultiRangeReadCommand()
        {
            return false;
        }

        /**
         * @return true if given index query plan is a top-k request
         */
        default boolean isTopK()
        {
            return false;
        }
    }

    /*
     * Status of index used to determine queryability
     */
    enum Status
    {
        UNKNOWN,
        FULL_REBUILD_STARTED,
        BUILD_FAILED,
        BUILD_SUCCEEDED,
        DROPPED
    }
}
