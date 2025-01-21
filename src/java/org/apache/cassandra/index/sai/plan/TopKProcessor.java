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

package org.apache.cassandra.index.sai.plan;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture; // checkstyle: permit this import
import java.util.concurrent.CompletionException;
import javax.annotation.Nullable;

import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.apache.cassandra.concurrent.ImmediateExecutor;
import org.apache.cassandra.concurrent.LocalAwareExecutorPlus;
import org.apache.cassandra.concurrent.SharedExecutorPool;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.BasePartitionIterator;
import org.apache.cassandra.db.partitions.ParallelCommandProcessor;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.index.sai.utils.InMemoryPartitionIterator;
import org.apache.cassandra.index.sai.utils.InMemoryUnfilteredPartitionIterator;
import org.apache.cassandra.index.sai.utils.PartitionInfo;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.TopKSelector;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * Processor applied to SAI based ORDER BY queries. This class could likely be refactored into either two filter
 * methods depending on where the processing is happening or into two classes.
 * </p>
 * This processor performs the following steps on a replica:
 * - collect LIMIT rows from partition iterator, making sure that all are valid.
 * - return rows in Primary Key order
 * </p>
 * This processor performs the following steps on a coordinator:
 * - consume all rows from the provided partition iterator and sort them according to the specified order.
 *   For vectors, that is similarit score and for all others, that is the ordering defined by their
 *   {@link org.apache.cassandra.db.marshal.AbstractType}. If there are multiple vector indexes,
 *   the final score is the sum of all vector index scores.
 * - remove rows with the lowest scores from PQ if PQ size exceeds limit
 * - return rows from PQ in primary key order to caller
 */
public class TopKProcessor
{
    public static final String INDEX_MAY_HAVE_BEEN_DROPPED = "An index may have been dropped. Ordering on non-clustering " +
                                                             "column requires the column to be indexed";
    protected static final Logger logger = LoggerFactory.getLogger(TopKProcessor.class);
    private static final LocalAwareExecutorPlus PARALLEL_EXECUTOR = getExecutor();
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    private final ReadCommand command;
    private final IndexContext indexContext;
    private final RowFilter.Expression expression;
    private final VectorFloat<?> queryVector;

    private final int limit;

    public TopKProcessor(ReadCommand command)
    {
        this.command = command;

        Pair<IndexContext, RowFilter.Expression> annIndexAndExpression = findTopKIndexContext();
        // this can happen in case an index was dropped after the query was initiated
        if (annIndexAndExpression == null)
            throw invalidRequest(INDEX_MAY_HAVE_BEEN_DROPPED);

        this.indexContext = annIndexAndExpression.left;
        this.expression = annIndexAndExpression.right;
        if (expression.operator() == Operator.ANN)
            this.queryVector = vts.createFloatVector(TypeUtil.decomposeVector(indexContext, expression.getIndexValue().duplicate()));
        else
            this.queryVector = null;
        this.limit = command.limits().count();
    }

    /**
     * Executor to use for parallel index reads.
     * Defined by -Dcassandra.index_read.parallele=true/false, true by default.
     * </p>
     * INDEX_READ uses 2 * cpus threads by default but can be overridden with {@literal -Dcassandra.index_read.parallel_thread_num=<value>}
     *
     * @return stage to use, default INDEX_READ
     */
    private static LocalAwareExecutorPlus getExecutor()
    {
        boolean isParallel = CassandraRelevantProperties.USE_PARALLEL_INDEX_READ.getBoolean();

        if (isParallel)
        {
            int numThreads = CassandraRelevantProperties.PARALLEL_INDEX_READ_NUM_THREADS.isPresent()
                                ? CassandraRelevantProperties.PARALLEL_INDEX_READ_NUM_THREADS.getInt()
                                : FBUtilities.getAvailableProcessors() * 2;
            return SharedExecutorPool.SHARED.newExecutor(numThreads, maximumPoolSize -> {}, "request", "IndexParallelRead");
        }
        else
            return ImmediateExecutor.INSTANCE;
    }

    /**
     * Sort the specified filtered rows according to the {@code ORDER BY} clause and keep the first {@link #limit} rows.
     * This is meant to be used on the coordinator-side to sort the rows collected from the replicas.
     * Caller must close the supplied iterator.
     *
     * @param partitions the partitions collected by the coordinator
     * @return the provided rows, sorted by the requested {@code ORDER BY} chriteria and trimmed to {@link #limit} rows
     */
    public PartitionIterator filter(PartitionIterator partitions)
    {
        // We consume the partitions iterator and create a new one. Use a try-with-resources block to ensure the
        // original iterator is closed. We do not expect exceptions here, but if they happen, we want to make sure the
        // original iterator is closed to prevent leaking resources, which could compound the effect of an exception.
        try (partitions)
        {
            Comparator<Triple<PartitionInfo, Row, ?>> comparator = comparator()
                    .thenComparing(Triple::getLeft, Comparator.comparing(p -> p.key))
                    .thenComparing(Triple::getMiddle, command.metadata().comparator);

            TopKSelector<Triple<PartitionInfo, Row, ?>> topK = new TopKSelector<>(comparator, limit);

            processPartitions(partitions, topK, null);

            List<Pair<PartitionInfo, Row>> sortedRows = new ArrayList<>(topK.size());
            for (Triple<PartitionInfo, Row, ?> triple : topK.getShared())
                sortedRows.add(Pair.create(triple.getLeft(), triple.getMiddle()));

            return InMemoryPartitionIterator.create(command, sortedRows);
        }
    }

    /**
     * Sort the specified unfiltered rows according to the {@code ORDER BY} clause, keep the first {@link #limit} rows,
     * and then order them again by primary key.
     * </p>
     * This is meant to be used on the replica-side, before reconciliation. We need to order the rows by primary key
     * after the top-k selection to avoid confusing reconciliation later, on the coordinator. Note that due to sstable
     * overlap and how the full data set of each node is queried for top-k queries we can have multiple versions of the
     * same row in the coordinator even with CL=ONE. Reconciliation should remove those duplicates, but it needs the
     * rows to be ordered by primary key to do so. See CNDB-12308 for details.
     * </p>
     * All tombstones will be kept. Caller must close the supplied iterator.
     *
     * @param partitions the partitions collected in the replica side of a query
     * @return the provided rows, sorted by the requested {@code ORDER BY} chriteria, trimmed to {@link #limit} rows,
     * and the sorted again by primary key.
     */
    public UnfilteredPartitionIterator filter(UnfilteredPartitionIterator partitions)
    {
        // We consume the partitions iterator and create a new one. Use a try-with-resources block to ensure the
        // original iterator is closed. We do not expect exceptions here, but if they happen, we want to make sure the
        // original iterator is closed to prevent leaking resources, which could compound the effect of an exception.
        try (partitions)
        {
            TopKSelector<Triple<PartitionInfo, Row, ?>> topK = new TopKSelector<>(comparator(), limit);

            TreeMap<PartitionInfo, TreeSet<Unfiltered>> unfilteredByPartition = new TreeMap<>(Comparator.comparing(p -> p.key));

            processPartitions(partitions, topK, unfilteredByPartition);

            // Reorder the rows by primary key.
            for (var triple : topK.getUnsortedShared())
                addUnfiltered(unfilteredByPartition, triple.getLeft(), triple.getMiddle());

            return new InMemoryUnfilteredPartitionIterator(command, unfilteredByPartition);
        }
    }

    private Comparator<Triple<PartitionInfo, Row, ?>> comparator()
    {
        Comparator<Triple<PartitionInfo, Row, ?>> comparator;
        if (queryVector != null)
        {
            comparator = Comparator.comparing((Triple<PartitionInfo, Row, ?> t) -> (Float) t.getRight()).reversed();
        }
        else
        {
            comparator = Comparator.comparing(t -> (ByteBuffer) t.getRight(), indexContext.getValidator());
            if (expression.operator() == Operator.ORDER_BY_DESC)
                comparator = comparator.reversed();
        }
        return comparator;
    }

    private <U extends Unfiltered, R extends BaseRowIterator<U>, P extends BasePartitionIterator<R>>
    void processPartitions(P partitions,
                           TopKSelector<Triple<PartitionInfo, Row, ?>> topK,
                           @Nullable TreeMap<PartitionInfo, TreeSet<Unfiltered>> unfilteredByPartition)
    {
        if (PARALLEL_EXECUTOR != ImmediateExecutor.INSTANCE && partitions instanceof ParallelCommandProcessor)
        {
            ParallelCommandProcessor pIter = (ParallelCommandProcessor) partitions;
            List<Pair<PrimaryKey, SinglePartitionReadCommand>> commands = pIter.getUninitializedCommands();
            List<CompletableFuture<PartitionResults>> results = new ArrayList<>(commands.size());

            int count = commands.size();
            for (var command: commands) {
                CompletableFuture<PartitionResults> future = new CompletableFuture<>();
                results.add(future);

                // run last command immediately, others in parallel (if possible)
                count--;
                LocalAwareExecutorPlus executor = count == 0 ? ImmediateExecutor.INSTANCE : PARALLEL_EXECUTOR;

                executor.maybeExecuteImmediately(() -> {
                    try (UnfilteredRowIterator partitionRowIterator = pIter.commandToIterator(command.left(), command.right()))
                    {
                        future.complete(partitionRowIterator == null ? null : processPartition(partitionRowIterator));
                    }
                    catch (Throwable t)
                    {
                        future.completeExceptionally(t);
                    }
                });
            }

            for (CompletableFuture<PartitionResults> triplesFuture: results)
            {
                PartitionResults pr;
                try
                {
                    pr = triplesFuture.join();
                }
                catch (CompletionException t)
                {
                    if (t.getCause() instanceof AbortedOperationException)
                        throw (AbortedOperationException) t.getCause();
                    throw t;
                }
                if (pr == null)
                    continue;
                topK.addAll(pr.rows);
                if (unfilteredByPartition != null)
                {
                    for (var uf : pr.tombstones)
                        addUnfiltered(unfilteredByPartition, pr.partitionInfo, uf);
                }
            }
        }
        else if (partitions instanceof StorageAttachedIndexSearcher.ScoreOrderedResultRetriever)
        {
            // FilteredPartitions does not implement ParallelizablePartitionIterator.
            // Realistically, this won't benefit from parallelizm as these are coming from in-memory/memtable data.
            int rowsMatched = 0;
            // Check rowsMatched first to prevent fetching one more partition than needed.
            while (rowsMatched < limit && partitions.hasNext())
            {
                // Must close to move to the next partition, otherwise hasNext() fails
                try (var partitionRowIterator = partitions.next())
                {
                    rowsMatched += processSingleRowPartition(unfilteredByPartition, partitionRowIterator);
                }
            }
        }
        else
        {
            // FilteredPartitions does not implement ParallelizablePartitionIterator.
            // Realistically, this won't benefit from parallelizm as these are coming from in-memory/memtable data.
            while (partitions.hasNext())
            {
                // have to close to move to the next partition, otherwise hasNext() fails
                try (var partitionRowIterator = partitions.next())
                {
                    if (queryVector != null)
                    {
                        PartitionResults pr = processPartition(partitionRowIterator);
                        topK.addAll(pr.rows);
                        if (unfilteredByPartition != null)
                        {
                            for (var uf : pr.tombstones)
                                addUnfiltered(unfilteredByPartition, pr.partitionInfo, uf);
                        }
                    }
                    else
                    {
                        while (partitionRowIterator.hasNext())
                        {
                            Row row = (Row) partitionRowIterator.next();
                            topK.add(Triple.of(PartitionInfo.create(partitionRowIterator), row, row.getCell(expression.column()).buffer()));
                        }
                    }
                }
            }
        }
    }

    private class PartitionResults
    {
        final PartitionInfo partitionInfo;
        final SortedSet<Unfiltered> tombstones = new TreeSet<>(command.metadata().comparator);
        final List<Triple<PartitionInfo, Row, Float>> rows = new ArrayList<>();

        PartitionResults(PartitionInfo partitionInfo)
        {
            this.partitionInfo = partitionInfo;
        }

        void addTombstone(Unfiltered uf)
        {
            tombstones.add(uf);
        }

        void addRow(Triple<PartitionInfo, Row, Float> triple)
        {
            rows.add(triple);
        }
    }

    /**
     * Processes a single partition, calculating scores for rows and extracting tombstones.
     */
    private PartitionResults processPartition(BaseRowIterator<?> partitionRowIterator)
    {
        // Compute key and static row score once per partition
        DecoratedKey key = partitionRowIterator.partitionKey();
        Row staticRow = partitionRowIterator.staticRow();
        PartitionInfo partitionInfo = PartitionInfo.create(partitionRowIterator);
        float keyAndStaticScore = getScoreForRow(key, staticRow);
        PartitionResults pr = new PartitionResults(partitionInfo);

        while (partitionRowIterator.hasNext())
        {
            Unfiltered unfiltered = partitionRowIterator.next();
            // Always include tombstones for coordinator. It relies on ReadCommand#withMetricsRecording to throw
            // TombstoneOverwhelmingException to prevent OOM.
            if (unfiltered.isRangeTombstoneMarker())
            {
                pr.addTombstone(unfiltered);
                continue;
            }

            Row row = (Row) unfiltered;
            float rowScore = getScoreForRow(null, row);
            pr.addRow(Triple.of(partitionInfo, row, keyAndStaticScore + rowScore));
        }

        return pr;
    }

    /**
     * Processes a single partition, without scoring it.
     */
    private int processSingleRowPartition(TreeMap<PartitionInfo, TreeSet<Unfiltered>> unfilteredByPartition,
                                          BaseRowIterator<?> partitionRowIterator)
    {
        if (!partitionRowIterator.hasNext())
            return 0;

        Unfiltered unfiltered = partitionRowIterator.next();
        assert !partitionRowIterator.hasNext() : "Only one row should be returned";
        // Always include tombstones for coordinator. It relies on ReadCommand#withMetricsRecording to throw
        // TombstoneOverwhelmingException to prevent OOM.
        PartitionInfo partitionInfo = PartitionInfo.create(partitionRowIterator);
        addUnfiltered(unfilteredByPartition, partitionInfo, unfiltered);
        return unfiltered.isRangeTombstoneMarker() ? 0 : 1;
    }

    private void addUnfiltered(SortedMap<PartitionInfo, TreeSet<Unfiltered>> unfilteredByPartition, PartitionInfo partitionInfo, Unfiltered unfiltered)
    {
        TreeSet<Unfiltered> map = unfilteredByPartition.computeIfAbsent(partitionInfo, k -> new TreeSet<>(command.metadata().comparator));
        map.add(unfiltered);
    }

    /**
     * Sum the scores from different vector indexes for the row
     */
    private float getScoreForRow(DecoratedKey key, Row row)
    {
        ColumnMetadata column = indexContext.getDefinition();

        if (column.isPrimaryKeyColumn() && key == null)
            return 0;

        if (column.isStatic() && !row.isStatic())
            return 0;

        if ((column.isClusteringColumn() || column.isRegular()) && row.isStatic())
            return 0;

        ByteBuffer value = indexContext.getValueOf(key, row, FBUtilities.nowInSeconds());
        if (value != null)
        {
            var vector = vts.createFloatVector(TypeUtil.decomposeVector(indexContext, value));
            return indexContext.getIndexWriterConfig().getSimilarityFunction().compare(vector, queryVector);
        }
        return 0;
    }


    private Pair<IndexContext, RowFilter.Expression> findTopKIndexContext()
    {
        ColumnFamilyStore cfs = Keyspace.openAndGetStore(command.metadata());

        for (RowFilter.Expression expression : command.rowFilter().expressions())
        {
            StorageAttachedIndex sai = findVectorIndexFor(cfs.indexManager, expression);
            if (sai != null)
            {
                return Pair.create(sai.getIndexContext(), expression);
            }
        }

        return null;
    }

    @Nullable
    private StorageAttachedIndex findVectorIndexFor(SecondaryIndexManager sim, RowFilter.Expression e)
    {
        if (e.operator() != Operator.ANN && e.operator() != Operator.ORDER_BY_ASC && e.operator() != Operator.ORDER_BY_DESC)
            return null;

        Optional<Index> index = sim.getBestIndexFor(e);
        return (StorageAttachedIndex) index.filter(i -> i instanceof StorageAttachedIndex).orElse(null);
    }
}
