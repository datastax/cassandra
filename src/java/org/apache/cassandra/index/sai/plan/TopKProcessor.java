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
import javax.annotation.Nullable;

import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.plan.StorageAttachedIndexSearcher.ScoreOrderedResultRetriever;
import org.apache.cassandra.index.sai.utils.InMemoryPartitionIterator;
import org.apache.cassandra.index.sai.utils.InMemoryUnfilteredPartitionIterator;
import org.apache.cassandra.index.sai.utils.PartitionInfo;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.TopKSelector;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * Processor applied to SAI based ORDER BY queries.
 *
 *  * On a replica:
 *  *  - filter(ScoreOrderedResultRetriever) is used to collect up to the top-K rows.
 *  *  - We store any tombstones as well, to avoid losing them during coordinator reconciliation.
 *  *  - The result is returned in PK order so that coordinator can merge from multiple replicas.
 *
 * On a coordinator:
 *  - reorder(PartitionIterator) is used to consume all rows from the provided partitions,
 *    compute the order based on either a column ordering or a similarity score, and keep top-K.
 *  - The result is returned in score/sortkey order.
 */
public class TopKProcessor
{
    public static final String INDEX_MAY_HAVE_BEEN_DROPPED = "An index may have been dropped. Ordering on non-clustering " +
                                                             "column requires the column to be indexed";
    protected static final Logger logger = LoggerFactory.getLogger(TopKProcessor.class);
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    private final ReadCommand command;
    private final IndexContext indexContext;
    private final RowFilter.Expression expression;
    private final VectorFloat<?> queryVector;
    private final ColumnMetadata scoreColumn;

    private final int limit;

    public TopKProcessor(ReadCommand command)
    {
        this.command = command;

        Pair<IndexContext, RowFilter.Expression> indexAndExpression = findTopKIndexContext();
        // this can happen in case an index was dropped after the query was initiated
        if (indexAndExpression == null)
            throw invalidRequest(INDEX_MAY_HAVE_BEEN_DROPPED);

        this.indexContext = indexAndExpression.left;
        this.expression = indexAndExpression.right;
        if (expression.operator() == Operator.ANN && !SelectStatement.ANN_USE_SYNTHETIC_SCORE)
            this.queryVector = vts.createFloatVector(TypeUtil.decomposeVector(indexContext, expression.getIndexValue().duplicate()));
        else
            this.queryVector = null;
        this.limit = command.limits().count();
        this.scoreColumn = ColumnMetadata.syntheticColumn(indexContext.getKeyspace(), indexContext.getTable(), ColumnMetadata.SYNTHETIC_SCORE_ID, FloatType.instance);
    }

    /**
     * Sort the specified filtered rows according to the {@code ORDER BY} clause and keep the first {@link #limit} rows.
     * Called on the coordinator side.
     *
     * @param partitions the partitions collected by the coordinator. It will be closed as a side-effect.
     * @return the provided rows, sorted and trimmed to {@link #limit} rows
     */
    public PartitionIterator reorder(PartitionIterator partitions)
    {
        // We consume the partitions iterator and create a new one. Use a try-with-resources block to ensure the
        // original iterator is closed. We do not expect exceptions here, but if they happen, we want to make sure the
        // original iterator is closed to prevent leaking resources, which could compound the effect of an exception.
        try (partitions)
        {
            Comparator<Triple<PartitionInfo, Row, ?>> comparator = comparator()
                   .thenComparing(Triple::getLeft, Comparator.comparing(pi -> pi.key))
                   .thenComparing(Triple::getMiddle, command.metadata().comparator);

            TopKSelector<Triple<PartitionInfo, Row, ?>> topK = new TopKSelector<>(comparator, limit);
            while (partitions.hasNext())
            {
                try (BaseRowIterator<?> partitionRowIterator = partitions.next())
                {
                    if (expression.operator() == Operator.ANN || expression.operator() == Operator.BM25)
                    {
                        PartitionResults pr = processScoredPartition(partitionRowIterator);
                        topK.addAll(pr.rows);
                    }
                    else
                    {
                        while (partitionRowIterator.hasNext())
                        {
                            Row row = (Row) partitionRowIterator.next();
                            ByteBuffer value = row.getCell(expression.column()).buffer();
                            topK.add(Triple.of(PartitionInfo.create(partitionRowIterator), row, value));
                        }
                    }
                }
            }

            // Convert the topK results to a PartitionIterator
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
     * @param partitions the partitions collected in the replica side of a query. It will be closed as a side-effect.
     * @return the provided rows, sorted by the requested {@code ORDER BY} chriteria, trimmed to {@link #limit} rows,
     * and the sorted again by primary key.
     */
    public UnfilteredPartitionIterator filter(ScoreOrderedResultRetriever partitions)
    {
        try (partitions)
        {
            TreeMap<PartitionInfo, TreeSet<Unfiltered>> unfilteredByPartition = new TreeMap<>(Comparator.comparing(pi -> pi.key));

            int rowsMatched = 0;
            // Because each “partition” from ScoreOrderedResultRetriever is actually a single row
            // or tombstone, we can simply read them until we have enough.
            while (rowsMatched < limit && partitions.hasNext())
            {
                try (BaseRowIterator<?> partitionRowIterator = partitions.next())
                {
                    rowsMatched += processSingleRowPartition(unfilteredByPartition, partitionRowIterator);
                }
            }

            return new InMemoryUnfilteredPartitionIterator(command, unfilteredByPartition);
        }
    }

    /**
     * Constructs a comparator for triple (PartitionInfo, Row, X) used for top-K ranking.
     * For ANN/BM25 we compare descending by X (float score). For ORDER_BY_ASC or DESC,
     * we compare ascending/descending by the row’s relevant ByteBuffer data.
     */
    private Comparator<Triple<PartitionInfo, Row, ?>> comparator()
    {
        if (expression.operator() == Operator.ANN || expression.operator() == Operator.BM25)
        {
            // For similarity, higher is better, so reversed
            return Comparator.comparing((Triple<PartitionInfo, Row, ?> t) -> (Float) t.getRight()).reversed();
        }

        Comparator<Triple<PartitionInfo, Row, ?>> comparator = Comparator.comparing(t -> (ByteBuffer) t.getRight(), indexContext.getValidator());
        if (expression.operator() == Operator.ORDER_BY_DESC)
            comparator = comparator.reversed();
        return comparator;
    }

    /**
     * Simple holder for partial results of a single partition (score-based path).
     */
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
     * Processes all rows in a single partition to compute scores (for ANN or BM25)
     */
    private PartitionResults processScoredPartition(BaseRowIterator<?> partitionRowIterator)
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

    private void addUnfiltered(SortedMap<PartitionInfo, TreeSet<Unfiltered>> unfilteredByPartition,
                               PartitionInfo partitionInfo,
                               Unfiltered unfiltered)
    {
        TreeSet<Unfiltered> map = unfilteredByPartition.computeIfAbsent(partitionInfo, k -> new TreeSet<>(command.metadata().comparator));
        map.add(unfiltered);
    }

    private float getScoreForRow(DecoratedKey key, Row row)
    {
        ColumnMetadata column = indexContext.getDefinition();

        if (column.isPrimaryKeyColumn() && key == null)
            return 0;

        if (column.isStatic() && !row.isStatic())
            return 0;

        if ((column.isClusteringColumn() || column.isRegular()) && row.isStatic())
            return 0;

        // If we have a synthetic score column, use it
        var scoreData = row.getColumnData(scoreColumn);
        if (scoreData != null)
        {
            var cell = (Cell<?>) scoreData;
            return FloatType.instance.compose(cell.buffer());
        }

        // TODO remove this once we enable ANN_USE_SYNTHETIC_SCORE
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
            StorageAttachedIndex sai = findOrderingIndexFor(cfs.indexManager, expression);
            if (sai != null)
                return Pair.create(sai.getIndexContext(), expression);
        }

        return null;
    }

    @Nullable
    private StorageAttachedIndex findOrderingIndexFor(SecondaryIndexManager sim, RowFilter.Expression e)
    {
        if (e.operator() != Operator.ANN
            && e.operator() != Operator.BM25
            && e.operator() != Operator.ORDER_BY_ASC
            && e.operator() != Operator.ORDER_BY_DESC)
        {
            return null;
        }

        Optional<Index> index = sim.getBestIndexFor(e);
        return (StorageAttachedIndex) index.filter(i -> i instanceof StorageAttachedIndex).orElse(null);
    }
}
