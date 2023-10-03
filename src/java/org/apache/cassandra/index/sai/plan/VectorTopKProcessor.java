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
import java.util.AbstractQueue;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.lang3.tuple.Triple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.BasePartitionIterator;
import org.apache.cassandra.db.partitions.ParallelizablePartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.utils.InMemoryPartitionIterator;
import org.apache.cassandra.index.sai.utils.InMemoryUnfilteredPartitionIterator;
import org.apache.cassandra.index.sai.utils.PartitionInfo;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

/**
 * Processor that scans all rows from given partitions and selects rows with top-k scores based on vector indexes.
 *
 * This processor performs the following steps:
 * - collect rows with score into PriorityQueue that sorts rows based on score. If there are multiple vector indexes,
 *   the final score is the sum of all vector index scores.
 * - remove rows with the lowest scores from PQ if PQ size exceeds limit
 * - return rows from PQ in primary key order to client
 *
 * Note that recall will be lower with paging, because:
 * - page size is used as limit
 * - for the first query, coordinator returns global top page-size rows within entire ring
 * - for the subsequent queries, coordinators returns global top page-size rows withom range from last-returned-row to max token
 */
public class VectorTopKProcessor
{
    protected static final Logger logger = LoggerFactory.getLogger(VectorTopKProcessor.class);
    private static final Stage PARALLEL_STAGE = getParallelStage();
    private final ReadCommand command;
    private final IndexContext indexContext;
    private final float[] queryVector;

    private final int limit;

    private int rowCount = 0;

    public VectorTopKProcessor(ReadCommand command)
    {
        this.command = command;

        Pair<IndexContext, float[]> annIndexAndExpression = findTopKIndexContext();
        Preconditions.checkNotNull(annIndexAndExpression);

        this.indexContext = annIndexAndExpression.left;
        this.queryVector = annIndexAndExpression.right;
        this.limit = command.limits().count();
    }

    /**
     * Stage to use for parallel index reads.
     * Defiend by -DIndexReadStage.cassandra.stage=<value> where valie matches one of the Stage enum values.
     * INDEX_READ is default, READ is a possibility, or IMMEDIATE to disable parallel reads.
     * Others are not recommended.
     * @see org.apache.cassandra.concurrent.Stage
     *
     * INDEX_READ uses 2 * cpus threads by default but can be overrriden with -DIndexReadStage.cassandra.threads=<value>
     *
     * @return stage to use, default INDEX_READ
     */
    private static Stage getParallelStage()
    {
        String prop = System.getProperty("IndexReadStage.cassandra.stage", "INDEX_READ").toUpperCase();
        try
        {
            return Stage.valueOf(prop);
        }
        catch (IllegalArgumentException iae)
        {
            logger.error("Cannot get stage for the property -DIndexReadStage.cassandra.stage={}; falling back to INDEX_READ", prop);
            return Stage.INDEX_READ;
        }
    }

    /**
     * Filter given partitions and keep the rows with highest scores. In case of {@link UnfilteredPartitionIterator},
     * all tombstones will be kept.
     */
    public <U extends Unfiltered, R extends BaseRowIterator<U>, P extends BasePartitionIterator<R>> BasePartitionIterator<?> filter(P partitions)
    {
        // priority queue ordered by score in ascending order. We fill the queue with at most limit + 1 rows, so that we
        // can then remove the lowest score row when exceeding limit. The capacity is limit + 1 to prevent resizing.
        PriorityQueue<Triple<PartitionInfo, Row, Float>> topK = new PriorityQueue<>(limit + 1, Comparator.comparing(Triple::getRight));
        // to store top-k results in primary key order
        TreeMap<PartitionInfo, TreeSet<Unfiltered>> unfilteredByPartition = new TreeMap<>(Comparator.comparing(p -> p.key));

        if (PARALLEL_STAGE != Stage.IMMEDIATE && partitions instanceof ParallelizablePartitionIterator) {
            ParallelizablePartitionIterator pIter = (ParallelizablePartitionIterator) partitions;
            var commands = pIter.getUninitializedCommands();
            List<CompletableFuture<List<Triple<PartitionInfo, Row, Float>>>> results = new LinkedList<>();

            int count = commands.size();
            for (var command: commands) {
                CompletableFuture<List<Triple<PartitionInfo, Row, Float>>> future = new CompletableFuture<>();
                results.add(future);

                // run last command immediately, others in parallel (if possible)
                count--;
                var executor = count == 0 ? Stage.IMMEDIATE : PARALLEL_STAGE;

                executor.maybeExecuteImmediately(() -> {
                    try (var part = pIter.commandToIterator(command.left(), command.right()))
                    {
                        future.complete(part == null ? null : processPart(part, unfilteredByPartition, topK, true));
                    }
                    catch (Throwable t)
                    {
                        future.completeExceptionally(t);
                    }
                });
            }

            for (var triplesFuture: results) {
                var triples = triplesFuture.join();
                if (triples == null)
                    continue;
                moveToPq(topK, triples);
            }
        } else {
            // FilteredPartitions does not implement ParallelizablePartitionIterator.
            // Realistically, this won't benefit from parallelizm as these are coming from in-memory/memtable data.
            while (partitions.hasNext())
            {
                // have to close to move to the next partition, otherwise hasNext() fails
                try (var part = partitions.next())
                {
                    var triples = processPart(part, unfilteredByPartition, topK, false);
                    moveToPq(topK, triples);
                }
            }
        }

        assert topK.size() <= limit;
        partitions.close();

        // reorder rows in partition/clustering order
        for (Triple<PartitionInfo, Row, Float> triple : topK)
            addUnfiltered(unfilteredByPartition, triple.getLeft(), triple.getMiddle());

        rowCount = topK.size();

        if (partitions instanceof PartitionIterator)
            return new InMemoryPartitionIterator(command, unfilteredByPartition);
        return new InMemoryUnfilteredPartitionIterator(command, unfilteredByPartition);
    }

    private <U extends Unfiltered> List<Triple<PartitionInfo, Row, Float>> processPart(BaseRowIterator<U> part,
                                                    SortedMap<PartitionInfo, TreeSet<Unfiltered>> unfilteredByPartition,
                                                    AbstractQueue<Triple<PartitionInfo, Row, Float>> topK,
                                                    boolean isParallel)
    {
        // compute key and static row score once per partition
        DecoratedKey key = part.partitionKey();
        Row staticRow = part.staticRow();
        final PartitionInfo partitionInfo = PartitionInfo.create(part);
        final float keyAndStaticScore = getScoreForRow(key, staticRow);

        List<Triple<PartitionInfo, Row, Float>> res = new LinkedList<>();
        while (part.hasNext())
        {
            Unfiltered unfiltered = part.next();
            // Always include tombstones for coordinator. It relies on ReadCommand#withMetricsRecording to throw
            // TombstoneOverwhelmingException to prevent OOM.
            if (!unfiltered.isRow())
            {
                if (isParallel)
                {
                    synchronized (unfilteredByPartition)
                    {
                        addUnfiltered(unfilteredByPartition, partitionInfo, unfiltered);
                    }
                }
                else
                {
                    addUnfiltered(unfilteredByPartition, partitionInfo, unfiltered);
                }
                continue;
            }

            Row row = (Row) unfiltered;
            float rowScore = getScoreForRow(null, row);
            var triple = Triple.of(partitionInfo, row, keyAndStaticScore + rowScore);
            res.add(triple);
        }

        return res;
    }

    private void addUnfiltered(SortedMap<PartitionInfo, TreeSet<Unfiltered>> unfilteredByPartition, PartitionInfo partitionInfo, Unfiltered unfiltered)
    {
        var map = unfilteredByPartition.computeIfAbsent(partitionInfo, k -> new TreeSet<>(command.metadata().comparator));
        map.add(unfiltered);
    }

    private void moveToPq(AbstractQueue<Triple<PartitionInfo, Row, Float>> topK, List<Triple<PartitionInfo, Row, Float>> res)
    {
        for (var triple : res)
        {
            topK.add(triple);
            if (topK.size() > limit)
                topK.poll();
        }
    }

    /**
     * @return num of collected rows
     */
    public int rowCount()
    {
        return rowCount;
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
            float[] vector = TypeUtil.decomposeVector(indexContext, value);
            return indexContext.getIndexWriterConfig().getSimilarityFunction().compare(vector, queryVector);
        }
        return 0;
    }


    private Pair<IndexContext, float[]> findTopKIndexContext()
    {
        ColumnFamilyStore cfs = Keyspace.openAndGetStore(command.metadata());

        for (RowFilter.Expression expression : command.rowFilter().getExpressions())
        {
            StorageAttachedIndex sai = findVectorIndexFor(cfs.indexManager, expression);
            if (sai != null)
            {

                float[] qv = TypeUtil.decomposeVector(sai.getIndexContext(), expression.getIndexValue().duplicate());
                return Pair.create(sai.getIndexContext(), qv);
            }
        }

        return null;
    }

    @Nullable
    private StorageAttachedIndex findVectorIndexFor(SecondaryIndexManager sim, RowFilter.Expression e)
    {
        if (e.operator() != Operator.ANN)
            return null;

        Optional<Index> index = sim.getBestIndexFor(e);
        return (StorageAttachedIndex) index.filter(i -> i instanceof StorageAttachedIndex).orElse(null);
    }
}
