/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.cql;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.vector.CompactionGraph;
import org.apache.cassandra.io.sstable.SSTableId;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Reproduces a recall collapse in vector index segments built by {@link CompactionGraph}.
 * <p>
 * {@link CompactionGraph#maybeAddVector} refines the PQ codebook once {@link CompactionGraph#PQ_TRAINING_SIZE}
 * vectors have been added, re-encodes the vectors inserted so far and rescores their edges. Segments that went
 * through that step return far fewer of the true neighbours than segments that did not. With the default training
 * size of 128k vectors this shows up as soon as a compaction produces a segment with more than 128k vectors: on a
 * 2M-row table of 384-dimensional embeddings, recall@1/@10/@100 fell from 0.96-1.0 to about 0.65 after the first
 * compaction, and the missing neighbours were almost exclusively the first 128k rows (in token order) of each
 * compacted segment, i.e. the vectors inserted before the refinement.
 * <p>
 * The test uses the siftsmall dataset (10k base vectors, 100 queries with exact top-100 ground truth) and lowers
 * {@link CompactionGraph#PQ_TRAINING_SIZE} so that the refinement happens halfway through a compaction. Recall is
 * then measured separately for the true neighbours that sit in the first half of the compacted sstable (token
 * order) and for the ones in the second half. A control compaction without refinement is run first and reaches
 * recall 1.0 for both halves; the compaction with refinement drops to somewhere between 0.35 and 0.85, with the
 * split between the two halves varying from run to run at this small scale.
 * <p>
 * With the default index options the graph is built in memory by {@link CompactionGraph} in the same way for every
 * on-disk version; the version only changes how the finished graph is written (plain PQ file, FusedPQ, NVQ). This
 * test therefore runs on {@link Version#LATEST} only, with and without FusedPQ, so that it always follows the
 * format that is currently being shipped; {@link VectorSiftSmallTest#testCompaction} runs the same refinement on
 * every supported version. Running all versions here would also not fit the per-class test timeout, since every
 * parameter combination inserts, flushes, compacts twice and queries the whole dataset.
 */
public class VectorCompactionGraphPqRefineRecallTest extends VectorTester.Versioned
{
    private static final String DATASET = "siftsmall";
    private static final int TOP_K = 100;
    private static final double MIN_RECALL = 0.9;

    /**
     * Restricts the combinations of {@link VectorTester.Versioned#data()} to {@link Version#LATEST} without NVQ,
     * see the class documentation.
     */
    @Parameterized.Parameters(name = "version={0} enableNVQ={1} enableFused={2}")
    public static Collection<Object[]> data()
    {
        return VectorTester.Versioned.data()
                                     .stream()
                                     .filter(params -> params[0] == Version.LATEST && !(boolean) params[1])
                                     .collect(Collectors.toList());
    }

    @Test
    public void rowsInsertedBeforePqRefinementStaySearchable() throws Throwable
    {
        var baseVectors = VectorSiftSmallTest.readFvecs(String.format("test/data/%s/%s_base.fvecs", DATASET, DATASET));
        var queryVectors = VectorSiftSmallTest.readFvecs(String.format("test/data/%s/%s_query.fvecs", DATASET, DATASET));
        var groundTruth = VectorSiftSmallTest.readIvecs(String.format("test/data/%s/%s_groundtruth.ivecs", DATASET, DATASET));

        createTable("CREATE TABLE %s (pk int, val vector<float, 128>, PRIMARY KEY(pk))");
        String index = createIndexAsync("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
        waitForIndexQueryable(KEYSPACE, index, 5, TimeUnit.MINUTES);
        disableCompaction();

        // One flushed sstable with more than MIN_PQ_ROWS vectors: its memtable-built index carries a PQ, which is
        // what makes the following compactions take the CompactionGraph path.
        IntStream.range(0, baseVectors.size()).parallel().forEach(i -> {
            try
            {
                execute("INSERT INTO %s (pk, val) VALUES (?, ?)", i, vector(baseVectors.get(i)));
            }
            catch (Throwable t)
            {
                throw new RuntimeException(t);
            }
        });
        flush();

        // Position of every row inside a compacted sstable is its rank in token order.
        Map<Integer, Integer> rowPosition = rowPositionsInTokenOrder();
        int refineAt = baseVectors.size() / 2;

        // Control: CompactionGraph build without PQ refinement (default training size is far above 10k vectors).
        Set<SSTableId> before = liveSSTableIds();
        compact();
        assertNotEquals("compaction did not rewrite the sstable", before, liveSSTableIds());
        double[] control = recallByPosition(queryVectors, groundTruth, rowPosition, refineAt);
        assertTrue("control (no refinement): recall of rows in the first half is " + control[0], control[0] > MIN_RECALL);
        assertTrue("control (no refinement): recall of rows in the second half is " + control[1], control[1] > MIN_RECALL);

        // Same build with the PQ refinement triggered after the first half of the rows.
        int savedTrainingSize = CompactionGraph.PQ_TRAINING_SIZE;
        CompactionGraph.PQ_TRAINING_SIZE = refineAt;
        try
        {
            before = liveSSTableIds();
            compact();
            assertNotEquals("compaction did not rewrite the sstable", before, liveSSTableIds());
        }
        finally
        {
            CompactionGraph.PQ_TRAINING_SIZE = savedTrainingSize;
        }

        double[] refined = recallByPosition(queryVectors, groundTruth, rowPosition, refineAt);
        String summary = String.format("recall@%d of rows inserted before the PQ refinement: %.3f, after it: %.3f (control without refinement: %.3f / %.3f)",
                                       TOP_K, refined[0], refined[1], control[0], control[1]);
        logger.info(summary);
        assertTrue(summary, refined[0] > MIN_RECALL && refined[1] > MIN_RECALL);
    }

    /**
     * Measures recall @{@link #TOP_K} separately for the exact neighbours that sit before and after {@code splitAt}
     * in the sstable.
     *
     * @return a two-element array: index 0 is the recall of the exact neighbours whose sstable position is below
     * {@code splitAt}, index 1 is the recall of the ones whose position is at or above {@code splitAt}
     */
    private double[] recallByPosition(List<float[]> queryVectors,
                                      List<List<Integer>> groundTruth,
                                      Map<Integer, Integer> rowPosition,
                                      int splitAt)
    {
        long[] hits = new long[2];
        long[] total = new long[2];
        for (int q = 0; q < queryVectors.size(); q++)
        {
            UntypedResultSet result = execute("SELECT pk FROM %s ORDER BY val ANN OF ? LIMIT " + TOP_K, vector(queryVectors.get(q)));
            Set<Integer> returned = new HashSet<>();
            for (UntypedResultSet.Row row : result)
                returned.add(row.getInt("pk"));

            for (int neighbour : groundTruth.get(q).subList(0, TOP_K))
            {
                int region = rowPosition.get(neighbour) < splitAt ? 0 : 1;
                total[region]++;
                if (returned.contains(neighbour))
                    hits[region]++;
            }
        }
        return new double[]{ (double) hits[0] / total[0], (double) hits[1] / total[1] };
    }

    private Map<Integer, Integer> rowPositionsInTokenOrder()
    {
        List<long[]> tokenAndPk = new ArrayList<>();
        for (UntypedResultSet.Row row : execute("SELECT token(pk) AS t, pk FROM %s"))
            tokenAndPk.add(new long[]{ row.getLong("t"), row.getInt("pk") });
        tokenAndPk.sort(Comparator.comparingLong(a -> a[0]));

        Map<Integer, Integer> positions = new HashMap<>();
        for (int i = 0; i < tokenAndPk.size(); i++)
            positions.put((int) tokenAndPk.get(i)[1], i);
        return positions;
    }

    private Set<SSTableId> liveSSTableIds()
    {
        Set<SSTableId> ids = new HashSet<>();
        getCurrentColumnFamilyStore().getLiveSSTables().forEach(s -> ids.add(s.descriptor.id));
        return ids;
    }
}
