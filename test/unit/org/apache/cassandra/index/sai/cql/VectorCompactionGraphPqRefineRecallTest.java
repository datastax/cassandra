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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.vector.CompactionGraph;
import org.apache.cassandra.io.sstable.SSTableId;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Reproduces a recall collapse in vector index segments built by {@link CompactionGraph}.
 * <p>
 * {@code CompactionGraph.maybeAddVector} refines the PQ codebook once {@code PQ_TRAINING_SIZE} vectors have been
 * added, re-encodes the vectors inserted so far and rescores their edges. Segments that went through that step
 * return far fewer of the true neighbours than segments that did not. With the default training size of 128k
 * vectors this shows up as soon as a compaction produces a segment with more than 128k vectors: on a 2M-row table
 * of 384-dimensional embeddings, recall@1/@10/@100 fell from 0.96-1.0 to about 0.65 after the first compaction, and
 * the missing neighbours were almost exclusively the first 128k rows (in token order) of each compacted segment,
 * i.e. the vectors inserted before the refinement.
 * <p>
 * The test uses the siftsmall dataset (10k base vectors, 100 queries with exact top-100 ground truth) and lowers
 * {@code PQ_TRAINING_SIZE} so that the refinement happens halfway through a compaction. Recall is then measured
 * separately for the true neighbours that sit in the first half of the compacted sstable (token order) and for the
 * ones in the second half. A control compaction without refinement is run first and reaches recall 1.0 for both
 * halves; the compaction with refinement drops to somewhere between 0.35 and 0.85, with the split between the two
 * halves varying from run to run at this small scale.
 * <p>
 * Only the latest on-disk version is exercised (with and without FusedPQ): every parameter combination inserts,
 * flushes, compacts twice and queries the whole dataset, and running all versions in one JVM does not fit the
 * per-class test timeout.
 */
public class VectorCompactionGraphPqRefineRecallTest extends VectorTester.Versioned
{
    private static final String DATASET = "siftsmall";
    private static final int TOP_K = 100;
    private static final double MIN_RECALL = 0.9;

    @Test
    public void rowsInsertedBeforePqRefinementStaySearchable() throws Throwable
    {
        Assume.assumeTrue("only the latest on-disk version is exercised", version == Version.LATEST);
        Assume.assumeFalse("NVQ variants are not exercised", ENABLE_NVQ);

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
     * Recall@{@link #TOP_K} of the exact neighbours whose sstable position is below {@code splitAt} (index 0) and
     * of the ones at or above it (index 1).
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
            UntypedResultSet result = execute("SELECT pk FROM %s ORDER BY val ANN OF " + Arrays.toString(queryVectors.get(q)) + " LIMIT " + TOP_K);
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
        tokenAndPk.sort((a, b) -> Long.compare(a[0], b[0]));

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
