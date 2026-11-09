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

package org.apache.cassandra.test.microbench.index.sai;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.vector.CompactionGraphMerger;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Benchmark for vector index compaction using large counts of randomly-generated vectors.
 *
 * Unlike {@link VectorCompactionBench} (which requires the SIFT dataset on disk), this
 * benchmark generates its own random data so it can run anywhere without external files.
 *
 * To avoid paying the insertion and flush cost on every iteration, the SSTables are written
 * exactly once in the trial setup and then snapshotted. Before each measured invocation the
 * snapshot files are hard-linked back into the live data directory and re-registered with
 * the {@link ColumnFamilyStore}, so every {@code @Benchmark} call starts from an identical
 * set of pre-built SSTables and only pays for the compaction itself.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class LargeRandomVectorCompactionBench extends SAITester
{
    private static final String SNAPSHOT_TAG = "bench_originals";

    /** Vector space dimensionality. */
    @Param({ "128" })
    public int dimension;

    /** Number of rows per SSTable. Values well above MIN_PQ_ROWS (1024) trigger PQ training. */
    @Param({ "100000" })
    public int rowsPerSstable;

    /** Number of SSTables to compact together. */
    @Param({ "2" })
    public int sstableCount;

    /**
     * When true, vector indexes are compacted by merging existing JVector graphs directly.
     * When false, the index is rebuilt from scratch during compaction (legacy behavior).
     */
    @Param({ "true", "false" })
    public boolean graphMergeEnabled;

    private ColumnFamilyStore cfs;
    private List<File> snapshotFiles;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        // VectorFeatureFlags has no defaults; set the baseline unless the forked JVM was given explicit -D values.
        if (CassandraRelevantProperties.SAI_VECTOR_AMORTIZE_PQ_ENCODING.getString() == null)
            CassandraRelevantProperties.SAI_VECTOR_AMORTIZE_PQ_ENCODING.setBoolean(false);
        if (CassandraRelevantProperties.SAI_VECTOR_SERIALIZE_FLUSH_PQ.getString() == null)
            CassandraRelevantProperties.SAI_VECTOR_SERIALIZE_FLUSH_PQ.setBoolean(false);
        if (CassandraRelevantProperties.SAI_VECTOR_FLUSH_REFINE_FINAL_GRAPH.getString() == null)
            CassandraRelevantProperties.SAI_VECTOR_FLUSH_REFINE_FINAL_GRAPH.setBoolean(false);

        CQLTester.setUpClass();
        CQLTester.prepareServer();
        CompactionGraphMerger.ENABLED = graphMergeEnabled;
        beforeTest();

        createTable("CREATE TABLE %s (pk int, val vector<float, " + dimension + ">, PRIMARY KEY(pk))");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function': 'cosine'}");
        disableCompaction();

        // Generate and flush random vectors into sstableCount SSTables.
        // Rows are distributed round-robin across SSTables so each gets exactly rowsPerSstable rows.
        int total = rowsPerSstable * sstableCount;
        Random rng = new Random(42);
        float[][] vectors = new float[total][];
        for (int i = 0; i < total; i++)
            vectors[i] = randomUnitVector(rng, dimension);

        for (int s = 0; s < sstableCount; s++)
        {
            int start = s * rowsPerSstable;
            int end = start + rowsPerSstable;
            IntStream.range(start, end).parallel().forEach(i -> {
                try
                {
                    execute("INSERT INTO %s (pk, val) VALUES (?, ?)", i, vector(vectors[i]));
                }
                catch (Throwable t)
                {
                    throw new RuntimeException(t);
                }
            });
            flush();
        }

        // Snapshot the flushed SSTables once.  Each invocation will restore from this snapshot
        // via hard links instead of re-inserting and re-flushing the data.
        cfs = getCurrentColumnFamilyStore();
        cfs.snapshot(SNAPSHOT_TAG);
        snapshotFiles = cfs.getDirectories().sstableLister(Directories.OnTxnErr.IGNORE).snapshots(SNAPSHOT_TAG).listFiles();
    }

    /**
     * Before each measured invocation: truncate the live data directory and restore the
     * original SSTables from the snapshot via hard links, then re-register them with the CFS.
     * This avoids re-inserting and re-flushing data on every iteration.
     */
    @Setup(Level.Invocation)
    public void restoreSnapshot()
    {
        cfs.truncateBlocking();

        // Remove any leftover live SSTable files (truncate leaves directories intact).
        for (File dir : cfs.getDirectories().getCFDirectories())
            for (File f : dir.tryList())
                if (!f.isDirectory())
                    FileUtils.delete(f);

        // Hard-link snapshot files back into the live data directory.
        // The snapshot path is .../snapshots/<tag>/<file>; the live directory is three levels up.
        for (File snapshotFile : snapshotFiles)
            FileUtils.createHardLink(snapshotFile,
                                     new File(snapshotFile.toPath().getParent().getParent().getParent().toFile(),
                                              snapshotFile.name()));

        cfs.loadNewSSTables();
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException, ExecutionException, InterruptedException
    {
        CompactionGraphMerger.ENABLED = CassandraRelevantProperties.SAI_VECTOR_GRAPH_COMPACTION_MERGE_ENABLED.getBoolean();
        CommitLog.instance.shutdownBlocking();
        CQLTester.cleanup();
    }

    /** Benchmark: compact all restored SSTables into one. */
    @Benchmark
    public void compactVectorIndex()
    {
        compact();
    }

    /** Random unit vector in [-1, 1]^dim, L2-normalised so cosine similarity is well-defined. */
    private static float[] randomUnitVector(Random rng, int dim)
    {
        float[] v = new float[dim];
        float sumSq = 0.0f;
        for (int i = 0; i < dim; i++)
        {
            v[i] = rng.nextFloat() * 2.0f - 1.0f;
            sumSq += v[i] * v[i];
        }
        float norm = (float) Math.sqrt(sumSq);
        if (norm > 0.0f)
            for (int i = 0; i < dim; i++)
                v[i] /= norm;
        return v;
    }
}
