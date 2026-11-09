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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
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
 * Measures the cost of graph-merge compaction when the compaction output is sharded.
 *
 * <p>Two sstables holding the {@code siftsmall} base vectors are compacted into {@link #shardCount} output sstables
 * (UCS with {@code base_shard_count}). Every output sstable has its own index writer, and with graph merge enabled
 * each of them runs its own {@link CompactionGraphMerger} over <em>all</em> the source graphs, keeping only the
 * nodes that fall into its token range. The source graphs are therefore traversed once per shard, whereas a standard
 * index build touches every vector exactly once. Comparing {@link #graphMergeEnabled} {@code true} vs {@code false}
 * shows that cost.
 *
 * <p>Data loading is excluded from the measurement: the source sstables are written and flushed exactly once in the
 * trial setup and snapshotted. Before every invocation (which JMH does not time) the snapshot is hard-linked back
 * into the live data directory and re-registered with the {@link ColumnFamilyStore}, so each {@code @Benchmark} call
 * starts from the same two sstables and only pays for the compaction itself.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class ShardedVectorCompactionBench extends SAITester
{
    private static final String DATASET = "siftsmall";
    private static final String SNAPSHOT_TAG = "bench_originals";
    private static final int SOURCE_SSTABLES = 2;

    /** Number of output sstables (UCS {@code base_shard_count}). */
    @Param({ "64" })
    public int shardCount;

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
        // VectorFeatureFlags has no defaults; set the baseline unless the forked JVM was given explicit -D values
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

        var vectors = readFvecs(String.format("test/data/%s/%s_base.fvecs", DATASET, DATASET));

        createTable("CREATE TABLE %s (pk int PRIMARY KEY, val vector<float, 128>) WITH compaction = " +
                    "{'class' : 'UnifiedCompactionStrategy', 'enabled' : false, 'base_shard_count' : " + shardCount +
                    ", 'min_sstable_size' : '1KiB'}");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex' WITH OPTIONS = {'similarity_function' : 'euclidean'}");
        disableCompaction();

        // Split the dataset evenly over the source sstables
        int perSSTable = vectors.size() / SOURCE_SSTABLES;
        for (int s = 0; s < SOURCE_SSTABLES; s++)
        {
            IntStream.range(s * perSSTable, (s + 1) * perSSTable).parallel().forEach(i -> {
                try
                {
                    execute("INSERT INTO %s (pk, val) VALUES (?, ?)", i, vector(vectors.get(i)));
                }
                catch (Throwable t)
                {
                    throw new RuntimeException(t);
                }
            });
            flush();
        }

        // Snapshot the flushed sstables once; each invocation restores them via hard links.
        cfs = getCurrentColumnFamilyStore();
        if (cfs.getLiveSSTables().size() != SOURCE_SSTABLES)
            throw new IllegalStateException("Expected " + SOURCE_SSTABLES + " source sstables but found " + cfs.getLiveSSTables().size());
        cfs.snapshot(SNAPSHOT_TAG);
        snapshotFiles = cfs.getDirectories().sstableLister(Directories.OnTxnErr.IGNORE).snapshots(SNAPSHOT_TAG).listFiles();
    }

    /**
     * Before each invocation (not timed by JMH): drop whatever the previous compaction produced and restore the
     * original sstables from the snapshot via hard links, then re-register them with the CFS.
     */
    @Setup(Level.Invocation)
    public void restoreSnapshot()
    {
        cfs.truncateBlocking();

        // Remove any leftover live sstable files (truncate leaves directories intact).
        for (File dir : cfs.getDirectories().getCFDirectories())
            for (File f : dir.tryList())
                if (!f.isDirectory())
                    FileUtils.delete(f);

        // The snapshot path is .../snapshots/<tag>/<file>; the live directory is three levels up.
        for (File snapshotFile : snapshotFiles)
            FileUtils.createHardLink(snapshotFile,
                                     new File(snapshotFile.toPath().getParent().getParent().getParent().toFile(),
                                              snapshotFile.name()));

        cfs.loadNewSSTables();
    }

    /** Sanity check, outside the timed region: the compaction must have produced one sstable per shard. */
    @TearDown(Level.Invocation)
    public void verifyOutput()
    {
        int outputSSTables = cfs.getLiveSSTables().size();
        if (outputSSTables != shardCount)
            throw new IllegalStateException("Expected " + shardCount + " output sstables but found " + outputSSTables);
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException, ExecutionException, InterruptedException
    {
        CompactionGraphMerger.ENABLED = CassandraRelevantProperties.SAI_VECTOR_GRAPH_COMPACTION_MERGE_ENABLED.getBoolean();
        CommitLog.instance.shutdownBlocking();
        CQLTester.cleanup();
    }

    /** Compacts the restored sstables into {@link #shardCount} sharded output sstables. */
    @Benchmark
    public void compactIntoShards()
    {
        cfs.forceMajorCompaction(false, 1);
    }

    /** Reads vectors from a .fvecs file (SIFT dataset format). */
    private static List<float[]> readFvecs(String filePath) throws IOException
    {
        var vectors = new ArrayList<float[]>();
        try (var dis = new DataInputStream(new BufferedInputStream(new FileInputStream(filePath))))
        {
            while (dis.available() > 0)
            {
                int dimension = Integer.reverseBytes(dis.readInt());
                var buffer = new byte[dimension * Float.BYTES];
                dis.readFully(buffer);
                var byteBuffer = ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN);
                var vector = new float[dimension];
                for (int i = 0; i < dimension; i++)
                    vector[i] = byteBuffer.getFloat();
                vectors.add(vector);
            }
        }
        return vectors;
    }
}
