package org.apache.cassandra.index.sai.cql;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

/** Temporary timing driver (not for commit): loads real vectors, builds the index per sstable, times one compaction. */
public class CompactionTimingTest extends VectorTester
{
    @Test
    public void timeCompaction() throws Throwable
    {
        String file = System.getProperty("bench.vectors");
        int n = Integer.getInteger("bench.n", 100_000);
        int sstables = Integer.getInteger("bench.sstables", 4);
        // bench.options is key=value,key=value (quotes do not survive the test runner's argument parsing)
        String options = java.util.Arrays.stream(System.getProperty("bench.options", "similarity_function=dot_product").split(","))
                                         .map(kv -> kv.split("=", 2))
                                         .map(kv -> "'" + kv[0] + "' : '" + kv[1] + "'")
                                         .collect(java.util.stream.Collectors.joining(", "));
        Path out = Path.of(System.getProperty("bench.out", "/tmp/compaction-timing.txt"));

        Files.writeString(out, "CTIMING start heapGB=" + Runtime.getRuntime().maxMemory() / (1L << 30) + " args=" + java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments().stream().filter(a -> a.startsWith("-Xm") || a.startsWith("-XX:Max") || a.startsWith("-XX:ActiveProc")).toList() + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        // Vectors are streamed from the file into the inserting threads, so only the vectors in flight are held.
        int dim;
        try (var probe = new DataInputStream(new FileInputStream(file)))
        {
            byte[] h = new byte[4];
            probe.readFully(h);
            dim = ByteBuffer.wrap(h).order(ByteOrder.LITTLE_ENDIAN).getInt();
        }
        // bench.gtFile supplies exact neighbors for the first bench.n base vectors instead of computing them
        if (System.getProperty("bench.sweepLimits") != null)
            sweepGt = System.getProperty("bench.gtFile") != null
                      ? readIvecs(System.getProperty("bench.gtFile"), Integer.getInteger("bench.nq", 1000))
                      : computeGroundTruth(file, n, dim, out);
        // bench.tableCompression=false creates the table without SSTable compression
        String tableOptions = Boolean.parseBoolean(System.getProperty("bench.tableCompression", "true")) ? "" : " WITH compression = {'enabled': 'false'}";
        createTable("CREATE TABLE %s (pk int, vec vector<float, " + dim + ">, PRIMARY KEY(pk))" + tableOptions);
        disableCompaction();

        long t0 = System.nanoTime();
        int per = (n + sstables - 1) / sstables;
        String importDir = System.getProperty("bench.importDir");
        if (importDir == null)
        try (var in = new DataInputStream(new BufferedInputStream(new FileInputStream(file), 1 << 22)))
        {
            byte[] record = new byte[4 + dim * 4];
            for (int s = 0; s < sstables; s++)
            {
                int from = s * per, to = Math.min(n, from + per);
                ExecutorService pool = Executors.newFixedThreadPool(16);
                var inFlight = new java.util.concurrent.Semaphore(4096);
                var failure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
                for (int i = from; i < to; i++)
                {
                    in.readFully(record);
                    float[] v = new float[dim];
                    ByteBuffer.wrap(record, 4, dim * 4).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().get(v);
                    final int pk = i;
                    if (i > 0 && i % 1_000_000 == 0)
                        Files.writeString(out, String.format("CTIMING progress %d rows in %.1fs%n", i, (System.nanoTime() - t0) / 1e9),
                                          StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                    inFlight.acquire();
                    pool.submit(() -> {
                        try { execute("INSERT INTO %s (pk, vec) VALUES (?, ?)", pk, vector(v)); }
                        catch (Throwable t) { failure.compareAndSet(null, t); }
                        finally { inFlight.release(); }
                    });
                }
                pool.shutdown();
                pool.awaitTermination(10, TimeUnit.HOURS);
                if (failure.get() != null)
                    throw new RuntimeException(failure.get());
                flush();
                Files.writeString(out, String.format("CTIMING loaded %d of %d in %.1fs%n", to, n, (System.nanoTime() - t0) / 1e9),
                                  StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }
        }
        long t1 = System.nanoTime();

        String index = createIndexAsync("CREATE CUSTOM INDEX ON %s(vec) USING 'StorageAttachedIndex' WITH OPTIONS = {" + options + "}");
        waitForIndexQueryable(KEYSPACE, index, Integer.getInteger("bench.indexWaitHours", 2), TimeUnit.HOURS);
        if (importDir != null)
        {
            // bench.importDir: SSTables written (and indexed, with the same index name and options) by an earlier run;
            // they are moved in, and SAI attaches their complete index files instead of rebuilding them
            long ti = System.nanoTime();
            var failed = getCurrentColumnFamilyStore().importNewSSTables(java.util.Set.of(importDir), false, false, false, false, false, false, false);
            waitForIndexQueryable(KEYSPACE, index, Integer.getInteger("bench.indexWaitHours", 2), TimeUnit.HOURS);
            Files.writeString(out, String.format("CTIMING imported %d sstables (failed %s) in %.1fs segments=%s%n",
                                                 getCurrentColumnFamilyStore().getLiveSSTables().size(), failed, (System.nanoTime() - ti) / 1e9, segments()),
                              StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
        long t2 = System.nanoTime();
        String built = recall("built");
        sweep("built", out);

        Files.writeString(out, String.format("CTIMING index built in %.1fs%n", (t2 - t1) / 1e9), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        if (!Boolean.getBoolean("bench.skipCompaction"))
            compact();
        long t3 = System.nanoTime();
        Files.writeString(out, String.format("CTIMING compacted in %.1fs%n", (t3 - t2) / 1e9), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        String compacted = recall("compacted");
        sweep("compacted", out);
        for (String k : System.getProperty("bench.rerank", "").split(","))
            if (!k.isEmpty())
                compacted += " " + recall("rerank_k=" + k, Integer.parseInt(k));
        Files.writeString(out, "CRECALL " + built + " " + compacted + " segments=" + segments() + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        for (String t : System.getProperty("bench.qpsThreads", "").split(","))
            if (!t.isEmpty())
                Files.writeString(out, qps(Integer.parseInt(t)) + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);

        String line = String.format("CTIMING processors=" + Runtime.getRuntime().availableProcessors() + " heapGB=" + Runtime.getRuntime().maxMemory() / (1L << 30) + " n=%d dim=%d sstables=%d options=[%s] load=%.1fs indexBuild=%.1fs compaction=%.1fs%n",
                                    n, dim, sstables, options, (t1 - t0) / 1e9, (t2 - t1) / 1e9, (t3 - t2) / 1e9);
        Files.writeString(out, line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }


    private int[][] sweepGt;

    /**
     * Exact top-100 by dot product over the first n base vectors for the first bench.nq queries, so recall can be
     * measured on any prefix of a dataset.
     */
    private static int[][] computeGroundTruth(String file, int n, int dim, Path out) throws Exception
    {
        long start = System.nanoTime();
        int nq = Integer.getInteger("bench.nq", 1000);
        int keep = 100;
        var vts = io.github.jbellis.jvector.vector.VectorizationProvider.getInstance().getVectorTypeSupport();
        float[][] qa = read(System.getProperty("bench.queries"), nq);
        var q = new java.util.ArrayList<io.github.jbellis.jvector.vector.types.VectorFloat<?>>();
        for (float[] v : qa)
            q.add(vts.createFloatVector(v));
        // per query, a min-heap of (score, ordinal) holding the best `keep`
        float[][] heapScore = new float[nq][keep];
        int[][] heapOrd = new int[nq][keep];
        int[] size = new int[nq];
        int block = 8192;
        ExecutorService pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        try (var in = new DataInputStream(new BufferedInputStream(new FileInputStream(file), 1 << 22)))
        {
            byte[] record = new byte[4 + dim * 4];
            for (int from = 0; from < n; from += block)
            {
                int count = Math.min(block, n - from);
                var base = new io.github.jbellis.jvector.vector.types.VectorFloat<?>[count];
                for (int i = 0; i < count; i++)
                {
                    in.readFully(record);
                    float[] v = new float[dim];
                    ByteBuffer.wrap(record, 4, dim * 4).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().get(v);
                    base[i] = vts.createFloatVector(v);
                }
                final int first = from;
                var futures = new java.util.ArrayList<java.util.concurrent.Future<?>>();
                for (int t = 0; t < nq; t++)
                {
                    final int qi = t;
                    futures.add(pool.submit(() -> {
                        for (int i = 0; i < count; i++)
                        {
                            float score = io.github.jbellis.jvector.vector.VectorUtil.dotProduct(q.get(qi), base[i]);
                            offer(heapScore[qi], heapOrd[qi], size, qi, score, first + i);
                        }
                    }));
                }
                for (var f : futures)
                    f.get();
            }
        }
        pool.shutdown();
        int[][] gt = new int[nq][];
        for (int qi = 0; qi < nq; qi++)
        {
            Integer[] idx = new Integer[size[qi]];
            for (int i = 0; i < idx.length; i++)
                idx[i] = i;
            final int fq = qi;
            java.util.Arrays.sort(idx, (a, b) -> Float.compare(heapScore[fq][b], heapScore[fq][a]));
            gt[qi] = new int[idx.length];
            for (int i = 0; i < idx.length; i++)
                gt[qi][i] = heapOrd[qi][idx[i]];
        }
        Files.writeString(out, String.format("CSWEEP groundTruth n=%d queries=%d in %.1fs%n", n, nq, (System.nanoTime() - start) / 1e9),
                          StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        return gt;
    }

    private static void offer(float[] score, int[] ord, int[] size, int qi, float s, int o)
    {
        int k = score.length;
        if (size[qi] < k)
        {
            int i = size[qi]++;
            score[i] = s; ord[i] = o;
            while (i > 0 && score[(i - 1) / 2] > score[i]) { swap(score, ord, i, (i - 1) / 2); i = (i - 1) / 2; }
            return;
        }
        if (s <= score[0])
            return;
        score[0] = s; ord[0] = o;
        int i = 0;
        while (true)
        {
            int l = 2 * i + 1, r = l + 1, m = i;
            if (l < k && score[l] < score[m]) m = l;
            if (r < k && score[r] < score[m]) m = r;
            if (m == i) break;
            swap(score, ord, i, m); i = m;
        }
    }

    private static void swap(float[] score, int[] ord, int a, int b)
    {
        float s = score[a]; score[a] = score[b]; score[b] = s;
        int o = ord[a]; ord[a] = ord[b]; ord[b] = o;
    }

    /**
     * Recall@L against rerank depth: for each LIMIT in bench.sweepLimits, Cassandra's default depth and then
     * k = ceil(m * LIMIT) for each multiple m in bench.sweepMults (k at most 4000). Queries run on bench.sweepThreads threads.
     */
    private void sweep(String state, Path out) throws Throwable
    {
        if (sweepGt == null || !java.util.Arrays.asList(System.getProperty("bench.sweepStates", "built,compacted").split(",")).contains(state))
            return;
        int nq = sweepGt.length;
        float[][] q = read(System.getProperty("bench.queries"), nq);
        int threads = Integer.getInteger("bench.sweepThreads", 16);
        String segs = segments();
        for (String ls : System.getProperty("bench.sweepLimits").split(","))
        {
            int limit = Integer.parseInt(ls);
            var depths = new java.util.TreeSet<Integer>();
            String explicit = System.getProperty("bench.sweepDepths");
            if (explicit != null)
            {
                for (String ks : explicit.split(","))
                    if (Integer.parseInt(ks) >= limit)
                        depths.add(Integer.parseInt(ks));
            }
            else
            {
            // absolute depths as well as multiples, so small LIMITs reach their plateau
            for (int k : new int[]{ 10, 16, 20, 32, 48, 64, 80, 100, 128, 160, 200, 256, 320, 400, 512, 640, 800, 1000 })
                if (k >= limit)
                    depths.add(k);
            for (String ms : System.getProperty("bench.sweepMults", "1,1.5,2,3,4,5,6,8,10,13,16,20,25,32,40,64,100").split(","))
            {
                int k = (int) Math.ceil(Double.parseDouble(ms) * limit);
                if (k <= 4000)
                    depths.add(k);
            }
            }
            var ordered = new java.util.ArrayList<Integer>();
            ordered.add(-1);
            ordered.addAll(depths);
            for (int k : ordered)
            {
                String cql = "SELECT pk FROM %s ORDER BY vec ANN OF ? LIMIT " + limit + (k < 0 ? "" : " WITH ann_options = {'rerank_k': " + k + "}");
                // bench.cold: empty Cassandra's chunk cache and the OS page cache, then time single-thread queries
                // from cold, and evict again before the concurrent pass
                boolean cold = Boolean.getBoolean("bench.cold");
                String coldLatency = "";
                if (cold)
                {
                    String resident = evictCaches();
                    coldLatency = " cold" + latency(cql, q, Math.min(nq, Integer.getInteger("bench.latencyQueries", 1000))).trim().replace(" ", " cold") + " pageCacheAfterEvict=" + resident;
                    evictCaches();
                }
                var hits = new java.util.concurrent.atomic.AtomicLong();
                var next = new java.util.concurrent.atomic.AtomicInteger();
                ExecutorService pool = Executors.newFixedThreadPool(threads);
                long start = System.nanoTime();
                var futures = new java.util.ArrayList<java.util.concurrent.Future<?>>();
                for (int t = 0; t < threads; t++)
                    futures.add(pool.submit(() -> {
                        int i;
                        while ((i = next.getAndIncrement()) < nq)
                        {
                            var expected = new java.util.HashSet<Integer>();
                            for (int j = 0; j < limit; j++)
                                expected.add(sweepGt[i][j]);
                            long h = 0;
                            for (var row : execute(cql, vector(q[i])))
                                if (expected.contains(row.getInt("pk")))
                                    h++;
                            hits.addAndGet(h);
                        }
                        return null;
                    }));
                for (var f : futures)
                    f.get();
                pool.shutdown();
                double seconds = (System.nanoTime() - start) / 1e9;
                // warm single-thread latency over the first bench.latencyQueries queries, after the concurrent pass
                int lq = Math.min(nq, Integer.getInteger("bench.latencyQueries", 0));
                String latency = lq > 0 ? latency(cql, q, lq) : "";
                Files.writeString(out, String.format("CSWEEP state=%s segments=[%s] limit=%d rerank_k=%s recall=%.4f qps=%.0f threads=%d%s%n",
                                                     state, segs, limit, k < 0 ? "default" : String.valueOf(k),
                                                     hits.get() / ((double) limit * nq), nq / seconds, threads, latency + (cold ? " coldQps=" + String.format("%.0f", nq / seconds) + coldLatency : "")),
                                  StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }
        }
    }


    /** Mean, p50 and p99 single-thread latency over the first n queries, as " meanMs=.. p50Ms=.. p99Ms=..". */
    private String latency(String cql, float[][] q, int n) throws Throwable
    {
        long[] nanos = new long[n];
        for (int i = 0; i < n; i++)
        {
            long t = System.nanoTime();
            execute(cql, vector(q[i]));
            nanos[i] = System.nanoTime() - t;
        }
        java.util.Arrays.sort(nanos);
        double mean = java.util.Arrays.stream(nanos).average().orElse(0) / 1e6;
        return String.format(" meanMs=%.3f p50Ms=%.3f p99Ms=%.3f", mean, nanos[n / 2] / 1e6, nanos[(int) (n * 0.99)] / 1e6);
    }

    /**
     * Empties Cassandra's chunk cache and drops the OS page cache (needs passwordless sudo), and returns the page cache
     * left afterwards and how many of the table's mappings were unmapped first.
     */
    private String evictCaches() throws Throwable
    {
        if (org.apache.cassandra.cache.ChunkCache.instance != null)
            org.apache.cassandra.cache.ChunkCache.instance.clear();
        // SAI memory-maps its index files whatever disk_access_mode says, and drop_caches keeps mapped pages, so
        // unmap this process's pages of the table's files first (MADV_DONTNEED; the mappings stay valid and refault)
        String dir = getCurrentColumnFamilyStore().getDirectories().getDirectoryForNewSSTables().toString();
        var linker = java.lang.foreign.Linker.nativeLinker();
        var madvise = linker.downcallHandle(linker.defaultLookup().find("madvise").orElseThrow(),
                                            java.lang.foreign.FunctionDescriptor.of(java.lang.foreign.ValueLayout.JAVA_INT,
                                                                                    java.lang.foreign.ValueLayout.ADDRESS,
                                                                                    java.lang.foreign.ValueLayout.JAVA_LONG,
                                                                                    java.lang.foreign.ValueLayout.JAVA_INT));
        int unmapped = 0;
        for (String line : Files.readAllLines(Path.of("/proc/self/maps")))
        {
            String[] f = line.trim().split("\\s+");
            if (f.length < 6 || !f[5].startsWith(dir))
                continue;
            String[] range = f[0].split("-");
            long start = Long.parseUnsignedLong(range[0], 16), end = Long.parseUnsignedLong(range[1], 16);
            int rc = (int) madvise.invokeExact(java.lang.foreign.MemorySegment.ofAddress(start), end - start, 4 /* MADV_DONTNEED */);
            if (rc == 0)
                unmapped++;
        }
        var drop = new ProcessBuilder("sudo", "-n", "sh", "-c", "sync; echo 3 > /proc/sys/vm/drop_caches").inheritIO().start();
        if (drop.waitFor() != 0)
            throw new IllegalStateException("dropping the page cache failed; cold runs need passwordless sudo");
        // the page cache left after eviction, system-wide, from /proc/meminfo
        for (String line : Files.readAllLines(Path.of("/proc/meminfo")))
            if (line.startsWith("Cached:"))
                return String.format("%.0fMB(unmapped %d)", Long.parseLong(line.replaceAll("[^0-9]", "")) / 1024.0, unmapped);
        return "na";
    }

    /** Recall@10 over the first bench.nq queries, by CQL ANN queries with LIMIT 10, or "" without bench.queries. */
    private String recall(String label) throws Throwable
    {
        return recall(label, -1);
    }

    private String recall(String label, int rerankK) throws Throwable
    {
        String queries = System.getProperty("bench.queries");
        if (queries == null || System.getProperty("bench.gt") == null)
            return "";
        int nq = Integer.getInteger("bench.nq", 1000);
        float[][] q = read(queries, nq);
        int[][] gt = readIvecs(System.getProperty("bench.gt"), nq);
        long hits = 0;
        long start = System.nanoTime();
        for (int i = 0; i < nq; i++)
        {
            var expected = new java.util.HashSet<Integer>();
            for (int k = 0; k < 10; k++)
                expected.add(gt[i][k]);
            String cql = "SELECT pk FROM %s ORDER BY vec ANN OF ? LIMIT 10" + (rerankK < 0 ? "" : " WITH ann_options = {'rerank_k': " + rerankK + "}");
            for (var row : execute(cql, vector(q[i])))
                if (expected.contains(row.getInt("pk")))
                    hits++;
        }
        return String.format("%s:recall@10=%.4f (%d queries, %.1f ms each)", label, hits / (10.0 * nq), nq, (System.nanoTime() - start) / 1e6 / nq);
    }

    /** Queries per second with {@code threads} threads issuing the bench.queries queries (LIMIT 10) through CQL. */
    private String qps(int threads) throws Throwable
    {
        int nq = Integer.getInteger("bench.nq", 1000);
        float[][] q = read(System.getProperty("bench.queries"), nq);
        // warm up once over all queries, then time two passes
        for (float[] query : q)
            execute("SELECT pk FROM %s ORDER BY vec ANN OF ? LIMIT 10", vector(query));
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        var next = new java.util.concurrent.atomic.AtomicInteger();
        int total = Integer.getInteger("bench.qpsPasses", 2) * nq;
        long start = System.nanoTime();
        var futures = new java.util.ArrayList<java.util.concurrent.Future<?>>();
        for (int t = 0; t < threads; t++)
            futures.add(pool.submit(() -> {
                int i;
                while ((i = next.getAndIncrement()) < total)
                    execute("SELECT pk FROM %s ORDER BY vec ANN OF ? LIMIT 10", vector(q[i % nq]));
                return null;
            }));
        for (var f : futures)
            f.get();
        double seconds = (System.nanoTime() - start) / 1e9;
        pool.shutdown();
        return String.format("CQPS threads=%d qps=%.0f (%d queries in %.2fs)", threads, total / seconds, total, seconds);
    }

    private String segments()
    {
        var sb = new StringBuilder();
        for (var index : getCurrentColumnFamilyStore().indexManager.listIndexes())
            for (var sstableIndex : ((org.apache.cassandra.index.sai.StorageAttachedIndex) index).getIndexContext().getView().getIndexes())
                for (var segment : sstableIndex.getSegments())
                {
                    var searcher = (org.apache.cassandra.index.sai.disk.v2.V2VectorIndexSearcher) segment.getIndexSearcher();
                    // the rerank source: NVQ, or the full vectors stored in the index
                    sb.append(segment.metadata.numRows).append('/').append(searcher.getCompression().type)
                      .append(searcher.graph.usesNVQ() ? "+NVQ" : "+FP").append(' ');
                }
        return sb.toString().trim();
    }

    private static int[][] readIvecs(String file, int n) throws IOException
    {
        try (var in = new DataInputStream(new BufferedInputStream(new FileInputStream(file), 1 << 20)))
        {
            int[][] result = new int[n][];
            byte[] header = new byte[4];
            for (int i = 0; i < n; i++)
            {
                in.readFully(header);
                int k = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN).getInt();
                byte[] body = new byte[k * 4];
                in.readFully(body);
                int[] v = new int[k];
                ByteBuffer.wrap(body).order(ByteOrder.LITTLE_ENDIAN).asIntBuffer().get(v);
                result[i] = v;
            }
            return result;
        }
    }

    private static float[][] read(String file, int n) throws IOException
    {
        try (var in = new DataInputStream(new BufferedInputStream(new FileInputStream(file), 1 << 20)))
        {
            float[][] result = new float[n][];
            byte[] header = new byte[4];
            for (int i = 0; i < n; i++)
            {
                in.readFully(header);
                int dim = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN).getInt();
                byte[] body = new byte[dim * 4];
                in.readFully(body);
                float[] v = new float[dim];
                ByteBuffer.wrap(body).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().get(v);
                result[i] = v;
            }
            return result;
        }
    }
}
