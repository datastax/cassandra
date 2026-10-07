/*
 * Copyright DataStax, Inc.
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

package org.apache.cassandra.index.sai.disk.vector;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.utils.LowPriorityThreadFactory;

public class JVectorVersionUtil
{
    private static final Logger logger = LoggerFactory.getLogger(JVectorVersionUtil.class);

    /*
     * Some attributes are volatile to allow for changing in unit tests. They are only accessed on flush and compaction,
     * so their access is infrequent.
     */

    /** Whether to fuse quantized vectors into the graph when writing indexes, assuming all other conditions are met. */
    public static volatile boolean ENABLE_FUSED = CassandraRelevantProperties.SAI_VECTOR_ENABLE_FUSED.getBoolean();
    public static volatile boolean ENABLE_NVQ = CassandraRelevantProperties.SAI_VECTOR_ENABLE_NVQ.getBoolean();
    public static final int NUM_SUB_VECTORS = CassandraRelevantProperties.SAI_VECTOR_NVQ_NUM_SUB_VECTORS.getInt();

    /** When true, the memtable-flush graph build runs on the shared jvector build pool instead of caller-runs. */
    public static volatile boolean FLUSH_BUILD_PARALLEL = CassandraRelevantProperties.SAI_VECTOR_FLUSH_BUILD_PARALLEL.getBoolean();

    private static final boolean POOL_ESCAPE_MONITOR =
        CassandraRelevantProperties.SAI_VECTOR_POOL_ESCAPE_MONITOR.getBoolean();
    private static final AtomicBoolean poolEscapeWarned = new AtomicBoolean();

    // --- Shared build pool -------------------------------------------------------------------

    private static final class BuildPool
    {
        final int threads;
        final ForkJoinPool pool;

        BuildPool(int threads)
        {
            this.threads = Math.max(1, threads);
            this.pool = new ForkJoinPool(this.threads, new LowPriorityThreadFactory(), null, false);
            logger.info("jvector build pool created: {} low-priority threads.", this.threads);
        }
    }

    private static volatile int desiredBuildThreads = resolveCompactionBuildThreads();
    private static volatile BuildPool buildPool = new BuildPool(desiredBuildThreads);
    private static final Object buildPoolLock = new Object();

    private static BuildPool currentBuildPool()
    {
        BuildPool p = buildPool;
        if (p.threads == desiredBuildThreads)
            return p;
        synchronized (buildPoolLock)
        {
            p = buildPool;
            if (p.threads != desiredBuildThreads)
                buildPool = p = new BuildPool(desiredBuildThreads);
            return p;
        }
    }

    /** The shared jvector build/compaction {@link ForkJoinPool}. */
    public static ForkJoinPool compactionBuildPool()
    {
        return currentBuildPool().pool;
    }

    /** The current worker-thread count of the shared build/compaction pool. */
    public static int compactionBuildThreads()
    {
        return currentBuildPool().threads;
    }

    /** The desired worker-thread count a running compaction will pick up on its next context request. */
    public static int getDesiredCompactionBuildThreads()
    {
        return desiredBuildThreads;
    }

    /** Set the desired worker-thread count for the shared build/compaction pool. */
    public static void setCompactionBuildThreads(int threads)
    {
        int resolved = threads > 0 ? threads : DatabaseDescriptor.getConcurrentCompactors();
        desiredBuildThreads = Math.max(1, resolved);
    }

    private static int resolveCompactionBuildThreads()
    {
        int configured = CassandraRelevantProperties.SAI_VECTOR_COMPACTION_BUILD_THREADS.getInt();
        int threads = configured > 0 ? configured : DatabaseDescriptor.getConcurrentCompactors();
        return Math.max(1, threads);
    }

    // --- Insert in-flight budget -------------------------------------------------------------

    private static volatile int desiredInflightPermits = resolveInsertInflightPermits();

    /** The current in-flight insert budget in MiB, or 0 if unbounded. */
    public static int getInsertInflightMb()
    {
        int permits = desiredInflightPermits;
        return permits <= 0 ? 0 : permits / (1024 * 1024);
    }

    /** Set the node-wide in-flight insert budget in MiB, or 0 to disable the bound. */
    public static void setInsertInflightMb(int mb)
    {
        desiredInflightPermits = mb <= 0 ? 0 : (int) Math.min(Integer.MAX_VALUE, (long) mb * 1024L * 1024L);
    }

    private static int resolveInsertInflightPermits()
    {
        int mb = CassandraRelevantProperties.SAI_VECTOR_COMPACTION_INSERT_INFLIGHT_MB.getInt();
        if (mb <= 0)
            return 0;
        return (int) Math.min(Integer.MAX_VALUE, (long) mb * 1024L * 1024L);
    }

    // --- Per-merge memory estimate -----------------------------------------------------------

    private static volatile int mergeBytesPerOrdinal =
        CassandraRelevantProperties.SAI_VECTOR_COMPACTION_MERGE_BYTES_PER_ORDINAL.getInt();

    /** The per-surviving-ordinal memory estimate (bytes) charged for a vector graph merge. */
    public static int getMergeBytesPerOrdinal()
    {
        return mergeBytesPerOrdinal;
    }

    /** Set the per-surviving-ordinal merge memory estimate (bytes). */
    public static void setMergeBytesPerOrdinal(int bytes)
    {
        mergeBytesPerOrdinal = Math.max(1, bytes);
    }

    // --- PQ encoding flags (lazy-strict, required-explicit via VectorFeatureFlags) ----------

    private static volatile Boolean amortizePqEncoding = null;

    /** Whether memtable flushes encode PQ incrementally during ingest. */
    public static boolean isAmortizePqEncoding()
    {
        Boolean v = amortizePqEncoding;
        if (v == null)
            amortizePqEncoding = v = VectorFeatureFlags.amortizePqEncoding();
        return v;
    }

    /** Enable/disable incremental PQ encoding during ingest. */
    public static void setAmortizePqEncoding(boolean enabled)
    {
        amortizePqEncoding = enabled;
    }

    private static volatile Boolean serializeFlushPq = null;

    /** Whether residual flush-time PQ work is serialized node-wide. */
    public static boolean isSerializeFlushPq()
    {
        Boolean v = serializeFlushPq;
        if (v == null)
            serializeFlushPq = v = VectorFeatureFlags.serializeFlushPq();
        return v;
    }

    /** Enable/disable node-wide serialization of residual flush-time PQ work. */
    public static void setSerializeFlushPq(boolean enabled)
    {
        serializeFlushPq = enabled;
    }

    // --- Graph compaction merge enable/disable -----------------------------------------------

    /** Whether vector-index compaction merges existing on-disk graphs (vs. the legacy rebuild path). */
    public static boolean isGraphCompactionMergeEnabled()
    {
        return CompactionGraphMerger.ENABLED;
    }

    /** Enable/disable the vector graph-compaction merge path. */
    public static void setGraphCompactionMergeEnabled(boolean enabled)
    {
        CompactionGraphMerger.ENABLED = enabled;
    }

    /**
     * Decide whether we should write NVQ vectors to disk.
     * With NVQ, we use M * (7 + D / M) bytes, where D is the number of dimensions and M is the number of subvectors.
     * For FP vectors, we trivially use 4D bytes
     * @param dimension vector dimension for the index
     * @param version SAI on disk version, which internally determines the jvector version
     * @return true if NVQ should be used for the graph or false otherwise
     */
    public static boolean shouldWriteNVQ(int dimension, Version version)
    {
        return ENABLE_NVQ && versionSupportsNVQ(version) && NUM_SUB_VECTORS * (7 + dimension / NUM_SUB_VECTORS) < 4 * dimension;
    }

    public static boolean versionSupportsNVQ(Version version)
    {
        return version.onDiskFormat().jvectorFileFormatVersion() >= 4;
    }

    /**
     * Decide whether to attempt to write the quantized vectors as fused parts of the graph. Note that this method
     * does not take into account whether the graph has enough information to build a quantization, as that depends on
     * external factors.
     * <p>
     * FusedPQ is not supported in versions before FA, so it is not enabled regardless of any config.
     * For version FA, FusedPQ is always enabled regardless of {@code ENABLE_FUSED}.
     * For version FB and later, FusedPQ is opt-in via {@code cassandra.sai.vector.enable_fused}.
     *
     * @param version the SAI on disk format to use when writing to disk
     * @return true if conditions are met, false otherwise
     */
    public static boolean shouldWriteFused(Version version)
    {
        if (!versionSupportsFused(version))
            return false;
        // FA always uses FusedPQ; FB+ requires the flag to be set
        return version.equals(Version.FA) || ENABLE_FUSED;
    }

    public static boolean versionSupportsFused(Version version)
    {
        return version.onDiskFormat().jvectorFileFormatVersion() >= 6;
    }
}
