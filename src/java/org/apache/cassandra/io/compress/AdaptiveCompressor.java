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

package org.apache.cassandra.io.compress;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.memtable.AbstractAllocatorMemtable;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.MetricNameFactory;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.ExpMovingAverage;

/**
 * A compressor that dynamically adapts the compression level to the load.
 * If the system is not heavily loaded by writes, data are compressed using high compression level.
 * If the number of compactions or flushes queue up, it decreases the compression level to speed up
 * flushing / compacting.
 * <p>
 * Underneath, the ZStandard compressor is used. The compression level can be changed between the frames,
 * even when compressing the same sstable file. ZStandard was chosen because at the fast end it
 * can reach compression speed of LZ4, but at moderate compression levels it usually offers much better
 * compression ratio (typically files are smaller by 20-40%) than LZ4 without compromising compression speed by too much.
 * <p>
 * This compressor can be used for either of Uses: FAST_COMPRESSION and GENERAL.
 * Each use can have different minimum and maximum compression level limits.
 * For FAST_COMPRESSION, the number of pending flushes is used as the indicator of write load.
 * For GENERAL compression, the number of pending compactions is used as the indicator of write load.
 * <p>
 * Valid compression levels are in range 0..15 (inclusive), where 0 means fastest compression and 15 means slowest/best.
 * Usually levels around 7-11 strike the best balance between performance and compresion ratio.
 * Going above level 12 usually only results in slower compression but not much compression ratio improvement.
 * <p>
 * Caution: This compressor decompresses about 2x-4x slower than LZ4Compressor, regardless of the compression level.
 * Therefore, it may negatively affect read speed from very read-heavy tables, especially when the chunk-cache
 * hit ratio is low. In synthetic tests with chunk cache disabled, read throughput turned out to be up to 10%
 * lower than when using LZ4 on some workloads.
 */
public class AdaptiveCompressor implements ICompressor
{
    @VisibleForTesting
    static final Map<Uses, AdaptiveCompressor.Metrics> metrics = new EnumMap<>(Map.of(
        Uses.FAST_COMPRESSION, new Metrics(Uses.FAST_COMPRESSION),
        Uses.GENERAL, new Metrics(Uses.GENERAL)
    ));

    protected static final String MIN_COMPRESSION_LEVEL_OPTION_NAME = "min_compression_level";
    protected static final String MAX_COMPRESSION_LEVEL_OPTION_NAME = "max_compression_level";
    protected static final String MAX_COMPACTION_QUEUE_LENGTH_OPTION_NAME = "max_compaction_queue_length";


    /**
     * Maps AdaptiveCompressor compression level to underlying ZStandard compression levels.
     * This mapping is needed because ZStandard levels are not continuous, zstd level 0 is special and means level 3.
     * Hence, we just use our own continuous scale starting at 0.
     */
    private static final int[] zstdCompressionLevels = {
        -7, // 0  (very fast but compresses poorly)
        -6, // 1
        -5, // 2  (LZ4 level is somewhere here)
        -4, // 3
        -3, // 4
        -2, // 5
        -1, // 6
        1,  // 7  (sweet spot area usually here)
        2,  // 8  (sweet spot area usually here)
        3,  // 9  (sweet spot area usually here, ~50% slower than LZ4)
        4,  // 10 (sweet spot area usually here)
        5,  // 11 (sweet spot area usually here)
        6,  // 12
        7,  // 13
        8,  // 14
        9,  // 15 (very slow, usually over 10x slower than LZ4)
    };

    public static final int MIN_COMPRESSION_LEVEL = 0;
    public static final int MAX_COMPRESSION_LEVEL = 15;

    public static final int DEFAULT_MIN_FAST_COMPRESSION_LEVEL = 2;     // zstd level -5
    public static final int DEFAULT_MAX_FAST_COMPRESSION_LEVEL = 9;     // zstd level 5
    public static final int DEFAULT_MIN_GENERAL_COMPRESSION_LEVEL = 7;  // zstd level 1
    public static final int DEFAULT_MAX_GENERAL_COMPRESSION_LEVEL = 12; // zstd level 6
    public static final int DEFAULT_MAX_COMPACTION_QUEUE_LENGTH = 16;

    private static final ConcurrentHashMap<Params, AdaptiveCompressor> instances = new ConcurrentHashMap<>();

    public static AdaptiveCompressor create(Map<String, String> options)
    {
        int minCompressionLevel = getMinCompressionLevel(Uses.GENERAL, options);
        int maxCompressionLevel = getMaxCompressionLevel(Uses.GENERAL, options);
        int maxCompactionQueueLength = getMaxCompactionQueueLength(options);
        return createForCompaction(minCompressionLevel, maxCompressionLevel, maxCompactionQueueLength);
    }

    private static AdaptiveCompressor createForCompaction(int minCompressionLevel, int maxCompressionLevel, int maxCompactionQueueLength)
    {
        Params params = new Params(Uses.GENERAL, minCompressionLevel, maxCompressionLevel, maxCompactionQueueLength);
        Supplier<Double> compactionPressureSupplier = () -> getCompactionPressure(maxCompactionQueueLength);
        return instances.computeIfAbsent(params, p -> new AdaptiveCompressor(p, compactionPressureSupplier));
    }

    /**
     * Creates a compressor that doesn't refer to any other C* components like compaction manager or memory pools.
     */
    @VisibleForTesting
    public static ICompressor createForUnitTesting()
    {
        Params params = new Params(Uses.GENERAL, 9, 9, 0);
        return new AdaptiveCompressor(params, () -> 0.0);
    }

    public static AdaptiveCompressor createForFlush(Map<String, String> options)
    {
        int minCompressionLevel = getMinCompressionLevel(Uses.FAST_COMPRESSION, options);
        int maxCompressionLevel = getMaxCompressionLevel(Uses.FAST_COMPRESSION, options);
        return createForFlush(minCompressionLevel, maxCompressionLevel);
    }

    private static AdaptiveCompressor createForFlush(int minCompressionLevel, int maxCompressionLevel)
    {
        Params params = new Params(Uses.FAST_COMPRESSION, minCompressionLevel, maxCompressionLevel, 0);
        return instances.computeIfAbsent(params, p -> new AdaptiveCompressor(p, AdaptiveCompressor::getFlushPressure));
    }

    private final Params params;
    private final ThreadLocal<State> state;
    private final Supplier<Double> writePressureSupplier;

    static class Params
    {
        final Uses use;
        final int minCompressionLevel;
        final int maxCompressionLevel;
        final int maxCompactionQueueLength;

        Params(Uses use, int minCompressionLevel, int maxCompressionLevel, int maxCompactionQueueLength)
        {
            if (minCompressionLevel < MIN_COMPRESSION_LEVEL || minCompressionLevel > MAX_COMPRESSION_LEVEL)
                throw new IllegalArgumentException("Min compression level " + minCompressionLevel + "out of range" +
                                                   " [" + MIN_COMPRESSION_LEVEL + ", " + MAX_COMPRESSION_LEVEL + ']');
            if (maxCompressionLevel < MIN_COMPRESSION_LEVEL || maxCompressionLevel > MAX_COMPRESSION_LEVEL)
                throw new IllegalArgumentException("Max compression level " + maxCompressionLevel + "out of range" +
                                                   " [" + MIN_COMPRESSION_LEVEL + ", " + MAX_COMPRESSION_LEVEL + ']');
            if (maxCompactionQueueLength < 0)
                throw new IllegalArgumentException("Negative max compaction queue length: " + maxCompactionQueueLength);

            this.use = use;
            this.minCompressionLevel = minCompressionLevel;
            this.maxCompressionLevel = maxCompressionLevel;
            this.maxCompactionQueueLength = maxCompactionQueueLength;
        }

        @Override
        public boolean equals(Object o)
        {
            if (o == null || getClass() != o.getClass()) return false;
            Params params = (Params) o;
            return minCompressionLevel == params.minCompressionLevel && maxCompressionLevel == params.maxCompressionLevel && use == params.use;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(use, minCompressionLevel, maxCompressionLevel);
        }
    }

    /**
     * Keeps thread local state.
     * We need this because we want to not only monitor pending flushes/compactions but also how much
     * time we spend in compression relative to time spent by the thread in non-compression tasks like preparation
     * of data or writing. Because ICompressor can be shared by multiple threads, we need to keep
     * track of each thread separately.
     */
    class State
    {
        final ZstdCompressCtx compressCtx = new ZstdCompressCtx().setChecksum(true);
        final ZstdDecompressCtx decompressCtx = new ZstdDecompressCtx();

        /**
         * ZStandard compression level that was used when compressing the previous chunk.
         * Can be adjusted up or down by at most 1 with every next block.
         */
        int currentCompressionLevel;

        /**
         * How much time is spent by the thread in compression relative to the time spent in non-compression code.
         * Valid range is [0.0, 1.0].
         * 1.0 means we're doing only compression and nothing else.
         * 0.0 means we're not spending any time doing compression.
         * This indicator allows us to detect whether we're bottlenecked by something else than compression,
         * e.g. by disk I/O or by preparation of data to compress (e.g. iterating the memtable trie).
         * If this value is low, then there is not much gain in decreasing the compression
         * level.
         */
        ExpMovingAverage relativeTimeSpentCompressing = ExpMovingAverage.decayBy10();

        long lastCompressionStartTime;
        long lastCompressionDuration;

        /**
         * Computes the new compression level to use for the next chunk, based on the load.
         */
        public void adjustCompressionLevel(long currentTime)
        {
            // The more write "pressure", the faster we want to go, so the lower the desired compression level.
            double pressure = getWritePressure();
            assert pressure >= 0.0 && pressure <= 1.0 : "pressure (" + pressure + ") out of valid range [0.0, 1.0]";

            // Use minCompressionLevel when pressure = 1.0, maxCompressionLevel when pressure = 0.0
            int pressurePoints = (int) (pressure * (params.maxCompressionLevel - params.minCompressionLevel));
            int compressionLevelTarget = params.maxCompressionLevel - pressurePoints;

            // We use wall clock time and not CPU time, because we also want to include time spent by I/O.
            // If we're bottlenecked by writing the data to disk, this indicator should be low.
            double relativeTimeSpentCompressing = (double) (1 + lastCompressionDuration) / (1 + currentTime - lastCompressionStartTime);

            // Some smoothing is needed to avoid changing level too fast due to performance hiccups
            this.relativeTimeSpentCompressing.update(relativeTimeSpentCompressing);

            // If we're under pressure to write data fast, we need to decrease compression level.
            // But we do that only if we're really spending significant amount of time doing compression.
            if (compressionLevelTarget < currentCompressionLevel && this.relativeTimeSpentCompressing.get() > 0.1)
                currentCompressionLevel--;
            // If we're not under heavy write pressure, or we're spending very little time compressing data,
            // we can increase the compression level and get some space savings at a low performance overhead:
            else if (compressionLevelTarget > currentCompressionLevel || this.relativeTimeSpentCompressing.get() < 0.02)
                currentCompressionLevel++;

            currentCompressionLevel = clampCompressionLevel(currentCompressionLevel);
            compressCtx.setLevel(zstdCompressionLevels[currentCompressionLevel]);
        }

        /**
         * Must be called after compressing a chunk,
         * so we can measure how much time we spend in compressing vs time spent not-compressing.
         */
        public void recordCompressionDuration(long startTime, long endTime)
        {
            this.lastCompressionDuration = endTime - startTime;
            this.lastCompressionStartTime = startTime;
        }

        @VisibleForTesting
        double getRelativeTimeSpentCompressing()
        {
            return this.relativeTimeSpentCompressing.get();
        }
    }

    /**
     * @param params user-provided configuration such as min/max compression level range
     * @param writePressureSupplier returns a non-negative score determining the write load on the system which
     *                               is used to control the desired compression level. Influences the compression
     *                               level linearly: an inceease of pressure by 1 point causes the target
     *                               compression level to be decreased by 1 point. Zero will select the
     *                               maximum allowed compression level.
     */
    @VisibleForTesting
    AdaptiveCompressor(Params params, Supplier<Double> writePressureSupplier)
    {
        this.params = params;
        this.state = new ThreadLocal<>();
        this.writePressureSupplier = writePressureSupplier;
    }
    
    @Override
    public int initialCompressedBufferLength(int chunkLength)
    {
        return (int) Zstd.compressBound(chunkLength);
    }


    @Override
    public void compress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        try
        {
            State state = getThreadLocalState();
            long startTime = Clock.Global.nanoTime();
            state.adjustCompressionLevel(startTime);
            long inputSize = input.remaining();
            state.compressCtx.compress(output, input);
            long endTime = Clock.Global.nanoTime();
            state.recordCompressionDuration(startTime, endTime);

            Metrics m = metrics.get(params.use);
            m.updateFrom(state);
            m.compressionRate.mark(inputSize);
        }
        catch (Exception e)
        {
            throw new IOException("Compression failed", e);
        }

    }

    @Override
    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException
    {
        State state = getThreadLocalState();
        long dsz = state.decompressCtx.decompressByteArray(output, outputOffset, output.length - outputOffset,
                                                           input, inputOffset, inputLength);

        if (Zstd.isError(dsz))
            throw new IOException(String.format("Decompression failed due to %s", Zstd.getErrorName(dsz)));

        metrics.get(params.use).decompressionRate.mark(dsz);
        return (int) dsz;
    }

    @Override
    public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        try
        {
            State state = getThreadLocalState();
            long dsz = state.decompressCtx.decompress(output, input);
            metrics.get(params.use).decompressionRate.mark(dsz);
        } catch (Exception e)
        {
            throw new IOException("Decompression failed", e);
        }
    }

    @Override
    public BufferType preferredBufferType()
    {
        return BufferType.OFF_HEAP;
    }

    @Override
    public Set<Uses> recommendedUses()
    {
        return params.minCompressionLevel <= DEFAULT_MIN_FAST_COMPRESSION_LEVEL
               ? EnumSet.of(Uses.GENERAL, Uses.FAST_COMPRESSION)
               : EnumSet.of(params.use);
    }

    @Override
    public ICompressor forUse(Uses use)
    {
        if (use == params.use)
            return this;

        switch (use)
        {
            case GENERAL:
                return createForCompaction(params.minCompressionLevel, params.maxCompressionLevel, params.maxCompactionQueueLength);
            case FAST_COMPRESSION:
                return createForFlush(params.minCompressionLevel, params.maxCompressionLevel);
        }

        return null;
    }

    @Override
    public boolean supports(BufferType bufferType)
    {
        return bufferType == BufferType.OFF_HEAP;
    }

    @Override
    public Set<String> supportedOptions()
    {
        return Set.of("max_compression_level", "min_compression_level");
    }

    private static int getMinCompressionLevel(Uses mode, Map<String, String> options)
    {
        int defaultValue = mode == Uses.FAST_COMPRESSION ? DEFAULT_MIN_FAST_COMPRESSION_LEVEL : DEFAULT_MIN_GENERAL_COMPRESSION_LEVEL;
        return getIntOption(options, MIN_COMPRESSION_LEVEL_OPTION_NAME, defaultValue);
    }

    private static int getMaxCompressionLevel(Uses mode, Map<String, String> options)
    {
        var defaultValue = mode == Uses.FAST_COMPRESSION ? DEFAULT_MAX_FAST_COMPRESSION_LEVEL : DEFAULT_MAX_GENERAL_COMPRESSION_LEVEL;
        return getIntOption(options, MAX_COMPRESSION_LEVEL_OPTION_NAME, defaultValue);
    }

    private static int getMaxCompactionQueueLength(Map<String, String> options)
    {
        return getIntOption(options, MAX_COMPACTION_QUEUE_LENGTH_OPTION_NAME, DEFAULT_MAX_COMPACTION_QUEUE_LENGTH);
    }

    private static int getIntOption(Map<String, String> options, String key, int defaultValue)
    {
        if (options == null)
            return defaultValue;

        String val = options.get(key);
        if (val == null)
            return defaultValue;

        return Integer.parseInt(val);
    }

    private double getWritePressure()
    {
        return writePressureSupplier.get();
    }

    private static double getFlushPressure()
    {
        var memoryPool = AbstractAllocatorMemtable.MEMORY_POOL;
        var usedRatio = Math.max(memoryPool.onHeap.usedRatio(), memoryPool.offHeap.usedRatio());
        var cleanupThreshold = DatabaseDescriptor.getMemtableCleanupThreshold();
        // we max out the pressure when we're halfway between the cleanupThreshold and max memory
        // so we still have some memory left while compression already working at max speed;
        // setting the compressor to maximum speed when we exhausted all memory would be too late
        return Math.min(1.0, Math.max(0.0, 2 * (usedRatio - cleanupThreshold)) / (1.0 - cleanupThreshold));
    }

    private static double getCompactionPressure(int maxCompactionQueueLength)
    {
        CompactionManager compactionManager = CompactionManager.instance;
        long rateLimit = DatabaseDescriptor.getCompactionThroughputMebibytesPerSecAsInt() * FileUtils.ONE_MIB;
        if (rateLimit == 0)
            rateLimit = Long.MAX_VALUE;
        double actualRate = compactionManager.getMetrics().bytesCompactedThroughput.getOneMinuteRate();
        // We don't want to speed up compression if we can keep up with the configured compression rate limit
        // 0.0 if actualRate >= rateLimit
        // 1.0 if actualRate <= 0.8 * rateLimit;
        double rateLimitFactor = Math.min(1.0, Math.max(0.0, (rateLimit - actualRate) / (0.2 * rateLimit)));

        long pendingCompactions = compactionManager.getPendingTasks();
        long activeCompactions = compactionManager.getActiveCompactions();
        long queuedCompactions = pendingCompactions - activeCompactions;
        double compactionQueuePressure = Math.min(1.0, (double) queuedCompactions / (maxCompactionQueueLength * DatabaseDescriptor.getConcurrentCompactors()));
        return compactionQueuePressure * rateLimitFactor;
    }

    private int clampCompressionLevel(long compressionLevel)
    {
        return (int) Math.min(params.maxCompressionLevel, Math.max(params.minCompressionLevel, compressionLevel));
    }

    @VisibleForTesting
    State getThreadLocalState()
    {
        State state = this.state.get();
        if (state == null)
        {
            state = new State();
            state.currentCompressionLevel = params.maxCompressionLevel;
            state.lastCompressionDuration = 0;
            this.state.set(state);
        }
        return state;
    }

    static class Metrics
    {
        private final Counter[] compressionLevelHistogram;  // separate counters for each compression level
        private final Histogram relativeTimeSpentCompressing; // in % (i.e. multiplied by 100 becaue Histogram can only keep integers)
        private final Meter compressionRate;
        private final Meter decompressionRate;


        Metrics(Uses use)
        {
            MetricNameFactory factory = new DefaultNameFactory("AdaptiveCompression");

            // cannot use Metrics.histogram for compression levels, because histograms do not handle negative numbers;
            // also this histogram is small enough that storing all buckets is not a problem, but it gives
            // much more information
            compressionLevelHistogram = new Counter[MAX_COMPRESSION_LEVEL + 1];
            for (int i = 0; i < compressionLevelHistogram.length; i++)
            {
                CassandraMetricsRegistry.MetricName metricName = factory.createMetricName(String.format("CompressionLevel_%s_%02d", use.name(), i));
                compressionLevelHistogram[i] = CassandraMetricsRegistry.Metrics.counter(metricName);
            }

            relativeTimeSpentCompressing = CassandraMetricsRegistry.Metrics.histogram(factory.createMetricName("RelativeTimeSpentCompressing_" + use.name()), true);

            compressionRate = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("CompressionRate_" + use.name()));
            decompressionRate = CassandraMetricsRegistry.Metrics.meter(factory.createMetricName("DecompressionRate_" + use.name()));
        }

        void updateFrom(State state)
        {
            compressionLevelHistogram[state.currentCompressionLevel].inc();
            relativeTimeSpentCompressing.update((int)(state.getRelativeTimeSpentCompressing() * 100.0));
        }
    }
}
