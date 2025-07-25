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

package org.apache.cassandra.db.compaction.unified;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.CompactionAggregate;
import org.apache.cassandra.db.compaction.CompactionPick;
import org.apache.cassandra.db.compaction.CompactionRealm;
import org.apache.cassandra.db.compaction.CompactionSSTable;
import org.apache.cassandra.db.compaction.CompactionStrategy;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileWriter;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.MetricNameFactory;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.Overlaps;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import static org.apache.cassandra.db.compaction.unified.DSECompatibilityUtils.getBooleanSystemProperty;
import static org.apache.cassandra.db.compaction.unified.DSECompatibilityUtils.getSystemProperty;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
* The controller provides compaction parameters to the unified compaction strategy
*/
public abstract class Controller
{
    protected static final Logger logger = LoggerFactory.getLogger(Controller.class);
    private static final ConcurrentMap<TableMetadata, Controller.Metrics> allMetrics = new ConcurrentHashMap<>();

    static final String PREFIX = "unified_compaction.";
    static final String LEGACY_PREFIX = "dse.unified_compaction.";
    static final String VECTOR_PREFIX = "vector_";

    /**
     * The data size in GB, it will be assumed that the node will have on disk roughly this size of data when it
     * reaches equilibrium. The default is calculated by looking at the free space on all data directories, adjusting
     * for ones belonging to the same drive.
     */
    public static final String DATASET_SIZE_OPTION = "dataset_size";
    @Deprecated
    public static final String DATASET_SIZE_OPTION_GB = "dataset_size_in_gb";
    static final long DEFAULT_DATASET_SIZE =
        FBUtilities.parseHumanReadableBytes(getSystemProperty(DATASET_SIZE_OPTION,
                                                              DatabaseDescriptor.getDataFileDirectoriesMinTotalSpaceInGB() + "GiB"));

    /**
     * The number of shards. This is the main configuration option for UCS V1 (i.e. before the density/overlap
     * improvements). If the value is set, the strategy will switch to V1 mode which entails:
     * <ul>
     * <li>base_shard_count = num_shards
     * <li>sstable_growth = 1 (i.e. always use the same number of shards)
     * <li>min_sstable_size = auto (i.e. set from the size of first flush)
     * <li>reserved_threads_per_level = max
     * </ul>
     * The option is undefined by default to engage the density version of UCS.
     */
    @Deprecated
    static final String NUM_SHARDS_OPTION = "num_shards";
    static final String L0_SHARDS_ENABLED_OPTION = "l0_shards_enabled";


    /**
     * The default number of shards defined via system property, see {@link #NUM_SHARDS_OPTION}.
     * The property exists for backward compatibility, and is deprecated. It allows for configuring compactors, writers
     * and replayers in CNDB without having to change the schema for each tenant.
     */
    @Deprecated
    static final Optional<Integer> DEFAULT_NUM_SHARDS = Optional.ofNullable(getSystemProperty(NUM_SHARDS_OPTION)).map(Integer::valueOf);

    /**
     * The minimum sstable size. Sharded writers split sstables over shard only if they are at least as large as the
     * minimum size.
     * <p>
     * This is mainly present to support UCS V1 mode, which relies heavily on minimal SSTable
     * size, and defaults to 0 which provides minimal parallelism on all levels of the hierarchy.
     * In UCS V1 mode (engaged by using "num_shards" above) the default 'auto'.
     */
    static final String MIN_SSTABLE_SIZE_OPTION = "min_sstable_size";
    @Deprecated
    static final String MIN_SSTABLE_SIZE_OPTION_MB = "min_sstable_size_in_mb";
    static final String MIN_SSTABLE_SIZE_OPTION_AUTO = "auto";

    static final long DEFAULT_MIN_SSTABLE_SIZE = FBUtilities.parseHumanReadableBytes(getSystemProperty(MIN_SSTABLE_SIZE_OPTION, "100MiB"));
    static final long DEFAULT_VECTOR_MIN_SSTABLE_SIZE = FBUtilities.parseHumanReadableBytes(getSystemProperty(VECTOR_PREFIX + MIN_SSTABLE_SIZE_OPTION, "1024MiB"));
    /**
     * Value to use to set the min sstable size from the flush size.
     */
    static final long MIN_SSTABLE_SIZE_AUTO = -1;

    /**
     * Override for the flush size in MB. The database should be able to calculate this from executing flushes, this
     * should only be necessary in rare cases.
     */
    static final String FLUSH_SIZE_OVERRIDE_OPTION = "flush_size_override";
    @Deprecated
    static final String FLUSH_SIZE_OVERRIDE_OPTION_MB = "flush_size_override_mb";

    /**
     * The maximum tolerable compaction-induced space amplification, as fraction of the dataset size. The idea behind
     * this property is to be able to tune how much to limit concurrent "oversized" compactions in different shards.
     * On one hand allowing such compactions concurrently running in all shards allows for STCS-like space
     * amplification, where at some point you might need free space double the size of your working set to do a (top
     * tier) compaction, while on the other hand limiting such compactions too much might lead to compaction lagging
     * behind, higher read amplification, and other problems of that nature.
     */
    public static final String MAX_SPACE_OVERHEAD_OPTION = "max_space_overhead";
    static final double DEFAULT_MAX_SPACE_OVERHEAD = FBUtilities.parsePercent(getSystemProperty(MAX_SPACE_OVERHEAD_OPTION, "0.2"));
    static final double MAX_SPACE_OVERHEAD_LOWER_BOUND = 0.01;
    static final double MAX_SPACE_OVERHEAD_UPPER_BOUND = 1.0;

    static final String BASE_SHARD_COUNT_OPTION = "base_shard_count";
    /**
     * Default base shard count, used when a base count is not explicitly supplied. This value applies to all tables as
     * long as they are larger than the minimum sstable size.
     *
     * For others a base count of 1 is used as system tables are usually small and do not need as much compaction
     * parallelism, while having directories defined provides for parallelism in a different way.
     */
    public static final int DEFAULT_BASE_SHARD_COUNT = Integer.parseInt(getSystemProperty(BASE_SHARD_COUNT_OPTION, "4"));
    public static final int DEFAULT_VECTOR_BASE_SHARD_COUNT = Integer.parseInt(getSystemProperty(VECTOR_PREFIX + BASE_SHARD_COUNT_OPTION, "1"));

    /**
     * The target SSTable size. This is the size of the SSTables that the controller will try to create.
     */
    static final String TARGET_SSTABLE_SIZE_OPTION = "target_sstable_size";
    public static final long DEFAULT_TARGET_SSTABLE_SIZE = FBUtilities.parseHumanReadableBytes(getSystemProperty(TARGET_SSTABLE_SIZE_OPTION, "1GiB"));
    public static final long DEFAULT_VECTOR_TARGET_SSTABLE_SIZE = FBUtilities.parseHumanReadableBytes(getSystemProperty(VECTOR_PREFIX + TARGET_SSTABLE_SIZE_OPTION, "5GiB"));
    static final long MIN_TARGET_SSTABLE_SIZE = 1L << 20;

    static final String IS_REPLICA_AWARE_OPTION = "is_replica_aware";
    public static final boolean DEFAULT_IS_REPLICA_AWARE = Boolean.parseBoolean(System.getProperty(PREFIX + IS_REPLICA_AWARE_OPTION));

    /**
     * Provision for growth of the constructed SSTables as the size of the data grows. By default the target SSTable
     * size is fixed for all levels. In some scenarios is may be better to reduce the overall number of SSTables when
     * the data size becomes larger to avoid using too much memory and processing for the corresponding structures.
     * The setting enables such control and determines how much we reduce the growth of the number of split points as
     * the data size grows. The number specifies the sstable growth part, and the difference from 1 is the shard count
     * growth component, which is a multiplier applied to the logarithm of the data size, before it is rounded and
     * applied as an exponent in the number of split points. In other words, the given value applies as a negative
     * exponent in the calculation of the number of split points.
     * <p>
     * Using 0 applies no correction to the number of split points, resulting in SSTables close to the
     * target size. Setting this number to 1 will make UCS never split beyong the base shard count. Using 0.5 will
     * make the number of split points a square root of the required number for the target SSTable size, making
     * the number of split points and the size of SSTables grow in lockstep as the density grows. Using
     * 0.333 (the default) makes the sstable growth the cubic root of the density growth, i.e. the sstable size
     * grows with the square root of the growth of the shard count.
     * <p>
     * For example, given a data size of 1TiB on the top density level and 1GiB target size with base shard count of 1,
     * growth 0 would result in 1024 SSTables of ~1GiB each, 0.333 would result in 128 SSTables of ~8 GiB each,
     * 0.5 would yield 32 SSTables of ~32GiB each, and 1 would yield 1 SSTable of 1TiB.
     * <p>
     * Note that this correction only applies after the base shard count is reached, so for the above example with
     * base count of 4, the number of SSTables will be 4 (~256GiB each) for a growth value of 1, 128 (~8GiB each) for
     * a growth value of 0.333, and 64 (~16GiB each) for a growth value of 0.5.
     */
    static final String SSTABLE_GROWTH_OPTION = "sstable_growth";
    static final double DEFAULT_SSTABLE_GROWTH = FBUtilities.parsePercent(getSystemProperty(SSTABLE_GROWTH_OPTION, "0.333"));
    static final double DEFAULT_VECTOR_SSTABLE_GROWTH = FBUtilities.parsePercent(getSystemProperty(VECTOR_PREFIX + SSTABLE_GROWTH_OPTION, "1.0"));

    /**
     * Number of reserved threads to keep for each compaction level. This is used to ensure that there are always
     * threads ready to start processing a level when new data arrives. This is most valuable to prevent large
     * compactions from keeping all threads busy for a long time; with smaller target sizes the overlap-driven
     * preference mechanism should achieve better results.
     * <p>
     * If the number is greater than the number of compaction threads divided by the number of levels rounded down, the
     * latter will apply. Specifying "max" reserves as many threads as possible for each level.
     * <p>
     * The default value is max, all compaction threads are distributed among the levels.
     */
    static final String RESERVED_THREADS_OPTION = "reserved_threads";
    public static final int DEFAULT_RESERVED_THREADS = FBUtilities.parseIntAllowingMax(getSystemProperty(RESERVED_THREADS_OPTION, "max"));
    public static final int DEFAULT_VECTOR_RESERVED_THREADS = FBUtilities.parseIntAllowingMax(getSystemProperty(VECTOR_PREFIX + RESERVED_THREADS_OPTION, "max"));

    /**
     * Reservation type, defining whether reservations can be used by lower levels. If set to `per_level`, the
     * reservations are only used by the specific level. If set to `level_or_below`, the reservations can be used by
     * the specific level as well as any one below it.
     * <p>
     * The default value is `level_or_below`.
     */
    static final String RESERVATIONS_TYPE_OPTION = "reservations_type";
    public static final Reservations.Type DEFAULT_RESERVED_THREADS_TYPE =
        Reservations.Type.valueOf(getSystemProperty(RESERVATIONS_TYPE_OPTION,
                                                     Reservations.Type.LEVEL_OR_BELOW.name()).toUpperCase());

    /**
     * This parameter is intended to modify the shape of the LSM by taking into account the survival ratio of data, for now it is fixed to one.
     */
    static final double DEFAULT_SURVIVAL_FACTOR = Double.parseDouble(getSystemProperty("survival_factor", "1"));
    static final double[] DEFAULT_SURVIVAL_FACTORS = new double[] { DEFAULT_SURVIVAL_FACTOR };

    /**
     * Either true or false. This parameter determines which controller will be used.
     */
    static final String ADAPTIVE_OPTION = "adaptive";
    static final boolean DEFAULT_ADAPTIVE = Boolean.parseBoolean(getSystemProperty(ADAPTIVE_OPTION, "false"));

    /**
     * The maximum number of sstables to compact in one operation.
     *
     * This is expected to be large and never be reached, but compaction going very very late may cause the accumulation
     * of thousands and even tens of thousands of sstables which may cause problems if compacted in one long operation.
     * The default is chosen to be half of the maximum permitted space overhead when the source sstables are of the
     * minimum sstable size.
     *
     * If the fanout factor is larger than the maximum number of sstables, the strategy will ignore the latter.
     */
    static final String MAX_SSTABLES_TO_COMPACT_OPTION = "max_sstables_to_compact";

    static final String ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_OPTION = "unsafe_aggressive_sstable_expiration";
    static final String ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_PROPERTY = Config.PROPERTY_PREFIX + "allow_unsafe_aggressive_sstable_expiration";
    static final boolean ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION = Boolean.parseBoolean(System.getProperty(ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_PROPERTY));
    static final boolean DEFAULT_ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION = false;

    /**
     * This property allows seperate defaults for vector and non-vector tables.  If this property is set to true
     * and the table has a {@link VectorType}, the "vector" defaults are used over the regular defaults.  For instance,
     * "-Dunified_compaction.vector_sstable_growth" will be used over "-Dunified_compaction.sstable_growth".
     */
    static final String OVERRIDE_UCS_CONFIG_FOR_VECTOR_TABLES_PROPERTY = PREFIX + "override_ucs_config_for_vector_tables";
    static final boolean OVERRIDE_UCS_CONFIG_FOR_VECTOR_TABLES = Boolean.parseBoolean(System.getProperty(OVERRIDE_UCS_CONFIG_FOR_VECTOR_TABLES_PROPERTY, "false"));

    static final int DEFAULT_EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS = 60 * 10;
    static final String EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_OPTION = "expired_sstable_check_frequency_seconds";

    /**
     * True if L0 data may be coming from different replicas.
     */
    public static final String SHARED_STORAGE = "shared_storage";

    /** The maximum exponent for shard splitting. The maximum number of shards is this number the base count shifted this many times left. */
    static final int MAX_SHARD_SHIFT = 20;
    /** The maximum splitting factor for shards. The maximum number of shards is this number multiplied by the base count. */
    static final double MAX_SHARD_SPLIT = Math.scalb(1, MAX_SHARD_SHIFT);

    private static final double INVERSE_LOG_2 = 1.0 / Math.log(2);
    private static final double INVERSE_SQRT_2 = Math.sqrt(0.5);

    /**
     * Overlap inclusion method. NONE for participating sstables only (not recommended), SINGLE to only include sstables
     * that overlap with participating (LCS-like, higher concurrency during upgrades but some double compaction),
     * TRANSITIVE to include overlaps of overlaps (likely to trigger whole level compactions, safest).
     */
    static final String OVERLAP_INCLUSION_METHOD_OPTION = "overlap_inclusion_method";
    static final Overlaps.InclusionMethod DEFAULT_OVERLAP_INCLUSION_METHOD =
        Overlaps.InclusionMethod.valueOf(getSystemProperty(OVERLAP_INCLUSION_METHOD_OPTION,
                                                          Overlaps.InclusionMethod.TRANSITIVE.toString()).toUpperCase());

    /**
     * Whether to create subtask for the output shards of individual compactions and execute them in parallel.
     * Defaults to true for improved parallelization and efficiency.
     */
    static final String PARALLELIZE_OUTPUT_SHARDS_OPTION = "parallelize_output_shards";
    static final boolean DEFAULT_PARALLELIZE_OUTPUT_SHARDS =
        Boolean.parseBoolean(getSystemProperty(PARALLELIZE_OUTPUT_SHARDS_OPTION,
                                               "true"));

    /**
     * The scaling parameters W, one per bucket index and separated by a comma.
     * Higher indexes will use the value of the last index with a W specified.
     */
    static final String SCALING_PARAMETERS_OPTION = "scaling_parameters";
    @Deprecated
    static final String STATIC_SCALING_FACTORS_OPTION = "static_scaling_factors";

    protected final MonotonicClock clock;
    protected final Environment env;
    protected final double[] survivalFactors;
    protected final long dataSetSize;
    protected volatile long minSSTableSize;
    protected final double maxSpaceOverhead;
    protected final long flushSizeOverride;
    protected volatile long currentFlushSize;
    protected final int maxSSTablesToCompact;
    protected final long expiredSSTableCheckFrequency;
    protected final boolean ignoreOverlapsInExpirationCheck;
    protected final boolean parallelizeOutputShards;
    protected final TableMetadata metadata;

    protected final int baseShardCount;
    private final boolean isReplicaAware;

    protected final long targetSSTableSize;
    protected final double sstableGrowthModifier;

    protected final int reservedThreads;
    protected final Reservations.Type reservationsType;

    @Nullable protected volatile CostsCalculator calculator;
    @Nullable private volatile Metrics metrics;

    protected final Overlaps.InclusionMethod overlapInclusionMethod;

    final boolean l0ShardsEnabled;
    final boolean hasVectorType;

    Controller(MonotonicClock clock,
               Environment env,
               double[] survivalFactors,
               long dataSetSize,
               long minSSTableSize,
               long flushSizeOverride,
               long currentFlushSize,
               double maxSpaceOverhead,
               int maxSSTablesToCompact,
               long expiredSSTableCheckFrequency,
               boolean ignoreOverlapsInExpirationCheck,
               int baseShardCount,
               boolean isReplicaAware,
               long targetSStableSize,
               double sstableGrowthModifier,
               int reservedThreads,
               Reservations.Type reservationsType,
               Overlaps.InclusionMethod overlapInclusionMethod,
               boolean parallelizeOutputShards,
               boolean hasVectorType,
               TableMetadata metadata)
    {
        this.clock = clock;
        this.env = env;
        this.survivalFactors = survivalFactors;
        this.dataSetSize = dataSetSize;
        this.minSSTableSize = minSSTableSize;
        this.flushSizeOverride = flushSizeOverride;
        this.currentFlushSize = currentFlushSize;
        this.expiredSSTableCheckFrequency = TimeUnit.MILLISECONDS.convert(expiredSSTableCheckFrequency, TimeUnit.SECONDS);
        this.baseShardCount = baseShardCount;
        this.isReplicaAware = isReplicaAware;
        this.targetSSTableSize = targetSStableSize;
        this.overlapInclusionMethod = overlapInclusionMethod;
        this.sstableGrowthModifier = sstableGrowthModifier;
        this.reservedThreads = reservedThreads;
        this.reservationsType = reservationsType;
        this.maxSpaceOverhead = maxSpaceOverhead;
        this.l0ShardsEnabled = Boolean.parseBoolean(getSystemProperty(L0_SHARDS_ENABLED_OPTION, "false")); // FIXME VECTOR-23
        this.parallelizeOutputShards = parallelizeOutputShards;
        this.hasVectorType = hasVectorType;
        this.metadata = metadata;

        if (maxSSTablesToCompact <= 0)  // use half the maximum permitted compaction size as upper bound by default
            maxSSTablesToCompact = (int) (dataSetSize * this.maxSpaceOverhead * 0.5 / getMinSstableSizeBytes());

        this.maxSSTablesToCompact = maxSSTablesToCompact;

        if (ignoreOverlapsInExpirationCheck && !ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION)
        {
            logger.warn("Not enabling aggressive SSTable expiration, as the system property '" + ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_PROPERTY + "' is set to 'false'. " +
                    "Set it to 'true' to enable aggressive SSTable expiration.");
        }
        this.ignoreOverlapsInExpirationCheck = ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION && ignoreOverlapsInExpirationCheck;
    }

    public static File getControllerConfigPath(TableMetadata metadata)
    {
        String suffix = "-controller-config.JSON";
        String fileName = metadata.keyspace + '.' + metadata.name + suffix;
        if (fileName.length() > 255)
        {
            int spaceLeft = 255 - suffix.length() - 36 - 2; // 36 is the length of a UUID, 2 - for two separators
            String keyspaceAbbrev = metadata.keyspace.substring(0, Math.min(metadata.keyspace.length(), spaceLeft / 2));
            spaceLeft -= keyspaceAbbrev.length();
            String tableAbbrev = metadata.name.substring(0, Math.min(metadata.name.length(), spaceLeft));
            fileName = String.format("%s.%s.%s%s", keyspaceAbbrev, tableAbbrev, metadata.id.toHexString(), suffix);
        }
        return new File(DatabaseDescriptor.getMetadataDirectory(), fileName);
    }

    public static void storeOptions(TableMetadata metadata, int[] scalingParameters, long flushSizeBytes)
    {
        if (SchemaConstants.isSystemKeyspace(metadata.keyspace))
            return;
        File f = getControllerConfigPath(metadata);
        try(FileWriter fileWriter = new FileWriter(f, File.WriteMode.OVERWRITE);)
        {
            JSONArray jsonArray = new JSONArray();
            JSONObject jsonObject = new JSONObject();
            for (int i = 0; i < scalingParameters.length; i++)
            {
                jsonArray.add(scalingParameters[i]);
            }
            jsonObject.put("scaling_parameters", jsonArray);
            jsonObject.put("current_flush_size", flushSizeBytes);
            fileWriter.write(jsonObject.toString());
            fileWriter.flush();

            logger.debug(String.format("Writing current scaling parameters and flush size to file %s: %s", f.toPath().toString(), jsonObject));
        }
        catch (IOException | FSError e)
        {
            logger.warn("Unable to save current scaling parameters and flush size. Current controller configuration will be lost if a node restarts: ", e);
        }
        catch (Throwable e)
        {
            logger.warn("Unable to save current scaling parameters and flush size. Current controller configuration will be lost if a node restarts: ", e);
            JVMStabilityInspector.inspectThrowable(e);
        }
    }

    public abstract void storeControllerConfig();

    @VisibleForTesting
    public Environment getEnv()
    {
        return env;
    }

    /**
     * @return the scaling parameter W
     * @param index
     */
    public abstract int getScalingParameter(int index);

    public abstract int getPreviousScalingParameter(int index);

    public abstract int getMaxRecentAdaptiveCompactions();
    public abstract boolean isRecentAdaptive(CompactionPick pick);

    public int getFanout(int index) {
        return UnifiedCompactionStrategy.fanoutFromScalingParameter(getScalingParameter(index));
    }

    public int getThreshold(int index) {
        return UnifiedCompactionStrategy.thresholdFromScalingParameter(getScalingParameter(index));
    }

    public int getPreviousFanout(int index) {
        return UnifiedCompactionStrategy.fanoutFromScalingParameter(getPreviousScalingParameter(index));
    }

    public int getPreviousThreshold(int index) {
        return UnifiedCompactionStrategy.thresholdFromScalingParameter(getPreviousScalingParameter(index));
    }

    public int getFlushShards(double density)
    {
        return areL0ShardsEnabled() ? getNumShards(density) : 1;
    }

    /**
     * Calculate the number of shards to split the local token space in for the given sstable density.
     * This is calculated as a power-of-two multiple of baseShardCount, so that the expected size of resulting sstables
     * is between sqrt(0.5) and sqrt(2) times the target size, which is calculated from targetSSTableSize to grow
     * at the given sstableGrowthModifier of the exponential growth of the density.
     * <p>
     * Additionally, if a minimum sstable size is set, we can go below the baseShardCount when that would result in
     * sstables smaller than that minimum. Note that in the case of a non-power-of-two base count this will cause
     * smaller sstables to not be aligned with the ones whose size is enough for the base count.
     * <p>
     * Note that to get the sstables resulting from this splitting within the bounds, the density argument must be
     * normalized to the span that is being split. In other words, if no disks are defined, the density should be
     * scaled by the token coverage of the locally-owned ranges. If multiple data directories are defined, the density
     * should be scaled by the token coverage of the respective data directory. That is, localDensity = size / span,
     * where the span is normalized so that span = 1 when the data covers the range that is being split.
     */
    public int getNumShards(double localDensity)
    {
        int shards;
        // Check the minimum size first.
        long minSize = getMinSstableSizeBytes();
        if (minSize > 0)
        {
            double count = localDensity / minSize;
            // Minimum size only applies if the base count would result in smaller sstables.
            // We also want to use the min size if we don't yet know the flush size (density is NaN).
            // Note: the minimum size cannot be larger than the target size's minimum.
            if (!(count >= baseShardCount)) // also true for count == NaN
            {
                // Make it a power of two, rounding down so that sstables are greater in size than the min.
                // Setting the bottom bit to 1 ensures the result is at least 1.
                // If baseShardCount is not a power of 2, split only to powers of two that are divisors of baseShardCount so boundaries match higher levels
                shards = Math.min(Integer.highestOneBit((int) count | 1), baseShardCount & -baseShardCount);
                if (logger.isDebugEnabled())
                    logger.debug("Shard count {} for density {}, {} times min size {}",
                                 shards,
                                 FBUtilities.prettyPrintBinary(localDensity, "B", " "),
                                 localDensity / minSize,
                                 FBUtilities.prettyPrintBinary(minSize, "B", " "));

                return shards;
            }
        }

        if (sstableGrowthModifier == 1)
        {
            shards = baseShardCount;
            logger.debug("Shard count {} for density {} in fixed shards mode. SStableGrowthModifier {}",
                         shards,
                         FBUtilities.prettyPrintBinary(localDensity, "B", " "),
                         sstableGrowthModifier);
            return shards;
        }
        else if (sstableGrowthModifier == 0)
        {
            // How many we would have to aim for the target size. Divided by the base shard count, so that we can ensure
            // the result is a multiple of it by multiplying back below. Adjusted by sqrt(0.5) to calculate the split
            // points needed for the minimum size.
            double count = localDensity / (targetSSTableSize * INVERSE_SQRT_2 * baseShardCount);
            if (count > MAX_SHARD_SPLIT)
                count = MAX_SHARD_SPLIT;
            assert !(count < 0);    // Must be positive, 0 or NaN, which should translate to baseShardCount

            // Make it a power of two multiple of the base count so that split points for lower levels remain split points for higher.
            // The conversion to int and highestOneBit round down, for which we compensate by using the sqrt(0.5) multiplier
            // already applied in the count.
            // Setting the bottom bit to 1 ensures the result is at least baseShardCount.
            shards = baseShardCount * Integer.highestOneBit((int) count | 1);

            if (logger.isDebugEnabled())
                logger.debug("Shard count {} for density {}, {} times target {}. SStableGrowthModifier {}",
                             shards,
                             FBUtilities.prettyPrintBinary(localDensity, "B", " "),
                             localDensity / targetSSTableSize,
                             FBUtilities.prettyPrintBinary(targetSSTableSize, "B", " "),
                             sstableGrowthModifier);
            return shards;
        }
        else
        {
            // How many we would have to aim for the target size. Divided by the base shard count, so that we can ensure
            // the result is a multiple of it by multiplying back below.
            double count = localDensity / (targetSSTableSize * baseShardCount);
            // Take a logarithm of the count (in base 2), and adjust it by the given growth modifier.
            // Adjust by 0.5 to round the exponent so that the result falls between targetSSTableSize * sqrt(0.5) and
            // targetSSTableSize * sqrt(2). Finally, make sure the exponent is at least 0 and not greater than the
            // fixed maximum.
            // Note: This code also works correctly for the special cases of sstableGrowthModifier == 0 and 1,
            //       but the above code avoids the floating point arithmetic for these common cases.
            // Note: We use log instead of getExponent because we also need the non-integer part of the logarithm
            //       in order to apply the growth modifier correctly.
            final double countLog = Math.log(count);
            double pow = countLog * INVERSE_LOG_2 * (1 - sstableGrowthModifier) + 0.5;
            if (pow >= MAX_SHARD_SHIFT)
                shards = baseShardCount << MAX_SHARD_SHIFT;
            else if (pow >= 0)
                shards = baseShardCount << (int) pow;
            else
                shards = baseShardCount;    // this also covers the case of pow == NaN

            if (logger.isDebugEnabled())
            {
                long targetSize = (long) (targetSSTableSize * Math.exp(countLog * sstableGrowthModifier));
                logger.debug("Shard count {} for density {}, {} times target {}. SStableGrowthModifier {}",
                             shards,
                             FBUtilities.prettyPrintBinary(localDensity, "B", " "),
                             localDensity / targetSize,
                             FBUtilities.prettyPrintBinary(targetSize, "B", " "),
                             sstableGrowthModifier);
            }
            return shards;
        }
    }

    public boolean parallelizeOutputShards()
    {
        return parallelizeOutputShards;
    }

    public boolean isReplicaAware()
    {
        return isReplicaAware;
    }

    /**
     * @return whether L0 should use shards
     */
    public boolean areL0ShardsEnabled()
    {
        return l0ShardsEnabled;
    }

    /**
     * @return the survival factor o
     * @param index
     */
    public double getSurvivalFactor(int index)
    {
        if (index < 0)
            throw new IllegalArgumentException("Index should be >= 0: " + index);

        return index < survivalFactors.length ? survivalFactors[index] : survivalFactors[survivalFactors.length - 1];
    }

    /**
     * The user specified dataset size.
     *
     * @return the target size of the entire data set, in bytes.
     */
    public long getDataSetSizeBytes()
    {
        return dataSetSize;
    }

    public long getTargetSSTableSize()
    {
        return targetSSTableSize;
    }

    /**
     * Return the sstable size in bytes.
     *
     * This is either set by the user in the options or calculated by rounding up the first flush size to 50 MB.
     *
     * @return the minimum sstable size in bytes.
     */
    public long getMinSstableSizeBytes()
    {
        if (minSSTableSize >= 0)
            return minSSTableSize;

        synchronized (this)
        {
            if (minSSTableSize >= 0)
                return minSSTableSize;

            // round the avg flush size to the nearest byte
            long envFlushSize = Math.round(env.flushSize());
            long fiftyMB = 50 << 20;

            // round up to 50 MB
            long flushSize = ((Math.max(1, envFlushSize) + fiftyMB - 1) / fiftyMB) * fiftyMB;

            // If the env flush size is positive, then we've flushed at least once and we use this value permanently
            if (envFlushSize > 0)
            {
                // When a target size is specified, the minimum cannot be higher than the lower bound for that target size.
                flushSize = Math.min(flushSize, (long) (targetSSTableSize * INVERSE_SQRT_2));
                minSSTableSize = flushSize;
            }

            return flushSize;
        }
    }

    /**
     * Return the flush sstable size in bytes.
     *
     * This is usually obtained from the observed sstable flush sizes, refreshed when it differs significantly
     * from the current values.
     * It can also be set by the user in the options.
     *
     * @return the flush size in bytes.
     */
    public long getFlushSizeBytes()
    {
        if (flushSizeOverride > 0)
            return flushSizeOverride;

        double envFlushSize = env.flushSize();
        if (currentFlushSize == 0 || Math.abs(1 - (currentFlushSize / envFlushSize)) > 0.5)
        {
            // The current size is not initialized, or it differs by over 50% from the observed.
            // Use the observed size rounded up to a whole megabyte.
            currentFlushSize = ((long) (Math.ceil(Math.scalb(envFlushSize, -20)))) << 20;
        }
        return currentFlushSize;
    }

    /**
     * Returns the maximum tolerable compaction-induced space amplification, as a fraction of the dataset size.
     * Currently this is not a strict limit for which compaction gives an ironclad guarantee never to exceed it, but
     * the main input in a simple heuristic that is designed to limit UCS' space amplification in exchange of some
     * delay in top bucket compactions.
     *
     * @return a {@code double} value between 0.01 and 1.0, representing the fraction of the expected uncompacted
     * dataset size that should be additionally available for compaction's space amplification overhead.
     */
    public double getMaxSpaceOverhead()
    {
        return maxSpaceOverhead;
    }

    /**
     * Returns the number of reserved threads per level. If the size of SSTables is small, this can be 0 as operations
     * finish quickly and the prioritization will do a good job of assigning threads to the levels. If the size of
     * SSTables can grow large, threads must be reserved to ensure that compactions, esp. on level 0, do not have to
     * wait for long operations to complete.
     */
    public int getReservedThreads()
    {
        return reservedThreads;
    }

    public Reservations.Type getReservationsType()
    {
        return reservationsType;
    }

    /**
     * @return whether is allowed to drop expired SSTables without checking if partition keys appear in other SSTables.
     * Same behavior as in TWCS.
     */
    public boolean getIgnoreOverlapsInExpirationCheck()
    {
        return ignoreOverlapsInExpirationCheck;
    }

    public long getExpiredSSTableCheckFrequency()
    {
        return expiredSSTableCheckFrequency;
    }

    /**
     * Perform any initialization that requires the strategy.
     */
    public void startup(UnifiedCompactionStrategy strategy, ScheduledExecutorService executorService)
    {
        if (calculator != null)
            throw new IllegalStateException("Already started");

        startup(strategy, new CostsCalculator(env, strategy, executorService));
    }

    @VisibleForTesting
    void startup(UnifiedCompactionStrategy strategy, CostsCalculator calculator)
    {
        this.calculator = calculator;
        metrics = allMetrics.computeIfAbsent(strategy.getMetadata(), Controller.Metrics::new);
        metrics.setController(this);
        logger.debug("Started compaction {}", this);
    }

    /**
     * Signals that the strategy is about to be deleted or stopped.
     */
    public void shutdown()
    {
        if (calculator == null)
            return;

        calculator.close();
        calculator = null;

        if (metrics != null)
        {
            metrics.release();
            metrics.removeController();
            metrics = null;
        }

        logger.debug("Stopped compaction controller {}", this);
    }

    public boolean hasVectorType()
    {
        return hasVectorType;
    }

    /**
     * @return true if the controller is running
     */
    public boolean isRunning()
    {
        return calculator != null;
    }

    /**
     * @return the cost calculator, will be null until {@link this#startup(UnifiedCompactionStrategy, ScheduledExecutorService)} is called.
     */
    @Nullable
    @VisibleForTesting
    public CostsCalculator getCalculator()
    {
        return calculator;
    }

    /**
     * The strategy will call this method each time {@link CompactionStrategy#getNextBackgroundTasks(int)} is called.
     */
    public void onStrategyBackgroundTaskRequest()
    {
    }

    /**
     * Calculate the read amplification assuming a single scaling parameter W and a given total
     * length of data on disk.
     *
     * @param length the total length on disk
     * @param scalingParameter the scaling parameter to use for the calculation
     *
     * @return the read amplification of all the buckets needed to cover the total length
     */
    public int readAmplification(long length, int scalingParameter)
    {
        double o = getSurvivalFactor(0);
        long m = getFlushSizeBytes();

        int F = UnifiedCompactionStrategy.fanoutFromScalingParameter(scalingParameter);
        int T = UnifiedCompactionStrategy.thresholdFromScalingParameter(scalingParameter);
        int maxIndex = maxBucketIndex(length, F);

        int ret = 0;
        for (int i = 0; i < maxIndex; i++)
            ret += T - 1;

        if (scalingParameter >= 0)
            ret += Math.max(0, Math.ceil(length / (m * Math.pow(o * F, maxIndex))) - 1);
        else
            ret += 1;

        return ret;
    }

    /**
     * Calculate the write amplification assuming a single scaling parameter W and a given total
     * length of data on disk.
     *
     * @param length the total length on disk
     * @param scalingParameter the scaling parameter to use for the calculation
     *
     * @return the write amplification of all the buckets needed to cover the total length
     */
    public int writeAmplification(long length, int scalingParameter)
    {
        double o = getSurvivalFactor(0);
        long m = getFlushSizeBytes();

        int F = UnifiedCompactionStrategy.fanoutFromScalingParameter(scalingParameter);
        int maxIndex = maxBucketIndex(length, F);

        int ret = 0;

        if (scalingParameter >= 0)
        {   // for tiered, at each level the WA is 1. We start at level 0 and end up at level maxIndex so that's a WA of maxIndex.
            ret += maxIndex + 1;
        }
        else
        {   // for leveled, at each level the WA is F - 1 except for the last one, where it's (size / size of previous level) - 1
            // or (size / (m*(o*F)^maxIndex)) - 1
            for (int i = 0; i < maxIndex; i++)
                ret += F - 1;

            ret += Math.max(0, Math.ceil(length / (m * Math.pow(o * F, maxIndex))));
        }

        return ret;
    }

    /**
     * Returns a maximum bucket index for the given data size and fanout.
     */
    private int maxBucketIndex(long totalLength, int fanout)
    {
        double o = getSurvivalFactor(0);
        long m = getFlushSizeBytes();
        return Math.max(0, (int) Math.floor((Math.log(totalLength) - Math.log(m)) / (Math.log(fanout) - Math.log(o))));
    }

    private double getReadIOCost()
    {
        if (calculator == null)
            return 0;

        int scalingParameter = getScalingParameter(0);
        long length = (long) Math.ceil(calculator.spaceUsed());
        return calculator.getReadCostForQueries(readAmplification(length, scalingParameter));
    }

    private double getWriteIOCost()
    {
        if (calculator == null)
            return 0;

        int scalingParameter = getScalingParameter(0);
        long length = (long) Math.ceil(calculator.spaceUsed());
        return calculator.getWriteCostForQueries(writeAmplification(length, scalingParameter));
    }

    public static Controller fromOptions(CompactionRealm realm, Map<String, String> options)
    {
        // Note: These options have been validated, but the defaults are configured with -D options that may be
        // different. We thus may end up with configurations combinations that do not make sense.
        // We will attempt to correct such combinations and issue warnings where possible.

        boolean hasVectorType = realm.metadata().hasVectorType();
        boolean vectorOverride = OVERRIDE_UCS_CONFIG_FOR_VECTOR_TABLES;
        boolean useVectorOptions = hasVectorType && vectorOverride;
        if (logger.isTraceEnabled())
        {
            if (useVectorOptions)
                logger.trace("Using UCS configuration optimized for vector for {}.{}", realm.getKeyspaceName(), realm.getTableName());
            else
                logger.trace("Using non-vector UCS configuration for {}.{}", realm.getKeyspaceName(), realm.getTableName());
        }
        boolean adaptive = options.containsKey(ADAPTIVE_OPTION) ? Boolean.parseBoolean(options.get(ADAPTIVE_OPTION)) : DEFAULT_ADAPTIVE;
        long dataSetSize = getSizeWithAlt(options, DATASET_SIZE_OPTION, DATASET_SIZE_OPTION_GB, 30, DEFAULT_DATASET_SIZE);
        long flushSizeOverride = getSizeWithAlt(options, FLUSH_SIZE_OVERRIDE_OPTION, FLUSH_SIZE_OVERRIDE_OPTION_MB, 20, 0);
        double maxSpaceOverhead = options.containsKey(MAX_SPACE_OVERHEAD_OPTION)
                ? FBUtilities.parsePercent(options.get(MAX_SPACE_OVERHEAD_OPTION))
                : DEFAULT_MAX_SPACE_OVERHEAD;
        int maxSSTablesToCompact = Integer.parseInt(options.getOrDefault(MAX_SSTABLES_TO_COMPACT_OPTION, "32"));
        long expiredSSTableCheckFrequency = options.containsKey(EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_OPTION)
                ? Long.parseLong(options.get(EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_OPTION))
                : DEFAULT_EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS;
        boolean ignoreOverlapsInExpirationCheck = options.containsKey(ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_OPTION)
                ? Boolean.parseBoolean(options.get(ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_OPTION))
                : DEFAULT_ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION;

        int baseShardCount;
        if (options.containsKey(BASE_SHARD_COUNT_OPTION))
        {
            baseShardCount = Integer.parseInt(options.get(BASE_SHARD_COUNT_OPTION));
        }
        else
        {
            baseShardCount = useVectorOptions ? DEFAULT_VECTOR_BASE_SHARD_COUNT : DEFAULT_BASE_SHARD_COUNT;
        }

        boolean isReplicaAware = options.containsKey(IS_REPLICA_AWARE_OPTION)
                                 ? Boolean.parseBoolean(options.get(IS_REPLICA_AWARE_OPTION))
                                 : DEFAULT_IS_REPLICA_AWARE;

        long targetSStableSize = options.containsKey(TARGET_SSTABLE_SIZE_OPTION)
                                 ? FBUtilities.parseHumanReadableBytes(options.get(TARGET_SSTABLE_SIZE_OPTION))
                                 : useVectorOptions ? DEFAULT_VECTOR_TARGET_SSTABLE_SIZE : DEFAULT_TARGET_SSTABLE_SIZE;

        long minSSTableSize;
        if (MIN_SSTABLE_SIZE_OPTION_AUTO.equalsIgnoreCase(options.get(MIN_SSTABLE_SIZE_OPTION)))
            minSSTableSize = MIN_SSTABLE_SIZE_AUTO;
        else
            minSSTableSize = getSizeWithAlt(options,
                                            MIN_SSTABLE_SIZE_OPTION,
                                            MIN_SSTABLE_SIZE_OPTION_MB,
                                            20,
                                            useVectorOptions ? DEFAULT_VECTOR_MIN_SSTABLE_SIZE : DEFAULT_MIN_SSTABLE_SIZE);

        double sstableGrowthModifier = useVectorOptions ? DEFAULT_VECTOR_SSTABLE_GROWTH : DEFAULT_SSTABLE_GROWTH;
        if (options.containsKey(SSTABLE_GROWTH_OPTION))
            sstableGrowthModifier = FBUtilities.parsePercent(options.get(SSTABLE_GROWTH_OPTION));

        int reservedThreadsPerLevel = options.containsKey(RESERVED_THREADS_OPTION)
                                      ? FBUtilities.parseIntAllowingMax(options.get(RESERVED_THREADS_OPTION))
                                      : useVectorOptions ? DEFAULT_VECTOR_RESERVED_THREADS : DEFAULT_RESERVED_THREADS;
        Reservations.Type reservationsType = options.containsKey(RESERVATIONS_TYPE_OPTION)
                                                  ? Reservations.Type.valueOf(options.get(RESERVATIONS_TYPE_OPTION).toUpperCase())
                                                  : DEFAULT_RESERVED_THREADS_TYPE;

        if (options.containsKey(NUM_SHARDS_OPTION) || DEFAULT_NUM_SHARDS.isPresent())
        {
            // Legacy V1 mode is enabled when the number of shards is defined and has a positive value.
            // Table property takes precendence over system property.
            int numShards = options.containsKey(NUM_SHARDS_OPTION)
                            ? Integer.parseInt(options.get(NUM_SHARDS_OPTION))
                            : DEFAULT_NUM_SHARDS.get();

            if (numShards > 0)
            {
                if (!options.containsKey(MIN_SSTABLE_SIZE_OPTION))
                    minSSTableSize = MIN_SSTABLE_SIZE_AUTO;
                baseShardCount = numShards;
                sstableGrowthModifier = 1.0;
                targetSStableSize = Long.MAX_VALUE; // this no longer plays a part, the result of getNumShards before
                // accounting for minimum size is always baseShardCount

                double maxSpaceOverheadLowerBound = 1.0d / numShards;
                if (maxSpaceOverhead < maxSpaceOverheadLowerBound)
                {
                    logger.warn("{} shards are not enough to maintain the required maximum space overhead of {}!\n" +
                                "Falling back to {}={} instead. If this limit needs to be satisfied, please increase the number" +
                                " of shards.",
                                numShards,
                                maxSpaceOverhead,
                                MAX_SPACE_OVERHEAD_OPTION,
                                String.format("%.3f", maxSpaceOverheadLowerBound));
                    maxSpaceOverhead = maxSpaceOverheadLowerBound;
                }
            }
        }

        if (baseShardCount > 1 && sstableGrowthModifier != 1.0 && minSSTableSize != MIN_SSTABLE_SIZE_AUTO && minSSTableSize > targetSStableSize * INVERSE_SQRT_2)
        {
            // Note: not checked for baseShardCount == 1 as min size is irrelevant when the base count is 1.
            // Note: not checked for sstableGrowthModifier = 1.0 as target size is irrelevant when the growth is 1.
            long newTargetSize = (long) (minSSTableSize / INVERSE_SQRT_2);
            logger.warn("Minimum sstable size {} is larger than target sstable size's minimum bound {}. Adjusting target size to {}.",
                        FBUtilities.prettyPrintMemory(minSSTableSize),
                        FBUtilities.prettyPrintMemory((long) (targetSStableSize * INVERSE_SQRT_2)),
                        FBUtilities.prettyPrintMemory(newTargetSize));
            targetSStableSize = newTargetSize;
        }

        Environment env = realm.makeUCSEnvironment();

        // For remote storage, the sstables on L0 are created by the different replicas, and therefore it is likely
        // that there are RF identical copies, so here we adjust the survival factor for L0
        double[] survivalFactors = getSystemProperty(SHARED_STORAGE) == null || !getBooleanSystemProperty(SHARED_STORAGE)
                                   ? DEFAULT_SURVIVAL_FACTORS
                                   : new double[] { DEFAULT_SURVIVAL_FACTOR / realm.getKeyspaceReplicationStrategy().getReplicationFactor().allReplicas, DEFAULT_SURVIVAL_FACTOR };

        Overlaps.InclusionMethod overlapInclusionMethod = options.containsKey(OVERLAP_INCLUSION_METHOD_OPTION)
                                                          ? Overlaps.InclusionMethod.valueOf(options.get(OVERLAP_INCLUSION_METHOD_OPTION).toUpperCase())
                                                          : DEFAULT_OVERLAP_INCLUSION_METHOD;

        boolean parallelizeOutputShards = options.containsKey(PARALLELIZE_OUTPUT_SHARDS_OPTION)
                                          ? Boolean.parseBoolean(options.get(PARALLELIZE_OUTPUT_SHARDS_OPTION))
                                          : DEFAULT_PARALLELIZE_OUTPUT_SHARDS;

        return adaptive
               ? AdaptiveController.fromOptions(env,
                                                survivalFactors,
                                                dataSetSize,
                                                minSSTableSize,
                                                flushSizeOverride,
                                                maxSpaceOverhead,
                                                maxSSTablesToCompact,
                                                expiredSSTableCheckFrequency,
                                                ignoreOverlapsInExpirationCheck,
                                                baseShardCount,
                                                isReplicaAware,
                                                targetSStableSize,
                                                sstableGrowthModifier,
                                                reservedThreadsPerLevel,
                                                reservationsType,
                                                overlapInclusionMethod,
                                                parallelizeOutputShards,
                                                hasVectorType,
                                                realm.metadata(),
                                                options)
               : StaticController.fromOptions(env,
                                              survivalFactors,
                                              dataSetSize,
                                              minSSTableSize,
                                              flushSizeOverride,
                                              maxSpaceOverhead,
                                              maxSSTablesToCompact,
                                              expiredSSTableCheckFrequency,
                                              ignoreOverlapsInExpirationCheck,
                                              baseShardCount,
                                              isReplicaAware,
                                              targetSStableSize,
                                              sstableGrowthModifier,
                                              reservedThreadsPerLevel,
                                              reservationsType,
                                              overlapInclusionMethod,
                                              parallelizeOutputShards,
                                              hasVectorType,
                                              realm.metadata(),
                                              options,
                                              useVectorOptions);
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        // Note: Validation must ignore the defaults set with -D options, because this node may be getting a configuration
        // applied via a different coordinator which had different -D settings. If we abort because of such differences,
        // we may cause schema mismatches between nodes which can quickly become a serious problem.

        String nonPositiveErr = "Invalid configuration, %s should be positive: %d";
        String intParseErr = "%s is not a parsable int (base10) for %s";
        String longParseErr = "%s is not a parsable long (base10) for %s";
        String floatParseErr = "%s is not a parsable float for %s";
        options = new HashMap<>(options);
        String s;
        long minSSTableSize = -1;
        long targetSSTableSize = -1;

        s = options.remove(NUM_SHARDS_OPTION);
        if (s != null)
        {
            try
            {
                int numShards = Integer.parseInt(s);
                if (numShards <= 0 && numShards != -1)
                    throw new ConfigurationException(String.format("Invalid configuration, %s=%d should be positive, or -1 " +
                                                                   "to explicitly disable static sharding for this table.",
                                                                   NUM_SHARDS_OPTION,
                                                                   numShards));
                if (numShards != -1)
                {
                    List<String> incompatibleOptions = List.of(TARGET_SSTABLE_SIZE_OPTION, SSTABLE_GROWTH_OPTION, BASE_SHARD_COUNT_OPTION);
                    if (incompatibleOptions.stream().anyMatch(options::containsKey))
                    {
                        throw new ConfigurationException(String.format("Option %s cannot be used in combination with %s",
                                                                       NUM_SHARDS_OPTION,
                                                                       incompatibleOptions.stream().filter(options::containsKey).collect(Collectors.joining(", "))));
                    }
                }
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format(intParseErr, s, NUM_SHARDS_OPTION), e);
            }
        }

        boolean adaptive = validateBoolean(options, ADAPTIVE_OPTION, DEFAULT_ADAPTIVE);
        validateBoolean(options, IS_REPLICA_AWARE_OPTION, DEFAULT_IS_REPLICA_AWARE);
        validateBoolean(options, PARALLELIZE_OUTPUT_SHARDS_OPTION, DEFAULT_PARALLELIZE_OUTPUT_SHARDS);

        validateSizeWithAlt(options, FLUSH_SIZE_OVERRIDE_OPTION, FLUSH_SIZE_OVERRIDE_OPTION_MB, 20);
        validateSizeWithAlt(options, DATASET_SIZE_OPTION, DATASET_SIZE_OPTION_GB, 30);

        s = options.remove(MAX_SSTABLES_TO_COMPACT_OPTION);
        if (s != null)
        {
             try
             {
                 Integer.parseInt(s); // values less than or equal to 0 enable the default
             }
             catch (NumberFormatException e)
             {
                 throw new ConfigurationException(String.format(intParseErr,
                                                                s,
                                                                MAX_SSTABLES_TO_COMPACT_OPTION),
                                                  e);
             }
        }
        s = options.remove(EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_OPTION);
        if (s != null)
        {
            try
            {
                long expiredSSTableCheckFrequency = Long.parseLong(s);
                if (expiredSSTableCheckFrequency <= 0)
                    throw new ConfigurationException(String.format(nonPositiveErr,
                                                                   EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_OPTION,
                                                                   expiredSSTableCheckFrequency));
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format(longParseErr,
                                                               s,
                                                               EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_OPTION),
                                                 e);
            }
        }

        s = options.remove(MAX_SPACE_OVERHEAD_OPTION);
        if (s != null)
        {
            try
            {
                double maxSpaceOverhead = FBUtilities.parsePercent(s);
                if (maxSpaceOverhead < MAX_SPACE_OVERHEAD_LOWER_BOUND || maxSpaceOverhead > MAX_SPACE_OVERHEAD_UPPER_BOUND)
                    throw new ConfigurationException(String.format("Invalid configuration, %s must be between %f and %f: %s",
                                                                   MAX_SPACE_OVERHEAD_OPTION,
                                                                   MAX_SPACE_OVERHEAD_LOWER_BOUND,
                                                                   MAX_SPACE_OVERHEAD_UPPER_BOUND,
                                                                   s));
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format(floatParseErr,
                                                               s,
                                                               MAX_SPACE_OVERHEAD_OPTION),
                                                 e);
            }
        }

        validateBoolean(options, ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_OPTION, false);

        s = options.remove(SSTABLE_GROWTH_OPTION);
        if (s != null)
        {
            try
            {
                double ssTableGrowthModifier = FBUtilities.parsePercent(s);
                if (ssTableGrowthModifier < 0 || ssTableGrowthModifier > 1)
                    throw new ConfigurationException(String.format("%s %s must be between 0 and 1",
                                                                   SSTABLE_GROWTH_OPTION,
                                                                   s));
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format("%s is not a valid number between 0 and 1: %s",
                                                               SSTABLE_GROWTH_OPTION,
                                                               e.getMessage()),
                                                 e);
            }
        }

        s = options.remove(BASE_SHARD_COUNT_OPTION);
        if (s != null)
        {
            try
            {
                int baseShardCount = Integer.parseInt(s);
                if (baseShardCount <= 0)
                    throw new ConfigurationException(String.format(nonPositiveErr,
                                                                   BASE_SHARD_COUNT_OPTION,
                                                                   baseShardCount));
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format(intParseErr, s, BASE_SHARD_COUNT_OPTION), e);
            }
        }

        s = options.remove(TARGET_SSTABLE_SIZE_OPTION);
        if (s != null)
        {
            try
            {
                targetSSTableSize = FBUtilities.parseHumanReadableBytes(s);
                if (targetSSTableSize < MIN_TARGET_SSTABLE_SIZE)
                    throw new ConfigurationException(String.format("%s %s is not acceptable, size must be at least %s",
                                                                   TARGET_SSTABLE_SIZE_OPTION,
                                                                   s,
                                                                   FBUtilities.prettyPrintMemory(MIN_TARGET_SSTABLE_SIZE)));
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format("%s is not a valid size in bytes: %s",
                                                               TARGET_SSTABLE_SIZE_OPTION,
                                                               e.getMessage()),
                                                 e);
            }
        }

        minSSTableSize = validateSizeWithAlt(options, MIN_SSTABLE_SIZE_OPTION, MIN_SSTABLE_SIZE_OPTION_MB, 20, MIN_SSTABLE_SIZE_OPTION_AUTO, -1, -1);
        // If both target and min sstable size are defined, check that they are compatible.
        if (minSSTableSize > 0 && targetSSTableSize > 0 && minSSTableSize > targetSSTableSize * INVERSE_SQRT_2)
            throw new ConfigurationException(String.format("The minimum sstable size %s cannot be larger than the target size's lower bound %s.",
                                                           FBUtilities.prettyPrintMemory(minSSTableSize),
                                                           FBUtilities.prettyPrintMemory((long) (targetSSTableSize * INVERSE_SQRT_2))));

        s = options.remove(RESERVED_THREADS_OPTION);
        if (s != null)
        {
            try
            {
                int reservedThreads = FBUtilities.parseIntAllowingMax(s);
                if (reservedThreads < 0)
                    throw new ConfigurationException(String.format("%s %s must be an integer >= 0 or \"max\"",
                                                                   RESERVED_THREADS_OPTION,
                                                                   s));
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format("%s is not a valid integer >= 0 or \"max\": %s",
                                                               RESERVED_THREADS_OPTION,
                                                               e.getMessage()),
                                                 e);
            }
        }

        s = options.remove(RESERVATIONS_TYPE_OPTION);
        if (s != null)
        {
            try
            {
                Reservations.Type.valueOf(s.toUpperCase());
            }
            catch (IllegalArgumentException e)
            {
                throw new ConfigurationException(String.format("Invalid reserved threads type %s. The valid options are %s.",
                                                               s,
                                                               Arrays.toString(Reservations.Type.values())));
            }
        }

        s = options.remove(OVERLAP_INCLUSION_METHOD_OPTION);
        if (s != null)
        {
            try
            {
                Overlaps.InclusionMethod.valueOf(s.toUpperCase());
            }
            catch (IllegalArgumentException e)
            {
                throw new ConfigurationException(String.format("Invalid overlap inclusion method %s. The valid options are %s.",
                                                               s,
                                                               Arrays.toString(Overlaps.InclusionMethod.values())));
            }
        }

        return adaptive ? AdaptiveController.validateOptions(options) : StaticController.validateOptions(options);
    }

    private static long getSizeWithAlt(Map<String, String> options, String optionHumanReadable, String optionAlt, int altShift, long defaultValue)
    {
        if (options.containsKey(optionHumanReadable))
            return FBUtilities.parseHumanReadableBytes(options.get(optionHumanReadable));
        else if (options.containsKey(optionAlt))
            return Long.parseLong(options.get(optionAlt)) << altShift;
        else
            return defaultValue;
    }

    private static boolean validateBoolean(Map<String, String> options, String option, boolean defaultValue) throws ConfigurationException
    {
        var s = options.remove(option);
        if (s != null)
        {
            if (!s.equalsIgnoreCase("true") && !s.equalsIgnoreCase("false"))
                throw new ConfigurationException(String.format("%s should either be 'true' or 'false', not %s", option, s));
            return Boolean.parseBoolean(s);
        }
        return defaultValue;
    }

    private static void validateSizeWithAlt(Map<String, String> options, String optionHumanReadable, String optionAlt, int altShift)
    {
        validateSizeWithAlt(options, optionHumanReadable, optionAlt, altShift, null, 0, 0);
    }

    private static long validateSizeWithAlt(Map<String, String> options, String optionHumanReadable, String optionAlt, int altShift, String specialText, long specialValue, long defaultValue)
    {
        validateOneOf(options, optionHumanReadable, optionAlt);
        long sizeInBytes;
        String s = null;
        String opt = optionHumanReadable;
        try
        {
            s = options.remove(opt);
            if (s != null)
            {
                if (s.equalsIgnoreCase(specialText))
                    return specialValue; // all good
                sizeInBytes = FBUtilities.parseHumanReadableBytes(s);
            }
            else
            {
                opt = optionAlt;
                s = options.remove(opt);
                if (s != null)
                    sizeInBytes = Long.parseLong(s) << altShift;
                else
                    return defaultValue;
            }

        }
        catch (NumberFormatException e)
        {
            if (specialText != null)
                throw new ConfigurationException(String.format("%s must be a valid size in bytes or %s for %s",
                                                               s,
                                                               specialText,
                                                               opt),
                                                 e);
            else
                throw new ConfigurationException(String.format("%s is not a valid size in bytes for %s",
                                                               s,
                                                               opt),
                                                 e);
        }

        if (sizeInBytes < 0)
            throw new ConfigurationException(String.format("Invalid configuration, %s should be positive: %s",
                                                           opt,
                                                           s));
        return sizeInBytes;
    }

    private static void validateOneOf(Map<String, String> options, String option1, String option2)
    {
        if (options.containsKey(option1) && options.containsKey(option2))
        {
            throw new ConfigurationException(String.format("Cannot specify both %s and %s",
                                                           option1,
                                                           option2));
        }
    }

    // The methods below are implemented here (rather than directly in UCS) to aid testability.

    public double getBaseSstableSize(int F)
    {
        // The compaction hierarchy should start at a minimum size which is close to the typical flush size, with
        // some leeway to make sure we don't overcompact when flushes end up a little smaller.
        // The leeway should be less than 1/F, though, to make sure we don't overshoot the boundary combining F-1
        // sources instead of F.
        // Note that while we have not had flushes, the size will be 0 and we will use 1MB as the flush size. With
        // fixed and positive W this should not hurt us, as the hierarchy will be in multiples of F and will still
        // result in the same buckets, but for negative W or hybrid strategies this may cause temporary overcompaction.
        // If this is a concern, the flush size override should be used to avoid it until DB-4401.
        return Math.max(1 << 20, getFlushSizeBytes()) * (1.0 - 0.9 / F);
    }

    public double getMaxLevelDensity(int index, double minSize)
    {
        return Math.floor(minSize * getFanout(index) * getSurvivalFactor(index));
    }

    public double maxThroughput()
    {
        return env.maxThroughput();
    }

    public long getOverheadSizeInBytes(Iterable<? extends CompactionSSTable> sstables, long totalDataSize)
    {
        return env.getOverheadSizeInBytes(sstables, totalDataSize);
    }

    public int maxConcurrentCompactions()
    {
        return env.maxConcurrentCompactions();
    }

    public long maxCompactionSpaceBytes()
    {
        // Note: Compaction will not proceed with operations larger than this size (i.e. it will compact on the lower
        // levels but will accumulate sstables on the top until the space on the drive fills up). This sounds risky but
        // is less of a problem than running out of space during compaction.
        return (long) (getDataSetSizeBytes() * getMaxSpaceOverhead());
    }

    public int maxSSTablesToCompact()
    {
        return maxSSTablesToCompact;
    }

    /**
     * Random number generator to be used for the selection of tasks.
     * Replaced by some tests.
     */
    public Random random()
    {
        return ThreadLocalRandom.current();
    }

    /**
     * Return the overlap inclusion method to use when combining overlap sections into a bucket. For example, with
     * SSTables A(0, 5), B(2, 9), C(6, 12), D(10, 12) whose overlap sections calculation returns [AB, BC, CD],
     *   - NONE means no sections are to be merged. AB, BC and CD will be separate buckets, compactions AB, BC and CD
     *     will be added separately, thus some SSTables will be partially used / single-source compacted, likely
     *     to be recompacted again with the next selected bucket.
     *   - SINGLE means only overlaps of the sstables in the selected bucket will be added. AB+BC will be one bucket,
     *     and CD will be another (as BC is already used). A middle ground of sorts, should reduce overcompaction but
     *     still has some.
     *   - TRANSITIVE means a transitive closure of overlapping sstables will be selected. AB+BC+CD will be in the same
     *     bucket, selected compactions will apply to all overlapping sstables and no overcompaction will be done, at
     *     the cost of reduced compaction parallelism and increased length of the operation.
     * TRANSITIVE is the default and makes most sense. NONE is a closer approximation to operation of legacy UCS.
     * The option is exposed for experimentation.
     */
    public Overlaps.InclusionMethod overlapInclusionMethod()
    {
        return overlapInclusionMethod;
    }

    public static int[] parseScalingParameters(String str)
    {
        String[] vals = str.split(",");
        int[] ret = new int[vals.length];
        for (int i = 0; i < vals.length; i++)
        {
            String value = vals[i].trim();
            int W = UnifiedCompactionStrategy.parseScalingParameter(value);
            ret[i] = W;
        }

        return ret;
    }

    public static String printScalingParameters(int[] parameters)
    {
        StringBuilder builder = new StringBuilder();
        int i;
        for (i = 0; i < parameters.length - 1; ++i)
        {
            builder.append(UnifiedCompactionStrategy.printScalingParameter(parameters[i]));
            builder.append(", ");
        }
        builder.append(UnifiedCompactionStrategy.printScalingParameter(parameters[i]));
        return builder.toString();
    }

    /**
     * Prioritize the given aggregates. Because overlap is the primary measure we aim to control, reducing the max
     * overlap of the aggregates is the primary goal. We do this by sorting the aggregates by max overlap, so that
     * the ones with the highest overlap are chosen first.
     * Among choices with matching overlap, we order randomly to give each level and bucket a good chance to run.
     */
    public List<CompactionAggregate.UnifiedAggregate> prioritize(List<CompactionAggregate.UnifiedAggregate> aggregates)
    {
        // Randomize the list.
        Collections.shuffle(aggregates, random());
        // Sort the array so that aggregates with the highest overlap come first. On ties, prefer lower levels.
        // Because this is a stable sort, entries with the same overlap and level will remain randomly ordered.
        aggregates.sort((a1, a2) -> {
            int cmp = Long.compare(a2.maxOverlap(), a1.maxOverlap());
            if (cmp != 0)
                return cmp;
            else
                return Integer.compare(a1.bucketIndex(), a2.bucketIndex());
        });
        return aggregates;
    }

    static final class Metrics
    {
        private final MetricNameFactory factory;
        private final AtomicReference<Controller> controllerRef;
        private final Gauge<Double> totWAGauge;
        private final Gauge<Double> readIOCostGauge;
        private final Gauge<Double> writeIOCostGauge;
        private final Gauge<Double> totIOCostGauge;

        Metrics(TableMetadata metadata)
        {
            this.factory = new DefaultNameFactory("CompactionCosts",
                                                  String.format("%s.%s", metadata.keyspace, metadata.name));
            this.controllerRef = new AtomicReference<>();
            this.totWAGauge = Metrics.register(factory.createMetricName("WA"), this::getMeasuredWA);
            this.readIOCostGauge = Metrics.register(factory.createMetricName("ReadIOCost"), this::getReadIOCost);
            this.writeIOCostGauge = Metrics.register(factory.createMetricName("WriteIOCost"), this::getWriteIOCost);
            this.totIOCostGauge = Metrics.register(factory.createMetricName("TotIOCost"), this::getTotalIOCost);
        }

        void setController(Controller controller)
        {
            this.controllerRef.set(controller);
        }

        void removeController()
        {
           this.controllerRef.set(null);
        }

        void release()
        {
            Metrics.remove(factory.createMetricName("WA"));
            Metrics.remove(factory.createMetricName("ReadIOCost"));
            Metrics.remove(factory.createMetricName("WriteIOCost"));
            Metrics.remove(factory.createMetricName("TotIOCost"));
        }

        double getMeasuredWA()
        {
            double ret = 0;
            Controller controller = controllerRef.get();
            if (controller != null)
                ret = controller.env.WA();

            return ret;
        }

        double getReadIOCost()
        {
            double ret = 0;
            Controller controller = controllerRef.get();
            if (controller != null)
                ret = controller.getReadIOCost();

            return ret;
        }

        double getWriteIOCost()
        {
            double ret = 0;
            Controller controller = controllerRef.get();
            if (controller != null)
                ret = controller.getWriteIOCost();

            return ret;
        }

        double getTotalIOCost()
        {
            return getReadIOCost() + getWriteIOCost();
        }
    }
}
