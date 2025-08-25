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
package org.apache.cassandra.metrics;

import java.lang.ref.WeakReference;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Timer;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.GaugeProvider;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.metrics.Sampler.SamplerType;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.ExpMovingAverage;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.MovingAverage;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.Refs;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * Metrics for {@link ColumnFamilyStore}.
 */
public class TableMetrics
{
    // CNDB will set this to MetricsAggregation.AGGREGATED in order to support large number of tenants and dedicated
    // tenants with a large number of tables
    public static final String TABLE_METRICS_DEFAULT_HISTOGRAMS_AGGREGATION =
    CassandraRelevantProperties.TABLE_METRICS_DEFAULT_HISTOGRAMS_AGGREGATION.getString();

    // CNDB will set this to false as it does not need global metrics since the aggregation is done in Prometheus
    public static final boolean EXPORT_GLOBAL_METRICS = CassandraRelevantProperties.TABLE_METRICS_EXPORT_GLOBALS.getBoolean();

    private static final Logger logger = LoggerFactory.getLogger(TableMetrics.class);

    public static final String TABLE_EXTENSIONS_HISTOGRAMS_METRICS_KEY = "HISTOGRAM_METRICS";

    public enum MetricsAggregation
    {
        AGGREGATED((byte) 0x00),
        INDIVIDUAL((byte) 0x01);

        public final byte val;

        MetricsAggregation(byte val)
        {
            this.val = val;
        }

        public static MetricsAggregation fromMetadata(TableMetadata metadata)
        {
            MetricsAggregation defaultValue = MetricsAggregation.valueOf(TABLE_METRICS_DEFAULT_HISTOGRAMS_AGGREGATION);
            ByteBuffer bb = null;
            try
            {
                bb = metadata.params.extensions.get(TABLE_EXTENSIONS_HISTOGRAMS_METRICS_KEY);
                return bb == null ? defaultValue : MetricsAggregation.fromByte(bb.get(bb.position())); // do not change the position of the ByteBuffer!
            }
            catch (BufferUnderflowException | IllegalStateException ex)
            {
                logger.error("Failed to decode metadata extensions for metrics aggregation ({}), using default value {}", bb, defaultValue);
                return defaultValue;
            }
        }

        public static MetricsAggregation fromByte(byte val) throws IllegalStateException
        {
            for (MetricsAggregation aggr : values())
            {
                if (aggr.val == val)
                    return aggr;
            }

            throw new IllegalStateException("Invalid byte: " + val);
        }

        public String asCQLString()
        {
            return "0x" + Hex.bytesToHex(val);
        }
    }

    /**
     * stores metrics that will be rolled into a single global metric
     */
    private static final ConcurrentMap<String, Set<Metric>> ALL_TABLE_METRICS = Maps.newConcurrentMap();
    public static final long[] EMPTY = new long[0];
    private static final MetricNameFactory GLOBAL_FACTORY = new AllTableMetricNameFactory("Table");
    private static final MetricNameFactory GLOBAL_ALIAS_FACTORY = new AllTableMetricNameFactory("ColumnFamily");

    public final static Optional<LatencyMetrics> GLOBAL_READ_LATENCY = EXPORT_GLOBAL_METRICS ? Optional.of(new LatencyMetrics(GLOBAL_FACTORY, GLOBAL_ALIAS_FACTORY, "Read")) : Optional.empty();
    public final static Optional<LatencyMetrics> GLOBAL_WRITE_LATENCY = EXPORT_GLOBAL_METRICS ? Optional.of(new LatencyMetrics(GLOBAL_FACTORY, GLOBAL_ALIAS_FACTORY, "Write")) : Optional.empty();
    public final static Optional<LatencyMetrics> GLOBAL_RANGE_LATENCY = EXPORT_GLOBAL_METRICS ? Optional.of(new LatencyMetrics(GLOBAL_FACTORY, GLOBAL_ALIAS_FACTORY, "Range")) : Optional.empty();

    /** Total amount of data stored in the memtable that resides on-heap, including column related overhead and partitions overwritten. */
    public final Gauge<Long> memtableOnHeapDataSize;
    /** Total amount of data stored in the memtable that resides off-heap, including column related overhead and partitions overwritten. */
    public final Gauge<Long> memtableOffHeapDataSize;
    /** Total amount of live data stored in the memtable, excluding any data structure overhead */
    public final Gauge<Long> memtableLiveDataSize;
    /** Total amount of data stored in the memtables (2i and pending flush memtables included) that resides on-heap. */
    public final Gauge<Long> allMemtablesOnHeapDataSize;
    /** Total amount of data stored in the memtables (2i and pending flush memtables included) that resides off-heap. */
    public final Gauge<Long> allMemtablesOffHeapDataSize;
    /** Total amount of live data stored in the memtables (2i and pending flush memtables included) that resides off-heap, excluding any data structure overhead */
    public final Gauge<Long> allMemtablesLiveDataSize;
    /** Total number of columns present in the memtable. */
    public final Gauge<Long> memtableColumnsCount;
    /** Number of times flush has resulted in the memtable being switched out. */
    public final Counter memtableSwitchCount;
    /** Current compression ratio for all SSTables */
    public final Gauge<Double> compressionRatio;
    /** Histogram of estimated partition size (in bytes). */
    public final Gauge<long[]> estimatedPartitionSizeHistogram;
    /** Approximate number of keys in table. */
    public final Gauge<Long> estimatedPartitionCount;
    /** This function is used to calculate estimated partition count in sstables and store the calculated value for the
     *  current set of sstables. */
    public final LongSupplier estimatedPartitionCountInSSTables;
    /** A cached version of the estimated partition count in sstables, used by compaction. This value will be more
     *  precise when the table has a small number of partitions that keep getting written to. */
    public final Gauge<Long> estimatedPartitionCountInSSTablesCached;
    /** Histogram of estimated number of columns. */
    public final Gauge<long[]> estimatedColumnCountHistogram;
    /** Approximate number of rows in SSTable*/
    public final Gauge<Long> estimatedRowCount;
    /** Histogram of the number of sstable data files accessed per single partition read */
    public final TableHistogram sstablesPerReadHistogram;
    /** Histogram of the number of sstable data files accessed per partition range read */
    public final TableHistogram sstablesPerRangeReadHistogram;
    /** An approximate measure of how long it takes to read a partition from an sstable, in nanoseconds. This is
     * a moving average of a very rough approximation: the total latency for a single partition
     * read command divided by the number of sstables that were accessed for that command.
     * Therefore it currently includes other costs, which is not ideal but it does give a rough estimate.
     * since disk costs would dominate computing costs. */
    public final MovingAverage sstablePartitionReadLatency;
    /** (Local) read metrics */
    public final TableLatencyMetrics readLatency;
    /** (Local) range slice metrics */
    public final TableLatencyMetrics rangeLatency;
    /** (Local) write metrics */
    public final TableLatencyMetrics writeLatency;
    /** The number of single partition read requests, including those dropped due to timeouts */
    public final Counter readRequests;
    /** The number of range read requests, including those dropped due to timeouts */
    public final Counter rangeRequests;
    /** Estimated number of tasks pending for this table */
    public final Counter pendingFlushes;
    /** Total number of bytes flushed since server [re]start */
    public final Counter bytesFlushed;
    /** The average flushed size for sstables, which is derived from {@link this#bytesFlushed}. */
    public final MovingAverage flushSize;
    /** The average on-disk flushed size for sstables. */
    private final MovingAverage flushSizeOnDisk;
    /** The average number of sstables created on flush. */
    public final MovingAverage flushSegmentCount;
    /** The average duration per 1Kb of data flushed, in nanoseconds. */
    public final MovingAverage flushTimePerKb;
    /** Time spent in flushing memtables */
    public final Counter flushTime;
    public final Counter storageAttachedIndexBuildTime;
    public final Counter storageAttachedIndexWritingTimeForIndexBuild;
    public final Counter storageAttachedIndexWritingTimeForCompaction;
    public final Counter storageAttachedIndexWritingTimeForFlush;
    public final Counter storageAttachedIndexWritingTimeForOther;
    /** Total number of bytes inserted into memtables since server [re]start. */
    public final Counter bytesInserted;
    /** Total number of bytes written by compaction since server [re]start */
    public final Counter compactionBytesWritten;
    /** Total number of bytes read by compaction since server [re]start */
    public final Counter compactionBytesRead;
    /** The average duration per 1Kb of data compacted, in nanoseconds. */
    public final MovingAverage compactionTimePerKb;
    /** Time spent in writing sstables during compaction  */
    public final Counter compactionTime;
    /** Estimate of number of pending compactions for this table */
    public final Gauge<Integer> pendingCompactions;
    /** Number of SSTables on disk for this CF */
    public final Gauge<Integer> liveSSTableCount;
    /** Number of SSTables with old version on disk for this CF */
    public final Gauge<Integer> oldVersionSSTableCount;
    /** Maximum duration of an SSTable for this table, computed as maxTimestamp - minTimestamp*/
    public final Gauge<Long> maxSSTableDuration;
    /** Maximum size of SSTable of this table - the physical size on disk of all components for such SSTable in bytes*/
    public final Gauge<Long> maxSSTableSize;
    /** Disk space used by SSTables belonging to this table */
    public final Counter liveDiskSpaceUsed;
    /** Uncompressed/logical disk space used by SSTables belonging to this table */
    public final Counter uncompressedLiveDiskSpaceUsed;
    /** Total disk space used by SSTables belonging to this table, including obsolete ones waiting to be GC'd */
    public final Counter totalDiskSpaceUsed;
    /** Size of the smallest compacted partition */
    public final Gauge<Long> minPartitionSize;
    /** Size of the largest compacted partition */
    public final Gauge<Long> maxPartitionSize;
    /** Size of the smallest compacted partition */
    public final Gauge<Long> meanPartitionSize;
    /** False positive ratio of bloom filter */
    public final Gauge<Double> bloomFilterFalseRatio;

    public final AtomicLong inFlightBloomFilterOffHeapMemoryUsed = new AtomicLong(0);
    /** Off heap memory used by compression meta data*/
    public final Gauge<Long> compressionMetadataOffHeapMemoryUsed;

    /** Shadowed keys scan metrics **/
    public final TableHistogram shadowedKeysScannedHistogram;
    public final TableHistogram shadowedKeysLoopsHistogram;

    /** Tombstones scanned in queries on this CF */
    public final TableHistogram tombstoneScannedHistogram;
    public final Counter tombstoneScannedCounter;
    /** Live rows scanned in queries on this CF */
    public final TableHistogram liveScannedHistogram;
    /** Column update time delta on this CF */
    public final TableHistogram colUpdateTimeDeltaHistogram;
    /** time taken acquiring the partition lock for materialized view updates for this table */
    public final TableTimer viewLockAcquireTime;
    /** time taken during the local read of a materialized view update */
    public final TableTimer viewReadTime;
    /** Disk space used by snapshot files which */
    public final Gauge<Long> trueSnapshotsSize;
    /** Row cache hits, but result out of range */
    public final Counter rowCacheHitOutOfRange;
    /** Number of row cache hits */
    public final Counter rowCacheHit;
    /** Number of row cache misses */
    public final Counter rowCacheMiss;
    /**
     * Number of tombstone read failures
     */
    public final Counter tombstoneFailures;
    /**
     * Number of tombstone read warnings
     */
    public final Counter tombstoneWarnings;
    /** CAS Prepare metrics */
    public final TableLatencyMetrics casPrepare;
    /** CAS Propose metrics */
    public final TableLatencyMetrics casPropose;
    /** CAS Commit metrics */
    public final TableLatencyMetrics casCommit;
    /** percent of the data that is repaired */
    public final Gauge<Double> percentRepaired;
    /** Reports the size of sstables in repaired, unrepaired, and any ongoing repair buckets */
    public final Gauge<Long> bytesRepaired;
    public final Gauge<Long> bytesUnrepaired;
    public final Gauge<Long> bytesPendingRepair;
    /** Number of started repairs as coordinator on this table */
    public final Counter repairsStarted;
    /** Number of completed repairs as coordinator on this table */
    public final Counter repairsCompleted;
    /** time spent anticompacting data before participating in a consistent repair */
    public final TableTimer anticompactionTime;
    /** time spent creating merkle trees */
    public final TableTimer validationTime;
    /** time spent syncing data in a repair */
    public final TableTimer repairSyncTime;
    /** approximate number of bytes read while creating merkle trees */
    public final TableHistogram bytesValidated;
    /** number of partitions read creating merkle trees */
    public final TableHistogram partitionsValidated;
    /** number of bytes read while doing anticompaction */
    public final Counter bytesAnticompacted;
    /** number of bytes where the whole sstable was contained in a repairing range so that we only mutated the repair status */
    public final Counter bytesMutatedAnticompaction;
    /** ratio of how much we anticompact vs how much we could mutate the repair status*/
    public final Gauge<Double> mutatedAnticompactionGauge;

    public final TableTimer<SnapshottingTimer> coordinatorReadLatency;
    public final TableTimer coordinatorScanLatency;
    public final TableTimer<SnapshottingTimer> coordinatorWriteLatency;

    private final MetricNameFactory factory;
    private final MetricNameFactory aliasFactory;

    public final Counter speculativeRetries;
    public final Counter speculativeFailedRetries;
    public final Counter speculativeInsufficientReplicas;
    public final Gauge<Long> speculativeSampleLatencyNanos;

    public final Counter additionalWrites;
    public final Gauge<Long> additionalWriteLatencyNanos;

    public final Gauge<Integer> unleveledSSTables;

    /**
     * Metrics for inconsistencies detected between repaired data sets across replicas. These
     * are tracked on the coordinator.
     */
    // Incremented where an inconsistency is detected and there are no pending repair sessions affecting
    // the data being read, indicating a genuine mismatch between replicas' repaired data sets.
    public final TableMeter confirmedRepairedInconsistencies;
    // Incremented where an inconsistency is detected, but there are pending & uncommitted repair sessions
    // in play on at least one replica. This may indicate a false positive as the inconsistency could be due to
    // replicas marking the repair session as committed at slightly different times and so some consider it to
    // be part of the repaired set whilst others do not.
    public final TableMeter unconfirmedRepairedInconsistencies;

    // Tracks the amount overreading of repaired data replicas perform in order to produce digests
    // at query time. For each query, on a full data read following an initial digest mismatch, the replicas
    // may read extra repaired data, up to the DataLimit of the command, so that the coordinator can compare
    // the repaired data on each replica. These are tracked on each replica.
    public final TableHistogram repairedDataTrackingOverreadRows;
    public final TableTimer repairedDataTrackingOverreadTime;

    /** When sampler activated, will track the most frequently read partitions **/
    public final Sampler<ByteBuffer> topReadPartitionFrequency;
    /** When sampler activated, will track the most frequently written to partitions **/
    public final Sampler<ByteBuffer> topWritePartitionFrequency;
    /** When sampler activated, will track the largest mutations **/
    public final Sampler<ByteBuffer> topWritePartitionSize;
    /** When sampler activated, will track the most frequent partitions with cas contention **/
    public final Sampler<ByteBuffer> topCasPartitionContention;
    /** When sampler activated, will track the slowest local reads **/
    public final Sampler<String> topLocalReadQueryTime;
    /** When sampler activated, will track partitions read with the most rows **/
    public final Sampler<ByteBuffer> topReadPartitionRowCount;
    /** When sampler activated, will track partitions read with the most tombstones **/
    public final Sampler<ByteBuffer> topReadPartitionTombstoneCount;
    /** When sample activated, will track partitions read with the most merged sstables **/
    public final Sampler<ByteBuffer> topReadPartitionSSTableCount;

    public final TableMeter clientTombstoneWarnings;
    public final TableMeter clientTombstoneAborts;

    public final TableMeter coordinatorReadSizeWarnings;
    public final TableMeter coordinatorReadSizeAborts;
    public final TableHistogram coordinatorReadSize;

    public final TableMeter localReadSizeWarnings;
    public final TableMeter localReadSizeAborts;
    public final TableHistogram localReadSize;

    public final TableMeter rowIndexSizeWarnings;
    public final TableMeter rowIndexSizeAborts;
    public final TableHistogram rowIndexSize;

    public final TableMeter tooManySSTableIndexesReadWarnings;
    public final TableMeter tooManySSTableIndexesReadAborts;

    public final ImmutableMap<SSTableFormat<?, ?>, ImmutableMap<String, Gauge<? extends Number>>> formatSpecificGauges;

    /**
     * This property determines if new metrics dedicated to this table are created, or if keyspace metrics are
     * used instead.
     * */
    public final MetricsAggregation metricsAggregation;

    private static Pair<Long, Long> totalNonSystemTablesSize(Predicate<SSTableReader> predicate)
    {
        long total = 0;
        long filtered = 0;
        for (String keyspace : Schema.instance.distributedKeyspaces().names())
        {

            Keyspace k = Schema.instance.getKeyspaceInstance(keyspace);
            if (SchemaConstants.DISTRIBUTED_KEYSPACE_NAME.equals(k.getName()))
                continue;
            if (k.getReplicationStrategy().getReplicationFactor().allReplicas < 2)
                continue;

            for (ColumnFamilyStore cf : k.getColumnFamilyStores())
            {
                if (!SecondaryIndexManager.isIndexColumnFamily(cf.name))
                {
                    for (SSTableReader sstable : cf.getSSTables(SSTableSet.CANONICAL))
                    {
                        if (predicate.test(sstable))
                        {
                            filtered += sstable.uncompressedLength();
                        }
                        total += sstable.uncompressedLength();
                    }
                }
            }
        }
        return Pair.create(filtered, total);
    }

    public static final Gauge<Double> globalPercentRepaired = Metrics.register(GLOBAL_FACTORY.createMetricName("PercentRepaired"),
                                                                               new Gauge<Double>()
    {
        public Double getValue()
        {
            Pair<Long, Long> result = totalNonSystemTablesSize(SSTableReader::isRepaired);
            double repaired = result.left;
            double total = result.right;
            return total > 0 ? (repaired / total) * 100 : 100.0;
        }
    });

    public static final Gauge<Long> globalBytesRepaired = Metrics.register(GLOBAL_FACTORY.createMetricName("BytesRepaired"),
                                                                           () -> totalNonSystemTablesSize(SSTableReader::isRepaired).left);

    public static final Gauge<Long> globalBytesUnrepaired =
        Metrics.register(GLOBAL_FACTORY.createMetricName("BytesUnrepaired"),
                         () -> totalNonSystemTablesSize(s -> !s.isRepaired() && !s.isPendingRepair()).left);

    public static final Gauge<Long> globalBytesPendingRepair =
        Metrics.register(GLOBAL_FACTORY.createMetricName("BytesPendingRepair"),
                         () -> totalNonSystemTablesSize(SSTableReader::isPendingRepair).left);

    public final Meter readRepairRequests;
    public final Meter shortReadProtectionRequests;

    public final Meter replicaFilteringProtectionRequests;

    /**
     * This histogram records the maximum number of rows {@link org.apache.cassandra.service.reads.ReplicaFilteringProtection}
     * caches at a point in time per query. With no replica divergence, this is equivalent to the maximum number of
     * cached rows in a single partition during a query. It can be helpful when choosing appropriate values for the
     * replica_filtering_protection thresholds in cassandra.yaml.
     */
    public final Histogram rfpRowsCachedPerQuery;

    public final EnumMap<SamplerType, Sampler<?>> samplers;

    /**
     * Stores all metrics created that can be used when unregistering
     */
    private final Set<ReleasableMetric> all = Sets.newHashSet();

    private interface GetHistogram
    {
        EstimatedHistogram getHistogram(SSTableReader reader);
    }

    private static long[] combineHistograms(Iterable<SSTableReader> sstables, GetHistogram getHistogram)
    {
        Iterator<SSTableReader> iterator = sstables.iterator();
        if (!iterator.hasNext())
        {
            return ArrayUtils.EMPTY_LONG_ARRAY;
        }
        long[] firstBucket = getHistogram.getHistogram(iterator.next()).getBuckets(false);
        long[] values = Arrays.copyOf(firstBucket, firstBucket.length);

        while (iterator.hasNext())
        {
            long[] nextBucket = getHistogram.getHistogram(iterator.next()).getBuckets(false);
            values = addHistogram(values, nextBucket);
        }
        return values;
    }

    @VisibleForTesting
    public static long[] addHistogram(long[] sums, long[] buckets)
    {
        if (buckets.length > sums.length)
        {
            sums = Arrays.copyOf(sums, buckets.length);
        }

        for (int i = 0; i < buckets.length; i++)
        {
            sums[i] += buckets[i];
        }
        return sums;
    }

    /**
     * Creates metrics for given {@link ColumnFamilyStore}.
     *
     * @param cfs ColumnFamilyStore to measure metrics
     */
    public TableMetrics(final ColumnFamilyStore cfs, ReleasableMetric memtableMetrics)
    {
        metricsAggregation = MetricsAggregation.fromMetadata(cfs.metadata());
        logger.trace("Using {} histograms for table={}", metricsAggregation, cfs.metadata());

        factory = new TableMetricNameFactory(cfs, "Table");
        aliasFactory = new TableMetricNameFactory(cfs, "ColumnFamily");

        if (memtableMetrics != null)
        {
            all.add(memtableMetrics);
        }

        samplers = new EnumMap<>(SamplerType.class);
        topReadPartitionFrequency = new FrequencySampler<ByteBuffer>()
        {
            public String toString(ByteBuffer value)
            {
                return cfs.metadata().partitionKeyType.getString(value);
            }
        };
        topWritePartitionFrequency = new FrequencySampler<ByteBuffer>()
        {
            public String toString(ByteBuffer value)
            {
                return cfs.metadata().partitionKeyType.getString(value);
            }
        };
        topWritePartitionSize = new MaxSampler<ByteBuffer>()
        {
            public String toString(ByteBuffer value)
            {
                return cfs.metadata().partitionKeyType.getString(value);
            }
        };
        topCasPartitionContention = new FrequencySampler<ByteBuffer>()
        {
            public String toString(ByteBuffer value)
            {
                return cfs.metadata().partitionKeyType.getString(value);
            }
        };
        topLocalReadQueryTime = new MaxSampler<String>()
        {
            public String toString(String value)
            {
                return value;
            }
        };

        topReadPartitionRowCount = new MaxSampler<ByteBuffer>()
        {
            public String toString(ByteBuffer value)
            {
                return cfs.metadata().partitionKeyType.getString(value);
            }
        };

        topReadPartitionTombstoneCount = new MaxSampler<ByteBuffer>()
        {
            public String toString(ByteBuffer value)
            {
                return cfs.metadata().partitionKeyType.getString(value);
            }
        };

        topReadPartitionSSTableCount = new MaxSampler<ByteBuffer>()
        {
            @Override
            public String toString(ByteBuffer value)
            {
                return cfs.metadata().partitionKeyType.getString(value);
            }
        };

        samplers.put(SamplerType.READS, topReadPartitionFrequency);
        samplers.put(SamplerType.WRITES, topWritePartitionFrequency);
        samplers.put(SamplerType.WRITE_SIZE, topWritePartitionSize);
        samplers.put(SamplerType.CAS_CONTENTIONS, topCasPartitionContention);
        samplers.put(SamplerType.LOCAL_READ_TIME, topLocalReadQueryTime);
        samplers.put(SamplerType.READ_ROW_COUNT, topReadPartitionRowCount);
        samplers.put(SamplerType.READ_TOMBSTONE_COUNT, topReadPartitionTombstoneCount);
        samplers.put(SamplerType.READ_SSTABLE_COUNT, topReadPartitionSSTableCount);

        memtableColumnsCount = createTableGauge("MemtableColumnsCount",
                                                () -> cfs.getTracker().getView().getCurrentMemtable().operationCount());

        // MemtableOnHeapSize naming deprecated in 4.0
        memtableOnHeapDataSize = createTableGaugeWithDeprecation("MemtableOnHeapDataSize", "MemtableOnHeapSize",
                                                                 () -> Memtable.getMemoryUsage(cfs.getTracker().getView().getCurrentMemtable()).ownsOnHeap,
                                                                 new GlobalTableGauge("MemtableOnHeapDataSize"));

        // MemtableOffHeapSize naming deprecated in 4.0
        memtableOffHeapDataSize = createTableGaugeWithDeprecation("MemtableOffHeapDataSize", "MemtableOffHeapSize",
                                                                  () -> Memtable.getMemoryUsage(cfs.getTracker().getView().getCurrentMemtable()).ownsOffHeap,
                                                                  new GlobalTableGauge("MemtableOnHeapDataSize"));

        memtableLiveDataSize = createTableGauge("MemtableLiveDataSize",
                                                () -> cfs.getTracker().getView().getCurrentMemtable().getLiveDataSize());

        // AllMemtablesHeapSize naming deprecated in 4.0
        allMemtablesOnHeapDataSize = createTableGaugeWithDeprecation("AllMemtablesOnHeapDataSize", "AllMemtablesHeapSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                return getMemoryUsageWithIndexes(cfs).ownsOnHeap;
            }
        }, new GlobalTableGauge("AllMemtablesOnHeapDataSize"));

        // AllMemtablesOffHeapSize naming deprecated in 4.0
        allMemtablesOffHeapDataSize = createTableGaugeWithDeprecation("AllMemtablesOffHeapDataSize", "AllMemtablesOffHeapSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                return getMemoryUsageWithIndexes(cfs).ownsOffHeap;
            }
        }, new GlobalTableGauge("AllMemtablesOffHeapDataSize"));
        allMemtablesLiveDataSize = createTableGauge("AllMemtablesLiveDataSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                long size = 0;
                for (ColumnFamilyStore cfs2 : cfs.concatWithIndexes())
                    size += cfs2.getTracker().getView().getCurrentMemtable().getLiveDataSize();
                return size;
            }
        });
        memtableSwitchCount = createTableCounter("MemtableSwitchCount");
        estimatedPartitionSizeHistogram = createTableGauge("EstimatedPartitionSizeHistogram", "EstimatedRowSizeHistogram",
                                                           () -> combineHistograms(cfs.getSSTables(SSTableSet.CANONICAL),
                                                                                   SSTableReader::getEstimatedPartitionSize), null);

        estimatedPartitionCountInSSTables = new LongSupplier()
        {
            // Since the sstables only change when the tracker view changes, we can cache the value.
            AtomicReference<Pair<WeakReference<View>, Long>> collected = new AtomicReference<>(Pair.create(new WeakReference<>(null), 0L));

            public long getAsLong()
            {
                final View currentView = cfs.getTracker().getView();
                final Pair<WeakReference<View>, Long> currentCollected = collected.get();
                if (currentView != currentCollected.left.get())
                {
                    Refs<SSTableReader> refs = Refs.tryRef(currentView.select(SSTableSet.CANONICAL));
                    if (refs != null)
                    {
                        try (refs)
                        {
                            long collectedValue = SSTableReader.getApproximateKeyCount(refs);
                            final Pair<WeakReference<View>, Long> newCollected = Pair.create(new WeakReference<>(currentView), collectedValue);
                            collected.compareAndSet(currentCollected, newCollected); // okay if failed, a different thread did it
                            return collectedValue;
                        }
                    }
                    // If we can't reference, simply return the previous collected value; it can only result in a delay
                    // in reporting the correct key count.
                }
                return currentCollected.right;
            }
        };
        estimatedPartitionCount = createTableGauge("EstimatedPartitionCount", "EstimatedRowCount", new Gauge<Long>()
        {
            public Long getValue()
            {
                long estimatedPartitions = estimatedPartitionCountInSSTables.getAsLong();
                for (Memtable memtable : cfs.getTracker().getView().getAllMemtables())
                    estimatedPartitions += memtable.partitionCount();
                return estimatedPartitions;
            }
        }, null);
        estimatedPartitionCountInSSTablesCached = new CachedGauge<Long>(1, TimeUnit.SECONDS)
        {
            public Long loadValue()
            {
                return estimatedPartitionCountInSSTables.getAsLong();
            }
        };

        estimatedColumnCountHistogram = createTableGauge("EstimatedColumnCountHistogram", "EstimatedColumnCountHistogram",
                                                         () -> combineHistograms(cfs.getSSTables(SSTableSet.CANONICAL),
                                                                                 SSTableReader::getEstimatedCellPerPartitionCount), null);

        estimatedRowCount = createTableGauge("EstimatedRowCount", "EstimatedRowCount", new CachedGauge<>(1, TimeUnit.SECONDS)
        {
            public Long loadValue()
            {
                long memtableRows = 0;
                OpOrder.Group readGroup = null;
                try
                {
                    for (Memtable memtable : cfs.getTracker().getView().getAllMemtables())
                    {
                        if (readGroup == null)
                        {
                            readGroup = memtable.readOrdering().start();
                        }
                        memtableRows += Memtable.estimateRowCount(memtable);
                    }
                }
                finally
                {
                    if (readGroup != null)
                        readGroup.close();
                }

                long sstableRows = 0;
                try(ColumnFamilyStore.RefViewFragment refViewFragment = cfs.selectAndReference(View.selectFunction(SSTableSet.CANONICAL)))
                {
                    for (SSTableReader reader: refViewFragment.sstables)
                    {
                        sstableRows += reader.getTotalRows();
                    }
                }
                return sstableRows + memtableRows;
            }
        }, null);

        sstablesPerReadHistogram = createTableHistogram("SSTablesPerReadHistogram", cfs.getKeyspaceMetrics().sstablesPerReadHistogram, true);
        sstablesPerRangeReadHistogram = createTableHistogram("SSTablesPerRangeReadHistogram", cfs.getKeyspaceMetrics().sstablesPerRangeReadHistogram, true);
        sstablePartitionReadLatency = ExpMovingAverage.decayBy100();
        compressionRatio = createTableGauge("CompressionRatio", new Gauge<Double>()
        {
            public Double getValue()
            {
                return computeCompressionRatio(cfs.getSSTables(SSTableSet.CANONICAL));
            }
        }, new Gauge<Double>() // global gauge
        {
            public Double getValue()
            {
                List<SSTableReader> sstables = new ArrayList<>();
                Keyspace.all().forEach(ks -> sstables.addAll(ks.getAllSSTables(SSTableSet.CANONICAL)));
                return computeCompressionRatio(sstables);
            }
        });
        percentRepaired = createTableGauge("PercentRepaired", new Gauge<Double>()
        {
            public Double getValue()
            {
                double repaired = 0;
                double total = 0;
                for (SSTableReader sstable : cfs.getSSTables(SSTableSet.CANONICAL))
                {
                    if (sstable.isRepaired())
                    {
                        repaired += sstable.uncompressedLength();
                    }
                    total += sstable.uncompressedLength();
                }
                return total > 0 ? (repaired / total) * 100 : 100.0;
            }
        });

        bytesRepaired = createTableGauge("BytesRepaired", new Gauge<Long>()
        {
            public Long getValue()
            {
                long size = 0;
                for (SSTableReader sstable: Iterables.filter(cfs.getSSTables(SSTableSet.CANONICAL), SSTableReader::isRepaired))
                {
                    size += sstable.uncompressedLength();
                }
                return size;
            }
        });

        bytesUnrepaired = createTableGauge("BytesUnrepaired", new Gauge<Long>()
        {
            public Long getValue()
            {
                long size = 0;
                for (SSTableReader sstable: Iterables.filter(cfs.getSSTables(SSTableSet.CANONICAL), s -> !s.isRepaired() && !s.isPendingRepair()))
                {
                    size += sstable.uncompressedLength();
                }
                return size;
            }
        });

        bytesPendingRepair = createTableGauge("BytesPendingRepair", new Gauge<Long>()
        {
            public Long getValue()
            {
                long size = 0;
                for (SSTableReader sstable: Iterables.filter(cfs.getSSTables(SSTableSet.CANONICAL), SSTableReader::isPendingRepair))
                {
                    size += sstable.uncompressedLength();
                }
                return size;
            }
        });

        readLatency = createLatencyMetrics("Read", cfs.getKeyspaceMetrics().readLatency, GLOBAL_READ_LATENCY);
        writeLatency = createLatencyMetrics("Write", cfs.getKeyspaceMetrics().writeLatency, GLOBAL_WRITE_LATENCY);
        rangeLatency = createLatencyMetrics("Range", cfs.getKeyspaceMetrics().rangeLatency, GLOBAL_RANGE_LATENCY);

        readRequests = createTableCounter("ReadRequests");
        rangeRequests = createTableCounter("RangeRequests");

        pendingFlushes = createTableCounter("PendingFlushes");
        bytesFlushed = createTableCounter("BytesFlushed");
        flushSize = ExpMovingAverage.decayBy100();
        flushSizeOnDisk = ExpMovingAverage.decayBy1000();
        flushSegmentCount = ExpMovingAverage.decayBy1000();
        flushTimePerKb = ExpMovingAverage.decayBy100();
        flushTime = createTableCounter("FlushTime");
        storageAttachedIndexBuildTime = createTableCounter("StorageAttachedIndexBuildTime");
        storageAttachedIndexWritingTimeForIndexBuild = createTableCounter("StorageAttachedIndexWritingTimeForIndexBuild");
        storageAttachedIndexWritingTimeForCompaction = createTableCounter("StorageAttachedIndexWritingTimeForCompaction");
        storageAttachedIndexWritingTimeForFlush = createTableCounter("StorageAttachedIndexWritingTimeForFlush");
        storageAttachedIndexWritingTimeForOther= createTableCounter("StorageAttachedIndexWritingTimeForOther");
        bytesInserted = createTableCounter("BytesInserted");

        compactionBytesWritten = createTableCounter("CompactionBytesWritten");
        compactionBytesRead = createTableCounter("CompactionBytesRead");
        compactionTimePerKb = ExpMovingAverage.decayBy100();
        compactionTime = createTableCounter("CompactionTime");
        pendingCompactions = createTableGauge("PendingCompactions", () -> cfs.getCompactionStrategy().getEstimatedRemainingTasks());
        liveSSTableCount = createTableGauge("LiveSSTableCount", () -> cfs.getLiveSSTables().size());
        oldVersionSSTableCount = createTableGauge("OldVersionSSTableCount", new Gauge<Integer>()
        {
            public Integer getValue()
            {
                int count = 0;
                for (SSTableReader sstable : cfs.getLiveSSTables())
                    if (!sstable.descriptor.version.isLatestVersion())
                        count++;
                return count;
            }
        });
        maxSSTableDuration = createTableGauge("MaxSSTableDuration", new Gauge<Long>()
        {
            @Override
            public Long getValue()
            {
                return cfs.getTracker()
                          .getView()
                          .liveSSTables()
                          .stream()
                          .filter(sstable -> sstable.getMinTimestamp() != Long.MAX_VALUE && sstable.getMaxTimestamp() != Long.MAX_VALUE)
                          .map(ssTableReader -> ssTableReader.getMaxTimestamp() - ssTableReader.getMinTimestamp())
                          .max(Long::compare)
                          .orElse(0L) / 1000;
            }
        });
        maxSSTableSize = createTableGauge("MaxSSTableSize", new Gauge<Long>()
        {
            @Override
            public Long getValue()
            {
                return cfs.getTracker()
                          .getView()
                          .liveSSTables()
                          .stream()
                          .map(SSTableReader::bytesOnDisk)
                          .max(Long::compare)
                          .orElse(0L);
            }
        });
        liveDiskSpaceUsed = createTableCounter("LiveDiskSpaceUsed");
        uncompressedLiveDiskSpaceUsed = createTableCounter("UncompressedLiveDiskSpaceUsed");
        totalDiskSpaceUsed = createTableCounter("TotalDiskSpaceUsed");
        minPartitionSize = createTableGauge("MinPartitionSize", "MinRowSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                long min = 0;
                for (SSTableReader sstable : cfs.getSSTables(SSTableSet.CANONICAL))
                {
                    if (min == 0 || sstable.getEstimatedPartitionSize().min() < min)
                        min = sstable.getEstimatedPartitionSize().min();
                }
                return min;
            }
        }, new Gauge<Long>() // global gauge
        {
            public Long getValue()
            {
                long min = Long.MAX_VALUE;
                for (Metric cfGauge : ALL_TABLE_METRICS.get("MinPartitionSize"))
                {
                    min = Math.min(min, ((Gauge<? extends Number>) cfGauge).getValue().longValue());
                }
                return min;
            }
        });
        maxPartitionSize = createTableGauge("MaxPartitionSize", "MaxRowSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                long max = 0;
                for (SSTableReader sstable : cfs.getSSTables(SSTableSet.CANONICAL))
                {
                    if (sstable.getEstimatedPartitionSize().max() > max)
                        max = sstable.getEstimatedPartitionSize().max();
                }
                return max;
            }
        }, new Gauge<Long>() // global gauge
        {
            public Long getValue()
            {
                long max = 0;
                for (Metric cfGauge : ALL_TABLE_METRICS.get("MaxPartitionSize"))
                {
                    max = Math.max(max, ((Gauge<? extends Number>) cfGauge).getValue().longValue());
                }
                return max;
            }
        });
        meanPartitionSize = createTableGauge("MeanPartitionSize", "MeanRowSize", new Gauge<Long>()
        {
            public Long getValue()
            {
                long sum = 0;
                long count = 0;
                for (SSTableReader sstable : cfs.getSSTables(SSTableSet.CANONICAL))
                {
                    long n = sstable.getEstimatedPartitionSize().count();
                    sum += sstable.getEstimatedPartitionSize().mean() * n;
                    count += n;
                }
                return count > 0 ? sum / count : 0;
            }
        }, new Gauge<Long>() // global gauge
        {
            public Long getValue()
            {
                long sum = 0;
                long count = 0;
                for (Keyspace keyspace : Keyspace.all())
                {
                    for (SSTableReader sstable : keyspace.getAllSSTables(SSTableSet.CANONICAL))
                    {
                        long n = sstable.getEstimatedPartitionSize().count();
                        sum += sstable.getEstimatedPartitionSize().mean() * n;
                        count += n;
                    }
                }
                return count > 0 ? sum / count : 0;
            }
        });
        bloomFilterFalseRatio = createTableGauge("BloomFilterFalseRatio", new Gauge<Double>()
        {
            public Double getValue()
            {
                long falsePositiveCount = cfs.getBloomFilterFalsePositiveCount();
                long truePositiveCount = cfs.getBloomFilterTruePositiveCount();
                long trueNegativeCount = cfs.getBloomFilterTrueNegativeCount();

                if (falsePositiveCount == 0L && truePositiveCount == 0L)
                    return 0d;
                return (double) falsePositiveCount / (truePositiveCount + falsePositiveCount + trueNegativeCount);
            }
        }, new Gauge<Double>() // global gauge
        {
            public Double getValue()
            {
                long falsePositiveCount = 0L;
                long truePositiveCount = 0L;
                long trueNegativeCount = 0L;
                for (Keyspace keyspace : Keyspace.all())
                {
                    for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
                    {
                        falsePositiveCount += cfs.getBloomFilterFalsePositiveCount();
                        truePositiveCount += cfs.getBloomFilterTruePositiveCount();
                        trueNegativeCount += cfs.getBloomFilterTrueNegativeCount();
                    }
                }
                if (falsePositiveCount == 0L && truePositiveCount == 0L)
                    return 0d;
                return (double) falsePositiveCount / (truePositiveCount + falsePositiveCount + trueNegativeCount);
            }
        });
        compressionMetadataOffHeapMemoryUsed = createTableGauge("CompressionMetadataOffHeapMemoryUsed", new Gauge<Long>()
        {
            public Long getValue()
            {
                long total = 0;
                for (SSTableReader sst : cfs.getSSTables(SSTableSet.LIVE))
                    total += sst.getCompressionMetadataOffHeapSize();
                return total;
            }
        });
        speculativeRetries = createTableCounter("SpeculativeRetries");
        speculativeFailedRetries = createTableCounter("SpeculativeFailedRetries");
        speculativeInsufficientReplicas = createTableCounter("SpeculativeInsufficientReplicas");
        speculativeSampleLatencyNanos = createTableGauge("SpeculativeSampleLatencyNanos", () -> MICROSECONDS.toNanos(cfs.sampleReadLatencyMicros));

        additionalWrites = createTableCounter("AdditionalWrites");
        additionalWriteLatencyNanos = createTableGauge("AdditionalWriteLatencyNanos", () -> MICROSECONDS.toNanos(cfs.additionalWriteLatencyMicros));

        tombstoneScannedHistogram = createTableHistogram("TombstoneScannedHistogram", cfs.getKeyspaceMetrics().tombstoneScannedHistogram, false);
        shadowedKeysScannedHistogram = createTableHistogram("ShadowedKeysScannedHistogram", cfs.getKeyspaceMetrics().shadowedKeysScannedHistogram, false);
        shadowedKeysLoopsHistogram = createTableHistogram("ShadowedKeysLoopsHistogram", cfs.getKeyspaceMetrics().shadowedKeysLoopsHistogram, false);
        tombstoneScannedCounter = createTableCounter("TombstoneScannedCounter");
        liveScannedHistogram = createTableHistogram("LiveScannedHistogram", cfs.getKeyspaceMetrics().liveScannedHistogram, false);
        colUpdateTimeDeltaHistogram = createTableHistogram("ColUpdateTimeDeltaHistogram", cfs.getKeyspaceMetrics().colUpdateTimeDeltaHistogram, false);
        coordinatorReadLatency = createTableTimer("CoordinatorReadLatency", cfs.getKeyspaceMetrics().coordinatorReadLatency);
        coordinatorScanLatency = createTableTimer("CoordinatorScanLatency", cfs.getKeyspaceMetrics().coordinatorScanLatency);
        coordinatorWriteLatency = createTableTimer("CoordinatorWriteLatency", cfs.getKeyspaceMetrics().coordinatorWriteLatency);

        // We do not want to capture view mutation specific metrics for a view
        // They only makes sense to capture on the base table
        if (cfs.metadata().isView())
        {
            viewLockAcquireTime = null;
            viewReadTime = null;
        }
        else
        {
            viewLockAcquireTime = createTableTimer("ViewLockAcquireTime", cfs.getKeyspaceMetrics().viewLockAcquireTime);
            viewReadTime = createTableTimer("ViewReadTime", cfs.getKeyspaceMetrics().viewReadTime);
        }

        trueSnapshotsSize = createTableGauge("SnapshotsSize", cfs::trueSnapshotsSize);
        rowCacheHitOutOfRange = createTableCounter("RowCacheHitOutOfRange");
        rowCacheHit = createTableCounter("RowCacheHit");
        rowCacheMiss = createTableCounter("RowCacheMiss");

        tombstoneFailures = createTableCounter("TombstoneFailures");
        tombstoneWarnings = createTableCounter("TombstoneWarnings");

        casPrepare = createLatencyMetrics("CasPrepare", cfs.getKeyspaceMetrics().casPrepare, Optional.empty());
        casPropose = createLatencyMetrics("CasPropose", cfs.getKeyspaceMetrics().casPropose, Optional.empty());
        casCommit = createLatencyMetrics("CasCommit", cfs.getKeyspaceMetrics().casCommit, Optional.empty());

        repairsStarted = createTableCounter("RepairJobsStarted");
        repairsCompleted = createTableCounter("RepairJobsCompleted");

        anticompactionTime = createTableTimer("AnticompactionTime", cfs.getKeyspaceMetrics().anticompactionTime);
        validationTime = createTableTimer("ValidationTime", cfs.getKeyspaceMetrics().validationTime);
        repairSyncTime = createTableTimer("RepairSyncTime", cfs.getKeyspaceMetrics().repairSyncTime);

        bytesValidated = createTableHistogram("BytesValidated", cfs.getKeyspaceMetrics().bytesValidated, false);
        partitionsValidated = createTableHistogram("PartitionsValidated", cfs.getKeyspaceMetrics().partitionsValidated, false);
        bytesAnticompacted = createTableCounter("BytesAnticompacted");
        bytesMutatedAnticompaction = createTableCounter("BytesMutatedAnticompaction");
        mutatedAnticompactionGauge = createTableGauge("MutatedAnticompactionGauge", () ->
        {
            double bytesMutated = bytesMutatedAnticompaction.getCount();
            double bytesAnticomp = bytesAnticompacted.getCount();
            if (bytesAnticomp + bytesMutated > 0)
                return bytesMutated / (bytesAnticomp + bytesMutated);
            return 0.0;
        });

        readRepairRequests = createTableMeter("ReadRepairRequests");
        shortReadProtectionRequests = createTableMeter("ShortReadProtectionRequests");
        replicaFilteringProtectionRequests = createTableMeter("ReplicaFilteringProtectionRequests");
        rfpRowsCachedPerQuery = createHistogram("ReplicaFilteringProtectionRowsCachedPerQuery", true);

        confirmedRepairedInconsistencies = createTableMeter("RepairedDataInconsistenciesConfirmed", cfs.getKeyspaceMetrics().confirmedRepairedInconsistencies);
        unconfirmedRepairedInconsistencies = createTableMeter("RepairedDataInconsistenciesUnconfirmed", cfs.getKeyspaceMetrics().unconfirmedRepairedInconsistencies);

        repairedDataTrackingOverreadRows = createTableHistogram("RepairedDataTrackingOverreadRows", cfs.getKeyspaceMetrics().repairedDataTrackingOverreadRows, false);
        repairedDataTrackingOverreadTime = createTableTimer("RepairedDataTrackingOverreadTime", cfs.getKeyspaceMetrics().repairedDataTrackingOverreadTime);

        unleveledSSTables = createTableGauge("UnleveledSSTables", cfs::getUnleveledSSTables, () -> {
            // global gauge
            int cnt = 0;
            for (Metric cfGauge : ALL_TABLE_METRICS.get("UnleveledSSTables"))
            {
                cnt += ((Gauge<? extends Number>) cfGauge).getValue().intValue();
            }
            return cnt;
        });

        clientTombstoneWarnings = createTableMeter("ClientTombstoneWarnings", cfs.getKeyspaceMetrics().clientTombstoneWarnings);
        clientTombstoneAborts = createTableMeter("ClientTombstoneAborts", cfs.getKeyspaceMetrics().clientTombstoneAborts);

        coordinatorReadSizeWarnings = createTableMeter("CoordinatorReadSizeWarnings", cfs.getKeyspaceMetrics().coordinatorReadSizeWarnings);
        coordinatorReadSizeAborts = createTableMeter("CoordinatorReadSizeAborts", cfs.getKeyspaceMetrics().coordinatorReadSizeAborts);
        coordinatorReadSize = createTableHistogram("CoordinatorReadSize", cfs.getKeyspaceMetrics().coordinatorReadSize, false);

        localReadSizeWarnings = createTableMeter("LocalReadSizeWarnings", cfs.getKeyspaceMetrics().localReadSizeWarnings);
        localReadSizeAborts = createTableMeter("LocalReadSizeAborts", cfs.getKeyspaceMetrics().localReadSizeAborts);
        localReadSize = createTableHistogram("LocalReadSize", cfs.getKeyspaceMetrics().localReadSize, false);

        rowIndexSizeWarnings = createTableMeter("RowIndexSizeWarnings", cfs.getKeyspaceMetrics().rowIndexSizeWarnings);
        rowIndexSizeAborts = createTableMeter("RowIndexSizeAborts", cfs.getKeyspaceMetrics().rowIndexSizeAborts);
        rowIndexSize = createTableHistogram("RowIndexSize", cfs.getKeyspaceMetrics().rowIndexSize, false);

        tooManySSTableIndexesReadWarnings = createTableMeter("TooManySSTableIndexesReadWarnings", cfs.getKeyspaceMetrics().tooManySSTableIndexesReadWarnings);
        tooManySSTableIndexesReadAborts = createTableMeter("TooManySSTableIndexesReadAborts", cfs.getKeyspaceMetrics().tooManySSTableIndexesReadAborts);

        formatSpecificGauges = createFormatSpecificGauges(cfs);
    }

    public MovingAverage flushSizeOnDisk()
    {
        return flushSizeOnDisk;
    }

    private Memtable.MemoryUsage getMemoryUsageWithIndexes(ColumnFamilyStore cfs)
    {
        Memtable.MemoryUsage usage = Memtable.newMemoryUsage();
        cfs.getTracker().getView().getCurrentMemtable().addMemoryUsageTo(usage);
        for (ColumnFamilyStore indexCfs : cfs.indexManager.getAllIndexColumnFamilyStores())
            indexCfs.getTracker().getView().getCurrentMemtable().addMemoryUsageTo(usage);
        return usage;
    }

    public void incLiveRows(long liveRows)
    {
        liveScannedHistogram.update(liveRows);
    }

    public void incShadowedKeys(long numLoops, long numShadowedKeys)
    {
        shadowedKeysLoopsHistogram.update(numLoops);
        shadowedKeysScannedHistogram.update(numShadowedKeys);
    }

    public void incTombstones(long tombstones, boolean triggerWarning)
    {
        tombstoneScannedHistogram.update(tombstones);
        tombstoneScannedCounter.inc(tombstones);

        if (triggerWarning)
            tombstoneWarnings.inc();
    }

    public void incBytesFlushed(long inputSize, long outputSize, long elapsedNanos)
    {
        bytesFlushed.inc(outputSize);
        flushSize.update(outputSize);
        // this assumes that at least 1 Kb was flushed, which should always be the case, then rounds down
        flushTimePerKb.update(elapsedNanos / (double) Math.max(1, inputSize / 1024L));
    }

    public void updateStorageAttachedIndexBuildTime(long totalTimeSpentNanos)
    {
        storageAttachedIndexBuildTime.inc(TimeUnit.NANOSECONDS.toMicros(totalTimeSpentNanos));
    }

    public void updateStorageAttachedIndexWritingTime(long totalTimeSpentNanos, OperationType opType)
    {
        long totalTimeSpentMicros = TimeUnit.NANOSECONDS.toMicros(totalTimeSpentNanos);
        switch (opType)
        {
            case INDEX_BUILD:
                storageAttachedIndexWritingTimeForIndexBuild.inc(totalTimeSpentMicros);
                break;
            case COMPACTION:
                storageAttachedIndexWritingTimeForCompaction.inc(totalTimeSpentMicros);
                break;
            case FLUSH:
                storageAttachedIndexWritingTimeForFlush.inc(totalTimeSpentMicros);
                break;
            default:
                storageAttachedIndexWritingTimeForOther.inc(totalTimeSpentMicros);
        }
    }

    public void memTableFlushCompleted(long totalTimeSpentNanos) {
        flushTime.inc(TimeUnit.NANOSECONDS.toMicros(totalTimeSpentNanos));
    }

    public void incBytesCompacted(long inputDiskSize, long outputDiskSize, long elapsedMillis)
    {
        compactionBytesRead.inc(inputDiskSize);
        compactionBytesWritten.inc(outputDiskSize);
        compactionTime.inc(TimeUnit.MILLISECONDS.toMicros(elapsedMillis));
        // only update compactionTimePerKb when there are non-expired sstables (inputDiskSize > 0)
        if (inputDiskSize > 0)
            compactionTimePerKb.update(1024.0 * elapsedMillis / inputDiskSize);
    }

    public void updateSSTableIterated(int count, int intersectingCount, long elapsedNanos)
    {
        sstablesPerReadHistogram.update(count);

        if (intersectingCount > 0)
            sstablePartitionReadLatency.update(elapsedNanos / (double) intersectingCount);
    }

    public void updateSSTableIteratedInRangeRead(int count)
    {
        sstablesPerRangeReadHistogram.update(count);
    }

    /**
     * Release all associated metrics.
     */
    public void release()
    {
        for (ReleasableMetric entry : all)
        {
            entry.release();
        }
    }

    private ImmutableMap<SSTableFormat<?, ?>, ImmutableMap<String, Gauge<? extends Number>>> createFormatSpecificGauges(ColumnFamilyStore cfs)
    {
        ImmutableMap.Builder<SSTableFormat<?, ?>, ImmutableMap<String, Gauge<? extends Number>>> builder = ImmutableMap.builder();
        for (SSTableFormat<?, ?> format : DatabaseDescriptor.getSSTableFormats().values())
        {
            ImmutableMap.Builder<String, Gauge<? extends Number>> gauges = ImmutableMap.builder();
            for (GaugeProvider<?> gaugeProvider : format.getFormatSpecificMetricsProviders().getGaugeProviders())
            {
                Gauge<? extends Number> gauge = createTableGauge(gaugeProvider.name, gaugeProvider.getTableGauge(cfs), gaugeProvider.getGlobalGauge());
                gauges.put(gaugeProvider.name, gauge);
            }
            builder.put(format, gauges.build());
        }
        return builder.build();
    }

    /**
     * Create a gauge that will be part of a merged version of all column families.  The global gauge
     * will merge each CF gauge by adding their values
     */
    protected <T extends Number> Gauge<T> createTableGauge(final String name, Gauge<T> gauge)
    {
        return createTableGauge(name, gauge, new GlobalTableGauge(name));
    }

    /**
     * Create a gauge that will be part of a merged version of all column families.  The global gauge
     * is defined as the globalGauge parameter
     */
    protected <G,T> Gauge<T> createTableGauge(String name, Gauge<T> gauge, Gauge<G> globalGauge)
    {
        return createTableGauge(name, name, gauge, globalGauge);
    }

    protected <G,T> Gauge<T> createTableGauge(String name, String alias, Gauge<T> gauge, Gauge<G> globalGauge)
    {
        Gauge<T> cfGauge = Metrics.register(factory.createMetricName(name), aliasFactory.createMetricName(alias), gauge);
        if (register(name, alias, cfGauge) && globalGauge != null)
        {
            Metrics.register(GLOBAL_FACTORY.createMetricName(name), GLOBAL_ALIAS_FACTORY.createMetricName(alias), globalGauge);
        }
        return cfGauge;
    }

    /**
     * Same as {@link #createTableGauge(String, Gauge, Gauge)} but accepts a deprecated
     * name for a table {@code Gauge}. Prefer that method when deprecation is not necessary.
     *
     * @param name the name of the metric registered with the "Table" type
     * @param deprecated the deprecated name for the metric registered with the "Table" type
     */
    protected <G,T> Gauge<T> createTableGaugeWithDeprecation(String name, String deprecated, Gauge<T> gauge, Gauge<G> globalGauge)
    {
        assert deprecated != null : "no deprecated metric name provided";
        assert globalGauge != null : "no global Gauge metric provided";
        
        Gauge<T> cfGauge = Metrics.register(factory.createMetricName(name), 
                                            gauge,
                                            aliasFactory.createMetricName(name),
                                            factory.createMetricName(deprecated),
                                            aliasFactory.createMetricName(deprecated));
        
        if (register(name, name, deprecated, cfGauge))
        {
            Metrics.register(GLOBAL_FACTORY.createMetricName(name),
                             globalGauge,
                             GLOBAL_ALIAS_FACTORY.createMetricName(name),
                             GLOBAL_FACTORY.createMetricName(deprecated),
                             GLOBAL_ALIAS_FACTORY.createMetricName(deprecated));
        }
        return cfGauge;
    }

    /**
     * Creates a counter that will also have a global counter thats the sum of all counters across
     * different column families
     */
    protected Counter createTableCounter(final String name)
    {
        return createTableCounter(name, name);
    }

    protected Counter createTableCounter(final String name, final String alias)
    {
        Counter cfCounter = Metrics.counter(factory.createMetricName(name), aliasFactory.createMetricName(alias));
        if (register(name, alias, cfCounter))
        {
            Metrics.register(GLOBAL_FACTORY.createMetricName(name),
                             GLOBAL_ALIAS_FACTORY.createMetricName(alias),
                             new Gauge<Long>()
                             {
                                 public Long getValue()
                                 {
                                     long total = 0;
                                     for (Metric cfGauge : ALL_TABLE_METRICS.get(name))
                                     {
                                         total += ((Counter) cfGauge).getCount();
                                     }
                                     return total;
                                 }
                             });
        }
        return cfCounter;
    }

    private Meter createTableMeter(final String name)
    {
        return createTableMeter(name, name);
    }

    private Meter createTableMeter(final String name, final String alias)
    {
        Meter tableMeter = Metrics.meter(factory.createMetricName(name), aliasFactory.createMetricName(alias));
        register(name, alias, tableMeter);
        return tableMeter;
    }
    
    private Histogram createHistogram(String name, boolean considerZeroes)
    {
        Histogram histogram = Metrics.histogram(factory.createMetricName(name), aliasFactory.createMetricName(name), considerZeroes);
        register(name, name, histogram);
        return histogram;
    }

    /**
     * Computes the compression ratio for the specified SSTables
     *
     * @param sstables the SSTables
     * @return the compression ratio for the specified SSTables
     */
    private static Double computeCompressionRatio(Iterable<SSTableReader> sstables)
    {
        double compressedLengthSum = 0;
        double dataLengthSum = 0;
        for (SSTableReader sstable : sstables)
        {
            if (sstable.compression)
            {
                // We should not have any sstable which are in an open early mode as the sstable were selected
                // using SSTableSet.CANONICAL.
                assert sstable.openReason != SSTableReader.OpenReason.EARLY;

                CompressionMetadata compressionMetadata = sstable.getCompressionMetadata();
                compressedLengthSum += compressionMetadata.compressedFileLength;
                dataLengthSum += compressionMetadata.dataLength;
            }
        }
        return dataLengthSum != 0 ? compressedLengthSum / dataLengthSum : MetadataCollector.NO_COMPRESSION_RATIO;
    }

    /**
     * Create a histogram-like interface that will register both a CF, keyspace and global level
     * histogram and forward any updates to both
     */
    protected TableHistogram createTableHistogram(String name, Histogram keyspaceHistogram, boolean considerZeroes)
    {
        return createTableHistogram(name, name, keyspaceHistogram, considerZeroes);
    }

    protected TableHistogram createTableHistogram(String name, String alias, Histogram keyspaceHistogram, boolean considerZeroes)
    {
        Histogram globalHistogram = null;
        if (EXPORT_GLOBAL_METRICS)
        {
            globalHistogram = Metrics.histogram(GLOBAL_FACTORY.createMetricName(name),
                                                GLOBAL_ALIAS_FACTORY.createMetricName(alias),
                                                considerZeroes);
        }

        Histogram tableHistogram = null;
        if (metricsAggregation == MetricsAggregation.INDIVIDUAL)
        {
            tableHistogram = Metrics.histogram(factory.createMetricName(name), aliasFactory.createMetricName(alias), considerZeroes);
            register(name, alias, tableHistogram);
        }

        return new TableHistogram(tableHistogram, keyspaceHistogram, globalHistogram);
    }

    protected Histogram createTableHistogram(String name, boolean considerZeroes)
    {
        return createTableHistogram(name, name, considerZeroes);
    }

    protected Histogram createTableHistogram(String name, String alias, boolean considerZeroes)
    {
        Histogram tableHistogram = Metrics.histogram(factory.createMetricName(name), aliasFactory.createMetricName(alias), considerZeroes);
        register(name, alias, tableHistogram);
        return tableHistogram;
    }

    protected TableTimer createTableTimer(String name, Timer keyspaceTimer)
    {
        Timer globalTimer = null;
        if (EXPORT_GLOBAL_METRICS)
        {
            globalTimer = Metrics.timer(GLOBAL_FACTORY.createMetricName(name), GLOBAL_ALIAS_FACTORY.createMetricName(name));
        }

        Timer tableTimer = null;
        if (metricsAggregation == MetricsAggregation.INDIVIDUAL)
        {
            tableTimer = Metrics.timer(factory.createMetricName(name), aliasFactory.createMetricName(name));
            register(name, name, keyspaceTimer);
        }

        return new TableTimer(tableTimer, keyspaceTimer, globalTimer);
    }

    protected SnapshottingTimer createTableTimer(String name)
    {
        SnapshottingTimer tableTimer = Metrics.timer(factory.createMetricName(name), aliasFactory.createMetricName(name));
        register(name, name, tableTimer);
        return tableTimer;
    }

    protected TableMeter createTableMeter(String name, Meter keyspaceMeter)
    {
        return createTableMeter(name, name, keyspaceMeter);
    }

    protected TableMeter createTableMeter(String name, String alias, Meter keyspaceMeter)
    {
        Meter globalMeter = null;
        if (EXPORT_GLOBAL_METRICS)
        {
            globalMeter = Metrics.meter(GLOBAL_FACTORY.createMetricName(name),
                                        GLOBAL_ALIAS_FACTORY.createMetricName(alias));
        }

        Meter tableMeter = null;
        if (metricsAggregation == MetricsAggregation.INDIVIDUAL)
        {
            tableMeter = Metrics.meter(factory.createMetricName(name), aliasFactory.createMetricName(alias));
            register(name, alias, tableMeter);
        }

        return new TableMeter(tableMeter, keyspaceMeter, globalMeter);
    }

    private TableLatencyMetrics createLatencyMetrics(String namePrefix, LatencyMetrics keyspace, Optional<LatencyMetrics> global)
    {
        TableLatencyMetrics metric;
        if (metricsAggregation == MetricsAggregation.INDIVIDUAL)
        {
            LatencyMetrics[] parents = Stream.of(Optional.of(keyspace), global).filter(Optional::isPresent)
                                             .map(Optional::get).toArray(LatencyMetrics[]::new);
            LatencyMetrics innerMetrics = new LatencyMetrics(factory, namePrefix, parents);
            metric = new TableLatencyMetrics.IndividualTableLatencyMetrics(innerMetrics);
        }
        else
        {
            metric = new TableLatencyMetrics.AggregatingTableLatencyMetrics(keyspace, global);
        }
        all.add(metric);
        return metric;
    }

    /**
     * Registers a metric to be removed when unloading CF.
     * @return true if first time metric with that name has been registered
     */
    private boolean register(String name, String alias, Metric metric)
    {
        return register(name, alias, null, metric);
    }

    /**
     * Registers a metric to be removed when unloading CF.
     * 
     * @param name the name of the metric registered with the "Table" type
     * @param alias the name of the metric registered with the legacy "ColumnFamily" type
     * @param deprecated an optionally null deprecated name for the metric registered with the "Table"
     * 
     * @return true if first time metric with that name has been registered
     */
    private boolean register(String name, String alias, String deprecated, Metric metric)
    {
        boolean ret = ALL_TABLE_METRICS.putIfAbsent(name, ConcurrentHashMap.newKeySet()) == null;
        ALL_TABLE_METRICS.get(name).add(metric);
        all.add(() -> releaseMetric(name, alias, deprecated));
        return ret;
    }

    private void releaseMetric(String tableMetricName, String cfMetricName, String tableMetricAlias)
    {
        CassandraMetricsRegistry.MetricName name = factory.createMetricName(tableMetricName);

        final Metric metric = Metrics.getMetrics().get(name.getMetricName());
        if (metric != null)
        {
            // Metric will be null if we are releasing a view metric.  Views have null for ViewLockAcquireTime and ViewLockReadTime
            ALL_TABLE_METRICS.get(tableMetricName).remove(metric);
            CassandraMetricsRegistry.MetricName cfAlias = aliasFactory.createMetricName(cfMetricName);
            
            if (tableMetricAlias != null)
            {
                Metrics.remove(name, cfAlias, factory.createMetricName(tableMetricAlias), aliasFactory.createMetricName(tableMetricAlias));
            }
            else
            {
                Metrics.remove(name, cfAlias);
            }
        }
    }

    public interface TableLatencyMetrics extends ReleasableMetric
    {
        void addNano(long latencyNanos);

        LatencyMetrics tableOrKeyspaceMetric();

        /**
         * Used when {@link MetricsAggregation#AGGREGATED} is set for this table.
         * <br/>
         * Table latency metrics that forwards all calls to the first parent metric (keyspace metric by convention).
         * Thanks to the forwarding, the table doesn't have to maintain its own metrics. The metrics for this table
         * are aggregated with metrics comming from other tables that use {@link MetricsAggregation#AGGREGATED}.
         */
        class AggregatingTableLatencyMetrics implements TableLatencyMetrics
        {
            private final LatencyMetrics keyspace;
            private final Optional<LatencyMetrics> global;

            public AggregatingTableLatencyMetrics(LatencyMetrics keyspace, Optional<LatencyMetrics> global)
            {
                this.keyspace = keyspace;
                this.global = global;
                Preconditions.checkState(keyspace != null, "Keyspace metrics should not be null");
            }

            @Override
            public void addNano(long latencyNanos)
            {
                keyspace.addNano(latencyNanos);
                global.ifPresent(g -> g.addNano(latencyNanos));
            }

            @Override
            public LatencyMetrics tableOrKeyspaceMetric()
            {
                return keyspace;
            }

            @Override
            public void release()
            {
                // noop
            }
        }

        /**
         * Used when {@link MetricsAggregation#INDIVIDUAL} is set for this table.
         * <br/>
         * Table latency metrics that don't aggreagte, i.e. the given table maintains its own latency metrics.
         */
        class IndividualTableLatencyMetrics implements TableLatencyMetrics
        {
            private final LatencyMetrics latencyMetrics;

            public IndividualTableLatencyMetrics(LatencyMetrics latencyMetrics)
            {
                this.latencyMetrics = latencyMetrics;
            }

            @Override
            public void addNano(long latencyNanos)
            {
                latencyMetrics.addNano(latencyNanos);
            }

            @Override
            public LatencyMetrics tableOrKeyspaceMetric()
            {
                return latencyMetrics;
            }

            @Override
            public void release()
            {
                latencyMetrics.release();
            }
        }
    }

    public static class TableMeter
    {
        public final Meter[] all;
        @Nullable
        private final Meter table;
        private final Meter keyspace;

        /**
         * Table meter wrapper that forwards updates to all provided non-null meters.
         *
         * @param table meter that is {@code null} if the metrics are not collected indidually for each table, see {@link TableMetrics#metricsAggregation}.
         * @param keyspace meter
         * @param global meter that is {@code null} if global metrics are not collected, see {@link TableMetrics#EXPORT_GLOBAL_METRICS}
         */
        private TableMeter(@Nullable Meter table, Meter keyspace, @Nullable Meter global)
        {
            Preconditions.checkState(keyspace != null, "Keyspace meter can't be null");
            this.table = table;
            this.keyspace = keyspace;
            this.all = Stream.of(table, keyspace, global).filter(Objects::nonNull).toArray(Meter[]::new);
        }

        public void mark()
        {
            for (Meter meter : all)
            {
                meter.mark();
            }
        }

        public Meter tableOrKeyspaceMeter()
        {
            return table == null ? keyspace : table;
        }
    }

    public static class TableHistogram
    {
        private final Histogram[] all;
        @Nullable
        private final Histogram table;
        private final Histogram keyspace;

        /**
         * Table histogram wrapper that forwards updates to all provided non-null histograms.
         *
         * @param table histogram that is {@code null} if the metrics are not collected indidually for each table, see {@link TableMetrics#metricsAggregation}.
         * @param keyspace histogram
         * @param global histogram that is {@code null} if global metrics are not collected, see {@link TableMetrics#EXPORT_GLOBAL_METRICS}
         */
        private TableHistogram(@Nullable Histogram table, Histogram keyspace, @Nullable Histogram global)
        {
            Preconditions.checkState(keyspace != null, "Keyspace histogram can't be null");
            this.table = table;
            this.keyspace = keyspace;
            this.all = Stream.of(table, keyspace, global).filter(Objects::nonNull).toArray(Histogram[]::new);
        }

        public void update(long i)
        {
            for (Histogram histo : all)
            {
                histo.update(i);
            }
        }

        public Histogram tableOrKeyspaceHistogram()
        {
            return table == null ? keyspace : table;
        }
    }

    public static class TableTimer<T extends Timer>
    {
        private final Timer[] all;
        @Nullable
        private final T cf;
        private final T keyspace;

        /**
         * Table timer wrapper that forwards updates to all provided non-null timers.
         *
         * @param cf timer that is {@code null} if the metrics are not collected indidually for each table, see {@link TableMetrics#metricsAggregation}.
         * @param keyspace timer
         * @param global timer that is {@code null} if global metrics are not collected, see {@link TableMetrics#EXPORT_GLOBAL_METRICS}
         */
        private TableTimer(@Nullable T cf, T keyspace, @Nullable T global)
        {
            Preconditions.checkState(keyspace != null, "Keyspace timer can't be null");
            this.cf = cf;
            this.keyspace = keyspace;
            this.all = Stream.of(cf, keyspace, global).filter(Objects::nonNull).map(t -> (Timer) t).toArray(Timer[]::new);
        }

        public void update(long i, TimeUnit unit)
        {
            for (Timer timer : all)
            {
                timer.update(i, unit);
            }
        }

        public Context time()
        {
            return new Context(all);
        }

        public T tableOrKeyspaceTimer()
        {
            return cf == null ? keyspace : cf;
        }

        public static class Context implements AutoCloseable
        {
            private final long start;
            private final Timer [] all;

            private Context(Timer [] all)
            {
                this.all = all;
                start = nanoTime();
            }

            public void close()
            {
                long duration = nanoTime() - start;
                for (Timer t : all)
                    t.update(duration, TimeUnit.NANOSECONDS);
            }
        }
    }

    static class TableMetricNameFactory implements MetricNameFactory
    {
        private final String keyspaceName;
        private final String tableName;
        private final boolean isIndex;
        private final String type;

        TableMetricNameFactory(ColumnFamilyStore cfs, String type)
        {
            this.keyspaceName = cfs.getKeyspaceName();
            this.tableName = cfs.getTableName();
            this.isIndex = cfs.isIndex();
            this.type = type;
        }

        public CassandraMetricsRegistry.MetricName createMetricName(String metricName)
        {
            String groupName = TableMetrics.class.getPackage().getName();
            String type = isIndex ? "Index" + this.type : this.type;

            StringBuilder mbeanName = new StringBuilder();
            mbeanName.append(groupName).append(":");
            mbeanName.append("type=").append(type);
            mbeanName.append(",keyspace=").append(keyspaceName);
            mbeanName.append(",scope=").append(tableName);
            mbeanName.append(",name=").append(metricName);

            return new CassandraMetricsRegistry.MetricName(groupName, type, metricName, keyspaceName + "." + tableName, mbeanName.toString());
        }
    }

    static class AllTableMetricNameFactory implements MetricNameFactory
    {
        private final String type;
        public AllTableMetricNameFactory(String type)
        {
            this.type = type;
        }

        public CassandraMetricsRegistry.MetricName createMetricName(String metricName)
        {
            String groupName = TableMetrics.class.getPackage().getName();
            StringBuilder mbeanName = new StringBuilder();
            mbeanName.append(groupName).append(":");
            mbeanName.append("type=").append(type);
            mbeanName.append(",name=").append(metricName);
            return new CassandraMetricsRegistry.MetricName(groupName, type, metricName, "all", mbeanName.toString());
        }
    }

    @FunctionalInterface
    public interface ReleasableMetric
    {
        void release();
    }

    private static class GlobalTableGauge implements Gauge<Long>
    {
        private final String name;

        public GlobalTableGauge(String name)
        {
            this.name = name;
        }

        public Long getValue()
        {
            long total = 0;
            for (Metric cfGauge : ALL_TABLE_METRICS.get(name))
            {
                total = total + ((Gauge<? extends Number>) cfGauge).getValue().longValue();
            }
            return total;
        }
    }
}
