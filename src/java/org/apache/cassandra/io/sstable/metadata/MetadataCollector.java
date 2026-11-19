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
package org.apache.cassandra.io.sstable.metadata;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringBoundOrBoundary;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.partitions.PartitionStatisticsCollector;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MurmurHash;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.streamhist.StreamingTombstoneHistogramBuilder;
import org.apache.cassandra.utils.streamhist.TombstoneHistogram;

public class MetadataCollector implements PartitionStatisticsCollector
{
    public static final double NO_COMPRESSION_RATIO = -1.0;

    private long currentPartitionCells = 0;

    /**
     * The bucket count of the cell count histogram DSE 6 writes, the count before CASSANDRA-15164: DSE's
     * {@code MetadataCollector.defaultCellPerPartitionCountHistogram} asks for 114 buckets, 115 entries (114 bucket
     * offsets and the overflow bucket) in {@code Statistics.db}, in each of the 94 releases DSE 6.8.0 to 6.8.66 and
     * 6.9.0 to 6.9.26, and CASSANDRA-15164's 118 is in none of them (the sstables DSE 6.8.65 and 6.9.26 wrote under
     * {@code test/data/dse-compat} agree); outside {@code DSE_6} this software writes 118 buckets, 119
     * entries. DSE adds the histograms of a table's sstables up in {@code TableMetrics.combineHistograms}, the code
     * before CASSANDRA-16259, with no check that they have the same length: when a shorter histogram
     * follows a longer one it fails, as DSE 6.8.65's {@code nodetool tablehistograms} and insights reporting did after
     * a rollback with {@code Index 115 out of bounds for length 115} on a table holding sstables of both lengths; in the
     * other order it does not fail, but the counts of the sstables between the first one and the first longer histogram
     * are lost.
     */
    static final int DSE_6_CELL_COUNT_BUCKETS = 114;

    /**
     * The cell count histogram of a new sstable: in {@code internode_compatibility_mode: DSE_6}, and for a tool that
     * reads a {@code DSE_6} configuration ({@link DatabaseDescriptor#isDse6SSTableOutput()}), with DSE 6's
     * {@link #DSE_6_CELL_COUNT_BUCKETS} buckets, whose largest offset is 1996099046 cells: a partition with more
     * cells is counted in the overflow bucket, as DSE counts it. When this software reads such an sstable from disk it
     * clears that bucket with a WARN ({@link StatsMetadata}), but the reader a flush or a compaction opens keeps the
     * stats it collected, whose mean and percentiles throw until the sstable is opened again (for that table, a
     * tombstone compaction check, a read's estimate of its results or SAI's query planning can then fail), as with 118
     * buckets above 4139110981 cells. With 114 buckets this happens above 1996099046 cells, just under Cassandra's
     * documented limit of 2 billion cells per partition. With {@code NONE}, 118 buckets.
     */
    static EstimatedHistogram defaultCellPerPartitionCountHistogram()
    {
        if (DatabaseDescriptor.isDse6SSTableOutput())
            return new EstimatedHistogram(DSE_6_CELL_COUNT_BUCKETS);
        // EH of 118 can track a max value of 4139110981, i.e., > 4B cells
        return new EstimatedHistogram(118);
    }

    static EstimatedHistogram defaultPartitionSizeHistogram()
    {
        // EH of 150 can track a max value of 1414838745986, i.e., ~ 1.5PB
        // see {@link MetadataCollectorTest#testFindMaxSampleWithoutOverflow} for details
        return new EstimatedHistogram(150);
    }

    static TombstoneHistogram defaultTombstoneDropTimeHistogram()
    {
        return TombstoneHistogram.createDefault();
    }

    public static StatsMetadata defaultStatsMetadata()
    {
        return new StatsMetadata(defaultPartitionSizeHistogram(),
                                 defaultCellPerPartitionCountHistogram(),
                                 IntervalSet.empty(),
                                 Long.MIN_VALUE,
                                 Long.MAX_VALUE,
                                 Integer.MAX_VALUE,
                                 Integer.MAX_VALUE,
                                 0,
                                 Integer.MAX_VALUE,
                                 NO_COMPRESSION_RATIO,
                                 defaultTombstoneDropTimeHistogram(),
                                 0,
                                 Collections.emptyList(),
                                 Slice.ALL,
                                 true,
                                 ActiveRepairService.UNREPAIRED_SSTABLE,
                                 -1,
                                 -1,
                                 Double.NaN,
                                 null,
                                 null,
                                 false,
                                 true,
                                 ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                 ByteBufferUtil.EMPTY_BYTE_BUFFER,
                                 Collections.emptyMap(),
                                 ZeroCopyMetadata.EMPTY);
    }

    protected EstimatedHistogram estimatedPartitionSize = defaultPartitionSizeHistogram();
    // TODO: cound the number of row per partition (either with the number of cells, or instead)
    protected EstimatedHistogram estimatedCellPerPartitionCount = defaultCellPerPartitionCountHistogram();
    protected IntervalSet<CommitLogPosition> commitLogIntervals = IntervalSet.empty();
    protected final MinMaxLongTracker timestampTracker = new MinMaxLongTracker();
    protected final MinMaxLongTracker localDeletionTimeTracker = new MinMaxLongTracker(Cell.NO_DELETION_TIME, Cell.NO_DELETION_TIME);
    protected final MinMaxIntTracker ttlTracker = new MinMaxIntTracker(Cell.NO_TTL, Cell.NO_TTL);
    protected double compressionRatio = NO_COMPRESSION_RATIO;
    protected StreamingTombstoneHistogramBuilder estimatedTombstoneDropTime = new StreamingTombstoneHistogramBuilder(SSTable.TOMBSTONE_HISTOGRAM_BIN_SIZE, SSTable.TOMBSTONE_HISTOGRAM_SPOOL_SIZE, SSTable.TOMBSTONE_HISTOGRAM_TTL_ROUND_SECONDS);
    protected int sstableLevel;

    /**
     * The smallest clustering prefix for any {@link Unfiltered} in the sstable.
     *
     * <p>This is always either a Clustering, or a start bound (since for any end range tombstone bound, there should
     * be a corresponding start bound that is smaller).
     */
    private ClusteringPrefix<?> minClustering = ClusteringBound.MAX_START;
    /**
     * The largest clustering prefix for any {@link Unfiltered} in the sstable.
     *
     * <p>This is always either a Clustering, or an end bound (since for any start range tombstone bound, there should
     * be a corresponding end bound that is bigger).
     */
    private ClusteringPrefix<?> maxClustering = ClusteringBound.MIN_END;

    protected boolean hasLegacyCounterShards = false;
    private boolean hasPartitionLevelDeletions = false;
    protected long totalColumnsSet;
    protected long totalRows;
    public int totalTombstones;

    protected double tokenSpaceCoverage = Double.NaN;

    /**
     * Default cardinality estimation method is to use HyperLogLog++.
     * Parameter here(p=13, sp=25) should give reasonable estimation
     * while lowering bytes required to hold information.
     * See CASSANDRA-5906 for detail.
     */
    protected ICardinality cardinality = new HyperLogLogPlus(13, 25);
    private final ClusteringComparator comparator;
    private final long nowInSec = FBUtilities.nowInSeconds();

    private final UUID originatingHostId;

    public MetadataCollector(ClusteringComparator comparator)
    {
        this(comparator, StorageService.instance.getLocalHostUUID());
    }

    public MetadataCollector(ClusteringComparator comparator, UUID originatingHostId)
    {
        this.comparator = comparator;
        this.originatingHostId = originatingHostId;
    }

    public MetadataCollector(Iterable<SSTableReader> sstables, ClusteringComparator comparator)
    {
        this(comparator);

        IntervalSet.Builder<CommitLogPosition> intervals = new IntervalSet.Builder<>();
        if (originatingHostId != null)
        {
            for (SSTableReader sstable : sstables)
            {
                if (originatingHostId.equals(sstable.getSSTableMetadata().originatingHostId))
                    intervals.addAll(sstable.getSSTableMetadata().commitLogIntervals);
            }
        }
        commitLogIntervals(intervals.build());
    }

    public MetadataCollector(Iterable<SSTableReader> sstables, ClusteringComparator comparator, int level)
    {
        this(sstables, comparator);
        sstableLevel(level);
    }

    public MetadataCollector addKey(ByteBuffer key)
    {
        long hashed = MurmurHash.hash2_64(key, key.position(), key.remaining(), 0);
        cardinality.offerHashed(hashed);
        totalTombstones = 0;
        return this;
    }

    public MetadataCollector addPartitionSizeInBytes(long partitionSize)
    {
        estimatedPartitionSize.add(partitionSize);
        return this;
    }

    public MetadataCollector addCellPerPartitionCount(long cellCount)
    {
        estimatedCellPerPartitionCount.add(cellCount);
        return this;
    }

    public MetadataCollector addCellPerPartitionCount()
    {
        estimatedCellPerPartitionCount.add(currentPartitionCells);
        currentPartitionCells = 0;
        return this;
    }

    /**
     * Ratio is compressed/uncompressed and it is
     * if you have 1.x then compression isn't helping
     */
    public MetadataCollector addCompressionRatio(long compressed, long uncompressed)
    {
        compressionRatio = (double) compressed/uncompressed;
        return this;
    }

    public void update(LivenessInfo newInfo)
    {
        if (newInfo.isEmpty())
            return;

        updateTimestamp(newInfo.timestamp());
        updateTTL(newInfo.ttl());
        updateLocalDeletionTime(newInfo.localExpirationTime());
        if (!newInfo.isLive(nowInSec))
            updateTombstoneCount();
    }

    public void update(Cell<?> cell)
    {
        ++currentPartitionCells;
        updateTimestamp(cell.timestamp());
        updateTTL(cell.ttl());
        updateLocalDeletionTime(cell.localDeletionTime());
        if (!cell.isLive(nowInSec))
            updateTombstoneCount();
    }

    public void updatePartitionDeletion(DeletionTime dt)
    {
        if (!dt.isLive())
            hasPartitionLevelDeletions = true;
        update(dt);
    }

    public void update(DeletionTime dt)
    {
        if (!dt.isLive())
        {
            updateTimestamp(dt.markedForDeleteAt());
            updateLocalDeletionTime(dt.localDeletionTime());
            updateTombstoneCount();
        }
    }

    public void updateColumnSetPerRow(long columnSetInRow)
    {
        totalColumnsSet += columnSetInRow;
        ++totalRows;
    }

    private void updateTimestamp(long newTimestamp)
    {
        timestampTracker.update(newTimestamp);
    }

    private void updateLocalDeletionTime(long newLocalDeletionTime)
    {
        localDeletionTimeTracker.update(newLocalDeletionTime);
        if (newLocalDeletionTime != Cell.NO_DELETION_TIME)
            estimatedTombstoneDropTime.update(newLocalDeletionTime);
    }

    private void updateTombstoneCount()
    {
        ++totalTombstones;
    }

    private void updateTTL(int newTTL)
    {
        ttlTracker.update(newTTL);
    }

    public MetadataCollector commitLogIntervals(IntervalSet<CommitLogPosition> commitLogIntervals)
    {
        this.commitLogIntervals = commitLogIntervals;
        return this;
    }

    public MetadataCollector sstableLevel(int sstableLevel)
    {
        this.sstableLevel = sstableLevel;
        return this;
    }

    public MetadataCollector tokenSpaceCoverage(double coverage)
    {
        tokenSpaceCoverage = coverage;
        return this;
    }

    public void updateClusteringValues(Clustering<?> clustering)
    {
        if (clustering == Clustering.STATIC_CLUSTERING)
            return;

        // In case of monotonically growing stream of clusterings, we will usually require only one comparison
        // because if we detected X is greater than the current MAX, then it cannot be lower than the current MIN
        // at the same time. The only case when we need to update MIN when the current MAX was detected to be updated
        // is the case when MIN was not yet initialized and still point the ClusteringBound.MAX_START
        if (comparator.compare(clustering, maxClustering) > 0)
        {
            maxClustering = clustering;
            if (minClustering == ClusteringBound.MAX_START)
                minClustering = clustering;
        }
        else if (comparator.compare(clustering, minClustering) < 0)
        {
            minClustering = clustering;
        }
    }

    public void updateClusteringValuesByBoundOrBoundary(ClusteringBoundOrBoundary<?> clusteringBoundOrBoundary)
    {
        // In a SSTable, every opening marker will be closed, so the start of a range tombstone marker will never be
        // the maxClustering (the corresponding close might though) and there is no point in doing the comparison
        // (and vice-versa for the close). By the same reasoning, a boundary will never be either the min or max
        // clustering, and we can save on comparisons.
        if (clusteringBoundOrBoundary.isBoundary())
            return;

        // see the comment in updateClusteringValues(Clustering)
        if (comparator.compare(clusteringBoundOrBoundary, maxClustering) > 0)
        {
            if (clusteringBoundOrBoundary.kind().isEnd())
                maxClustering = clusteringBoundOrBoundary;

            // note that since we excluded boundaries above, there is no way that the provided clustering prefix is
            // a start and en end at the same time
            else if (minClustering == ClusteringBound.MAX_START)
                minClustering = clusteringBoundOrBoundary;
        }
        else if (comparator.compare(clusteringBoundOrBoundary, minClustering) < 0)
        {
            if (clusteringBoundOrBoundary.kind().isStart())
                minClustering = clusteringBoundOrBoundary;
            else if (maxClustering == ClusteringBound.MIN_END)
                maxClustering = clusteringBoundOrBoundary;
        }
    }

    public void updateHasLegacyCounterShards(boolean hasLegacyCounterShards)
    {
        this.hasLegacyCounterShards = this.hasLegacyCounterShards || hasLegacyCounterShards;
    }

    public Map<MetadataType, MetadataComponent> finalizeMetadata(String partitioner, double bloomFilterFPChance, long repairedAt, TimeUUID pendingRepair, boolean isTransient, SerializationHeader header, ByteBuffer firstKey, ByteBuffer lastKey)
    {
        assert minClustering.kind() == ClusteringPrefix.Kind.CLUSTERING || minClustering.kind().isStart();
        assert maxClustering.kind() == ClusteringPrefix.Kind.CLUSTERING || maxClustering.kind().isEnd();

        Map<MetadataType, MetadataComponent> components = new EnumMap<>(MetadataType.class);
        components.put(MetadataType.VALIDATION, new ValidationMetadata(partitioner, bloomFilterFPChance));
        components.put(MetadataType.STATS, new StatsMetadata(estimatedPartitionSize,
                                                             estimatedCellPerPartitionCount,
                                                             commitLogIntervals != null ? commitLogIntervals : IntervalSet.empty(),
                                                             timestampTracker.min(),
                                                             timestampTracker.max(),
                                                             localDeletionTimeTracker.min(),
                                                             localDeletionTimeTracker.max(),
                                                             ttlTracker.min(),
                                                             ttlTracker.max(),
                                                             compressionRatio,
                                                             estimatedTombstoneDropTime.build(),
                                                             sstableLevel,
                                                             comparator.subtypes(),
                                                             Slice.make(minClustering.retainable().asStartBound(), maxClustering.retainable().asEndBound()),
                                                             hasLegacyCounterShards,
                                                             repairedAt,
                                                             totalColumnsSet,
                                                             totalRows,
                                                             tokenSpaceCoverage,
                                                             originatingHostId,
                                                             pendingRepair,
                                                             isTransient,
                                                             hasPartitionLevelDeletions,
                                                             firstKey,
                                                             lastKey,
                                                             Collections.emptyMap(),
                                                             ZeroCopyMetadata.EMPTY));
        components.put(MetadataType.COMPACTION, new CompactionMetadata(cardinality));
        components.put(MetadataType.HEADER, header.toComponent());
        return components;
    }

    /**
     * Release large memory objects while keeping metrics intact
     */
    public void release()
    {
        estimatedTombstoneDropTime.releaseBuffers();
    }

    public static class MinMaxLongTracker
    {
        private final long defaultMin;
        private final long defaultMax;

        private boolean isSet = false;
        private long min;
        private long max;

        public MinMaxLongTracker()
        {
            this(Long.MIN_VALUE, Long.MAX_VALUE);
        }

        public MinMaxLongTracker(long defaultMin, long defaultMax)
        {
            this.defaultMin = defaultMin;
            this.defaultMax = defaultMax;
        }

        public void update(long value)
        {
            if (!isSet)
            {
                min = max = value;
                isSet = true;
            }
            else
            {
                if (value < min)
                    min = value;
                if (value > max)
                    max = value;
            }
        }

        public long min()
        {
            return isSet ? min : defaultMin;
        }

        public long max()
        {
            return isSet ? max : defaultMax;
        }
    }

    public static class MinMaxIntTracker
    {
        private final int defaultMin;
        private final int defaultMax;

        private boolean isSet = false;
        private int min;
        private int max;

        public MinMaxIntTracker()
        {
            this(Integer.MIN_VALUE, Integer.MAX_VALUE);
        }

        public MinMaxIntTracker(int defaultMin, int defaultMax)
        {
            this.defaultMin = defaultMin;
            this.defaultMax = defaultMax;
        }

        public void update(int value)
        {
            if (!isSet)
            {
                min = max = value;
                isSet = true;
            }
            else
            {
                if (value < min)
                    min = value;
                if (value > max)
                    max = value;
            }
        }

        public int min()
        {
            return isSet ? min : defaultMin;
        }

        public int max()
        {
            return isSet ? max : defaultMax;
        }
    }
}