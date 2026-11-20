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

package org.apache.cassandra.io.sstable.metadata;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.dsecompat.DseFixtures.DseVersion;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.EstimatedHistogram;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The histograms of the stats component in {@code internode_compatibility_mode: DSE_6}: the cell count histogram has
 * DSE 6's 114 buckets (115 entries on disk), as the sstables DSE 6.8.65 and 6.9.26 wrote have, so that DSE can add
 * them up with its own after a rollback, and a compaction in {@code DSE_6} rewrites an earlier sstable's 118 into
 * DSE's 114; with {@code NONE} it keeps this software's 118 (CASSANDRA-15164). The partition size histogram has 150
 * buckets in both modes, as DSE's. The mode is switched at runtime, which changes the histogram but not the sstable
 * version this test's node writes: only the histograms are under test here (InternodeCompatibilityModeToolTest and
 * Dse6ModeSmokeTest cover the {@code bb} side).
 */
public class MetadataCollectorDse6Test extends CQLTester
{
    private InternodeCompatibilityMode modeBefore;

    @Before
    public void saveMode()
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
    }

    @After
    public void restoreMode()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
    }

    @Test
    public void theCellCountHistogramHasDsesBucketsInDse6ModeOnly() throws IOException
    {
        List<StatsMetadata> dseStats = dseWrittenStats();
        assertThat(dseStats).hasSize(DseVersion.values().length + 4);

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        EstimatedHistogram dse6 = MetadataCollector.defaultCellPerPartitionCountHistogram();
        assertThat(dse6.getBucketOffsets()).hasSize(MetadataCollector.DSE_6_CELL_COUNT_BUCKETS);
        assertThat(dse6.getLargestBucketOffset()).isEqualTo(1996099046L);
        assertThat(MetadataCollector.defaultStatsMetadata().estimatedCellPerPartitionCount.getBuckets(false)).hasSize(115);
        for (StatsMetadata dse : dseStats)
        {
            assertThat(dse.estimatedCellPerPartitionCount.getBuckets(false)).hasSize(115);
            assertThat(dse6.getBucketOffsets()).isEqualTo(dse.estimatedCellPerPartitionCount.getBucketOffsets());
            assertThat(MetadataCollector.defaultPartitionSizeHistogram().getBucketOffsets()).isEqualTo(dse.estimatedPartitionSize.getBucketOffsets());
        }

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        EstimatedHistogram none = MetadataCollector.defaultCellPerPartitionCountHistogram();
        assertThat(none.getBucketOffsets()).hasSize(118);
        assertThat(none.getLargestBucketOffset()).isEqualTo(4139110981L);
        assertThat(MetadataCollector.defaultStatsMetadata().estimatedCellPerPartitionCount.getBuckets(false)).hasSize(119);
        for (StatsMetadata dse : dseStats)
            assertThat(MetadataCollector.defaultPartitionSizeHistogram().getBucketOffsets()).isEqualTo(dse.estimatedPartitionSize.getBucketOffsets());
    }

    @Test
    public void aFlushInDse6ModeWritesDsesCellCountHistogramAndWithNoneItsOwn() throws Throwable
    {
        // the tables are created with NONE: only the flushes are under test
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        String dse6Table = createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))");
        String noneTable = createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))");

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        StatsMetadata dse6 = flushAndReadStats(dse6Table);
        assertThat(dse6.estimatedCellPerPartitionCount.getBuckets(false)).hasSize(115);
        assertThat(dse6.estimatedCellPerPartitionCount.getBucketOffsets()).isEqualTo(dseWrittenStats().get(0).estimatedCellPerPartitionCount.getBucketOffsets());
        assertThat(dse6.estimatedPartitionSize.getBuckets(false)).hasSize(151);

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        StatsMetadata none = flushAndReadStats(noneTable);
        assertThat(none.estimatedCellPerPartitionCount.getBuckets(false)).hasSize(119);
        assertThat(none.estimatedPartitionSize.getBuckets(false)).hasSize(151);
    }

    @Test
    public void aCompactionInDse6ModeRewritesTheHistogramOfAnEarlierSSTableWithDsesBuckets() throws Throwable
    {
        // an sstable written with NONE, or in DSE_6 by a build before DSE's bucket count, has 119 entries; a compaction
        // in DSE_6 writes its output with DSE's 115
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        String table = createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))");
        getColumnFamilyStore(KEYSPACE, table).disableAutoCompaction();
        assertThat(flushAndReadStats(table).estimatedCellPerPartitionCount.getBuckets(false)).hasSize(119);

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        writeRows(table, 3);
        flush(KEYSPACE, table);
        assertThat(getColumnFamilyStore(KEYSPACE, table).getLiveSSTables()).hasSize(2);
        compact(KEYSPACE, table);
        StatsMetadata compacted = readStats(table);
        assertThat(compacted.estimatedCellPerPartitionCount.getBuckets(false)).hasSize(115);
        assertThat(compacted.estimatedCellPerPartitionCount.getBucketOffsets()).isEqualTo(dseWrittenStats().get(0).estimatedCellPerPartitionCount.getBucketOffsets());
        assertThat(compacted.estimatedCellPerPartitionCount.count()).isEqualTo(6);
    }

    /**
     * Writes three partitions of two rows from partition 0 into {@code table}, flushes it and reads the stats of its one
     * sstable back from disk.
     */
    private StatsMetadata flushAndReadStats(String table) throws Throwable
    {
        writeRows(table, 0);
        flush(KEYSPACE, table);
        StatsMetadata stats = readStats(table);
        // each partition has two rows of one cell
        assertThat(stats.estimatedCellPerPartitionCount.count()).isEqualTo(3);
        assertThat(stats.estimatedCellPerPartitionCount.percentile(0.5)).isEqualTo(2);
        return stats;
    }

    private void writeRows(String table, int firstPk) throws Throwable
    {
        for (int pk = firstPk; pk < firstPk + 3; pk++)
        {
            execute("INSERT INTO " + KEYSPACE + '.' + table + " (pk, ck, v) VALUES (?, 0, 'a')", pk);
            execute("INSERT INTO " + KEYSPACE + '.' + table + " (pk, ck, v) VALUES (?, 1, 'b')", pk);
        }
    }

    /** The stats of the one live sstable of {@code table}, read back from its {@code Statistics.db}. */
    private StatsMetadata readStats(String table) throws IOException
    {
        List<SSTableReader> sstables = new ArrayList<>(getColumnFamilyStore(KEYSPACE, table).getLiveSSTables());
        assertThat(sstables).hasSize(1);
        Descriptor descriptor = sstables.get(0).descriptor;
        return (StatsMetadata) descriptor.getMetadataSerializer().deserialize(descriptor, MetadataType.STATS);
    }

    /**
     * The stats of sstables DSE wrote, among the fixtures: the collection table's of each DSE version of the fixtures,
     * and four more: a view's and its base table's on DSE 6.8.65, {@code system.paxos}'s on DSE 6.9.26 and
     * {@code system_auth.role_permissions}'s on DSE 6.8.65.
     */
    private static List<StatsMetadata> dseWrittenStats() throws IOException
    {
        List<StatsMetadata> stats = new ArrayList<>();
        for (DseVersion version : DseVersion.values())
            stats.add(stats(DseFixtures.sstableDirectory(version), 1));
        stats.add(stats(DseFixtures.viewSSTableDirectory(), 1));
        stats.add(stats(DseFixtures.viewBaseSSTableDirectory(), 1));
        stats.add(stats(DseFixtures.dse6926PaxosSSTableDirectory(), 4));
        stats.add(stats(DseFixtures.schemaDataDirectory().resolve("system_auth").resolve("role_permissions-3afbe79f219431a7add7f5ab90d8ec9c"), 1));
        return stats;
    }

    private static StatsMetadata stats(Path directory, int generation) throws IOException
    {
        Descriptor descriptor = new Descriptor("bb", new File(directory), "ks", "t", new SequenceBasedSSTableId(generation), BtiFormat.getInstance());
        assertThat(descriptor.fileFor(Components.STATS).exists()).as("%s", descriptor).isTrue();
        return (StatsMetadata) descriptor.getMetadataSerializer().deserialize(descriptor, MetadataType.STATS);
    }
}
