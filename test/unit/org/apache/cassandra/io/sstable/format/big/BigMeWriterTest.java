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

package org.apache.cassandra.io.sstable.format.big;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.jpountz.lz4.LZ4Factory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.IScrubber;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.StorageCompatibilityMode;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The writer of big {@code me} sstables of a node in {@code internode_compatibility_mode: DSE_5_1} and
 * {@code storage_compatibility_mode: DSE_5_1}, which this JVM is initialised in: the node writes {@code me} on flush,
 * compaction, scrub and upgradesstables, and the sstables DSE 5.1.51 wrote (the user sstables of the DSE 5.1.51
 * fixtures), rewritten here from the rows this software reads in them, come out with the very bytes DSE 5.1.51 wrote,
 * component by component, but for the order of the lines of {@code TOC.txt}; the host id and the commit log intervals
 * of the statistics, the only fields that depend on the node and its commit log rather than on the rows, are given
 * DSE's values. The big {@code nb} and {@code oa} sstables stay readable while {@code me} is written.
 */
public class BigMeWriterTest extends CQLTester
{
    private static final String KS = "test_ks";
    private static final String LZ4_64K = " WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': 64}";

    /** The components of a compressed big me sstable, which DSE 5.1.51 writes, but for the TOC. */
    private static final List<Component> COMPONENTS = List.of(Components.DATA, Components.COMPRESSION_INFO,
                                                              Components.DIGEST, Components.FILTER,
                                                              BigFormat.Components.PRIMARY_INDEX,
                                                              BigFormat.Components.SUMMARY, Components.STATS);

    /** The components that hold the compressed bytes, or are computed from them (the digest, the ratio). */
    private static final Set<Component> OF_THE_COMPRESSED_BYTES = Set.of(Components.DATA, Components.COMPRESSION_INFO,
                                                                         Components.DIGEST, Components.STATS);

    private static WithProperties allowDdl;

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    /** This JVM is the node in the DSE 5.1 compatibility mode, from its first initialisation on. */
    @BeforeClass
    public static void setUpClass()
    {
        // the schema is frozen in the mode
        allowDdl = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true);
        Config config = DatabaseDescriptor.loadConfig();
        config.internode_compatibility_mode = InternodeCompatibilityMode.DSE_5_1;
        config.storage_compatibility_mode = StorageCompatibilityMode.DSE_5_1;
        config.paxos_variant = Config.PaxosVariant.v1;
        config.uuid_sstable_identifiers_enabled = false;
        config.sstable.selected_format = BigFormat.NAME;
        DatabaseDescriptor.daemonInitialization(() -> config);
        CQLTester.setUpClass();
        Guardrails.instance.setCompactTablesEnabled(true);
        DatabaseDescriptor.setMaterializedViewsEnabled(true);
    }

    @AfterClass
    public static void clearAllowDdl()
    {
        allowDdl.close();
    }

    /**
     * Flush, compaction, scrub and upgradesstables (which the guards of the mode let run in it) all write big me; the
     * old sstables of a rewrite are replaced.
     */
    @Test
    public void theNodeWritesBigMeOnEveryRewrite() throws Throwable
    {
        assertThat(DatabaseDescriptor.isDse51CompatibilityMode()).isTrue();
        Version latest = BigFormat.getInstance().getLatestVersion();
        assertThat(latest.version).isEqualTo(InternodeCompatibilityMode.DSE_51_BIG_VERSION);
        assertThat(DatabaseDescriptor.getSelectedSSTableFormat().getLatestVersion()).isEqualTo(latest);

        createTable("CREATE TABLE %s (k int, c int, v text, l list<int>, PRIMARY KEY (k, c))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int flush = 0; flush < 2; flush++)
        {
            for (int k = 0; k < 10; k++)
                execute("INSERT INTO %s (k, c, v, l) VALUES (?, ?, ?, ?)", k, flush, "v" + k, List.of(k, flush));
            flush();
        }
        assertThat(cfs.getLiveSSTables()).hasSize(2);
        for (SSTableReader sstable : cfs.getLiveSSTables())
            assertWrittenForDse51(sstable);

        cfs.forceMajorCompaction();
        assertThat(cfs.getLiveSSTables()).hasSize(1);
        SSTableReader compacted = cfs.getLiveSSTables().iterator().next();
        assertWrittenForDse51(compacted);
        assertRowCount(execute("SELECT * FROM %s"), 20);
        assertRows(execute("SELECT v, l FROM %s WHERE k = 3 AND c = 1"), row("v3", List.of(3, 1)));

        assertThat(CompactionManager.instance.performScrub(cfs, new IScrubber.Options.Builder().checkData().build(), 1))
        .isEqualTo(CompactionManager.AllSSTableOpStatus.SUCCESSFUL);
        assertThat(cfs.getLiveSSTables()).hasSize(1).doesNotContain(compacted);
        SSTableReader scrubbed = cfs.getLiveSSTables().iterator().next();
        assertWrittenForDse51(scrubbed);

        assertThat(StorageService.instance.upgradeSSTables(KEYSPACE, false, cfs.name)).isZero();
        assertThat(cfs.getLiveSSTables()).hasSize(1).doesNotContain(scrubbed);
        assertWrittenForDse51(cfs.getLiveSSTables().iterator().next());
        assertRowCount(execute("SELECT * FROM %s"), 20);
        assertRows(execute("SELECT v, l FROM %s WHERE k = 3 AND c = 1"), row("v3", List.of(3, 1)));
    }

    /**
     * A row whose list is appended to and whose other list is assigned has a complex deletion for the second only: the
     * first's is LIVE, and the flush writes it in {@code Data.db} as Cassandra 3.x writes it, the delta from the
     * header's minimum local deletion time that a 32-bit reader adds to it to get {@code Integer.MAX_VALUE}, where the
     * same rows written at nb carry the zero delta of this software; this software reads both back as LIVE.
     */
    @Test
    public void aLiveComplexDeletionIsWrittenForA32BitReader() throws Throwable
    {
        // uncompressed, so that the bytes of the rows are those of the data file
        createTable("CREATE TABLE %s (k int PRIMARY KEY, l1 list<int>, l2 list<int>) " +
                    "WITH compression = {'enabled': false}");
        execute("UPDATE %s SET l1 = [1], l2 = l2 + [2] WHERE k = 0");
        flush();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader me = cfs.getLiveSSTables().iterator().next();
        assertThat(me.descriptor.version.version).isEqualTo("me");
        assertLiveAppendedList(me);
        assertRows(execute("SELECT l1, l2 FROM %s WHERE k = 0"), row(List.of(1), List.of(2)));

        Path work = temporaryFolder.newFolder().toPath();
        try
        {
            Path dir = Files.createDirectories(work.resolve(KEYSPACE).resolve(cfs.name));
            Descriptor nbDescriptor = new Descriptor(BigFormat.getInstance().getVersion("nb"), new File(dir), KEYSPACE,
                                                     cfs.name, new SequenceBasedSSTableId(1));
            rewrite(cfs, me, nbDescriptor, 1);
            byte[] meData = Files.readAllBytes(me.descriptor.fileFor(Components.DATA).toPath());
            byte[] nbData = Files.readAllBytes(nbDescriptor.fileFor(Components.DATA).toPath());
            // the LIVE deletion time of l2: at me, the timestamp delta of LIVE followed by the local deletion time
            // delta that a 32-bit reader adds to the minimum to get Integer.MAX_VALUE; at nb, followed by 0
            long minimum = me.header.stats().minLocalDeletionTime;
            long minTimestamp = me.header.stats().minTimestamp;
            byte[] live32 = vints(Long.MIN_VALUE - minTimestamp, Integer.MAX_VALUE - minimum);
            byte[] liveZero = vints(Long.MIN_VALUE - minTimestamp, 0);
            assertThat(indexOf(meData, live32)).isNotNegative();
            assertThat(indexOf(nbData, live32)).isNegative();
            assertThat(indexOf(nbData, liveZero)).isNotNegative();
            // the row is longer by the bytes of the delta, and its size, written before it, by one byte at most
            int longer = live32.length - liveZero.length;
            assertThat(meData.length).isBetween(nbData.length + longer, nbData.length + longer + 1);
            SSTableReader nb = SSTableReader.open(cfs, nbDescriptor, cfs.metadata);
            try
            {
                assertLiveAppendedList(nb);
            }
            finally
            {
                nb.selfRef().release();
            }
        }
        finally
        {
            FileUtils.deleteRecursive(new File(work));
        }
    }

    /** The unsigned vints of {@code values}, one after the other. */
    private static byte[] vints(long... values) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            for (long value : values)
                out.writeUnsignedVInt(value);
            return out.toByteArray();
        }
    }

    /** Whether this software compresses with the native LZ4 library, as DSE 5.1.51 did. */
    private static boolean nativeLz4()
    {
        try
        {
            return LZ4Factory.fastestInstance() == LZ4Factory.nativeInstance();
        }
        catch (LinkageError | RuntimeException e)
        {
            // the native library does not load here
            return false;
        }
    }

    /** The first position of {@code part} in {@code bytes}, or -1. */
    private static int indexOf(byte[] bytes, byte[] part)
    {
        for (int i = 0; i + part.length <= bytes.length; i++)
            if (Arrays.equals(bytes, i, i + part.length, part, 0, part.length))
                return i;
        return -1;
    }

    /** The row of {@link #aLiveComplexDeletionIsWrittenForA32BitReader}, as this software reads it back. */
    private static void assertLiveAppendedList(SSTableReader sstable)
    {
        try (ISSTableScanner scanner = sstable.getScanner(); UnfilteredRowIterator partition = scanner.next())
        {
            Row row = (Row) partition.next();
            ColumnMetadata l1 = sstable.metadata().getColumn(ColumnIdentifier.getInterned("l1", true));
            ColumnMetadata l2 = sstable.metadata().getColumn(ColumnIdentifier.getInterned("l2", true));
            ComplexColumnData assigned = row.getComplexColumnData(l1);
            ComplexColumnData appended = row.getComplexColumnData(l2);
            assertThat(assigned.complexDeletion().isLive()).isFalse();
            assertThat(appended.complexDeletion().isLive()).isTrue();
        }
    }

    /**
     * The six user sstables of the DSE 5.1.51 fixtures, rewritten from the rows read in them, come out with DSE's bytes
     * (see the class javadoc). The components that hold compressed bytes, or are computed from them, are compared with
     * what DSE 5.1.51 compressed with the native LZ4 library only where this software compresses with it too (another
     * implementation need not give the same bytes); the others are compared everywhere.
     */
    @Test
    public void theSSTablesOfDse51AreWrittenAsDse51WroteThem() throws Throwable
    {
        schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KS + " WITH replication = " +
                     "{'class': 'SimpleStrategy', 'replication_factor': 1}");
        schemaChange("CREATE TYPE IF NOT EXISTS " + KS + ".addr (street text, zip int)");
        schemaChange("CREATE TABLE " + KS + ".t (id int, ck text, fu frozen<addr>, l list<int>, m map<text, int>, " +
                     "n bigint, s int static, tp frozen<tuple<int, text>>, u addr, v text, PRIMARY KEY (id, ck))" +
                     LZ4_64K + " AND ID = 6facf040-d9c7-11f1-9bf3-35f92a6d7d8d");
        schemaChange("CREATE TABLE " + KS + ".wide (pk int, ck int, v text, PRIMARY KEY (pk, ck))" + LZ4_64K +
                     " AND ID = 70c51702-d9c7-11f1-9bf3-35f92a6d7d8d");
        schemaChange("CREATE MATERIALIZED VIEW " + KS + ".wide_by_v AS SELECT pk, ck, v FROM " + KS + ".wide " +
                     "WHERE v IS NOT NULL AND pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (v, pk, ck)" + LZ4_64K +
                     " AND ID = 712d4eb0-d9c7-11f1-9bf3-35f92a6d7d8d");
        // the SASI index is left out: this software has no SASI
        schemaChange("CREATE TABLE " + KS + ".sasi_t (id int PRIMARY KEY, v text)" + LZ4_64K +
                     " AND ID = 71b56a71-d9c7-11f1-9bf3-35f92a6d7d8d");
        schemaChange("CREATE TABLE " + KS + ".compact_dense (id int, ck int, v text, PRIMARY KEY (id, ck))" + LZ4_64K +
                     " AND COMPACT STORAGE AND ID = 72b5ea81-d9c7-11f1-9bf3-35f92a6d7d8d");
        schemaChange("CREATE TABLE " + KS + ".compact_static (id int PRIMARY KEY, a int, b text)" + LZ4_64K +
                     " AND COMPACT STORAGE AND ID = 72ffc4c0-d9c7-11f1-9bf3-35f92a6d7d8d");

        // rows (partitions) of each table, as DSE 5.1.51's flush holds them
        assertSameAsDse51("t", 100);
        assertSameAsDse51("wide", 10);
        assertSameAsDse51("wide_by_v", 7);
        assertSameAsDse51("sasi_t", 20);
        assertSameAsDse51("compact_dense", 5);
        assertSameAsDse51("compact_static", 5);
    }

    /**
     * The big nb and oa sstables, which this software writes outside the mode, are still read while me is written: a
     * flushed me sstable rewritten at each version is read back with the same rows.
     */
    @Test
    public void theBigNbAndOaSSTablesAreStillRead() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v text, s set<int>, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        for (int pk = 0; pk < 5; pk++)
            for (int ck = 0; ck < 3; ck++)
                execute("INSERT INTO %s (pk, ck, v, s) VALUES (?, ?, ?, ?)", pk, ck, "v" + ck, Set.of(pk, 10 + ck));
        execute("DELETE FROM %s WHERE pk = 4 AND ck = 1");
        flush();
        SSTableReader me = cfs.getLiveSSTables().iterator().next();
        assertWrittenForDse51(me);
        for (String version : new String[]{ "nb", "oa" })
        {
            Version written = BigFormat.getInstance().getVersion(version);
            assertThat(written.isCompatible()).as(version).isTrue();
            assertThat(written.isLatestVersion()).as(version).isFalse();
            Path work = temporaryFolder.newFolder().toPath();
            try
            {
                Path dir = Files.createDirectories(work.resolve(KEYSPACE).resolve(cfs.name));
                Descriptor descriptor = new Descriptor(written, new File(dir), KEYSPACE, cfs.name,
                                                       new SequenceBasedSSTableId(1));
                rewrite(cfs, me, descriptor, 5);
                SSTableReader reader = SSTableReader.open(cfs, descriptor, cfs.metadata);
                try
                {
                    assertThat(reader.descriptor.version.version).isEqualTo(version);
                    assertThat(contentOf(reader)).as(version).isEqualTo(contentOf(me));
                }
                finally
                {
                    reader.selfRef().release();
                }
            }
            finally
            {
                FileUtils.deleteRecursive(new File(work));
            }
        }
        // no version newer than oa is read
        assertThat(BigFormat.getInstance().getVersion("pa").isCompatible()).isFalse();
    }

    /**
     * A partition large enough for the row index blocks of {@code Index.db} ({@code IndexInfo}) is written at me with
     * several of them, read back through them (slices, both orders), and holds the same rows when rewritten at nb. A
     * round trip through this software only: no partition of the DSE 5.1.51 fixtures is large enough for a row index
     * block, so the bytes of the blocks are not compared with DSE's; a DSE 5.1.51 node reading them is left to the lab.
     */
    @Test
    public void aWidePartitionIsWrittenWithItsRowIndex() throws Throwable
    {
        int columnIndexSize = DatabaseDescriptor.getColumnIndexSizeInKiB();
        DatabaseDescriptor.setColumnIndexSizeInKiB(1);
        try
        {
            createTable("CREATE TABLE %s (pk int, ck int, v text, PRIMARY KEY (pk, ck))");
            String value = "x".repeat(100);
            for (int ck = 0; ck < 500; ck++)
                execute("INSERT INTO %s (pk, ck, v) VALUES (0, ?, ?)", ck, value + ck);
            flush();
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            BigTableReader me = (BigTableReader) cfs.getLiveSSTables().iterator().next();
            assertWrittenForDse51(me);
            DecoratedKey key = cfs.decorateKey(Int32Type.instance.decompose(0));
            RowIndexEntry entry = me.getRowIndexEntry(key, SSTableReader.Operator.EQ);
            assertThat(entry.blockCount()).isGreaterThan(10);

            assertRowCount(execute("SELECT * FROM %s WHERE pk = 0"), 500);
            assertRows(execute("SELECT ck FROM %s WHERE pk = 0 AND ck >= 250 AND ck < 253"),
                       row(250), row(251), row(252));
            assertRows(execute("SELECT ck FROM %s WHERE pk = 0 AND ck >= 250 AND ck < 253 ORDER BY ck DESC"),
                       row(252), row(251), row(250));

            Path dir = temporaryFolder.newFolder(KEYSPACE, cfs.name).toPath();
            Descriptor nbDescriptor = new Descriptor(BigFormat.getInstance().getVersion("nb"), new File(dir), KEYSPACE,
                                                     cfs.name, new SequenceBasedSSTableId(1));
            rewrite(cfs, me, nbDescriptor, 1);
            SSTableReader nb = SSTableReader.open(cfs, nbDescriptor, cfs.metadata);
            try
            {
                assertThat(contentOf(nb)).isEqualTo(contentOf(me));
            }
            finally
            {
                nb.selfRef().release();
            }
        }
        finally
        {
            DatabaseDescriptor.setColumnIndexSizeInKiB(columnIndexSize);
        }
    }

    /**
     * A writer of big me refuses a pending repair session (and with it a transient sstable, which only an sstable
     * pending repair can be), which its version cannot hold and the mode refuses before it gets there.
     */
    @Test
    public void aMeWriterRefusesAPendingRepair() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Path dir = temporaryFolder.newFolder(KEYSPACE, cfs.name).toPath();
        Descriptor descriptor = new Descriptor(BigFormat.getInstance().getLatestVersion(), new File(dir), KEYSPACE,
                                               cfs.name, new SequenceBasedSSTableId(1));
        for (boolean transientSSTable : new boolean[]{ false, true })
        {
            try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE, cfs.metadata))
            {
                MetadataCollector collector = new MetadataCollector(cfs.metadata().comparator);
                SerializationHeader header = SerializationHeader.makeWithoutStats(cfs.metadata());
                SSTableWriter.Builder<?, ?> builder = descriptor.getFormat().getWriterFactory().builder(descriptor);
                builder.setKeyCount(1)
                       .setTableMetadataRef(cfs.metadata)
                       .setMetadataCollector(collector)
                       .setSerializationHeader(header)
                       .setPendingRepair(TimeUUID.Generator.nextTimeUUID())
                       .setTransientSSTable(transientSSTable)
                       .addDefaultComponents(cfs.indexManager.listIndexGroups())
                       .setSecondaryIndexGroups(cfs.indexManager.listIndexGroups());
                assertThatThrownBy(() -> builder.build(txn, cfs))
                .as("transient %s", transientSSTable)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("sstable version me cannot hold the pending repair session ");
            }
        }
    }

    private static void assertWrittenForDse51(SSTableReader sstable)
    {
        assertThat(sstable.descriptor.version.version).isEqualTo("me");
        assertThat(sstable.descriptor.id).isInstanceOf(SequenceBasedSSTableId.class);
        StatsMetadata stats = sstable.getSSTableMetadata();
        assertThat(stats.pendingRepair).isEqualTo(ActiveRepairService.NO_PENDING_REPAIR);
        assertThat(stats.isTransient).isFalse();
        // the components DSE 5.1 writes
        Set<Component> expected = new HashSet<>(COMPONENTS);
        expected.add(Components.TOC);
        assertThat(sstable.components()).containsExactlyInAnyOrderElementsOf(expected);
    }

    /**
     * Rewrites the sstable DSE 5.1.51 wrote for {@code table} from the rows this software reads in it, with a writer of
     * the version the node writes, the host id and the commit log intervals of DSE's statistics, and compares every
     * component with DSE's.
     */
    private void assertSameAsDse51(String table, int partitions) throws Throwable
    {
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KS, table);
        Path fixture = DseFixtures.dse51().tableDirectory(DseFixtures.dse51().userSSTables().resolve(KS), table);
        Path work = temporaryFolder.newFolder().toPath();
        try
        {
            Path in = Files.createDirectories(work.resolve("in").resolve(KS).resolve(fixture.getFileName()));
            Path out = Files.createDirectories(work.resolve("out").resolve(KS).resolve(fixture.getFileName()));
            for (Path file : DseFixtures.dse51().sstableComponents(fixture))
                if (!file.getFileName().toString().contains("-SI_"))
                    Files.copy(file, in.resolve(file.getFileName()));
            Descriptor dseDescriptor = Descriptor.fromFile(new File(in.resolve("me-1-big-Data.db")));
            Descriptor ours = new Descriptor(BigFormat.getInstance().getLatestVersion(), new File(out), KS, table,
                                             new SequenceBasedSSTableId(1));
            SSTableReader dse = SSTableReader.open(cfs, dseDescriptor, cfs.metadata);
            try
            {
                assertThat(partitionsOf(dse)).as(table).isEqualTo(partitions);
                rewrite(cfs, dse, ours, partitions);
            }
            finally
            {
                dse.selfRef().release();
            }

            boolean nativeLz4 = nativeLz4();
            for (Component component : COMPONENTS)
            {
                if (!nativeLz4 && OF_THE_COMPRESSED_BYTES.contains(component))
                    continue;
                assertThat(ours.fileFor(component).toPath())
                .as("%s %s", table, component.name)
                .hasSameBinaryContentAs(dseDescriptor.fileFor(component).toPath());
            }
            // the TOC lists the same components, in another order
            assertThat(Files.readAllLines(ours.fileFor(Components.TOC).toPath()))
            .containsExactlyInAnyOrderElementsOf(Files.readAllLines(dseDescriptor.fileFor(Components.TOC).toPath()));

            // read back by this software, the rewritten sstable holds the rows DSE's does
            SSTableReader rewritten = SSTableReader.open(cfs, ours, cfs.metadata);
            SSTableReader original = SSTableReader.open(cfs, dseDescriptor, cfs.metadata);
            try
            {
                assertThat(contentOf(rewritten)).isEqualTo(contentOf(original));
            }
            finally
            {
                rewritten.selfRef().release();
                original.selfRef().release();
            }
        }
        finally
        {
            FileUtils.deleteRecursive(new File(work));
        }
    }

    private static void rewrite(ColumnFamilyStore cfs, SSTableReader dse, Descriptor ours, int partitions)
    {
        StatsMetadata dseStats = dse.getSSTableMetadata();
        try (LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.WRITE, cfs.metadata))
        {
            MetadataCollector collector = new MetadataCollector(cfs.metadata().comparator, dseStats.originatingHostId)
                                          .commitLogIntervals(dseStats.commitLogIntervals);
            // closed before it commits (a failed append), the writer aborts and removes its files
            try (SSTableWriter writer = ours.getFormat().getWriterFactory().builder(ours)
                                            .setKeyCount(partitions)
                                            .setTableMetadataRef(cfs.metadata)
                                            .setMetadataCollector(collector)
                                            .setSerializationHeader(dse.header)
                                            .addDefaultComponents(cfs.indexManager.listIndexGroups())
                                            .setSecondaryIndexGroups(cfs.indexManager.listIndexGroups())
                                            .build(txn, cfs);
                 ISSTableScanner scanner = dse.getScanner())
            {
                while (scanner.hasNext())
                {
                    try (UnfilteredRowIterator partition = scanner.next())
                    {
                        writer.append(partition);
                    }
                }
                writer.prepareToCommit();
                txn.prepareToCommit();
                Throwables.maybeFail(writer.commit(txn.commit(null)));
            }
        }
    }

    private static int partitionsOf(SSTableReader sstable)
    {
        int partitions = 0;
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    partitions++;
                }
            }
        }
        return partitions;
    }

    /** Every partition of the sstable: its key, deletion and static row, then its unfiltereds. */
    private static List<Object> contentOf(SSTableReader sstable)
    {
        List<Object> content = new ArrayList<>();
        try (ISSTableScanner scanner = sstable.getScanner())
        {
            while (scanner.hasNext())
            {
                try (UnfilteredRowIterator partition = scanner.next())
                {
                    content.add(partition.partitionKey());
                    content.add(partition.partitionLevelDeletion());
                    content.add(partition.staticRow());
                    while (partition.hasNext())
                        content.add(partition.next());
                }
            }
        }
        return content;
    }
}
