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

package org.apache.cassandra.distributed.test.dsecompat;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTransformations;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A node in {@code internode_compatibility_mode: DSE_5_1} refuses to start, once its schema is loaded, while a table is
 * compressed with a compressor DSE 5.1 does not read, and starts while every table uses one DSE 5.1 reads. The node
 * runs in the mode from its first start ({@link Dse51NewNodes}), and the Zstd table reaches its schema as no schema
 * statement in the mode can bring it, through the schema directly, as a table created on the node before the mode
 * would. The advice of the refusal, followed, keeps every sstable at big {@code me} and leads back to a start in the
 * mode. In the mode a {@code CREATE TABLE} with such a compressor is refused when it runs, {@code allow_ddl} or not.
 */
public class Dse51CompressorsStartupTest extends TestBaseImpl
{
    private static final CassandraRelevantProperties IGNORE_DSE51_COMPRESSION =
        CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE51_COMPRESSION;

    @Test
    public void aTableCompressedWithZstdRefusesTheStartInTheMode() throws Throwable
    {
        try (Cluster cluster = nodeInTheMode())
        {
            IInvokableInstance node = cluster.get(1);
            addZstdTable(node);
            node.shutdown().get();
            assertThatThrownBy(node::startup)
            .hasStackTraceContaining("internode_compatibility_mode DSE_5_1 refused: " + KEYSPACE + ".zstd " +
                                     "(ZstdCompressor) is compressed with a compressor DSE 5.1 does not read");
        }
    }

    /**
     * The advice of the refusal leads back to the mode with every sstable at big {@code me}: one start in the mode with
     * {@code ignore_dse51_compression}, which logs the refusals at WARN, in which the compression is changed (with
     * {@code allow_ddl}) and the sstables rewritten ({@code nodetool upgradesstables -a}, which the guards of the mode
     * let run, and which rewrites those of the secondary index too, whose table takes the compression of its base); the
     * next start in the mode, without the properties, is accepted. The refused start itself is the test above (an
     * in-JVM instance cannot start again after it).
     */
    @Test
    public void theAdviceOfTheRefusalLeadsBackToTheMode() throws Throwable
    {
        try (Cluster cluster = nodeInTheMode())
        {
            IInvokableInstance node = cluster.get(1);
            addZstdTable(node);
            // a secondary index, whose table takes the compression of its base table
            try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
            {
                cluster.schemaChange(withKeyspace("CREATE INDEX zstd_v ON %s.zstd (v) USING 'legacy_local_table'"));
            }
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.zstd (k, v) VALUES (1, 1)"),
                                           ConsistencyLevel.ONE);
            node.flush(KEYSPACE);
            // a flush compresses with LZ4 (flush_compression: fast), a compaction with the compressor of the table
            node.forceCompact(KEYSPACE, "zstd");
            node.runOnInstance(() -> indexTable(KEYSPACE, "zstd").forceMajorCompaction());
            assertThat(node.callOnInstance(() -> compressorsOfTheSSTables(KEYSPACE, "zstd")))
            .containsExactly("ZstdCompressor");
            assertThat(node.callOnInstance(() -> compressorsOfTheIndexSSTables(KEYSPACE, "zstd")))
            .containsExactly("ZstdCompressor");
            node.shutdown().get();

            try (WithProperties properties = new WithProperties().set(IGNORE_DSE51_COMPRESSION, true)
                                                                 .set(INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
            {
                node.startup();
                assertThat(node.logs().grep("internode_compatibility_mode DSE_5_1 starts with " +
                                            "-Dcassandra.internode_compatibility_mode.ignore_dse51_compression=true " +
                                            "although " + KEYSPACE + ".zstd \\(ZstdCompressor\\) is compressed")
                               .getResult())
                .hasSize(1);
                // and the same start names the sstables on disk, of the table and of its index
                assertThat(node.logs().grep("internode_compatibility_mode DSE_5_1 starts with " +
                                            "-Dcassandra.internode_compatibility_mode.ignore_dse51_compression=true " +
                                            "although 2 sstables on disk are compressed with a compressor DSE 5.1 " +
                                            "does not read, or encrypted \\(" + KEYSPACE + "\\.zstd: 1 sstable, " +
                                            ".*; " + KEYSPACE + "\\.zstd index zstd_v: 1 sstable, ")
                               .getResult())
                .hasSize(1);
                cluster.schemaChange(withKeyspace("ALTER TABLE %s.zstd WITH compression = {'class': 'LZ4Compressor'}"));
                node.nodetoolResult("upgradesstables", "-a", KEYSPACE, "zstd").asserts().success();
            }
            // every sstable of the table and of its index is now compressed with LZ4, which DSE 5.1 reads, and still
            // big me
            assertThat(node.callOnInstance(() -> compressorsOfTheSSTables(KEYSPACE, "zstd")))
            .containsExactly("LZ4Compressor");
            assertThat(node.callOnInstance(() -> versionsOfTheSSTables(KEYSPACE, "zstd"))).containsExactly("me");
            assertThat(node.callOnInstance(() -> compressorsOfTheIndexSSTables(KEYSPACE, "zstd")))
            .containsExactly("LZ4Compressor");
            assertThat(node.callOnInstance(() -> versionsOfTheIndexSSTables(KEYSPACE, "zstd"))).containsExactly("me");
            // the WARN of the start with the property, the only one about the sstables on disk until now
            assertThat(node.logs().grep("sstables? on disk").getResult()).hasSize(1);
            node.shutdown().get();

            node.startup();
            assertThat(node.callOnInstance(DatabaseDescriptor::isDse51CompatibilityMode)).isTrue();
            // the last start names no sstable on disk
            assertThat(node.logs().grep("sstables? on disk").getResult()).hasSize(1);
            assertThat(cluster.coordinator(1).execute(withKeyspace("SELECT v FROM %s.zstd WHERE k = 1"),
                                                      ConsistencyLevel.ONE))
            .isDeepEqualTo(new Object[][]{ { 1 } });
        }
    }

    /**
     * In the mode, with {@code allow_ddl}, a {@code CREATE TABLE} with Zstd is refused when the statement runs, so that
     * the table never reaches the schema and the next start; one with LZ4 runs.
     */
    @Test
    public void aCreateTableWithZstdIsRefusedInTheModeWithAllowDdl() throws Throwable
    {
        try (Cluster cluster = nodeInTheMode())
        {
            try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
            {
                String zstd = withKeyspace("CREATE TABLE %s.zstd (k int PRIMARY KEY, v int) " +
                                           "WITH compression = {'class': 'ZstdCompressor'}");
                assertThatThrownBy(() -> cluster.coordinator(1).execute(zstd, ConsistencyLevel.ONE))
                .hasMessageContaining("internode_compatibility_mode DSE_5_1 refuses the statement: " + KEYSPACE +
                                      ".zstd " +
                                      "(ZstdCompressor) would be compressed with a compressor DSE 5.1 does not read");
                cluster.schemaChange(withKeyspace("CREATE TABLE %s.lz4 (k int PRIMARY KEY, v int) " +
                                                  "WITH compression = {'class': 'LZ4Compressor'}"));
            }
            IInvokableInstance node = cluster.get(1);
            assertThat(node.callOnInstance(() -> Keyspace.open(KEYSPACE).getMetadata().getTableOrViewNullable("zstd")
                                                 == null)).isTrue();
            assertThat(node.callOnInstance(() -> Keyspace.open(KEYSPACE).getMetadata().getTableOrViewNullable("lz4")
                                                 != null)).isTrue();
            // the node starts in the mode again
            node.shutdown().get();
            node.startup();
            assertThat(node.callOnInstance(DatabaseDescriptor::isDse51CompatibilityMode)).isTrue();
        }
    }

    @Test
    public void tablesCompressedAsDse51ReadsStartInTheMode() throws Throwable
    {
        try (Cluster cluster = nodeInTheMode())
        {
            try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
            {
                for (String compressor : new String[]{ "LZ4Compressor", "SnappyCompressor", "DeflateCompressor" })
                    cluster.schemaChange(withKeyspace("CREATE TABLE %s." + compressor.toLowerCase() +
                                                      " (k int PRIMARY KEY, v int) WITH compression = {'class': '" +
                                                      compressor + "'}"));
            }
            IInvokableInstance node = cluster.get(1);
            node.shutdown().get();
            node.startup();
            assertThat(node.callOnInstance(DatabaseDescriptor::isDse51CompatibilityMode)).isTrue();
            // announced at each start in the mode, with the version the node writes
            assertThat(node.logs().grep("Running in internode_compatibility_mode DSE_5_1: the node writes big me " +
                                        "sstables, which DSE 5.1 reads, with numeric generations").getResult())
            .hasSize(2);
        }
    }

    /** The simple names of the compressors of the live sstables of the table, run inside an instance. */
    private static Set<String> compressorsOfTheSSTables(String keyspace, String table)
    {
        Set<String> compressors = new HashSet<>();
        for (SSTableReader sstable : Keyspace.open(keyspace).getColumnFamilyStore(table).getLiveSSTables())
            compressors.add(sstable.getCompressionMetadata().compressor().getClass().getSimpleName());
        return compressors;
    }

    /** The table of the one secondary index of the table, run inside an instance. */
    private static ColumnFamilyStore indexTable(String keyspace, String table)
    {
        return Keyspace.open(keyspace).getColumnFamilyStore(table).indexManager.getAllIndexColumnFamilyStores()
                       .iterator().next();
    }

    /** The simple names of the compressors of the live sstables of the index of the table, run inside an instance. */
    private static Set<String> compressorsOfTheIndexSSTables(String keyspace, String table)
    {
        Set<String> compressors = new HashSet<>();
        for (SSTableReader sstable : indexTable(keyspace, table).getLiveSSTables())
            compressors.add(sstable.getCompressionMetadata().compressor().getClass().getSimpleName());
        return compressors;
    }

    /** The versions of the live sstables of the index of the table, run inside an instance. */
    private static Set<String> versionsOfTheIndexSSTables(String keyspace, String table)
    {
        Set<String> versions = new HashSet<>();
        for (SSTableReader sstable : indexTable(keyspace, table).getLiveSSTables())
            versions.add(sstable.descriptor.version.version);
        return versions;
    }

    /** The versions of the live sstables of the table, run inside an instance. */
    private static Set<String> versionsOfTheSSTables(String keyspace, String table)
    {
        Set<String> versions = new HashSet<>();
        for (SSTableReader sstable : Keyspace.open(keyspace).getColumnFamilyStore(table).getLiveSSTables())
            versions.add(sstable.descriptor.version.version);
        return versions;
    }

    /**
     * A Zstd table added to the schema of the node directly, as no statement in the mode can add it: a table created
     * before the mode, or by a node without it.
     */
    private static void addZstdTable(IInvokableInstance node)
    {
        node.runOnInstance(() -> {
            TableMetadata table = TableMetadata.builder(KEYSPACE, "zstd")
                                               .addPartitionKeyColumn("k", Int32Type.instance)
                                               .addRegularColumn("v", Int32Type.instance)
                                               .compression(CompressionParams.zstd())
                                               .build();
            Schema.instance.transform(SchemaTransformations.addTable(table, false));
        });
    }

    /**
     * A column of a type DSE 5.1 cannot read refuses the start in the mode, before the compressor of its table, since
     * its only remedy, the flag drop, ends the check of the compressors too.
     */
    @Test
    public void aColumnTypeDse51CannotReadRefusesTheStartBeforeTheCompressor() throws Throwable
    {
        try (Cluster cluster = nodeInTheMode())
        {
            IInvokableInstance node = cluster.get(1);
            // a schema the mode never writes, as a node outside the mode would
            node.runOnInstance(() -> {
                TableMetadata table = TableMetadata.builder(KEYSPACE, "both")
                                                   .addPartitionKeyColumn("k", Int32Type.instance)
                                                   .addRegularColumn("e", VectorType.getInstance(FloatType.instance, 2))
                                                   .compression(CompressionParams.zstd())
                                                   .build();
                Schema.instance.transform(SchemaTransformations.addTable(table, false));
            });
            node.shutdown().get();
            assertThatThrownBy(node::startup)
            .hasStackTraceContaining("internode_compatibility_mode DSE_5_1 refused: " + KEYSPACE + ".both.e (")
            .hasStackTraceContaining(") is of a type DSE 5.1 cannot read, so this node cannot write the sstables DSE " +
                                     "5.1 reads for it, and its rollback to DSE 5.1 is no longer possible")
            .hasStackTraceContaining("End the mode on this node")
            .satisfies(e -> assertThat(stackTrace(e)).doesNotContain("(ZstdCompressor) is compressed"));
        }
    }

    private static String stackTrace(Throwable t)
    {
        StringWriter out = new StringWriter();
        t.printStackTrace(new PrintWriter(out));
        return out.toString();
    }

    /** One node of this software in the mode, with the test keyspace. */
    private static Cluster nodeInTheMode() throws IOException
    {
        Cluster cluster = Cluster.build(1)
                                 .withDynamicPortAllocation(true)
                                 .withInstanceInitializer(Dse51NewNodes::install)
                                 .withConfig(Dse51NewNodes::configure)
                                 .start();
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
        {
            init(cluster);
        }
        return cluster;
    }
}
