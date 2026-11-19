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

package org.apache.cassandra.schema;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.diag.DiagnosticEventService;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.DeflateCompressor;
import org.apache.cassandra.io.compress.EncryptorTest;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.io.compress.ZstdCompressor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.DseCompatRefusals;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.DseUpgradeGuards;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * In {@code internode_compatibility_mode: DSE_5_1} the node refuses to start while a table or a view is compressed with
 * a compressor DSE 5.1 does not read, encrypted, or given a {@code min_compress_ratio}; LZ4, Snappy, Deflate and no
 * compression are accepted, and the other modes check nothing. In the mode a schema statement, {@code allow_ddl} or
 * not, and a schema change received from another node that set such a compression are refused too.
 */
public class Dse51CompressorsTest extends CQLTester
{
    /** How the advice of the refusal and of its WARN has the sstables rewritten. */
    private static final String REWRITE_COMMANDS = "nodetool upgradesstables -a <keyspace> <table>, which rewrites " +
                                                   "the sstables of the table and of its secondary indexes";

    private static final String REWRITE = "rewrite the sstables of those with a compressor DSE 5.1 does not read or " +
                                          "encrypted (" + REWRITE_COMMANDS + "; a table named for its " +
                                          "min_compress_ratio alone needs no rewrite)";

    private static final CassandraRelevantProperties IGNORE_DSE51_COMPRESSION =
        CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE51_COMPRESSION;

    private InternodeCompatibilityMode modeBefore;

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @BeforeClass
    public static void enableViews()
    {
        // views need system_distributed, which the network setup creates
        requireNetwork();
        DatabaseDescriptor.setMaterializedViewsEnabled(true);
    }

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

    private KeyspaceMetadata keyspaceMetadata()
    {
        return Schema.instance.getKeyspaceMetadata(KEYSPACE);
    }

    private List<String> unreadable()
    {
        return Dse51Compressors.unreadable(List.of(keyspaceMetadata()));
    }

    private List<String> encrypted()
    {
        return Dse51Compressors.encrypted(List.of(keyspaceMetadata()));
    }

    private List<String> withMinCompressRatio()
    {
        return Dse51Compressors.withMinCompressRatio(List.of(keyspaceMetadata()));
    }

    /** A table of two int columns compressed as {@code compression} says. */
    private String createTableCompressedWith(String compression)
    {
        return createTable("CREATE TABLE %s (k int PRIMARY KEY, v int) WITH compression = " + compression);
    }

    @Test
    public void theCompressorsDse51ReadsAreAccepted() throws Throwable
    {
        String lz4 = createTableCompressedWith("{'class': 'LZ4Compressor'}");
        String snappy = createTableCompressedWith("{'class': 'SnappyCompressor'}");
        String deflate = createTableCompressedWith("{'class': 'DeflateCompressor'}");
        String none = createTableCompressedWith("{'enabled': false}");
        String byDefault = createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        assertThat(unreadable()).isEmpty();
        assertThat(encrypted()).isEmpty();
        for (String table : new String[]{ lz4, snappy, deflate, none, byDefault })
            assertThat(keyspaceMetadata().getTableOrViewNullable(table)).as(table).isNotNull();

        // nor does the schema of this software's own keyspaces hold another compressor
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        assertThatCode(Dse51Compressors::verifyAtStartup).doesNotThrowAnyException();
    }

    @Test
    public void anotherCompressorRefusesTheStartInDse51ModeOnly() throws Throwable
    {
        String zstd = createTableCompressedWith("{'class': 'ZstdCompressor'}");
        String noop = createTableCompressedWith("{'class': 'NoopCompressor'}");
        String adaptive = createTableCompressedWith("{'class': 'AdaptiveCompressor'}");
        String lz4 = createTableCompressedWith("{'class': 'LZ4Compressor'}");
        List<String> expected = List.of(KEYSPACE + '.' + zstd + " (ZstdCompressor)",
                                        KEYSPACE + '.' + noop + " (NoopCompressor)",
                                        KEYSPACE + '.' + adaptive + " (AdaptiveCompressor)");
        // the LZ4 table is not named
        assertThat(unreadable()).containsExactlyInAnyOrderElementsOf(expected).isSorted();
        assertThat(keyspaceMetadata().getTableOrViewNullable(lz4)).isNotNull();

        for (InternodeCompatibilityMode mode : new InternodeCompatibilityMode[]{ InternodeCompatibilityMode.NONE,
                                                                                 InternodeCompatibilityMode.DSE_6 })
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(mode);
            assertThatCode(Dse51Compressors::verifyAtStartup).as(mode.name()).doesNotThrowAnyException();
        }

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        assertThatThrownBy(Dse51Compressors::verifyAtStartup)
        .isInstanceOf(StartupException.class)
        .hasMessage(Dse51Compressors.refusal(unreadable(), List.of(), List.of()))
        .satisfies(e -> assertThat(((StartupException) e).returnCode).isEqualTo(StartupException.ERR_WRONG_CONFIG));

        // altered to a compressor DSE 5.1 reads, or dropped, without the mode (its schema is frozen): the start is
        // accepted
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        execute(String.format("ALTER TABLE %s.%s WITH compression = {'class': 'SnappyCompressor'}", KEYSPACE, zstd));
        execute(String.format("DROP TABLE %s.%s", KEYSPACE, noop));
        execute(String.format("DROP TABLE %s.%s", KEYSPACE, adaptive));
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        assertThatCode(Dse51Compressors::verifyAtStartup).doesNotThrowAnyException();
    }

    /**
     * A table given a {@code min_compress_ratio} refuses the start in {@code DSE_5_1}, with its own reason, as do the
     * statement and the JMX setting that would give it one; with {@code ignore_dse51_compression} the start goes on,
     * the refusal logged at WARN, and the statement that removes the ratio runs.
     */
    @Test
    public void aMinCompressRatioIsRefusedInDse51ModeAndTheStartCanBePassedOnce() throws Throwable
    {
        String ratio = createTableCompressedWith("{'class': 'LZ4Compressor', 'min_compress_ratio': 1.5}");
        String named = KEYSPACE + '.' + ratio + " (LZ4Compressor), min_compress_ratio 1.5";
        assertThat(withMinCompressRatio()).containsExactly(named);
        assertThat(unreadable()).isEmpty();

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        assertThatThrownBy(Dse51Compressors::verifyAtStartup)
        .isInstanceOf(StartupException.class)
        .hasMessage(Dse51Compressors.refusal(List.of(), List.of(), List.of(named)));

        try (WithProperties properties = new WithProperties().set(IGNORE_DSE51_COMPRESSION, true)
                                                             .set(INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
        {
            withLogAppender(Dse51Compressors.class, appender -> {
                Dse51Compressors.verifyAtStartup();
                assertThat(appender.list).hasSize(1);
                assertThat(appender.list.get(0).getLevel()).isEqualTo(Level.WARN);
                assertThat(appender.list.get(0).getFormattedMessage())
                .isEqualTo(Dse51Compressors.ignoredRefusal(List.of(), List.of(), List.of(named)))
                .isEqualTo("internode_compatibility_mode DSE_5_1 starts with " +
                           "-Dcassandra.internode_compatibility_mode.ignore_dse51_compression=true although " + named +
                           " is ignored: the sstables this node writes for DSE 5.1 have no uncompressed chunk. " +
                           "DSE 5.1 reads LZ4Compressor, SnappyCompressor and DeflateCompressor, every chunk " +
                           "compressed. Change the compression of the tables named to one of them without " +
                           "min_compress_ratio, or to none (ALTER TABLE or ALTER MATERIALIZED VIEW, with " +
                           "-Dcassandra.internode_compatibility_mode.allow_ddl=true), " + REWRITE + ", and restart " +
                           "the node without those properties");
                // a compressor DSE 5.1 does not read: the sstables written until then are unreadable to it
                assertThat(Dse51Compressors.ignoredRefusal(List.of("ks.z (ZstdCompressor)"), List.of(), List.of()))
                .endsWith(", and restart the node without those properties: until then a DSE 5.1 node could not open " +
                          "the sstables this node writes for the tables of those compressors");
            });

            // the property does not let a statement or a JMX setting give a table a ratio
            String lz4 = createTableCompressedWith("{'class': 'LZ4Compressor'}");
            String created = "created_with_ratio";
            assertRefused("CREATE TABLE " + KEYSPACE + '.' + created + " (k int PRIMARY KEY, v int) WITH " +
                          "compression = {'class': 'SnappyCompressor', 'min_compress_ratio': 2}",
                          List.of(), List.of(),
                          List.of(KEYSPACE + '.' + created + " (SnappyCompressor), min_compress_ratio 2.0"));
            assertRefused("ALTER TABLE " + KEYSPACE + '.' + lz4 + " WITH compression = {'class': 'LZ4Compressor', " +
                          "'min_compress_ratio': 1.5}",
                          List.of(), List.of(),
                          List.of(KEYSPACE + '.' + lz4 + " (LZ4Compressor), min_compress_ratio 1.5"));
            ColumnFamilyStore store = getColumnFamilyStore(KEYSPACE, lz4);
            assertThatThrownBy(() -> store.setCompressionParameters(Map.of("class", "LZ4Compressor",
                                                                           "min_compress_ratio", "1.5")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("internode_compatibility_mode DSE_5_1 refuses the compression parameters of " + KEYSPACE +
                        '.' + lz4 + " (LZ4Compressor), min_compress_ratio 1.5: the sstables this node writes for DSE " +
                        "5.1 have no uncompressed chunk, so it would be ignored. DSE 5.1 reads LZ4Compressor, " +
                        "SnappyCompressor and DeflateCompressor, every chunk compressed; use one of them without " +
                        "min_compress_ratio, or no compression");

            // the remedy: the ratio removed in the mode, after which the start is accepted without the property
            execute("ALTER TABLE " + KEYSPACE + '.' + ratio + " WITH compression = {'class': 'LZ4Compressor'}");
        }
        assertThat(withMinCompressRatio()).isEmpty();
        assertThatCode(Dse51Compressors::verifyAtStartup).doesNotThrowAnyException();
    }

    /**
     * The sstables on disk whose compression info names a compressor DSE 5.1 does not read, or encryption, are named
     * by table, those of a secondary index and of a table directory linked from the data directory included, snapshots,
     * backups and a hidden directory not of an index excluded; the compressors DSE 5.1 reads are not named, a
     * compression info that cannot be read is named apart, and the directory of a dropped table is not inspected.
     */
    @Test
    public void theSSTablesLeftWithACompressorDse51DoesNotReadAreNamed() throws Throwable
    {
        String tableName = createTableCompressedWith("{'class': 'LZ4Compressor'}");
        createIndex("CREATE INDEX t_v ON %s (v) USING 'legacy_local_table'");
        TableMetadata table = tableMetadata(tableName);
        Path directory = directoryOf(table);
        Path zstd = compressionInfo(directory.resolve("me-1-big-CompressionInfo.db"), "ZstdCompressor");
        compressionInfo(directory.resolve("me-2-big-CompressionInfo.db"), "LZ4Compressor");
        compressionInfo(directory.resolve("me-3-big-CompressionInfo.db"), DeflateCompressor.class.getName());
        Path index = compressionInfo(Files.createDirectories(directory.resolve(".t_v"))
                                          .resolve("me-4-big-CompressionInfo.db"), "Encryptor");
        Path unknown = compressionInfo(directory.resolve("me-5-big-CompressionInfo.db"),
                                       "com.example.NoSuchCompressor");
        compressionInfo(Files.createDirectories(directory.resolve(Directories.SNAPSHOT_SUBDIR).resolve("s"))
                             .resolve("me-6-big-CompressionInfo.db"), "ZstdCompressor");
        compressionInfo(Files.createDirectories(directory.resolve(Directories.BACKUPS_SUBDIR))
                             .resolve("me-7-big-CompressionInfo.db"), "ZstdCompressor");
        Path empty = Files.createFile(directory.resolve("me-8-big-CompressionInfo.db"));
        compressionInfo(Files.createDirectories(directory.resolve(".stray")).resolve("me-9-big-CompressionInfo.db"),
                        "ZstdCompressor");

        // a table directory on another disk, linked from the data directory
        TableMetadata linked = tableMetadata(createTableCompressedWith("{'class': 'LZ4Compressor'}"));
        Path link = directoryOf(linked);
        Path target = temporaryFolder.newFolder().toPath().resolve(link.getFileName());
        Files.move(link, target);
        Files.createSymbolicLink(link, target);
        compressionInfo(target.resolve("me-1-big-CompressionInfo.db"), "ZstdCompressor");

        // a dropped table, whose directory DSE 5.1 does not open either
        TableMetadata dropped = tableMetadata(createTableCompressedWith("{'class': 'LZ4Compressor'}"));
        Path droppedDirectory = directoryOf(dropped);
        execute("DROP TABLE " + KEYSPACE + '.' + dropped.name);
        compressionInfo(droppedDirectory.resolve("me-1-big-CompressionInfo.db"), "ZstdCompressor");

        Dse51Compressors.SSTablesOnDisk onDisk = Dse51Compressors.sstablesOnDisk(List.of(table, linked));
        String name = KEYSPACE + '.' + table.name;
        String linkedName = KEYSPACE + '.' + linked.name;
        assertThat(onDisk.unreadableByDse51).containsOnlyKeys(name, name + " index t_v", linkedName);
        assertThat(onDisk.unreadableByDse51.get(name))
        .containsExactly(zstd + " (ZstdCompressor)", unknown + " (com.example.NoSuchCompressor)");
        assertThat(onDisk.unreadableByDse51.get(name + " index t_v")).containsExactly(index + " (Encryptor)");
        assertThat(onDisk.unreadableByDse51.get(linkedName))
        .containsExactly(link.resolve("me-1-big-CompressionInfo.db") + " (ZstdCompressor)");
        assertThat(onDisk.notRead).containsExactly(empty + " (EOFException)");
        assertThat(Dse51Compressors.notReadWarning(onDisk.notRead))
        .isEqualTo("internode_compatibility_mode DSE_5_1 could not check the compressor of the sstables at " + empty +
                   " (EOFException): whether a DSE 5.1 node could open them is not known; check those paths before a " +
                   "rollback to DSE 5.1");
        // the paths named are capped
        List<String> paths = new ArrayList<>();
        for (int i = 0; i < 12; i++)
            paths.add("p" + i);
        assertThat(Dse51Compressors.notReadWarning(paths))
        .contains(" at p0, p1, p2, p3, p4, p5, p6, p7, p8, p9 and 2 more: ");

        // the start in the mode: the dropped table is not named
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        try
        {
            withLogAppender(Dse51Compressors.class, appender -> {
                assertThatThrownBy(Dse51Compressors::verifySSTablesAtStartup)
                .isInstanceOf(StartupException.class)
                .hasMessageStartingWith("internode_compatibility_mode DSE_5_1 refused: 4 sstables on disk are " +
                                        "compressed with a compressor DSE 5.1 does not read, or encrypted, although " +
                                        "the schema of their tables is not (" + name + ": 2 sstables, " + zstd)
                .hasMessageContaining("; " + linkedName + ": 1 sstable, ")
                .hasMessageNotContaining(droppedDirectory.toString())
                .satisfies(e -> assertThat(((StartupException) e).returnCode)
                                .isEqualTo(StartupException.ERR_WRONG_CONFIG));
                assertThat(appender.list).extracting(ILoggingEvent::getFormattedMessage)
                                         .containsExactly(Dse51Compressors.notReadWarning(onDisk.notRead));
            });
        }
        finally
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
            Files.delete(link);
            Files.move(target, link);
        }

        // the texts, for one table
        SortedMap<String, List<String>> one = new TreeMap<>(Map.of("ks.t", List.of(zstd + " (ZstdCompressor)")));
        assertThat(Dse51Compressors.filesRefusal(one))
        .isEqualTo("internode_compatibility_mode DSE_5_1 refused: 1 sstable on disk is compressed with a compressor " +
                   "DSE 5.1 does not read, or encrypted, although the schema of its table is not (ks.t: 1 sstable, " +
                   zstd + " (ZstdCompressor)): a rewrite of them did not finish, or did not run since the " +
                   "compression of that table changed, and a DSE 5.1 node could not open them. DSE 5.1 reads " +
                   "LZ4Compressor, SnappyCompressor and DeflateCompressor, every chunk compressed. Start this node " +
                   "once more in the mode with " +
                   "-Dcassandra.internode_compatibility_mode.ignore_dse51_compression=true, which logs this at WARN " +
                   "instead; then rewrite the sstables of that table (" + REWRITE_COMMANDS + "), and restart the " +
                   "node in the mode without that property");
        assertThat(Dse51Compressors.ignoredFilesRefusal(one))
        .isEqualTo("internode_compatibility_mode DSE_5_1 starts with " +
                   "-Dcassandra.internode_compatibility_mode.ignore_dse51_compression=true although 1 sstable on " +
                   "disk is compressed with a compressor DSE 5.1 does not read, or encrypted (ks.t: 1 sstable, " +
                   zstd + " (ZstdCompressor)): rewrite the sstables of that table (" + REWRITE_COMMANDS + "), and " +
                   "restart the node without that property: until then a DSE 5.1 node could not open them");
        // and the files named per table capped
        SortedMap<String, List<String>> many = new TreeMap<>(Map.of("ks.t", List.of("a", "b", "c", "d", "e"),
                                                                    "ks.u", List.of("f")));
        assertThat(Dse51Compressors.filesRefusal(many))
        .startsWith("internode_compatibility_mode DSE_5_1 refused: 6 sstables on disk are compressed with a " +
                    "compressor DSE 5.1 does not read, or encrypted, although the schema of their tables is not " +
                    "(ks.t: 5 sstables, a, b, c and 2 more; ks.u: 1 sstable, f): a rewrite of them did not finish, " +
                    "or did not run since the compression of those tables changed")
        .contains("; then rewrite the sstables of those tables (");
    }

    /**
     * A directory that cannot be listed is named apart, as a compression info that cannot be read is: an index
     * directory, after which the rest of the table directory is still checked, or the table directory itself. Skipped
     * on a file system without POSIX permissions, and for a user who reads every directory (root).
     */
    @Test
    public void aDirectoryThatCannotBeListedIsNamedApart() throws Throwable
    {
        Assume.assumeTrue("POSIX permissions",
                          FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
        String tableName = createTableCompressedWith("{'class': 'LZ4Compressor'}");
        createIndex("CREATE INDEX t_x ON %s (v) USING 'legacy_local_table'");
        TableMetadata table = tableMetadata(tableName);
        Path directory = directoryOf(table);
        Path zstd = compressionInfo(directory.resolve("me-1-big-CompressionInfo.db"), "ZstdCompressor");
        Path index = Files.createDirectories(directory.resolve(".t_x"));
        compressionInfo(index.resolve("me-2-big-CompressionInfo.db"), "ZstdCompressor");
        Path other = compressionInfo(directory.resolve("me-3-big-CompressionInfo.db"), "ZstdCompressor");
        Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(directory);
        Files.setPosixFilePermissions(index, Set.of());
        try
        {
            // a superuser reads the directory anyway
            Assume.assumeFalse("the directory can still be read", Files.isReadable(index));
            Dse51Compressors.SSTablesOnDisk onDisk = Dse51Compressors.sstablesOnDisk(List.of(table));
            assertThat(onDisk.unreadableByDse51).containsOnlyKeys(KEYSPACE + '.' + table.name);
            assertThat(onDisk.unreadableByDse51.get(KEYSPACE + '.' + table.name))
            .containsExactly(zstd + " (ZstdCompressor)", other + " (ZstdCompressor)");
            assertThat(onDisk.notRead).containsExactly(index + " (AccessDeniedException)");

            Files.setPosixFilePermissions(directory, Set.of());
            onDisk = Dse51Compressors.sstablesOnDisk(List.of(table));
            assertThat(onDisk.unreadableByDse51).isEmpty();
            assertThat(onDisk.notRead).containsExactly(directory + " (AccessDeniedException)");
        }
        finally
        {
            Files.setPosixFilePermissions(directory, permissions);
            Files.setPosixFilePermissions(index, permissions);
        }
    }

    /**
     * In {@code DSE_5_1} the sstables of the table of a secondary index are written with the new compression of its
     * base table at once (the node-local parameters the writers read), so that {@code upgradesstables -a} rewrites
     * them with it; outside the mode they keep the compression the index was created with until the next start, as
     * before.
     */
    @Test
    public void theTableOfAnIndexTakesTheNewCompressionOfItsBaseInDse51Mode() throws Throwable
    {
        for (InternodeCompatibilityMode mode : new InternodeCompatibilityMode[]{ InternodeCompatibilityMode.NONE,
                                                                                 InternodeCompatibilityMode.DSE_5_1 })
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
            createTableCompressedWith("{'class': 'ZstdCompressor'}");
            String index = createIndex("CREATE INDEX ON %s (v) USING 'legacy_local_table'");
            ColumnFamilyStore indexTable = getCurrentColumnFamilyStore().indexManager.getIndexByName(index)
                                                                                     .getBackingTable().get();
            assertThat(indexTable.metadata.getLocal().params.compression.klass()).isEqualTo(ZstdCompressor.class);

            DatabaseDescriptor.setInternodeCompatibilityMode(mode);
            try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
            {
                execute("ALTER TABLE %s WITH compression = {'class': 'LZ4Compressor'}");
            }
            assertThat(indexTable.metadata.getLocal().params.compression.klass())
            .as(mode.name())
            .isEqualTo(mode == InternodeCompatibilityMode.DSE_5_1 ? LZ4Compressor.class : ZstdCompressor.class);
            // no override at all outside the mode
            if (mode == InternodeCompatibilityMode.NONE)
                assertThat(indexTable.metadata.getLocal()).isSameAs(indexTable.metadata.get());
            // the other parameters of the table of an index stay those of an index table
            assertThat(indexTable.metadata.getLocal().params.gcGraceSeconds).as(mode.name()).isZero();
            // and a compaction of the index writes with them (a flush compresses with LZ4, flush_compression: fast)
            execute("INSERT INTO %s (k, v) VALUES (1, 1)");
            indexTable.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
            indexTable.forceMajorCompaction();
            assertThat(indexTable.getLiveSSTables()).hasSize(1);
            assertThat(indexTable.getLiveSSTables().iterator().next().getCompressionMetadata().compressor().getClass())
            .as(mode.name())
            .isEqualTo(mode == InternodeCompatibilityMode.DSE_5_1 ? LZ4Compressor.class : ZstdCompressor.class);
        }
    }

    /**
     * A table whose compression was changed to one DSE 5.1 reads after a compaction wrote a Zstd sstable of it, never
     * rewritten since, refuses the start in {@code DSE_5_1}, with the sstable named, though its schema is accepted;
     * {@code ignore_dse51_compression} logs it at WARN.
     */
    @Test
    public void anSSTableNotRewrittenRefusesTheStartInDse51Mode() throws Throwable
    {
        String table = createTableCompressedWith("{'class': 'ZstdCompressor'}");
        execute("INSERT INTO %s (k, v) VALUES (1, 1)");
        flush();
        execute("INSERT INTO %s (k, v) VALUES (2, 2)");
        flush();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        // a flush compresses with LZ4 (flush_compression: fast), a compaction with the compressor of the table
        cfs.forceMajorCompaction();
        execute("ALTER TABLE %s WITH compression = {'class': 'LZ4Compressor'}");
        assertThat(unreadable()).isEmpty();
        List<TableMetadata> tables = List.of(cfs.metadata());
        SortedMap<String, List<String>> left = Dse51Compressors.sstablesOnDisk(tables).unreadableByDse51;
        assertThat(left).containsOnlyKeys(KEYSPACE + '.' + table);
        assertThat(left.get(KEYSPACE + '.' + table)).hasSize(1);
        assertThat(left.get(KEYSPACE + '.' + table).get(0)).endsWith("-CompressionInfo.db (ZstdCompressor)");

        // nothing is checked outside the mode
        for (InternodeCompatibilityMode mode : new InternodeCompatibilityMode[]{ InternodeCompatibilityMode.NONE,
                                                                                 InternodeCompatibilityMode.DSE_6 })
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(mode);
            withLogAppender(Dse51Compressors.class, appender -> {
                assertThatCode(Dse51Compressors::verifySSTablesAtStartup).as(mode.name()).doesNotThrowAnyException();
                assertThat(appender.list).as(mode.name()).isEmpty();
            });
        }

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        assertThatCode(Dse51Compressors::verifyAtStartup).doesNotThrowAnyException();
        assertThatThrownBy(Dse51Compressors::verifySSTablesAtStartup)
        .isInstanceOf(StartupException.class)
        .hasMessageContaining(Dse51Compressors.filesRefusal(left));
        try (WithProperties properties = new WithProperties().set(IGNORE_DSE51_COMPRESSION, true))
        {
            withLogAppender(Dse51Compressors.class, appender -> {
                Dse51Compressors.verifySSTablesAtStartup();
                assertThat(appender.list).hasSize(1);
                assertThat(appender.list.get(0).getLevel()).isEqualTo(Level.WARN);
                assertThat(appender.list.get(0).getFormattedMessage())
                .isEqualTo(Dse51Compressors.ignoredFilesRefusal(left));
            });
        }

        // the other tests of this class check the start: the sstable is removed
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
        cfs.truncateBlocking();
        Util.spinAssertEquals(true, () -> Dse51Compressors.sstablesOnDisk(tables).unreadableByDse51.isEmpty(), 60);
    }

    private static TableMetadata tableMetadata(String table)
    {
        return Schema.instance.getTableMetadata(KEYSPACE, table);
    }

    /** The directory of {@code table} in the only data directory of the tests. */
    private static Path directoryOf(TableMetadata table)
    {
        List<File> directories = new Directories(table).getCFDirectories();
        assertThat(directories).hasSize(1);
        return directories.get(0).toPath();
    }

    /** A compression info file that names {@code compressor}, its first field, followed by placeholder bytes. */
    private static Path compressionInfo(Path path, String compressor) throws IOException
    {
        try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(path)))
        {
            out.writeUTF(compressor);
            out.writeInt(0);
        }
        return path;
    }

    /** A compressor class of another package is named in full, so that it is not taken for one DSE 5.1 reads. */
    @Test
    public void aCompressorOfAnotherPackageIsNamedInFull() throws Throwable
    {
        String custom = PassThroughCompressor.class.getName();
        String table = createTableCompressedWith("{'class': '" + custom + "'}");
        assertThat(unreadable()).containsExactly(KEYSPACE + '.' + table + " (" + custom + ')');
    }

    @Test
    public void theCompressorOfAViewIsCheckedToo() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");
        // the view is not built: only its definition is checked
        String view = createViewAsync("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                      "WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, k) " +
                                      "WITH compression = {'class': 'ZstdCompressor'}");
        // the base table, compressed with LZ4, is not named
        assertThat(unreadable()).containsExactly(KEYSPACE + '.' + view + " (ZstdCompressor)");
    }

    /** A table encrypted by this software is refused with its own reason, not as a compressor DSE 5.1 lacks. */
    @Test
    public void anEncryptedTableIsRefusedAsSuch() throws Throwable
    {
        String encrypted = createTableCompressedWith("{'class': 'Encryptor', " +
                                                     "'cipher_algorithm': 'AES/ECB/PKCS5Padding', " +
                                                     "'secret_key_strength': 128, 'key_provider': '" +
                                                     EncryptorTest.KeyProviderFactoryStub.class.getName() + "'}");
        String zstd = createTableCompressedWith("{'class': 'ZstdCompressor'}");
        assertThat(encrypted()).containsExactly(KEYSPACE + '.' + encrypted + " (Encryptor)");
        assertThat(unreadable()).containsExactly(KEYSPACE + '.' + zstd + " (ZstdCompressor)");

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        assertThatThrownBy(Dse51Compressors::verifyAtStartup)
        .isInstanceOf(StartupException.class)
        .hasMessage(Dse51Compressors.refusal(unreadable(), encrypted(), List.of()))
        .hasMessageContaining(KEYSPACE + '.' + encrypted + " (Encryptor) is encrypted, which this mode does not " +
                              "support");
    }

    @Test
    public void theRefusalNamesEveryTableAndTheFix()
    {
        // the remedy stays in the mode, so that every sstable stays at the version DSE 5.1 reads: one more start in
        // it, past the refusal, changes the compression and rewrites the sstables
        String fix = ". DSE 5.1 reads LZ4Compressor, SnappyCompressor and DeflateCompressor, every chunk compressed. " +
                     "Start this node once more in the mode with " +
                     "-Dcassandra.internode_compatibility_mode.ignore_dse51_compression=true, which logs this at " +
                     "WARN instead; then change the compression of the tables named to one of them without " +
                     "min_compress_ratio, or to none (ALTER TABLE or ALTER MATERIALIZED VIEW, with " +
                     "-Dcassandra.internode_compatibility_mode.allow_ddl=true too), " +
                     REWRITE + ", and restart the node in the mode without those properties";
        assertThat(Dse51Compressors.refusal(List.of("ks.t (ZstdCompressor)"), List.of(), List.of()))
        .isEqualTo("internode_compatibility_mode DSE_5_1 refused: ks.t (ZstdCompressor) is compressed with a " +
                   "compressor DSE 5.1 does not read, so a DSE 5.1 node could not open the sstables this node writes " +
                   "for it" + fix);
        assertThat(Dse51Compressors.refusal(List.of("ks.a (NoopCompressor)", "ks.b (ZstdCompressor)"), List.of(),
                                            List.of()))
        .isEqualTo("internode_compatibility_mode DSE_5_1 refused: ks.a (NoopCompressor), ks.b (ZstdCompressor) " +
                   "are compressed with a compressor DSE 5.1 does not read, so a DSE 5.1 node could not open the " +
                   "sstables this node writes for them" + fix);
        assertThat(Dse51Compressors.refusal(List.of(), List.of("ks.e (Encryptor)"), List.of()))
        .isEqualTo("internode_compatibility_mode DSE_5_1 refused: ks.e (Encryptor) is encrypted, which this mode " +
                   "does not support" + fix);
        List<String> twoEncrypted = List.of("ks.e (Encryptor)", "ks.f (Encryptor)");
        assertThat(Dse51Compressors.refusal(List.of("ks.t (ZstdCompressor)"), twoEncrypted, List.of()))
        .isEqualTo("internode_compatibility_mode DSE_5_1 refused: ks.t (ZstdCompressor) is compressed with a " +
                   "compressor DSE 5.1 does not read, so a DSE 5.1 node could not open the sstables this node writes " +
                   "for it; ks.e (Encryptor), ks.f (Encryptor) are encrypted, which this mode does not support" + fix);
        String r = "ks.r (LZ4Compressor), min_compress_ratio 1.5";
        assertThat(Dse51Compressors.refusal(List.of(), List.of(), List.of(r)))
        .isEqualTo("internode_compatibility_mode DSE_5_1 refused: ks.r (LZ4Compressor), min_compress_ratio 1.5 is " +
                   "ignored: the sstables this node writes for DSE 5.1 have no uncompressed chunk" + fix);
        String s = "ks.s (SnappyCompressor), min_compress_ratio 2.0";
        assertThat(Dse51Compressors.refusal(List.of(), List.of(), List.of(r, s)))
        .startsWith("internode_compatibility_mode DSE_5_1 refused: ks.r (LZ4Compressor), min_compress_ratio 1.5, " +
                    "ks.s (SnappyCompressor), min_compress_ratio 2.0 are ignored: ");
    }

    /**
     * In {@code DSE_5_1}, with {@code allow_ddl}, a {@code CREATE TABLE}, an {@code ALTER TABLE}, a
     * {@code CREATE MATERIALIZED VIEW} and an {@code ALTER MATERIALIZED VIEW} that would compress with Zstd, and a
     * {@code CREATE TABLE} and an {@code ALTER TABLE} that would encrypt, are refused, each counted, and change
     * nothing; one with a compressor DSE 5.1 reads, or another option, runs.
     */
    @Test
    public void aStatementSettingACompressorDse51DoesNotReadIsRefusedInDse51ModeWithAllowDdl() throws Throwable
    {
        String lz4 = createTableCompressedWith("{'class': 'LZ4Compressor'}");
        String base = createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");
        String view = createViewAsync("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s " +
                                      "WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, k)");
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
        {
            long before = DseCompatRefusals.count(Dse51Compressors.REFUSED_SCHEMA_CHANGE);
            String zstd = "{'class': 'ZstdCompressor'}";
            String created = "created_with_zstd";
            assertRefused("CREATE TABLE " + KEYSPACE + '.' + created + " (k int PRIMARY KEY, v int) WITH " +
                          "compression = " + zstd,
                          List.of(KEYSPACE + '.' + created + " (ZstdCompressor)"), List.of());
            assertThat(keyspaceMetadata().getTableOrViewNullable(created)).isNull();
            String createdEncrypted = "created_encrypted";
            assertRefused("CREATE TABLE " + KEYSPACE + '.' + createdEncrypted + " (k int PRIMARY KEY, v int) WITH " +
                          "compression = " + ENCRYPTOR,
                          List.of(), List.of(KEYSPACE + '.' + createdEncrypted + " (Encryptor)"));
            assertThat(keyspaceMetadata().getTableOrViewNullable(createdEncrypted)).isNull();

            assertRefused("ALTER TABLE " + KEYSPACE + '.' + lz4 + " WITH compression = " + zstd,
                          List.of(KEYSPACE + '.' + lz4 + " (ZstdCompressor)"), List.of());
            assertRefused("ALTER TABLE " + KEYSPACE + '.' + lz4 + " WITH compression = " + ENCRYPTOR,
                          List.of(), List.of(KEYSPACE + '.' + lz4 + " (Encryptor)"));
            assertThat(keyspaceMetadata().getTableOrViewNullable(lz4).params.compression.klass().getSimpleName())
            .isEqualTo("LZ4Compressor");

            String createdView = "view_with_zstd";
            assertRefused("CREATE MATERIALIZED VIEW " + KEYSPACE + '.' + createdView + " AS SELECT * FROM " +
                          KEYSPACE + '.' + base + " WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, k) WITH " +
                          "compression = " + zstd,
                          List.of(KEYSPACE + '.' + createdView + " (ZstdCompressor)"), List.of());
            assertRefused("ALTER MATERIALIZED VIEW " + KEYSPACE + '.' + view + " WITH compression = " + zstd,
                          List.of(KEYSPACE + '.' + view + " (ZstdCompressor)"), List.of());
            assertThat(DseCompatRefusals.count(Dse51Compressors.REFUSED_SCHEMA_CHANGE)).isEqualTo(before + 6);

            // a compressor DSE 5.1 reads, or another option of a table, runs
            execute("ALTER TABLE " + KEYSPACE + '.' + lz4 + " WITH compression = {'class': 'SnappyCompressor'}");
            execute("ALTER TABLE " + KEYSPACE + '.' + lz4 + " WITH comment = 'altered'");
            String deflate = createTableCompressedWith("{'class': 'DeflateCompressor'}");
            assertThat(keyspaceMetadata().getTableOrViewNullable(deflate)).isNotNull();
            assertThat(DseCompatRefusals.count(Dse51Compressors.REFUSED_SCHEMA_CHANGE)).isEqualTo(before + 6);
        }
    }

    /**
     * The schema announcement event of a statement that runs in {@code DSE_5_1} with {@code allow_ddl} carries the
     * statement, which the compressor check wraps ({@code AlterSchemaStatement.statementOf}).
     */
    @Test
    public void theAnnouncementOfAStatementInDse51ModeCarriesTheStatement() throws Throwable
    {
        String table = createTableCompressedWith("{'class': 'LZ4Compressor'}");
        boolean diagnosticsBefore = DatabaseDescriptor.diagnosticEventsEnabled();
        List<SchemaAnnouncementEvent> events = new CopyOnWriteArrayList<>();
        Consumer<SchemaAnnouncementEvent> listener = events::add;
        DatabaseDescriptor.setDiagnosticEventsEnabled(true);
        DiagnosticEventService.instance().subscribe(SchemaAnnouncementEvent.class, listener);
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
        {
            QueryProcessor.process("ALTER TABLE " + KEYSPACE + '.' + table + " WITH comment = 'announced'",
                                   ConsistencyLevel.ONE);
            Util.spinAssertEquals(true, () -> events.stream().anyMatch(Dse51CompressorsTest::announcesAStatement), 10);
            SchemaAnnouncementEvent announced = events.stream().filter(Dse51CompressorsTest::announcesAStatement)
                                                      .findFirst().orElseThrow();
            @SuppressWarnings("unchecked")
            Map<String, String> statement = (Map<String, String>) announced.toMap().get("statement");
            assertThat(statement).containsEntry("keyspace", KEYSPACE).containsEntry("table", table);
        }
        finally
        {
            DiagnosticEventService.instance().unsubscribe(listener);
            DatabaseDescriptor.setDiagnosticEventsEnabled(diagnosticsBefore);
        }
    }

    private static boolean announcesAStatement(SchemaAnnouncementEvent event)
    {
        return event.getType() == SchemaAnnouncementEvent.SchemaAnnouncementEventType.SCHEMA_TRANSFORMATION_ANNOUNCED &&
               event.toMap().containsKey("statement");
    }

    /**
     * The compression parameters set by JMX (a local override, which the start check does not see) are checked in
     * {@code DSE_5_1} as the statements are: Zstd and encryption refused and counted, LZ4 accepted; with {@code NONE}
     * they are set as before.
     */
    @Test
    public void theCompressionSetByJmxIsCheckedInDse51Mode() throws Throwable
    {
        String table = createTableCompressedWith("{'class': 'LZ4Compressor'}");
        ColumnFamilyStore store = getColumnFamilyStore(KEYSPACE, table);
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        long before = DseCompatRefusals.count(Dse51Compressors.REFUSED_SCHEMA_CHANGE);
        String reads = ". DSE 5.1 reads LZ4Compressor, SnappyCompressor and DeflateCompressor, every chunk " +
                       "compressed; use one of them without min_compress_ratio, or no compression";
        assertThatThrownBy(() -> store.setCompressionParameters(Map.of("class", "ZstdCompressor")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("internode_compatibility_mode DSE_5_1 refuses the compression parameters of " + KEYSPACE + '.' +
                    table + " (ZstdCompressor): a DSE 5.1 node could not open the sstables this node writes with " +
                    "them" + reads);
        Map<String, String> encryptor = Map.of("class", "Encryptor",
                                               "cipher_algorithm", "AES/ECB/PKCS5Padding",
                                               "secret_key_strength", "128",
                                               "key_provider", EncryptorTest.KeyProviderFactoryStub.class.getName());
        assertThatThrownBy(() -> store.setCompressionParameters(encryptor))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("internode_compatibility_mode DSE_5_1 refuses the compression parameters of " + KEYSPACE + '.' +
                    table + " (Encryptor): they encrypt, which this mode does not support" + reads);
        assertThat(DseCompatRefusals.count(Dse51Compressors.REFUSED_SCHEMA_CHANGE)).isEqualTo(before + 2);
        assertThat(store.getCompressionParameters()).containsEntry("class", LZ4Compressor.class.getName());

        store.setCompressionParameters(Map.of("class", "SnappyCompressor"));
        assertThat(store.getCompressionParameters()).containsEntry("class", SnappyCompressor.class.getName());
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        store.setCompressionParameters(Map.of("class", "ZstdCompressor"));
        assertThat(store.getCompressionParameters()).containsEntry("class", ZstdCompressor.class.getName());
        assertThat(DseCompatRefusals.count(Dse51Compressors.REFUSED_SCHEMA_CHANGE)).isEqualTo(before + 2);
    }

    /** Without {@code allow_ddl} the schema freeze refuses the statement first, with its own error. */
    @Test
    public void theSchemaFreezeRefusesTheStatementFirstInDse51Mode() throws Throwable
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        long before = DseCompatRefusals.count(Dse51Compressors.REFUSED_SCHEMA_CHANGE);
        assertThatThrownBy(() -> execute("CREATE TABLE " + KEYSPACE + ".frozen_zstd (k int PRIMARY KEY) WITH " +
                                         "compression = {'class': 'ZstdCompressor'}"))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageStartingWith(DseUpgradeGuards.schemaFrozen("DSE 5.1"));
        assertThat(DseCompatRefusals.count(Dse51Compressors.REFUSED_SCHEMA_CHANGE)).isEqualTo(before);
    }

    /** The same statements run with {@code NONE}, and in {@code DSE_6} with {@code allow_ddl}. */
    @Test
    public void theStatementsRunInTheOtherModes() throws Throwable
    {
        long before = DseCompatRefusals.count(Dse51Compressors.REFUSED_SCHEMA_CHANGE);
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        String withNone = createTableCompressedWith("{'class': 'ZstdCompressor'}");
        execute("ALTER TABLE " + KEYSPACE + '.' + withNone + " WITH compression = " + ENCRYPTOR);
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true))
        {
            String inDse6 = createTableCompressedWith("{'class': 'ZstdCompressor'}");
            execute("ALTER TABLE " + KEYSPACE + '.' + inDse6 + " WITH compression = " + ENCRYPTOR);
            assertThat(encrypted()).containsExactlyInAnyOrder(KEYSPACE + '.' + withNone + " (Encryptor)",
                                                              KEYSPACE + '.' + inDse6 + " (Encryptor)");
        }
        assertThat(DseCompatRefusals.count(Dse51Compressors.REFUSED_SCHEMA_CHANGE)).isEqualTo(before);
    }

    /**
     * In {@code DSE_5_1} a schema change from another node of this software (one not in the mode) that compresses a
     * table with Zstd is not applied, counted and logged at WARN; the whole schema of that node, which this node then
     * pulls, is not applied either, an unrelated table of the same keyspace held back with it; a push that does not
     * carry the table is applied. With {@code NONE} the change is applied.
     */
    @Test
    public void aSchemaChangeWithACompressorDse51DoesNotReadIsNotAppliedInDse51Mode() throws Throwable
    {
        InetAddressAndPort sender = InetAddressAndPort.getByName("127.0.0.240");
        MessagingService.instance().versions.set(sender, MessagingService.current_version);
        DefaultSchemaUpdateHandler handler = (DefaultSchemaUpdateHandler) Schema.instance.updateHandler;
        try
        {
            KeyspaceMetadata keyspace = keyspaceMetadata();
            TableMetadata zstdTable = TableMetadata.builder(KEYSPACE, "pushed_zstd")
                                                   .addPartitionKeyColumn("k", Int32Type.instance)
                                                   .compression(CompressionParams.zstd())
                                                   .build();
            TableMetadata unrelated = TableMetadata.builder(KEYSPACE, "pulled_unrelated")
                                                   .addPartitionKeyColumn("k", Int32Type.instance)
                                                   .build();
            long timestamp = FBUtilities.timestampMicros();
            Mutation zstdPush = SchemaKeyspace.makeCreateTableMutation(keyspace, zstdTable, timestamp).build();
            Collection<Mutation> pushed = List.of(zstdPush);
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
            assertThat(Dse51Compressors.refusedSchemaMutations(pushed))
            .containsExactly(KEYSPACE + ".pushed_zstd (ZstdCompressor)");
            long before = DseCompatRefusals.count(Dse51Compressors.REFUSED_SCHEMA_CHANGE);
            withLogAppender(DefaultSchemaUpdateHandler.class, appender -> {
                SchemaPushVerbHandler.instance.doVerb(Message.builder(Verb.SCHEMA_PUSH_REQ, pushed).from(sender)
                                                             .build());
                assertThat(appender.list.stream().map(ILoggingEvent::getFormattedMessage))
                .contains("Not applying a schema change from " + sender + " in internode_compatibility_mode DSE_5_1: " +
                          "it compresses [" + KEYSPACE + ".pushed_zstd (ZstdCompressor)] with a compressor DSE 5.1 " +
                          "does not read, encrypts them, or gives them a min_compress_ratio (DSE 5.1 reads " +
                          "LZ4Compressor, SnappyCompressor and DeflateCompressor, every chunk compressed). Only a " +
                          "node not in the mode sends such a change: this node's schema " +
                          "disagrees with that node's, and the schema this node pulls from it is not applied either, " +
                          "until the change is reverted there");
            });
            assertThat(keyspaceMetadata().getTableOrViewNullable("pushed_zstd")).isNull();
            assertThat(DseCompatRefusals.count(Dse51Compressors.REFUSED_SCHEMA_CHANGE)).isEqualTo(before + 1);

            // the answer to a pull of the sender's schema carries the table too: the unrelated table is held back with
            // it
            KeyspaceMetadata sendersKeyspace = keyspace.withSwapped(keyspace.tables.with(zstdTable).with(unrelated));
            Mutation pulledZstd = SchemaKeyspace.makeCreateTableMutation(sendersKeyspace, zstdTable, timestamp).build();
            Mutation pulledOther = SchemaKeyspace.makeCreateTableMutation(sendersKeyspace, unrelated, timestamp)
                                                 .build();
            List<Mutation> pulled = List.of(pulledZstd, pulledOther);
            handler.applyMutationsFromCoordinator(sender, pulled);
            assertThat(keyspaceMetadata().getTableOrViewNullable("pulled_unrelated")).isNull();
            assertThat(DseCompatRefusals.count(Dse51Compressors.REFUSED_SCHEMA_CHANGE)).isEqualTo(before + 2);

            // a push that does not carry the table is applied
            Collection<Mutation> unrelatedPush = List.of(pulledOther);
            SchemaPushVerbHandler.instance.doVerb(Message.builder(Verb.SCHEMA_PUSH_REQ, unrelatedPush).from(sender)
                                                         .build());
            assertThat(keyspaceMetadata().getTableOrViewNullable("pulled_unrelated")).isNotNull();
            assertThat(DseCompatRefusals.count(Dse51Compressors.REFUSED_SCHEMA_CHANGE)).isEqualTo(before + 2);

            // with NONE the push is applied
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
            assertThat(Dse51Compressors.refusedSchemaMutations(pushed)).isEmpty();
            SchemaPushVerbHandler.instance.doVerb(Message.builder(Verb.SCHEMA_PUSH_REQ, pushed).from(sender).build());
            assertThat(keyspaceMetadata().getTableOrViewNullable("pushed_zstd")).isNotNull();
            assertThat(DseCompatRefusals.count(Dse51Compressors.REFUSED_SCHEMA_CHANGE)).isEqualTo(before + 2);
        }
        finally
        {
            MessagingService.instance().versions.reset(sender);
        }
    }

    /** An encrypted table in a schema change is named as such, also when this node cannot load its parameters. */
    @Test
    public void anEncryptedTableInASchemaChangeIsNamedAsSuch()
    {
        KeyspaceMetadata keyspace = keyspaceMetadata();
        Map<String, String> options = Map.of("class", "Encryptor",
                                             "cipher_algorithm", "AES/ECB/PKCS5Padding",
                                             "secret_key_strength", "128",
                                             "key_provider", EncryptorTest.KeyProviderFactoryStub.class.getName());
        TableMetadata encrypted = TableMetadata.builder(KEYSPACE, "pushed_encrypted")
                                               .addPartitionKeyColumn("k", Int32Type.instance)
                                               .compression(CompressionParams.fromMap(options))
                                               .build();
        Mutation push = SchemaKeyspace.makeCreateTableMutation(keyspace, encrypted, FBUtilities.timestampMicros())
                                      .build();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        assertThat(Dse51Compressors.refusedSchemaMutations(List.of(push)))
        .containsExactly(KEYSPACE + ".pushed_encrypted (Encryptor), encrypted");

        // the Encryptor is told by its class, its key provider not loaded; a map this node cannot load is named by
        // its class
        Map<String, String> unloadable = Map.of("class", "Encryptor", "key_provider", "no.such.KeyProvider");
        assertThat(Dse51Compressors.refusal("ks", "e", unloadable)).isEqualTo("ks.e (Encryptor), encrypted");
        assertThat(Dse51Compressors.refusal("ks", "f", Map.of("class", "org.apache.cassandra.io.compress.Encryptor",
                                                               "key_provider", "no.such.KeyProvider")))
        .isEqualTo("ks.f (Encryptor), encrypted");
        assertThat(Dse51Compressors.refusal("ks", "u", Map.of("class", "com.example.NoSuchCompressor")))
        .isEqualTo("ks.u (com.example.NoSuchCompressor)");
        // a subclass of the Encryptor is encrypted too, told by its class
        assertThat(Dse51Compressors.refusal("ks", "s", Map.of("class", SubEncryptor.class.getName(),
                                                               "key_provider", "no.such.KeyProvider")))
        .isEqualTo("ks.s (" + SubEncryptor.class.getName() + "), encrypted");
        // no compression is read; a stored LZ4 map with a ratio, even one only the lenient parsing of stored schemas
        // accepts, is named with it
        assertThat(Dse51Compressors.refusal("ks", "n", Map.of("enabled", "false"))).isNull();
        assertThat(Dse51Compressors.refusal("ks", "l", Map.of("class", "LZ4Compressor", "chunk_length_in_kb", "16",
                                                               "min_compress_ratio", "NaN")))
        .isEqualTo("ks.l (LZ4Compressor), min_compress_ratio NaN");
        assertThat(Dse51Compressors.refusal("ks", "r", Map.of("class", "LZ4Compressor", "min_compress_ratio", "1.5")))
        .isEqualTo("ks.r (LZ4Compressor), min_compress_ratio 1.5");
    }

    @Test
    public void theRefusalOfAStatementNamesEveryTable()
    {
        String reads = ". DSE 5.1 reads LZ4Compressor, SnappyCompressor and DeflateCompressor, every chunk " +
                       "compressed; use one of them without min_compress_ratio, or no compression";
        assertThat(Dse51Compressors.statementRefusal(List.of("ks.t (ZstdCompressor)"), List.of(), List.of()))
        .isEqualTo("internode_compatibility_mode DSE_5_1 refuses the statement: ks.t (ZstdCompressor) would be " +
                   "compressed with a compressor DSE 5.1 does not read, so a DSE 5.1 node could not open the " +
                   "sstables this node writes for it" + reads);
        assertThat(Dse51Compressors.statementRefusal(List.of("ks.a (NoopCompressor)", "ks.b (ZstdCompressor)"),
                                                     List.of("ks.e (Encryptor)"), List.of()))
        .isEqualTo("internode_compatibility_mode DSE_5_1 refuses the statement: ks.a (NoopCompressor), ks.b " +
                   "(ZstdCompressor) would be compressed with a compressor DSE 5.1 does not read, so a DSE 5.1 node " +
                   "could not open the sstables this node writes for them; ks.e (Encryptor) would be encrypted, " +
                   "which this mode does not support" + reads);
        assertThat(Dse51Compressors.statementRefusal(List.of(), List.of(),
                                                     List.of("ks.r (LZ4Compressor), min_compress_ratio 1.5")))
        .isEqualTo("internode_compatibility_mode DSE_5_1 refuses the statement: ks.r (LZ4Compressor), " +
                   "min_compress_ratio 1.5 would be ignored: the sstables this node writes for DSE 5.1 have no " +
                   "uncompressed chunk" + reads);
    }

    /** A subclass of the {@link org.apache.cassandra.io.compress.Encryptor}, never instantiated here. */
    public abstract static class SubEncryptor extends org.apache.cassandra.io.compress.Encryptor
    {
        protected SubEncryptor()
        {
            super(null);
        }
    }

    private static final String ENCRYPTOR = "{'class': 'Encryptor', 'cipher_algorithm': 'AES/ECB/PKCS5Padding', " +
                                            "'secret_key_strength': 128, 'key_provider': '" +
                                            EncryptorTest.KeyProviderFactoryStub.class.getName() + "'}";

    private void assertRefused(String statement, List<String> unreadable, List<String> encrypted)
    {
        assertRefused(statement, unreadable, encrypted, List.of());
    }

    private void assertRefused(String statement, List<String> unreadable, List<String> encrypted,
                               List<String> withRatio)
    {
        assertThatThrownBy(() -> execute(statement)).as(statement)
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage(Dse51Compressors.statementRefusal(unreadable, encrypted, withRatio));
    }

    /** A compressor outside {@code org.apache.cassandra.io.compress} that copies its input; no test runs it. */
    public static class PassThroughCompressor implements ICompressor
    {
        public static PassThroughCompressor create(Map<String, String> options)
        {
            return new PassThroughCompressor();
        }

        @Override
        public int initialCompressedBufferLength(int chunkLength)
        {
            return chunkLength;
        }

        @Override
        public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset)
        {
            System.arraycopy(input, inputOffset, output, outputOffset, inputLength);
            return inputLength;
        }

        @Override
        public void compress(ByteBuffer input, ByteBuffer output)
        {
            output.put(input);
        }

        @Override
        public void uncompress(ByteBuffer input, ByteBuffer output)
        {
            output.put(input);
        }

        @Override
        public BufferType preferredBufferType()
        {
            return BufferType.ON_HEAP;
        }

        @Override
        public boolean supports(BufferType bufferType)
        {
            return true;
        }

        @Override
        public Set<String> supportedOptions()
        {
            return Set.of();
        }
    }
}
