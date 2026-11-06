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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import ch.qos.logback.classic.spi.ILoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.diag.DiagnosticEventService;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.EncryptorTest;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.io.compress.ZstdCompressor;
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
 * a compressor DSE 5.1 does not read; LZ4, Snappy, Deflate and no compression are accepted, and the other modes check
 * nothing. In the mode a schema statement, {@code allow_ddl} or not, and a schema change received from another node
 * that set such a compressor are refused too.
 */
public class Dse51CompressorsTest extends CQLTester
{
    private InternodeCompatibilityMode modeBefore;

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
        .hasMessage(Dse51Compressors.refusal(unreadable(), List.of()))
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
        .hasMessage(Dse51Compressors.refusal(unreadable(), encrypted()))
        .hasMessageContaining(KEYSPACE + '.' + encrypted + " (Encryptor) is encrypted, which this mode does not " +
                              "support");
    }

    @Test
    public void theRefusalNamesEveryTableAndTheFix()
    {
        // the remedy starts the node without the mode past the DSE 5.1 upgrade marker, which refuses such a start
        String fix = ". DSE 5.1 reads LZ4Compressor, SnappyCompressor and DeflateCompressor. Start this node without " +
                     "the mode, with -Dcassandra.internode_compatibility_mode.ignore_dse_peers=true (the DSE 5.1 " +
                     "upgrade marker this node wrote at its start in the mode refuses that start otherwise), keeping " +
                     "storage_compatibility_mode: HCD_1, sstable.selected_format: big and " +
                     "uuid_sstable_identifiers_enabled: false, change the compression of the tables named to one of " +
                     "them, rewrite their sstables (nodetool upgradesstables -a), then start the node in the mode " +
                     "again without that property";
        assertThat(Dse51Compressors.refusal(List.of("ks.t (ZstdCompressor)"), List.of()))
        .isEqualTo("internode_compatibility_mode DSE_5_1 refused: ks.t (ZstdCompressor) is compressed with a " +
                   "compressor DSE 5.1 does not read, so a DSE 5.1 node could not open the sstables this node writes " +
                   "for it" + fix);
        assertThat(Dse51Compressors.refusal(List.of("ks.a (NoopCompressor)", "ks.b (ZstdCompressor)"), List.of()))
        .isEqualTo("internode_compatibility_mode DSE_5_1 refused: ks.a (NoopCompressor), ks.b (ZstdCompressor) " +
                   "are compressed with a compressor DSE 5.1 does not read, so a DSE 5.1 node could not open the " +
                   "sstables this node writes for them" + fix);
        assertThat(Dse51Compressors.refusal(List.of(), List.of("ks.e (Encryptor)")))
        .isEqualTo("internode_compatibility_mode DSE_5_1 refused: ks.e (Encryptor) is encrypted, which this mode " +
                   "does not support" + fix);
        List<String> twoEncrypted = List.of("ks.e (Encryptor)", "ks.f (Encryptor)");
        assertThat(Dse51Compressors.refusal(List.of("ks.t (ZstdCompressor)"), twoEncrypted))
        .isEqualTo("internode_compatibility_mode DSE_5_1 refused: ks.t (ZstdCompressor) is compressed with a " +
                   "compressor DSE 5.1 does not read, so a DSE 5.1 node could not open the sstables this node writes " +
                   "for it; ks.e (Encryptor), ks.f (Encryptor) are encrypted, which this mode does not support" + fix);
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
        String reads = ". DSE 5.1 reads LZ4Compressor, SnappyCompressor and DeflateCompressor; use one of them, or " +
                       "no compression";
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
                          "does not read, or encrypts them (DSE 5.1 reads LZ4Compressor, SnappyCompressor and " +
                          "DeflateCompressor). Only a node not in the mode sends such a change: this node's schema " +
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
        // no compression, and a stored LZ4 map with a ratio only the lenient parsing of stored schemas accepts
        assertThat(Dse51Compressors.refusal("ks", "n", Map.of("enabled", "false"))).isNull();
        assertThat(Dse51Compressors.refusal("ks", "l", Map.of("class", "LZ4Compressor", "chunk_length_in_kb", "16",
                                                               "min_compress_ratio", "NaN"))).isNull();
    }

    @Test
    public void theRefusalOfAStatementNamesEveryTable()
    {
        assertThat(Dse51Compressors.statementRefusal(List.of("ks.t (ZstdCompressor)"), List.of()))
        .isEqualTo("internode_compatibility_mode DSE_5_1 refuses the statement: ks.t (ZstdCompressor) would be " +
                   "compressed with a compressor DSE 5.1 does not read, so a DSE 5.1 node could not open the " +
                   "sstables this node writes for it. DSE 5.1 reads LZ4Compressor, SnappyCompressor and " +
                   "DeflateCompressor; use one of them, or no compression");
        assertThat(Dse51Compressors.statementRefusal(List.of("ks.a (NoopCompressor)", "ks.b (ZstdCompressor)"),
                                                     List.of("ks.e (Encryptor)")))
        .isEqualTo("internode_compatibility_mode DSE_5_1 refuses the statement: ks.a (NoopCompressor), ks.b " +
                   "(ZstdCompressor) would be compressed with a compressor DSE 5.1 does not read, so a DSE 5.1 node " +
                   "could not open the sstables this node writes for them; ks.e (Encryptor) would be encrypted, " +
                   "which this mode does not support. DSE 5.1 reads LZ4Compressor, SnappyCompressor and " +
                   "DeflateCompressor; use one of them, or no compression");
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
        assertThatThrownBy(() -> execute(statement)).as(statement)
        .isInstanceOf(InvalidRequestException.class)
        .hasMessage(Dse51Compressors.statementRefusal(unreadable, encrypted));
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
