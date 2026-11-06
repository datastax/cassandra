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

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.compress.EncryptorTest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * In {@code internode_compatibility_mode: DSE_5_1} the node refuses to start while a table or a view is compressed with
 * a compressor DSE 5.1 does not read; LZ4, Snappy, Deflate and no compression are accepted, and the other modes check
 * nothing.
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
        String lz4 = createTableCompressedWith("{'class': 'LZ4Compressor'}");
        List<String> expected = List.of(KEYSPACE + '.' + zstd + " (ZstdCompressor)",
                                        KEYSPACE + '.' + noop + " (NoopCompressor)");
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

        // altered to a compressor DSE 5.1 reads, or dropped: the start is accepted
        execute(String.format("ALTER TABLE %s.%s WITH compression = {'class': 'SnappyCompressor'}", KEYSPACE, zstd));
        execute(String.format("DROP TABLE %s.%s", KEYSPACE, noop));
        assertThatCode(Dse51Compressors::verifyAtStartup).doesNotThrowAnyException();
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
        String fix = ". DSE 5.1 reads LZ4Compressor, SnappyCompressor and DeflateCompressor. Start this node without " +
                     "the mode, keeping sstable.selected_format: big and with uuid_sstable_identifiers_enabled: " +
                     "false, change the compression of the tables named to one of them, rewrite their sstables " +
                     "(nodetool upgradesstables -a), then start the node in the mode again";
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
}
