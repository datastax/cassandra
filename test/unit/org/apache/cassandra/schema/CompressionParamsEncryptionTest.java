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

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Throwables;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.EncryptionConfig;
import org.apache.cassandra.io.compress.Encryptor;
import org.apache.cassandra.io.compress.EncryptorTest;
import org.apache.cassandra.io.compress.LZ4Compressor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code min_compress_ratio} lets a chunk that does not compress well enough be stored as is, which for an
 * encrypting compressor means in plaintext. The combination must therefore be rejected at schema validation time,
 * whether or not the (optional, defaulted) encryption options are spelled out. Older versions did accept and store
 * it, so such stored definitions must still load, normalised to always encrypt.
 */
public class CompressionParamsEncryptionTest extends CQLTester
{
    private static final String KEY_PROVIDER = EncryptorTest.KeyProviderFactoryStub.class.getName();
    private static final String REJECTION_MESSAGE = CompressionParams.MIN_COMPRESS_RATIO + " must be 0 or omitted when using encrypting compressor Encryptor";

    private static Map<String, String> encryptionOptions(boolean withCipherAlgorithm, String minCompressRatio)
    {
        Map<String, String> opts = new HashMap<>();
        opts.put(CompressionParams.CLASS, Encryptor.class.getName());
        opts.put(EncryptionConfig.KEY_PROVIDER, KEY_PROVIDER);
        if (withCipherAlgorithm)
        {
            opts.put(EncryptionConfig.CIPHER_ALGORITHM, "AES/ECB/PKCS5Padding");
            opts.put(EncryptionConfig.SECRET_KEY_STRENGTH, "128");
        }
        if (minCompressRatio != null)
            opts.put(CompressionParams.MIN_COMPRESS_RATIO, minCompressRatio);
        return opts;
    }

    private static void assertRejected(Map<String, String> opts)
    {
        assertThatThrownBy(() -> CompressionParams.fromMap(opts))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining(REJECTION_MESSAGE)
        .hasMessageContaining("unencrypted");
    }

    @Test
    public void testMinCompressRatioRejectedWithEncryption()
    {
        assertRejected(encryptionOptions(true, "1.1"));
        assertRejected(encryptionOptions(true, "2"));
        // cipher_algorithm and secret_key_strength have defaults: the decision must not depend on their presence
        assertRejected(encryptionOptions(false, "1.1"));
        // "1" is the boundary case: maxCompressedLength == chunkLength
        assertRejected(encryptionOptions(false, "1"));
        // Out of range for any compressor, but the encryption error is the relevant one
        assertRejected(encryptionOptions(true, "0.5"));
        assertRejected(encryptionOptions(false, "0.5"));
        // Negative zero is not 0: it yields a negative maxCompressedLength, and must get the encryption error before
        // the Encryptor (and its key provider) is created
        assertRejected(encryptionOptions(true, "-0"));
        assertRejected(encryptionOptions(false, "-0"));
    }

    @Test
    public void testNegligibleMinCompressRatioAcceptedWithEncryption()
    {
        // A ratio so small that maxCompressedLength saturates to Integer.MAX_VALUE is equivalent to 0: every chunk is
        // encrypted, so the pre-check and validate() agree to accept it
        CompressionParams params = CompressionParams.fromMap(encryptionOptions(false, "1e-10"));
        assertThat(params.getSstableCompressor().encryptionOnly()).isNotNull();
        assertThat(params.maxCompressedLength()).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    public void testInvalidMinCompressRatioRejected()
    {
        for (String ratio : new String[]{ "NaN", "Infinity", "-Infinity", "abc", "" })
        {
            for (String klass : new String[]{ LZ4Compressor.class.getName(), Encryptor.class.getName() })
            {
                Map<String, String> opts = encryptionOptions(false, ratio);
                opts.put(CompressionParams.CLASS, klass);
                if (!klass.equals(Encryptor.class.getName()))
                    opts.remove(EncryptionConfig.KEY_PROVIDER);
                assertThatThrownBy(() -> CompressionParams.fromMap(opts))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("Invalid value for " + CompressionParams.MIN_COMPRESS_RATIO);
            }
        }
    }

    @Test
    public void testEncryptionWithoutMinCompressRatioAlwaysEncrypts()
    {
        for (boolean withCipherAlgorithm : new boolean[]{ true, false })
        {
            for (String ratio : new String[]{ null, "0" })
            {
                CompressionParams params = CompressionParams.fromMap(encryptionOptions(withCipherAlgorithm, ratio));
                assertThat(params.getSstableCompressor().encryptionOnly()).isNotNull();
                // Integer.MAX_VALUE means "never store a chunk uncompressed", i.e. always encrypt
                assertThat(params.maxCompressedLength()).isEqualTo(Integer.MAX_VALUE);
                assertThat(params.asMap()).doesNotContainKey(CompressionParams.MIN_COMPRESS_RATIO);
            }
        }
    }

    @Test
    public void testMinCompressRatioStillAcceptedWithoutEncryption()
    {
        Map<String, String> opts = new HashMap<>();
        opts.put(CompressionParams.CLASS, LZ4Compressor.class.getName());
        opts.put(CompressionParams.MIN_COMPRESS_RATIO, "1.1");
        CompressionParams params = CompressionParams.fromMap(opts);
        assertThat(params.getSstableCompressor().encryptionOnly()).isNull();
        assertThat(params.maxCompressedLength()).isLessThan(params.chunkLength());
        assertThat(params.asMap()).containsEntry(CompressionParams.MIN_COMPRESS_RATIO, "1.1");
    }

    private static String compressionClause(boolean withCipherAlgorithm, String minCompressRatio)
    {
        StringBuilder sb = new StringBuilder(" WITH compression = {'class': 'Encryptor', 'key_provider': '" + KEY_PROVIDER + '\'');
        if (withCipherAlgorithm)
            sb.append(", 'cipher_algorithm': 'AES/ECB/PKCS5Padding', 'secret_key_strength': '128'");
        if (minCompressRatio != null)
            sb.append(", 'min_compress_ratio': '").append(minCompressRatio).append('\'');
        return sb.append('}').toString();
    }

    private static void assertSchemaChangeRejected(Runnable schemaChange)
    {
        assertThatThrownBy(schemaChange::run)
        .satisfies(t -> assertThat(Throwables.getCausalChain(t))
                        .filteredOn(c -> c instanceof ConfigurationException)
                        .first()
                        .satisfies(c -> assertThat(c).hasMessageContaining(REJECTION_MESSAGE)
                                                     .hasMessageContaining("unencrypted")));
    }

    @Test
    public void testCreateTableRejectsMinCompressRatioWithEncryption()
    {
        for (boolean withCipherAlgorithm : new boolean[]{ true, false })
        {
            String clause = compressionClause(withCipherAlgorithm, "1.1");
            assertSchemaChangeRejected(() -> createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)" + clause));
        }
    }

    @Test
    public void testAlterTableRejectsMinCompressRatioWithEncryption() throws Throwable
    {
        for (boolean withCipherAlgorithm : new boolean[]{ true, false })
        {
            // The table is created without min_compress_ratio, so it is valid and encrypts everything.
            createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)" + compressionClause(withCipherAlgorithm, null));
            CompressionParams before = getCurrentColumnFamilyStore().metadata().params.compression;
            assertThat(before.getSstableCompressor().encryptionOnly()).isNotNull();

            String clause = compressionClause(withCipherAlgorithm, "1.1");
            assertSchemaChangeRejected(() -> alterTable("ALTER TABLE %s" + clause));

            // The rejected ALTER must not have changed anything, and the table must still be flushable.
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            assertThat(cfs.metadata().params.compression).isEqualTo(before);
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "value");
            flush();
            assertThat(cfs.getLiveSSTables()).hasSize(1);
            assertRows(execute("SELECT v FROM %s WHERE pk = ?", 1), row("value"));
        }
    }

    @Test
    public void testJmxRejectsMinCompressRatioWithEncryption()
    {
        for (boolean withCipherAlgorithm : new boolean[]{ true, false })
        {
            createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)" + compressionClause(withCipherAlgorithm, null));
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            // JMX changes only the local overrides, i.e. TableMetadataRef.getLocal(), not cfs.metadata()
            CompressionParams before = cfs.metadata.getLocal().params.compression;

            // ColumnFamilyStore.setCompressionParameters rethrows the ConfigurationException's message only
            assertThatThrownBy(() -> cfs.setCompressionParameters(encryptionOptions(withCipherAlgorithm, "1.1")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(REJECTION_MESSAGE)
            .hasMessageContaining("unencrypted");
            assertThat(cfs.metadata.getLocal().params.compression).isEqualTo(before);
            assertThat(cfs.getCompressionParameters()).isEqualTo(before.asMap());
        }
    }

    @Test
    public void testStoredMinCompressRatioWithEncryptionIsNormalised()
    {
        for (boolean withCipherAlgorithm : new boolean[]{ true, false })
        {
            Map<String, String> stored = encryptionOptions(withCipherAlgorithm, "1.1");
            stored.put(CompressionParams.CHUNK_LENGTH_IN_KB, "16");
            CompressionParams params = CompressionParams.fromStoredMap(stored, "ks", "tbl");
            assertThat(params.getSstableCompressor().encryptionOnly()).isNotNull();
            assertThat(params.maxCompressedLength()).isEqualTo(Integer.MAX_VALUE);
            assertThat(params.asMap()).doesNotContainKey(CompressionParams.MIN_COMPRESS_RATIO);
            params.validate();
            // The input map is not modified, and the strict entry point still refuses it
            assertThat(stored).containsEntry(CompressionParams.MIN_COMPRESS_RATIO, "1.1");
            assertRejected(stored);
        }
    }

    @Test
    public void testStoredMinCompressRatioWithoutEncryptionIsKept()
    {
        Map<String, String> stored = new HashMap<>();
        stored.put(CompressionParams.CLASS, LZ4Compressor.class.getName());
        stored.put(CompressionParams.MIN_COMPRESS_RATIO, "1.1");
        CompressionParams params = CompressionParams.fromStoredMap(stored, "ks", "tbl");
        assertThat(params).isEqualTo(CompressionParams.fromMap(stored));
        assertThat(params.asMap()).containsEntry(CompressionParams.MIN_COMPRESS_RATIO, "1.1");
    }

    /**
     * End-to-end version of {@link #testStoredMinCompressRatioWithEncryptionIsNormalised}: a table definition stored
     * by an older version must be loadable from the schema tables, and the next schema change on it must drop the
     * ignored option from the stored definition.
     */
    @Test
    public void testSchemaLoadNormalisesStoredMinCompressRatioWithEncryption() throws Throwable
    {
        for (boolean withCipherAlgorithm : new boolean[]{ true, false })
        {
            String table = createTable("CREATE TABLE %s (pk int PRIMARY KEY, v text)" + compressionClause(withCipherAlgorithm, null));
            CompressionParams before = getCurrentColumnFamilyStore().metadata().params.compression;

            // Simulate a definition written by an older version, which accepted the option
            Map<String, String> stored = new HashMap<>(before.asMap());
            stored.put(CompressionParams.MIN_COMPRESS_RATIO, "1.1");
            String update = String.format("UPDATE %s.%s SET compression = ? WHERE keyspace_name = ? AND table_name = ?",
                                          SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.TABLES);
            QueryProcessor.executeInternal(update, stored, keyspace(), table);
            assertThat(storedCompression(table)).containsEntry(CompressionParams.MIN_COMPRESS_RATIO, "1.1");

            // Parsing the schema tables normalises the stored option instead of failing
            TableMetadata loaded = SchemaKeyspace.fetchNonSystemKeyspaces().getNullable(keyspace()).getTableOrViewNullable(table);
            CompressionParams params = loaded.params.compression;
            assertThat(params.getSstableCompressor().encryptionOnly()).isNotNull();
            assertThat(params.maxCompressedLength()).isEqualTo(Integer.MAX_VALUE);
            assertThat(params).isEqualTo(before);
            loaded.validate();

            // So does reloading the live schema, after which the table is still usable and alterable
            Schema.instance.reloadSchemaAndAnnounceVersion();
            ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
            assertThat(cfs.metadata().params.compression).isEqualTo(before);
            execute("INSERT INTO %s (pk, v) VALUES (?, ?)", 1, "value");
            flush();
            assertRows(execute("SELECT v FROM %s WHERE pk = ?", 1), row("value"));

            // An unrelated ALTER rewrites the stored definition without the ignored option
            alterTable("ALTER TABLE %s WITH comment = 'altered'");
            assertThat(storedCompression(table)).doesNotContainKey(CompressionParams.MIN_COMPRESS_RATIO);
        }
    }

    private Map<String, String> storedCompression(String table)
    {
        String select = String.format("SELECT compression FROM %s.%s WHERE keyspace_name = ? AND table_name = ?",
                                      SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.TABLES);
        UntypedResultSet rows = QueryProcessor.executeInternal(select, keyspace(), table);
        return rows.one().getFrozenTextMap("compression");
    }

    @Test
    public void testStoredNonFiniteMinCompressRatio()
    {
        // Older versions accepted non-finite ratios, so the load path must not reject them
        Map<String, String> lz4 = new HashMap<>();
        lz4.put(CompressionParams.CLASS, LZ4Compressor.class.getName());
        lz4.put(CompressionParams.MIN_COMPRESS_RATIO, "NaN");
        CompressionParams params = CompressionParams.fromStoredMap(lz4, "ks", "tbl_lz4_nan");
        assertThat(params.getSstableCompressor().encryptionOnly()).isNull();
        params.validate();

        // ... and for an encrypting compressor they are normalised like any other ratio
        for (boolean withCipherAlgorithm : new boolean[]{ true, false })
        {
            for (String ratio : new String[]{ "NaN", "Infinity" })
            {
                params = CompressionParams.fromStoredMap(encryptionOptions(withCipherAlgorithm, ratio), "ks", "tbl_encrypted_nan");
                assertThat(params.getSstableCompressor().encryptionOnly()).isNotNull();
                assertThat(params.maxCompressedLength()).isEqualTo(Integer.MAX_VALUE);
                assertThat(params.asMap()).doesNotContainKey(CompressionParams.MIN_COMPRESS_RATIO);
                params.validate();
            }
        }
    }

    @Test
    public void testStoredMinCompressRatioWithEncryptionWarnsOncePerTable()
    {
        Logger logger = (Logger) LoggerFactory.getLogger(CompressionParams.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try
        {
            // A table name no other test uses, as the warning is rate limited per table for the whole JVM
            String table = "tbl_warn_once";
            for (int i = 0; i < 3; i++)
                CompressionParams.fromStoredMap(encryptionOptions(false, "1.1"), "ks", table);

            assertThat(appender.list)
            .filteredOn(e -> e.getLevel() == Level.WARN && e.getFormattedMessage().contains("ks." + table + ' '))
            .hasSize(1)
            .first()
            .satisfies(e -> assertThat(e.getFormattedMessage()).contains(CompressionParams.MIN_COMPRESS_RATIO + "=1.1")
                                                               .contains("ignored on this node")
                                                               .contains("nodetool upgradesstables -a ks " + table));
        }
        finally
        {
            logger.detachAppender(appender);
        }
    }
}
