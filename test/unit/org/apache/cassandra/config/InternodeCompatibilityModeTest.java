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

package org.apache.cassandra.config;

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.schema.DefaultCompressionSelector;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE;
import static org.apache.cassandra.config.CassandraRelevantProperties.SSTABLE_COMPRESSION_SELECTOR_CLASS;
import static org.apache.cassandra.config.CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code internode_compatibility_mode} setting: how it is read, and what {@code DSE_6} requires
 * and forces. The unit test JVM runs with the default trie index format version, {@code cc}, which
 * {@code BtiFormat} reads once; every test starts with the trie version property absent and restores it.
 */
public class InternodeCompatibilityModeTest
{
    private static String trieVersionOfThisJvm;
    private StorageCompatibilityMode storageModeBefore;
    private InternodeCompatibilityMode internodeModeBefore;
    private Config.PaxosVariant paxosVariantBefore;
    private WithProperties trieVersionProperty;

    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.daemonInitialization();
        trieVersionOfThisJvm = BtiFormat.getInstance().getLatestVersion().version;
    }

    @Before
    public void save()
    {
        storageModeBefore = DatabaseDescriptor.getStorageCompatibilityMode();
        internodeModeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        paxosVariantBefore = Paxos.getPaxosVariant();
        trieVersionProperty = new WithProperties().clear(TRIE_INDEX_FORMAT_VERSION);
    }

    @After
    public void restore()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(storageModeBefore);
        DatabaseDescriptor.setInternodeCompatibilityMode(internodeModeBefore);
        Paxos.setPaxosVariant(paxosVariantBefore);
        trieVersionProperty.close();
    }

    private static Config dse6Config()
    {
        Config config = new Config();
        config.internode_compatibility_mode = InternodeCompatibilityMode.DSE_6;
        config.storage_compatibility_mode = StorageCompatibilityMode.HCD_1;
        config.paxos_variant = Config.PaxosVariant.v1;
        config.uuid_sstable_identifiers_enabled = false;
        return config;
    }

    @Test
    public void defaultIsNoneAndChangesNothing()
    {
        Config config = new Config();
        boolean uuidIds = config.uuid_sstable_identifiers_enabled;
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.NONE);
        DatabaseDescriptor.applyInternodeCompatibilityMode(config);
        assertThat(DatabaseDescriptor.getInternodeCompatibilityMode()).isEqualTo(InternodeCompatibilityMode.NONE);
        assertThat(DatabaseDescriptor.isDse6CompatibilityMode()).isFalse();
        assertThat(DatabaseDescriptor.isDse6SSTableOutput()).isFalse();
        assertThat(config.uuid_sstable_identifiers_enabled).isEqualTo(uuidIds);
        assertThat(TRIE_INDEX_FORMAT_VERSION.isPresent()).isFalse();
        assertThat(DatabaseDescriptor.getStorageCompatibilityMode()).isEqualTo(StorageCompatibilityMode.NONE);
        // the trie version of this JVM is cc, and NONE does not care
        DatabaseDescriptor.verifyDse6SSTableFormat();
    }

    @Test
    public void nullConfigMeansNone()
    {
        DatabaseDescriptor.applyInternodeCompatibilityMode(null);
        assertThat(DatabaseDescriptor.getInternodeCompatibilityMode()).isEqualTo(InternodeCompatibilityMode.NONE);
    }

    @Test
    public void dse6RequiresHcd1StorageMode()
    {
        for (StorageCompatibilityMode storage : new StorageCompatibilityMode[]{ StorageCompatibilityMode.NONE, StorageCompatibilityMode.UPGRADING, StorageCompatibilityMode.CASSANDRA_4 })
        {
            DatabaseDescriptor.setStorageCompatibilityMode(storage);
            assertThatThrownBy(() -> DatabaseDescriptor.applyInternodeCompatibilityMode(dse6Config()))
            .isInstanceOf(ConfigurationException.class)
            .hasMessage("internode_compatibility_mode DSE_6 requires storage_compatibility_mode HCD_1, found " + storage.name() +
                        ": DSE 6 cannot read data written in a newer storage format");
            assertThat(DatabaseDescriptor.getInternodeCompatibilityMode()).isEqualTo(internodeModeBefore);
        }
    }

    @Test
    public void dse6RequiresBtiFormat()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.HCD_1);
        Config config = dse6Config();
        config.sstable.selected_format = "big";
        assertThatThrownBy(() -> DatabaseDescriptor.applyInternodeCompatibilityMode(config))
        .isInstanceOf(ConfigurationException.class)
        .hasMessage("internode_compatibility_mode DSE_6 requires sstable.selected_format bti, found big: DSE 6 only reads its own trie format");
    }

    private static String paxosVariantRefusal(Config.PaxosVariant variant)
    {
        String reason = variant == Config.PaxosVariant.v1_without_linearizable_reads_or_rejected_writes
                        ? "with it SERIAL and LOCAL_SERIAL reads propose nothing, which breaks the linearizability of LWTs against the DSE nodes"
                        : "DSE 6 has no Paxos v2";
        return "internode_compatibility_mode DSE_6 requires paxos_variant v1, found " + variant + ": " + reason;
    }

    @Test
    public void dse6RequiresPaxosV1()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.HCD_1);
        int refused = 0;
        for (Config.PaxosVariant variant : Config.PaxosVariant.values())
        {
            if (variant == Config.PaxosVariant.v1)
                continue;
            Config config = dse6Config();
            config.paxos_variant = variant;
            assertThatThrownBy(() -> DatabaseDescriptor.applyInternodeCompatibilityMode(config))
            .isInstanceOf(ConfigurationException.class)
            .hasMessage(paxosVariantRefusal(variant));
            assertThat(DatabaseDescriptor.getInternodeCompatibilityMode()).isEqualTo(internodeModeBefore);
            refused++;
        }
        // v1_without_linearizable_reads_or_rejected_writes and the three v2 variants
        assertThat(refused).isEqualTo(Config.PaxosVariant.values().length - 1).isEqualTo(4);
    }

    @Test
    public void dse6AcceptsPaxosV1()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.HCD_1);
        Config config = dse6Config();
        config.paxos_variant = Config.PaxosVariant.v1;
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        Paxos.setPaxosVariant(Config.PaxosVariant.v2);
        DatabaseDescriptor.applyInternodeCompatibilityMode(config);
        assertThat(DatabaseDescriptor.isDse6CompatibilityMode()).isTrue();
        assertThat(DatabaseDescriptor.isDse6SSTableOutput()).isTrue();
        // and at runtime (the variant is changed before the mode only to make setting v1 an actual change)
        Paxos.setPaxosVariant(Config.PaxosVariant.v1);
        assertThat(Paxos.getPaxosVariant()).isEqualTo(Config.PaxosVariant.v1);
        assertThat(DatabaseDescriptor.getPaxosVariant()).isEqualTo(Config.PaxosVariant.v1);
    }

    @Test
    public void noneAcceptsEveryPaxosVariant()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.NONE);
        for (Config.PaxosVariant variant : Config.PaxosVariant.values())
        {
            Config config = new Config();
            config.paxos_variant = variant;
            DatabaseDescriptor.applyInternodeCompatibilityMode(config);
            assertThat(DatabaseDescriptor.getInternodeCompatibilityMode()).isEqualTo(InternodeCompatibilityMode.NONE);
            // and at runtime
            Paxos.setPaxosVariant(variant);
            assertThat(Paxos.getPaxosVariant()).isEqualTo(variant);
            assertThat(DatabaseDescriptor.getPaxosVariant()).isEqualTo(variant);
        }
    }

    @Test
    public void dse6RefusesAnotherTrieVersionProperty()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.HCD_1);
        try (WithProperties properties = new WithProperties().set(TRIE_INDEX_FORMAT_VERSION, "cc"))
        {
            assertThatThrownBy(() -> DatabaseDescriptor.applyInternodeCompatibilityMode(dse6Config()))
            .isInstanceOf(ConfigurationException.class)
            .hasMessage("internode_compatibility_mode DSE_6 requires trie index format version bb, but " +
                        "cassandra.trie_index_format_version=cc: DSE 6 cannot read newer trie indexes");
        }
    }

    /**
     * The refusal of a custom compression selector, also asserted for tools by
     * {@link InternodeCompatibilityModeToolTest}.
     */
    static String compressionSelectorRefusal(String selectorClass)
    {
        return "internode_compatibility_mode DSE_6 requires the default compression selector, but " +
               "cassandra.sstable.compression.selector.class=" + selectorClass + ": that class chooses the compressor of new " +
               "tables, flushes and compactions, which DSE 6 is not known to read; remove " +
               "-Dcassandra.sstable.compression.selector.class from the JVM options (conf/jvm-server.options or " +
               "JVM_EXTRA_OPTS for the node, conf/jvm-clients.options or JVM_OPTS for a tool), then restart the node or " +
               "run the tool again";
    }

    /**
     * The refusal of a flush compression other than fast and table, also asserted for tools by
     * {@link InternodeCompatibilityModeToolTest}.
     */
    static String flushCompressionRefusal(Config.FlushCompression flushCompression, String compressor)
    {
        return "internode_compatibility_mode DSE_6 requires flush_compression fast or table, found " + flushCompression + ": " +
               "flushes would write sstables compressed with " + compressor + ", which DSE 6 is not known to read; " +
               "set flush_compression: fast in conf/cassandra.yaml, then restart the node or run the tool again";
    }

    @Test
    public void dse6RefusesACustomCompressionSelector()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.HCD_1);
        try (WithProperties properties = new WithProperties().set(SSTABLE_COMPRESSION_SELECTOR_CLASS, "org.example.CustomSelector"))
        {
            Config config = dse6Config();
            config.uuid_sstable_identifiers_enabled = true;
            assertThatThrownBy(() -> DatabaseDescriptor.applyInternodeCompatibilityMode(config))
            .isInstanceOf(ConfigurationException.class)
            .hasMessage(compressionSelectorRefusal("org.example.CustomSelector"));
            // refused before anything is set or forced
            assertThat(DatabaseDescriptor.getInternodeCompatibilityMode()).isEqualTo(internodeModeBefore);
            assertThat(TRIE_INDEX_FORMAT_VERSION.isPresent()).isFalse();
            assertThat(config.uuid_sstable_identifiers_enabled).isTrue();

            // NONE does not care
            DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.NONE);
            DatabaseDescriptor.applyInternodeCompatibilityMode(new Config());
            assertThat(DatabaseDescriptor.getInternodeCompatibilityMode()).isEqualTo(InternodeCompatibilityMode.NONE);
        }
    }

    @Test
    public void dse6AcceptsTheDefaultCompressionSelector()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.HCD_1);
        for (String selector : new String[]{ "", DefaultCompressionSelector.class.getName() })
        {
            try (WithProperties properties = new WithProperties().set(SSTABLE_COMPRESSION_SELECTOR_CLASS, selector))
            {
                DatabaseDescriptor.applyInternodeCompatibilityMode(dse6Config());
                assertThat(DatabaseDescriptor.getInternodeCompatibilityMode()).isEqualTo(InternodeCompatibilityMode.DSE_6);
            }
        }
    }

    @Test
    public void dse6RefusesAdaptiveAndNoFlushCompression()
    {
        Map<Config.FlushCompression, String> refused = Map.of(Config.FlushCompression.adaptive, "AdaptiveCompressor",
                                                              Config.FlushCompression.none, "NoopCompressor");
        for (Map.Entry<Config.FlushCompression, String> flushCompression : refused.entrySet())
        {
            DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.HCD_1);
            Config config = dse6Config();
            config.flush_compression = flushCompression.getKey();
            config.uuid_sstable_identifiers_enabled = true;
            assertThatThrownBy(() -> DatabaseDescriptor.applyInternodeCompatibilityMode(config))
            .isInstanceOf(ConfigurationException.class)
            .hasMessage(flushCompressionRefusal(flushCompression.getKey(), flushCompression.getValue()));
            // refused before anything is set or forced
            assertThat(DatabaseDescriptor.getInternodeCompatibilityMode()).isEqualTo(internodeModeBefore);
            assertThat(TRIE_INDEX_FORMAT_VERSION.isPresent()).isFalse();
            assertThat(config.uuid_sstable_identifiers_enabled).isTrue();

            // NONE does not care
            DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.NONE);
            config.internode_compatibility_mode = InternodeCompatibilityMode.NONE;
            DatabaseDescriptor.applyInternodeCompatibilityMode(config);
            assertThat(DatabaseDescriptor.getInternodeCompatibilityMode()).isEqualTo(InternodeCompatibilityMode.NONE);
        }
    }

    @Test
    public void dse6AcceptsFastAndTableFlushCompression()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.HCD_1);
        Config.FlushCompression[] accepted = { null, Config.FlushCompression.fast, Config.FlushCompression.table };
        for (Config.FlushCompression flushCompression : accepted)
        {
            Config config = dse6Config();
            config.flush_compression = flushCompression;
            DatabaseDescriptor.applyInternodeCompatibilityMode(config);
            assertThat(DatabaseDescriptor.getInternodeCompatibilityMode()).isEqualTo(InternodeCompatibilityMode.DSE_6);
        }
    }

    @Test
    public void dse6WithMatchingSettings()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.HCD_1);
        Config config = dse6Config();
        config.uuid_sstable_identifiers_enabled = true;
        try (WithProperties properties = new WithProperties().set(TRIE_INDEX_FORMAT_VERSION, InternodeCompatibilityMode.DSE_6_TRIE_INDEX_FORMAT_VERSION))
        {
            DatabaseDescriptor.applyInternodeCompatibilityMode(config);
            assertThat(DatabaseDescriptor.isDse6CompatibilityMode()).isTrue();
            assertThat(DatabaseDescriptor.getInternodeCompatibilityMode()).isEqualTo(InternodeCompatibilityMode.DSE_6);
            // forced, not refused
            assertThat(config.uuid_sstable_identifiers_enabled).isFalse();

            // the format of this JVM was initialised with cc before the mode applied: the property came too late and
            // the node must say so (the positive path is InternodeCompatibilityModeToolTest and Dse6ModeSmokeTest)
            assertThat(trieVersionOfThisJvm).isEqualTo("cc");
            assertThatThrownBy(DatabaseDescriptor::verifyDse6SSTableFormat)
            .isInstanceOf(ConfigurationException.class)
            .hasMessage("internode_compatibility_mode DSE_6 requires trie index format version bb, but the format was already " +
                        "initialised with cc; start with -Dcassandra.trie_index_format_version=bb");
        }
    }

    @Test
    public void formatVerificationIsSilentInNoneMode()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        DatabaseDescriptor.verifyDse6SSTableFormat();
    }

    @Test
    public void propertyOverridesYaml()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.HCD_1);
        Config config = dse6Config();
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE, "NONE"))
        {
            DatabaseDescriptor.applyInternodeCompatibilityMode(config);
            assertThat(DatabaseDescriptor.getInternodeCompatibilityMode()).isEqualTo(InternodeCompatibilityMode.NONE);
            // and the DSE_6 requirements are not enforced when the property says NONE
            config.paxos_variant = Config.PaxosVariant.v2;
            DatabaseDescriptor.applyInternodeCompatibilityMode(config);
            assertThat(DatabaseDescriptor.isDse6CompatibilityMode()).isFalse();
        }
    }

    @Test
    public void propertyCanRequestDse6()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.NONE);
        Config config = new Config();
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE, "DSE_6"))
        {
            // the requirements apply to the property route too: storage mode NONE is refused
            assertThatThrownBy(() -> DatabaseDescriptor.applyInternodeCompatibilityMode(config))
            .isInstanceOf(ConfigurationException.class)
            .hasMessage("internode_compatibility_mode DSE_6 requires storage_compatibility_mode HCD_1, found NONE: " +
                        "DSE 6 cannot read data written in a newer storage format");
        }
    }

    @Test
    public void emptyYamlValue()
    {
        // an empty value in cassandra.yaml is refused by the loader, like any other non-nullable setting
        Map<String, Object> yaml = new HashMap<>();
        yaml.put("internode_compatibility_mode", null);
        assertThatThrownBy(() -> YamlConfigurationLoader.fromMap(yaml, Config.class))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("internode_compatibility_mode");

        // and a configuration built without the value means NONE
        Config config = new Config();
        config.internode_compatibility_mode = null;
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.NONE);
        DatabaseDescriptor.applyInternodeCompatibilityMode(config);
        assertThat(DatabaseDescriptor.getInternodeCompatibilityMode()).isEqualTo(InternodeCompatibilityMode.NONE);
    }

    @Test
    public void unknownPropertyValueIsAConfigurationError()
    {
        for (String value : new String[]{ "dse6", "DSE6", "" })
        {
            try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE, value))
            {
                assertThatThrownBy(() -> DatabaseDescriptor.applyInternodeCompatibilityMode(new Config()))
                .isInstanceOf(ConfigurationException.class)
                .hasMessage("internode_compatibility_mode: unknown value " + value + ", expected NONE or DSE_6");
            }
        }
    }

    @Test
    public void dse6SetsTheTrieVersionPropertyWhenAbsent()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.HCD_1);
        // the property is absent (cleared before every test)
        assertThat(TRIE_INDEX_FORMAT_VERSION.isPresent()).isFalse();
        DatabaseDescriptor.applyInternodeCompatibilityMode(dse6Config());
        assertThat(DatabaseDescriptor.isDse6CompatibilityMode()).isTrue();
        assertThat(TRIE_INDEX_FORMAT_VERSION.isPresent()).isTrue();
        assertThat(TRIE_INDEX_FORMAT_VERSION.getString()).isEqualTo("bb");
    }

    @Test
    public void dse6WarnsWhenForcingNumericGenerations()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.HCD_1);
        Logger ddLogger = (Logger) LoggerFactory.getLogger(DatabaseDescriptor.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        ddLogger.addAppender(appender);
        try (WithProperties properties = new WithProperties().set(TRIE_INDEX_FORMAT_VERSION, InternodeCompatibilityMode.DSE_6_TRIE_INDEX_FORMAT_VERSION))
        {
            Config config = dse6Config();
            config.uuid_sstable_identifiers_enabled = true;
            DatabaseDescriptor.applyInternodeCompatibilityMode(config);
            assertThat(config.uuid_sstable_identifiers_enabled).isFalse();
            assertThat(appender.list)
            .anyMatch(e -> e.getLevel() == Level.WARN &&
                           e.getFormattedMessage().equals("internode_compatibility_mode DSE_6 forces uuid_sstable_identifiers_enabled off: " +
                                                          "DSE 6 only parses numeric sstable generations"));

            // already off: nothing to warn about
            appender.list.clear();
            DatabaseDescriptor.applyInternodeCompatibilityMode(dse6Config());
            assertThat(appender.list).noneMatch(e -> e.getFormattedMessage().contains("uuid_sstable_identifiers_enabled"));
        }
        finally
        {
            ddLogger.detachAppender(appender);
        }
    }

    @Test
    public void runtimePaxosVariantChangeIsRefusedInDse6()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        Paxos.setPaxosVariant(Config.PaxosVariant.v1);
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.HCD_1);
        for (Config.PaxosVariant variant : Config.PaxosVariant.values())
        {
            if (variant == Config.PaxosVariant.v1)
                continue;
            String refusal = paxosVariantRefusal(variant);
            assertThatThrownBy(() -> Paxos.setPaxosVariant(variant))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage(refusal);
            assertThatThrownBy(() -> DatabaseDescriptor.setPaxosVariant(variant))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage(refusal);
            assertThat(Paxos.getPaxosVariant()).isEqualTo(Config.PaxosVariant.v1);
            assertThat(DatabaseDescriptor.getPaxosVariant()).isEqualTo(Config.PaxosVariant.v1);

            // the startup refusal says the same: one text for both paths
            Config config = dse6Config();
            config.paxos_variant = variant;
            assertThatThrownBy(() -> DatabaseDescriptor.applyInternodeCompatibilityMode(config))
            .isInstanceOf(ConfigurationException.class)
            .hasMessage(refusal);
        }
    }
}
