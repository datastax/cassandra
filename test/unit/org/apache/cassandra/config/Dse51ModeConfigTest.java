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
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.net.DseCompat;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.DefaultCompressionSelector;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE;
import static org.apache.cassandra.config.CassandraRelevantProperties.SSTABLE_COMPRESSION_SELECTOR_CLASS;
import static org.apache.cassandra.config.CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION;
import static org.apache.cassandra.config.InternodeCompatibilityMode.DSE_5_1;
import static org.apache.cassandra.config.InternodeCompatibilityMode.DSE_6;
import static org.apache.cassandra.config.InternodeCompatibilityMode.NONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What {@code internode_compatibility_mode: DSE_5_1} requires and forces on the node:
 * {@code storage_compatibility_mode: HCD_1}, the big sstable format, {@code paxos_variant: v1} at startup and at run
 * time, the default compression selector and {@code flush_compression} {@code fast} or {@code table}, and numeric
 * sstable generations, with the texts of the DSE 6 mode worded for DSE 5.1. The tool and client paths are
 * {@link Dse51ModeToolTest} and {@link Dse51ModeClientTest}.
 */
public class Dse51ModeConfigTest
{
    private StorageCompatibilityMode storageModeBefore;
    private InternodeCompatibilityMode internodeModeBefore;
    private Config.PaxosVariant paxosVariantBefore;
    private WithProperties trieVersionProperty;

    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.daemonInitialization();
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

    /** A configuration the DSE_5_1 mode accepts as it is. */
    private static Config dse51Config()
    {
        Config config = new Config();
        config.internode_compatibility_mode = DSE_5_1;
        config.storage_compatibility_mode = StorageCompatibilityMode.HCD_1;
        config.paxos_variant = Config.PaxosVariant.v1;
        config.uuid_sstable_identifiers_enabled = false;
        config.sstable.selected_format = "big";
        return config;
    }

    private static Config dse6Config()
    {
        Config config = new Config();
        config.internode_compatibility_mode = DSE_6;
        config.storage_compatibility_mode = StorageCompatibilityMode.HCD_1;
        config.paxos_variant = Config.PaxosVariant.v1;
        config.uuid_sstable_identifiers_enabled = false;
        return config;
    }

    private static void apply(Config config)
    {
        DatabaseDescriptor.applyInternodeCompatibilityMode(config);
    }

    private static InternodeCompatibilityMode mode()
    {
        return DatabaseDescriptor.getInternodeCompatibilityMode();
    }

    @Test
    public void theModeValueAndItsGates()
    {
        assertThat(InternodeCompatibilityMode.valueOf("DSE_5_1")).isSameAs(DSE_5_1);
        assertThat(DSE_5_1.isDse51()).isTrue();
        assertThat(DSE_5_1.isDse()).isTrue();
        assertThat(DSE_5_1.isDse6()).isFalse();
        assertThat(DSE_6.isDse()).isTrue();
        assertThat(DSE_6.isDse51()).isFalse();
        assertThat(NONE.isDse()).isFalse();
        assertThat(NONE.isDse51()).isFalse();
        assertThat(DSE_5_1.source()).isEqualTo("DSE 5.1");
        assertThat(DSE_6.source()).isEqualTo("DSE 6");
        assertThat(NONE.source()).isNull();
    }

    @Test
    public void dse51WithMatchingSettings()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.HCD_1);
        apply(dse51Config());
        assertThat(mode()).isEqualTo(DSE_5_1);
        assertThat(DatabaseDescriptor.isDse51CompatibilityMode()).isTrue();
        assertThat(DatabaseDescriptor.isDseCompatibilityMode()).isTrue();
        assertThat(DatabaseDescriptor.isDse6CompatibilityMode()).isFalse();
        // nothing of the on-disk half of DSE_6: no trie version, no DSE 6 transaction logs or histograms
        assertThat(DatabaseDescriptor.isDse6SSTableOutput()).isFalse();
        assertThat(TRIE_INDEX_FORMAT_VERSION.isPresent()).isFalse();
        assertThatCode(DatabaseDescriptor::verifyDse6SSTableFormat).doesNotThrowAnyException();
        // and no pre-4.0 dialect: the gate of every pre-4.0 code path stays closed
        assertThat(DseCompat.enabled()).isFalse();
        assertThat(DseCompat.isPre40Layout(MessagingService.VERSION_3014)).isFalse();
    }

    /**
     * The daemon announces the mode once the sstable formats are loaded, naming the format and version written, and
     * not in the other modes. This JVM loaded its formats before the test, so the text names the format it loaded; the
     * in-JVM {@code Dse51CompressorsStartupTest} checks that a node in the mode names big nb.
     */
    @Test
    public void theModeIsAnnouncedWithTheVersionWritten()
    {
        Logger ddLogger = (Logger) LoggerFactory.getLogger(DatabaseDescriptor.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        ddLogger.addAppender(appender);
        try
        {
            for (InternodeCompatibilityMode silent : new InternodeCompatibilityMode[]{ NONE, DSE_6 })
            {
                DatabaseDescriptor.setInternodeCompatibilityMode(silent);
                DatabaseDescriptor.announceDse51Mode();
            }
            assertThat(appender.list).isEmpty();

            DatabaseDescriptor.setInternodeCompatibilityMode(DSE_5_1);
            DatabaseDescriptor.announceDse51Mode();
        }
        finally
        {
            ddLogger.detachAppender(appender);
        }
        SSTableFormat<?, ?> format = DatabaseDescriptor.getSelectedSSTableFormat();
        String announcement = "Running in internode_compatibility_mode DSE_5_1: the node writes " + format.name() +
                              ' ' + format.getLatestVersion().version + " sstables, which DSE 5.1 does not read, " +
                              "with numeric generations, and requires paxos_variant v1; it does not connect to " +
                              "DSE 5.1 nodes";
        assertThat(appender.list).hasSize(1)
                                 .allMatch(e -> e.getLevel() == Level.WARN &&
                                                e.getFormattedMessage().equals(announcement));
    }

    @Test
    public void dse51RequiresHcd1StorageMode()
    {
        StorageCompatibilityMode[] refused = { StorageCompatibilityMode.NONE,
                                               StorageCompatibilityMode.UPGRADING,
                                               StorageCompatibilityMode.CASSANDRA_4 };
        for (StorageCompatibilityMode storage : refused)
        {
            DatabaseDescriptor.setStorageCompatibilityMode(storage);
            assertThatThrownBy(() -> apply(dse51Config()))
            .isInstanceOf(ConfigurationException.class)
            .hasMessage("internode_compatibility_mode DSE_5_1 requires storage_compatibility_mode HCD_1, found " +
                        storage.name() + ": HCD_1 keeps the storage formats of the 4.x line, the oldest this " +
                        "software writes");
            assertThat(mode()).isEqualTo(internodeModeBefore);
        }
    }

    @Test
    public void dse51RequiresTheBigFormat()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.HCD_1);
        // bti, also when it is the default of an sstable section that does not name a format
        for (String format : new String[]{ "bti", null, "" })
        {
            Config config = dse51Config();
            config.sstable.selected_format = format;
            config.uuid_sstable_identifiers_enabled = true;
            assertThatThrownBy(() -> apply(config))
            .as("%s", format)
            .isInstanceOf(ConfigurationException.class)
            .hasMessage(bigFormatRefusal("bti"));
            // refused before anything is set or forced
            assertThat(mode()).isEqualTo(internodeModeBefore);
            assertThat(config.uuid_sstable_identifiers_enabled).isTrue();
        }
        // a configuration that has none
        assertThatThrownBy(() -> {
            try (WithProperties property = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE, "DSE_5_1"))
            {
                apply(null);
            }
        }).isInstanceOf(ConfigurationException.class).hasMessage(bigFormatRefusal("bti"));
    }

    /** The refusal of another format than big, also asserted for tools by {@link Dse51ModeToolTest}. */
    static String bigFormatRefusal(String found)
    {
        return "internode_compatibility_mode DSE_5_1 requires sstable.selected_format big, found " + found +
               ": DSE 5.1 only reads the big format; set selected_format: big in the sstable section of " +
               "conf/cassandra.yaml, then restart the node or run the tool again";
    }

    /** The refusal of a Paxos variant other than v1 in {@code mode}, at startup and at run time. */
    private static String paxosVariantRefusal(InternodeCompatibilityMode mode, Config.PaxosVariant variant)
    {
        String reason = variant == Config.PaxosVariant.v1_without_linearizable_reads_or_rejected_writes
                        ? "with it SERIAL and LOCAL_SERIAL reads propose nothing, which breaks the linearizability " +
                          "of LWTs against the DSE nodes"
                        : (mode.isDse6() ? "DSE 6" : "DSE 5.1") + " has no Paxos v2";
        return "internode_compatibility_mode " + mode.name() + " requires paxos_variant v1, found " + variant +
               ": " + reason;
    }

    @Test
    public void dse51RequiresPaxosV1AtStartup()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.HCD_1);
        int refused = 0;
        for (Config.PaxosVariant variant : Config.PaxosVariant.values())
        {
            if (variant == Config.PaxosVariant.v1)
                continue;
            Config config = dse51Config();
            config.paxos_variant = variant;
            assertThatThrownBy(() -> apply(config))
            .isInstanceOf(ConfigurationException.class)
            .hasMessage(paxosVariantRefusal(DSE_5_1, variant));
            assertThat(mode()).isEqualTo(internodeModeBefore);
            refused++;
        }
        // v1_without_linearizable_reads_or_rejected_writes and the three v2 variants
        assertThat(refused).isEqualTo(Config.PaxosVariant.values().length - 1).isEqualTo(4);
    }

    @Test
    public void dse51RefusesAnotherPaxosVariantAtRuntime()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.HCD_1);
        Paxos.setPaxosVariant(Config.PaxosVariant.v1);
        apply(dse51Config());
        for (Config.PaxosVariant variant : Config.PaxosVariant.values())
        {
            if (variant == Config.PaxosVariant.v1)
                continue;
            String refusal = paxosVariantRefusal(DSE_5_1, variant);
            assertThatThrownBy(() -> Paxos.setPaxosVariant(variant))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage(refusal);
            assertThatThrownBy(() -> DatabaseDescriptor.setPaxosVariant(variant))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage(refusal);
            // the MBean operation: the guards of the upgrade are not active in this mode, the check of the variant is
            assertThatThrownBy(() -> StorageProxy.instance.setPaxosVariant(variant.name()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage(refusal);
            assertThat(Paxos.getPaxosVariant()).isEqualTo(Config.PaxosVariant.v1);
            assertThat(DatabaseDescriptor.getPaxosVariant()).isEqualTo(Config.PaxosVariant.v1);
        }
        // v1 itself is accepted
        StorageProxy.instance.setPaxosVariant(Config.PaxosVariant.v1.name());
        assertThat(Paxos.getPaxosVariant()).isEqualTo(Config.PaxosVariant.v1);

        // the same check names DSE 6 in DSE_6 mode
        DatabaseDescriptor.setInternodeCompatibilityMode(DSE_6);
        assertThatThrownBy(() -> Paxos.setPaxosVariant(Config.PaxosVariant.v2))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(paxosVariantRefusal(DSE_6, Config.PaxosVariant.v2));
    }

    @Test
    public void dse51RefusesACustomCompressionSelector()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.HCD_1);
        String custom = "org.example.CustomSelector";
        try (WithProperties properties = new WithProperties().set(SSTABLE_COMPRESSION_SELECTOR_CLASS, custom))
        {
            Config config = dse51Config();
            config.uuid_sstable_identifiers_enabled = true;
            assertThatThrownBy(() -> apply(config))
            .isInstanceOf(ConfigurationException.class)
            .hasMessage(compressionSelectorRefusal(custom));
            assertThat(mode()).isEqualTo(internodeModeBefore);
            assertThat(config.uuid_sstable_identifiers_enabled).isTrue();
        }
        for (String selector : new String[]{ "", DefaultCompressionSelector.class.getName() })
        {
            try (WithProperties properties = new WithProperties().set(SSTABLE_COMPRESSION_SELECTOR_CLASS, selector))
            {
                apply(dse51Config());
                assertThat(mode()).isEqualTo(DSE_5_1);
            }
        }
    }

    /** The refusal of a custom compression selector, also asserted for tools by {@link Dse51ModeToolTest}. */
    static String compressionSelectorRefusal(String selectorClass)
    {
        return "internode_compatibility_mode DSE_5_1 requires the default compression selector, but " +
               "cassandra.sstable.compression.selector.class=" + selectorClass + ": that class chooses the " +
               "compressor of new tables, flushes and compactions, which DSE 5.1 is not known to read; remove " +
               "-Dcassandra.sstable.compression.selector.class from the JVM options (conf/jvm-server.options or " +
               "JVM_EXTRA_OPTS for the node, conf/jvm-clients.options or JVM_OPTS for a tool), then restart the " +
               "node or run the tool again";
    }

    /**
     * The refusal of a flush compression other than fast and table, also asserted for tools by
     * {@link Dse51ModeToolTest}.
     */
    static String flushCompressionRefusal(Config.FlushCompression flushCompression, String compressor)
    {
        return "internode_compatibility_mode DSE_5_1 requires flush_compression fast or table, found " +
               flushCompression + ": flushes would write sstables compressed with " + compressor + ", which " +
               "DSE 5.1 is not known to read; set flush_compression: fast in conf/cassandra.yaml, then restart the " +
               "node or run the tool again";
    }

    @Test
    public void dse51RefusesAdaptiveAndNoFlushCompression()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.HCD_1);
        Map<Config.FlushCompression, String> refused = Map.of(Config.FlushCompression.adaptive, "AdaptiveCompressor",
                                                              Config.FlushCompression.none, "NoopCompressor");
        for (Map.Entry<Config.FlushCompression, String> flushCompression : refused.entrySet())
        {
            Config config = dse51Config();
            config.flush_compression = flushCompression.getKey();
            config.uuid_sstable_identifiers_enabled = true;
            assertThatThrownBy(() -> apply(config))
            .isInstanceOf(ConfigurationException.class)
            .hasMessage(flushCompressionRefusal(flushCompression.getKey(), flushCompression.getValue()));
            assertThat(mode()).isEqualTo(internodeModeBefore);
            assertThat(config.uuid_sstable_identifiers_enabled).isTrue();
        }
        Config.FlushCompression[] accepted = { null, Config.FlushCompression.fast, Config.FlushCompression.table };
        for (Config.FlushCompression flushCompression : accepted)
        {
            Config config = dse51Config();
            config.flush_compression = flushCompression;
            apply(config);
            assertThat(mode()).isEqualTo(DSE_5_1);
        }
    }

    /** The WARN of the forced generations, in the words of each source. */
    @Test
    public void bothSourcesWarnWhenForcingNumericGenerations()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.HCD_1);
        Logger ddLogger = (Logger) LoggerFactory.getLogger(DatabaseDescriptor.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        ddLogger.addAppender(appender);
        String trieVersion = InternodeCompatibilityMode.DSE_6_TRIE_INDEX_FORMAT_VERSION;
        try (WithProperties properties = new WithProperties().set(TRIE_INDEX_FORMAT_VERSION, trieVersion))
        {
            Config dse51 = dse51Config();
            dse51.uuid_sstable_identifiers_enabled = true;
            apply(dse51);
            assertThat(dse51.uuid_sstable_identifiers_enabled).isFalse();
            assertThat(appender.list).anyMatch(e -> e.getLevel() == Level.WARN &&
                                                    e.getFormattedMessage().equals(uuidWarning(DSE_5_1, "DSE 5.1")));

            appender.list.clear();
            Config dse6 = dse6Config();
            dse6.uuid_sstable_identifiers_enabled = true;
            apply(dse6);
            assertThat(dse6.uuid_sstable_identifiers_enabled).isFalse();
            assertThat(appender.list).anyMatch(e -> e.getLevel() == Level.WARN &&
                                                    e.getFormattedMessage().equals(uuidWarning(DSE_6, "DSE 6")));

            // already off: nothing to warn about
            appender.list.clear();
            apply(dse51Config());
            assertThat(appender.list).noneMatch(e -> e.getFormattedMessage().contains("uuid_sstable_identifiers"));
        }
        finally
        {
            ddLogger.detachAppender(appender);
        }
    }

    private static String uuidWarning(InternodeCompatibilityMode mode, String source)
    {
        return "internode_compatibility_mode " + mode.name() + " forces uuid_sstable_identifiers_enabled off: " +
               source + " only parses numeric sstable generations";
    }

    @Test
    public void thePropertyCanRequestDse51()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.HCD_1);
        Config config = dse51Config();
        config.internode_compatibility_mode = NONE;
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE, "DSE_5_1"))
        {
            apply(config);
            assertThat(mode()).isEqualTo(DSE_5_1);
            // and its requirements apply to the property route too
            DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.NONE);
            assertThatThrownBy(() -> apply(config))
            .isInstanceOf(ConfigurationException.class)
            .hasMessageStartingWith("internode_compatibility_mode DSE_5_1 requires storage_compatibility_mode " +
                                    "HCD_1, found NONE");
        }
        // the property says NONE: nothing of DSE_5_1 is checked
        try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE, "NONE"))
        {
            Config bti = dse51Config();
            bti.sstable.selected_format = "bti";
            bti.paxos_variant = Config.PaxosVariant.v2;
            bti.uuid_sstable_identifiers_enabled = true;
            apply(bti);
            assertThat(mode()).isEqualTo(NONE);
            assertThat(DatabaseDescriptor.isDseCompatibilityMode()).isFalse();
            // and nothing is forced
            assertThat(bti.sstable.selected_format).isEqualTo("bti");
            assertThat(bti.uuid_sstable_identifiers_enabled).isTrue();
        }
    }

    @Test
    public void unknownPropertyValuesNameTheThreeModes()
    {
        for (String value : new String[]{ "DSE_5", "DSE51", "DSE_5_1_51" })
        {
            try (WithProperties properties = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE, value))
            {
                assertThatThrownBy(() -> apply(new Config()))
                .isInstanceOf(ConfigurationException.class)
                .hasMessage("internode_compatibility_mode: unknown value " + value +
                            ", expected NONE, DSE_6 or DSE_5_1");
            }
        }
    }
}
