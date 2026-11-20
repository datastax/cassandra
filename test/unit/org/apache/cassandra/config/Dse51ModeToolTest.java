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

import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.big.BigFormat;

import static org.apache.cassandra.config.CassandraRelevantProperties.CONFIG_ALLOW_SYSTEM_PROPERTIES;
import static org.apache.cassandra.config.CassandraRelevantProperties.SSTABLE_COMPRESSION_SELECTOR_CLASS;
import static org.apache.cassandra.config.CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION;
import static org.apache.cassandra.config.InternodeCompatibilityMode.DSE_5_1;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Offline tools reading a cassandra.yaml that says {@code internode_compatibility_mode: DSE_5_1} run with the mode
 * {@code NONE} and inherit the checks of its on-disk settings: the big format, the compression selector and
 * {@code flush_compression}, and numeric generations. In a class of its own: {@code toolInitialization} happens once
 * per JVM.
 */
public class Dse51ModeToolTest
{
    private static final String SETTINGS = "cassandra.settings.";

    @Test
    public void toolsInheritTheOnDiskChecksOfDse51()
    {
        try (WithProperties properties = new WithProperties().clear(TRIE_INDEX_FORMAT_VERSION)
                                                             .set(CONFIG_ALLOW_SYSTEM_PROPERTIES, true)
                                                             .with(SETTINGS + "internode_compatibility_mode",
                                                                   "DSE_5_1",
                                                                   SETTINGS + "uuid_sstable_identifiers_enabled",
                                                                   "true",
                                                                   SETTINGS + "sstable.selected_format", "big",
                                                                   // the daemon would refuse both; a tool does not
                                                                   // check them
                                                                   SETTINGS + "storage_compatibility_mode", "NONE",
                                                                   SETTINGS + "paxos_variant", "v2"))
        {
            Logger ddLogger = (Logger) LoggerFactory.getLogger(DatabaseDescriptor.class);
            ListAppender<ILoggingEvent> appender = new ListAppender<>();
            appender.start();
            ddLogger.addAppender(appender);
            try
            {
                DatabaseDescriptor.toolInitialization();
            }
            finally
            {
                ddLogger.detachAppender(appender);
            }
            String warning = "internode_compatibility_mode DSE_5_1 forces uuid_sstable_identifiers_enabled off: " +
                             "DSE 5.1 only parses numeric sstable generations";
            assertThat(appender.list).anyMatch(e -> e.getLevel() == Level.WARN &&
                                                    e.getFormattedMessage().equals(warning));
            // the mode is announced by a node only
            assertThat(appender.list).noneMatch(e -> e.getFormattedMessage()
                                                      .startsWith("Running in internode_compatibility_mode DSE_5_1"));

            assertThat(DatabaseDescriptor.getInternodeCompatibilityMode()).isEqualTo(InternodeCompatibilityMode.NONE);
            assertThat(DatabaseDescriptor.isDseCompatibilityMode()).isFalse();
            assertThat(DatabaseDescriptor.isDse51CompatibilityMode()).isFalse();
            // nothing of DSE 6's on-disk half
            assertThat(DatabaseDescriptor.isDse6SSTableOutput()).isFalse();
            assertThat(TRIE_INDEX_FORMAT_VERSION.isPresent()).isFalse();
            assertThat(DatabaseDescriptor.getSelectedSSTableFormat().name()).isEqualTo(BigFormat.NAME);
            assertThat(DatabaseDescriptor.isUUIDSSTableIdentifiersEnabled()).isFalse();
            assertThat(DatabaseDescriptor.getPaxosVariant()).isEqualTo(Config.PaxosVariant.v2);

            // a tool refuses the format and the compressors the daemon refuses
            Config bti = new Config();
            bti.internode_compatibility_mode = DSE_5_1;
            bti.uuid_sstable_identifiers_enabled = true;
            assertThatThrownBy(() -> DatabaseDescriptor.applyInternodeCompatibilityMode(bti))
            .isInstanceOf(ConfigurationException.class)
            .hasMessage(Dse51ModeConfigTest.bigFormatRefusal("bti"));
            assertThat(bti.uuid_sstable_identifiers_enabled).isTrue();
            Config.FlushCompression[] refused = { Config.FlushCompression.adaptive, Config.FlushCompression.none };
            for (Config.FlushCompression flushCompression : refused)
            {
                Config config = new Config();
                config.internode_compatibility_mode = DSE_5_1;
                config.sstable.selected_format = BigFormat.NAME;
                config.flush_compression = flushCompression;
                String compressor = flushCompression == Config.FlushCompression.adaptive ? "AdaptiveCompressor"
                                                                                         : "NoopCompressor";
                assertThatThrownBy(() -> DatabaseDescriptor.applyInternodeCompatibilityMode(config))
                .isInstanceOf(ConfigurationException.class)
                .hasMessage(Dse51ModeConfigTest.flushCompressionRefusal(flushCompression, compressor));
            }
            String custom = "org.example.CustomSelector";
            try (WithProperties selector = new WithProperties().set(SSTABLE_COMPRESSION_SELECTOR_CLASS, custom))
            {
                Config config = new Config();
                config.internode_compatibility_mode = DSE_5_1;
                config.sstable.selected_format = BigFormat.NAME;
                assertThatThrownBy(() -> DatabaseDescriptor.applyInternodeCompatibilityMode(config))
                .isInstanceOf(ConfigurationException.class)
                .hasMessage(Dse51ModeConfigTest.compressionSelectorRefusal(custom));
            }
        }
    }
}
