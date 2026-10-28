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
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;

import static org.apache.cassandra.config.CassandraRelevantProperties.CONFIG_ALLOW_SYSTEM_PROPERTIES;
import static org.apache.cassandra.config.CassandraRelevantProperties.SSTABLE_COMPRESSION_SELECTOR_CLASS;
import static org.apache.cassandra.config.CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Offline tools (sstablescrub, sstableupgrade, sstablesplit, ...) reading a cassandra.yaml that says
 * {@code internode_compatibility_mode: DSE_6} do not speak the internode protocol, so for them the mode is
 * {@code NONE}, but they inherit its on-disk half: what they write stays readable by DSE 6. In a class of its own:
 * {@code toolInitialization} and the trie version of {@code BtiFormat} happen once per JVM.
 */
public class InternodeCompatibilityModeToolTest
{
    @Test
    public void toolsInheritTheOnDiskHalfOfDse6()
    {
        try (WithProperties properties = new WithProperties().clear(TRIE_INDEX_FORMAT_VERSION)
                                                             .set(CONFIG_ALLOW_SYSTEM_PROPERTIES, true)
                                                             .with("cassandra.settings.internode_compatibility_mode", "DSE_6",
                                                                   "cassandra.settings.uuid_sstable_identifiers_enabled", "true",
                                                                   // the daemon would refuse both; a tool does not check them
                                                                   "cassandra.settings.storage_compatibility_mode", "NONE",
                                                                   "cassandra.settings.paxos_variant", "v2"))
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
            // the mode is not announced for a tool, but forcing the uuid setting that the tool read from the yaml is
            assertThat(appender.list).noneMatch(e -> e.getFormattedMessage().startsWith("Running in internode_compatibility_mode DSE_6"));
            assertThat(appender.list).anyMatch(e -> e.getLevel() == Level.WARN &&
                                                    e.getFormattedMessage().startsWith("internode_compatibility_mode DSE_6 forces uuid_sstable_identifiers_enabled off"));

            assertThat(DatabaseDescriptor.getInternodeCompatibilityMode()).isEqualTo(InternodeCompatibilityMode.NONE);
            assertThat(DatabaseDescriptor.isDse6CompatibilityMode()).isFalse();
            // the gate of the on-disk half, which names and writes the transaction logs as DSE 6 completes them
            // (DseTransactionLogsTest covers what follows from it)
            assertThat(DatabaseDescriptor.isDse6SSTableOutput()).isTrue();
            // so the sstables a tool writes carry DSE 6's cell count histogram (MetadataCollectorDse6Test covers the rest)
            assertThat(MetadataCollector.defaultStatsMetadata().estimatedCellPerPartitionCount.getBucketOffsets()).hasSize(114);
            assertThat(TRIE_INDEX_FORMAT_VERSION.getString()).isEqualTo(InternodeCompatibilityMode.DSE_6_TRIE_INDEX_FORMAT_VERSION);
            assertThat(BtiFormat.getInstance().getLatestVersion().version).isEqualTo(InternodeCompatibilityMode.DSE_6_TRIE_INDEX_FORMAT_VERSION);
            assertThat(DatabaseDescriptor.getSelectedSSTableFormat().getLatestVersion().version).isEqualTo(InternodeCompatibilityMode.DSE_6_TRIE_INDEX_FORMAT_VERSION);
            assertThat(DatabaseDescriptor.isUUIDSSTableIdentifiersEnabled()).isFalse();
            assertThat(DatabaseDescriptor.getPaxosVariant()).isEqualTo(Config.PaxosVariant.v2);

            // the compressor checks are part of the on-disk half: a tool refuses what the daemon refuses
            for (Config.FlushCompression refused : new Config.FlushCompression[]{ Config.FlushCompression.adaptive, Config.FlushCompression.none })
            {
                Config config = new Config();
                config.internode_compatibility_mode = InternodeCompatibilityMode.DSE_6;
                config.flush_compression = refused;
                config.uuid_sstable_identifiers_enabled = true;
                String compressor = refused == Config.FlushCompression.adaptive ? "AdaptiveCompressor" : "NoopCompressor";
                assertThatThrownBy(() -> DatabaseDescriptor.applyInternodeCompatibilityMode(config))
                .isInstanceOf(ConfigurationException.class)
                .hasMessage(InternodeCompatibilityModeTest.flushCompressionRefusal(refused, compressor));
                // refused before the uuid setting is forced
                assertThat(config.uuid_sstable_identifiers_enabled).isTrue();
            }
            try (WithProperties selector = new WithProperties().set(SSTABLE_COMPRESSION_SELECTOR_CLASS, "org.example.CustomSelector"))
            {
                Config dse6 = new Config();
                dse6.internode_compatibility_mode = InternodeCompatibilityMode.DSE_6;
                dse6.uuid_sstable_identifiers_enabled = true;
                assertThatThrownBy(() -> DatabaseDescriptor.applyInternodeCompatibilityMode(dse6))
                .isInstanceOf(ConfigurationException.class)
                .hasMessage(InternodeCompatibilityModeTest.compressionSelectorRefusal("org.example.CustomSelector"));
                assertThat(dse6.uuid_sstable_identifiers_enabled).isTrue();
            }
        }
    }
}
