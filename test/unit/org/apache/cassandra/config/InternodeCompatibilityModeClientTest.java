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
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE;
import static org.apache.cassandra.config.CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Clients that write sstables without reading cassandra.yaml ({@code CQLSSTableWriter}, the bulk loader) inherit
 * the on-disk half of {@code DSE_6} through the {@code cassandra.internode_compatibility_mode} system property:
 * trie version {@code bb} and numeric generations, with the internode mode left at {@code NONE}. In a class of its
 * own: {@code clientInitialization} and the trie version of {@code BtiFormat} happen once per JVM.
 */
public class InternodeCompatibilityModeClientTest
{
    @Test
    public void clientsInheritTheOnDiskHalfOfDse6ThroughTheProperty()
    {
        try (WithProperties properties = new WithProperties().clear(TRIE_INDEX_FORMAT_VERSION)
                                                             .set(INTERNODE_COMPATIBILITY_MODE, "DSE_6"))
        {
            Logger ddLogger = (Logger) LoggerFactory.getLogger(DatabaseDescriptor.class);
            ListAppender<ILoggingEvent> appender = new ListAppender<>();
            appender.start();
            ddLogger.addAppender(appender);
            try
            {
                DatabaseDescriptor.clientInitialization();
            }
            finally
            {
                ddLogger.detachAppender(appender);
            }
            // the mode is not announced for a client, and forcing the uuid setting it cannot change is not a warning
            assertThat(appender.list).noneMatch(e -> e.getFormattedMessage().startsWith("Running in internode_compatibility_mode DSE_6"));
            assertThat(appender.list).noneMatch(e -> e.getLevel() == Level.WARN && e.getFormattedMessage().contains("internode_compatibility_mode"));
            assertThat(appender.list).anyMatch(e -> e.getLevel() == Level.INFO &&
                                                    e.getFormattedMessage().startsWith("internode_compatibility_mode DSE_6 forces uuid_sstable_identifiers_enabled off"));

            assertThat(DatabaseDescriptor.getInternodeCompatibilityMode()).isEqualTo(InternodeCompatibilityMode.NONE);
            assertThat(DatabaseDescriptor.isDse6CompatibilityMode()).isFalse();
            assertThat(TRIE_INDEX_FORMAT_VERSION.getString()).isEqualTo(InternodeCompatibilityMode.DSE_6_TRIE_INDEX_FORMAT_VERSION);
            assertThat(BtiFormat.getInstance().getLatestVersion().version).isEqualTo(InternodeCompatibilityMode.DSE_6_TRIE_INDEX_FORMAT_VERSION);
            assertThat(DatabaseDescriptor.getSelectedSSTableFormat().getLatestVersion().version).isEqualTo(InternodeCompatibilityMode.DSE_6_TRIE_INDEX_FORMAT_VERSION);
            // the default of a client configuration is true
            assertThat(new Config().uuid_sstable_identifiers_enabled).isTrue();
            assertThat(DatabaseDescriptor.isUUIDSSTableIdentifiersEnabled()).isFalse();
        }
    }
}
