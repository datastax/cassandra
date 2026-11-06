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
import org.apache.cassandra.io.sstable.format.big.BigFormat;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE;
import static org.apache.cassandra.config.CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Clients that write sstables without reading cassandra.yaml ({@code CQLSSTableWriter}, the bulk loader) and set
 * {@code -Dcassandra.internode_compatibility_mode=DSE_5_1} cannot select the sstable format themselves: they get the
 * big format and numeric generations forced, each with an INFO line, and the internode mode stays {@code NONE}. In a
 * class of its own: {@code clientInitialization} happens once per JVM.
 */
public class Dse51ModeClientTest
{
    @Test
    public void clientsGetTheBigFormatAndNumericGenerations()
    {
        try (WithProperties properties = new WithProperties().clear(TRIE_INDEX_FORMAT_VERSION)
                                                             .set(INTERNODE_COMPATIBILITY_MODE, "DSE_5_1"))
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
            assertThat(appender.list).noneMatch(e -> e.getLevel() == Level.WARN &&
                                                     e.getFormattedMessage().contains("internode_compatibility_mode"));
            String format = "internode_compatibility_mode DSE_5_1 selects sstable.selected_format big in place of " +
                            "bti: DSE 5.1 only reads the big format";
            assertThat(appender.list).anyMatch(e -> e.getLevel() == Level.INFO &&
                                                    e.getFormattedMessage().equals(format));
            String generations = "internode_compatibility_mode DSE_5_1 forces uuid_sstable_identifiers_enabled off";
            assertThat(appender.list).anyMatch(e -> e.getLevel() == Level.INFO &&
                                                    e.getFormattedMessage().startsWith(generations));

            assertThat(DatabaseDescriptor.getInternodeCompatibilityMode()).isEqualTo(InternodeCompatibilityMode.NONE);
            assertThat(DatabaseDescriptor.isDse6SSTableOutput()).isFalse();
            assertThat(TRIE_INDEX_FORMAT_VERSION.isPresent()).isFalse();
            // the defaults of a client configuration are bti and uuid generations
            assertThat(new Config().sstable.selected_format).isNotEqualTo(BigFormat.NAME);
            assertThat(new Config().uuid_sstable_identifiers_enabled).isTrue();
            assertThat(DatabaseDescriptor.getSelectedSSTableFormat().name()).isEqualTo(BigFormat.NAME);
            assertThat(DatabaseDescriptor.isUUIDSSTableIdentifiersEnabled()).isFalse();
        }
    }
}
