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

import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.apache.cassandra.config.CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A node in {@code internode_compatibility_mode: DSE_6} whose trie format version was frozen before the mode could
 * set it (because {@code BtiFormat} was initialised first) must refuse to start rather than write sstables DSE cannot
 * read. In a class of its own: the check depends on the class initialisation order of a fresh JVM.
 */
public class Dse6TrieVersionInitialisedEarlyTest
{
    @Test
    public void daemonRefusesATrieVersionFrozenBeforeTheModeApplies() throws Exception
    {
        try (WithProperties properties = new WithProperties().clear(TRIE_INDEX_FORMAT_VERSION))
        {
            // freezes the trie version of this JVM at the default, cc
            Class.forName("org.apache.cassandra.io.sstable.format.bti.BtiFormat$BtiVersion", true, getClass().getClassLoader());

            Config config = DatabaseDescriptor.loadConfig();
            config.internode_compatibility_mode = InternodeCompatibilityMode.DSE_6;
            config.storage_compatibility_mode = StorageCompatibilityMode.HCD_1;
            config.paxos_variant = Config.PaxosVariant.v1;
            config.sstable.selected_format = "bti";

            assertThatThrownBy(() -> DatabaseDescriptor.daemonInitialization(() -> config))
            .isInstanceOf(ConfigurationException.class)
            .hasMessage("internode_compatibility_mode DSE_6 requires trie index format version bb, but the format was already initialised " +
                        "with cc; start with -Dcassandra.trie_index_format_version=bb");
            // the mode had asked for bb, too late
            assertThat(TRIE_INDEX_FORMAT_VERSION.getString()).isEqualTo("bb");
        }
    }
}
