/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils;

import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.net.MessagingService;
import org.assertj.core.api.Assertions;

import static org.junit.Assert.assertEquals;

public class StorageCompatibilityModeTest
{
    @Test
    public void testBtiFormatAndStorageCompatibilityMode()
    {
        SSTableFormat<?, ?> big = new BigFormat(null);
        SSTableFormat<?, ?> trie = new BtiFormat(null);

        for (StorageCompatibilityMode mode : StorageCompatibilityMode.values())
        {
            switch (mode)
            {
                case CC_4:
                case UPGRADING:
                case NONE:
                    mode.validateSstableFormat(big);
                    mode.validateSstableFormat(trie);
                    break;
                case CASSANDRA_4:
                    mode.validateSstableFormat(big);
                    Assertions.assertThatThrownBy(() -> mode.validateSstableFormat(trie))
                              .isInstanceOf(ConfigurationException.class)
                              .hasMessageContaining("is not available when in storage compatibility mode");
                    break;
                default:
                    throw new AssertionError("Undefined behaviour for mode " + mode);
            }
        }
    }

    @Test
    public void testStorageMessagingVersion()
    {
        // CASSANDRA_4 and CC_4 should use VERSION_40 for storage compatibility
        assertEquals(MessagingService.VERSION_40, StorageCompatibilityMode.CASSANDRA_4.storageMessagingVersion());
        assertEquals(MessagingService.VERSION_40, StorageCompatibilityMode.CC_4.storageMessagingVersion());

        // UPGRADING and NONE should use the current messaging version
        assertEquals(MessagingService.current_version, StorageCompatibilityMode.UPGRADING.storageMessagingVersion());
        assertEquals(MessagingService.current_version, StorageCompatibilityMode.NONE.storageMessagingVersion());
    }

    @Test
    public void testStorageMessagingVersionForAllModes()
    {
        // Ensure all modes have defined behavior
        for (StorageCompatibilityMode mode : StorageCompatibilityMode.values())
        {
            int version = mode.storageMessagingVersion();
            switch (mode)
            {
                case CASSANDRA_4:
                case CC_4:
                    assertEquals("Mode " + mode + " should use VERSION_40 for storage",
                                 MessagingService.VERSION_40, version);
                    break;
                case UPGRADING:
                case NONE:
                    assertEquals("Mode " + mode + " should use current_version for storage",
                                 MessagingService.current_version, version);
                    break;
                default:
                    throw new AssertionError("Undefined storage messaging version behaviour for mode " + mode);
            }
        }
    }
}
