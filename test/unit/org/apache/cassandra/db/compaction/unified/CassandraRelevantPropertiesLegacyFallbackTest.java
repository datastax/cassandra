/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.compaction.unified;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_PREFIX;
import static org.apache.cassandra.config.CassandraRelevantProperties.LEGACY_PREFIX;
import static org.apache.cassandra.config.CassandraRelevantProperties.UCS_ADAPTIVE_MIN_COST;
import static org.apache.cassandra.config.CassandraRelevantProperties.UCS_MIN_SSTABLE_SIZE;
import static org.apache.cassandra.config.CassandraRelevantProperties.UCS_SHARED_STORAGE;
import static org.apache.cassandra.db.compaction.unified.Controller.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

// checkstyle: suppress below 'blockSystemPropertyUsage'

public class CassandraRelevantPropertiesLegacyFallbackTest
{

    /**
     * Making sure to start each test with clean system property state
     */
    @After
    public void doAfter()
    {
        CassandraRelevantProperties[] options = new CassandraRelevantProperties[]{ UCS_MIN_SSTABLE_SIZE, UCS_ADAPTIVE_MIN_COST, UCS_SHARED_STORAGE };
        String[] prefixes = new String[]{ CASSANDRA_PREFIX, LEGACY_PREFIX };

        for (CassandraRelevantProperties option : options)
        {
            option.clearValueWithLegacyPrefix();
        }
    }

    @Test
    public void testGetStringProperty()
    {
        try (WithProperties properties = new WithProperties().clear(UCS_MIN_SSTABLE_SIZE))
        {
            String propValue = UCS_MIN_SSTABLE_SIZE.getStringWithLegacyFallback();
            assertEquals(DEFAULT_MIN_SSTABLE_SIZE, FBUtilities.parseHumanReadableBytes(propValue));

            UCS_MIN_SSTABLE_SIZE.setString("50MiB");
            assertEquals("50MiB", UCS_MIN_SSTABLE_SIZE.getStringWithLegacyFallback());

            System.setProperty(UCS_MIN_SSTABLE_SIZE.getKeyWithLegacyPrefix(), "1MiB");
            // Since PREFIX is set it takes precedence
            assertEquals("50MiB", UCS_MIN_SSTABLE_SIZE.getStringWithLegacyFallback());

            UCS_MIN_SSTABLE_SIZE.clearValue(); // checkstyle: suppress nearby 'clearValueSystemPropertyUsage'
            assertEquals("1MiB", UCS_MIN_SSTABLE_SIZE.getStringWithLegacyFallback());
        }
    }

    @Test
    public void testGetIntegerSystemProperty()
    {
        try (WithProperties properties = new WithProperties().clear(UCS_ADAPTIVE_MIN_COST))
        {
            int minCost = UCS_ADAPTIVE_MIN_COST.getIntWithLegacyFalback();
            assertEquals(1000, minCost);

            UCS_ADAPTIVE_MIN_COST.setString("500");
            assertEquals(500, UCS_ADAPTIVE_MIN_COST.getIntWithLegacyFalback());

            System.setProperty(UCS_ADAPTIVE_MIN_COST.getKeyWithLegacyPrefix(), "100");
            assertEquals(500, UCS_ADAPTIVE_MIN_COST.getIntWithLegacyFalback());

            UCS_ADAPTIVE_MIN_COST.clearValue(); // checkstyle: suppress nearby 'clearValueSystemPropertyUsage'
            assertEquals(100, UCS_ADAPTIVE_MIN_COST.getIntWithLegacyFalback());
        }
    }

    @Test
    public void testGetBooleanSystemProperty()
    {
        try (WithProperties properties = new WithProperties().clear(UCS_SHARED_STORAGE))
        {
            boolean sharedStorage = UCS_SHARED_STORAGE.getBooleanWithLegacyFallback();
            assertFalse(sharedStorage);

            System.setProperty(UCS_SHARED_STORAGE.getKeyWithLegacyPrefix(), "false");
            assertFalse(UCS_SHARED_STORAGE.getBooleanWithLegacyFallback());

            System.setProperty(UCS_SHARED_STORAGE.getKeyWithLegacyPrefix(), "true");
            assertTrue(UCS_SHARED_STORAGE.getBooleanWithLegacyFallback());

            UCS_SHARED_STORAGE.setString("false");
            assertFalse(UCS_SHARED_STORAGE.getBooleanWithLegacyFallback());

            UCS_SHARED_STORAGE.setString("true");
            assertTrue(UCS_SHARED_STORAGE.getBooleanWithLegacyFallback());
        }
    }
}
