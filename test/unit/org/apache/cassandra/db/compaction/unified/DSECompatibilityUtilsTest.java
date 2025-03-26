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

import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.db.compaction.unified.AdaptiveController.MIN_COST;
import static org.apache.cassandra.db.compaction.unified.Controller.*;
import static org.apache.cassandra.db.compaction.unified.DSECompatibilityUtils.getBooleanSystemProperty;
import static org.apache.cassandra.db.compaction.unified.DSECompatibilityUtils.getIntegerSystemProperty;
import static org.apache.cassandra.db.compaction.unified.DSECompatibilityUtils.getSystemProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DSECompatibilityUtilsTest
{

    /**
     * Making sure to start each test with clean system property state
     */
    @After
    public void doAfter()
    {
        String[] options = new String[]{MIN_SSTABLE_SIZE_OPTION, MIN_COST, SHARED_STORAGE};
        String[] prefixes = new String[]{PREFIX, LEGACY_PREFIX};

        for(String option: options)
            for(String prefix: prefixes)
                System.clearProperty(prefix + option);
    }
    @Test
    public void testGetSystemProperty()
    {
       String propValue = getSystemProperty(MIN_SSTABLE_SIZE_OPTION, "100MiB");
       assertEquals(DEFAULT_MIN_SSTABLE_SIZE , FBUtilities.parseHumanReadableBytes(propValue));

       System.setProperty(PREFIX + MIN_SSTABLE_SIZE_OPTION, "50MiB");
       assertEquals("50MiB", getSystemProperty(MIN_SSTABLE_SIZE_OPTION));

       System.setProperty(LEGACY_PREFIX + MIN_SSTABLE_SIZE_OPTION, "1MiB");
       // Since PREFIX is set it takes precedence
       assertEquals("50MiB", getSystemProperty(MIN_SSTABLE_SIZE_OPTION));

       System.clearProperty(PREFIX + MIN_SSTABLE_SIZE_OPTION);
       assertEquals("1MiB", getSystemProperty(MIN_SSTABLE_SIZE_OPTION));
    }

    @Test
    public void testGetIntegerSystemProperty()
    {
        int minCost = getIntegerSystemProperty(MIN_COST, 1000);
        assertEquals(1000, minCost);

        System.setProperty(PREFIX + MIN_COST, "500");
        assertEquals(500, (int) getIntegerSystemProperty(MIN_COST, 0));

        System.setProperty(LEGACY_PREFIX + MIN_COST, "100");
        assertEquals(500, (int) getIntegerSystemProperty(MIN_COST, 0));

        System.clearProperty(PREFIX + MIN_COST);
        assertEquals(100, (int) getIntegerSystemProperty(MIN_COST, 0));
    }

    @Test
    public void testGetBooleanSystemProperty()
    {
        boolean sharedStorage = getBooleanSystemProperty(SHARED_STORAGE);
        assertFalse(sharedStorage);

        System.setProperty(LEGACY_PREFIX + SHARED_STORAGE, "false");
        assertFalse(getBooleanSystemProperty(SHARED_STORAGE));

        System.setProperty(LEGACY_PREFIX + SHARED_STORAGE, "true");
        assertTrue(getBooleanSystemProperty(SHARED_STORAGE));

        System.setProperty(PREFIX + SHARED_STORAGE, "false");
        assertFalse(getBooleanSystemProperty(SHARED_STORAGE));

        System.setProperty(PREFIX + SHARED_STORAGE, "true");
        assertTrue(getBooleanSystemProperty(SHARED_STORAGE));
    }
}
