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

package org.apache.cassandra.guardrails;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class GuardrailPartitionSizeTest extends GuardrailTester
{
    private static int partitionSizeThreshold;
    private AtomicBoolean warnTriggered = new AtomicBoolean(false);
    private AtomicBoolean failTriggered = new AtomicBoolean(false);

    @BeforeClass
    public static void setupClass()
    {
        partitionSizeThreshold = DatabaseDescriptor.getGuardrailsConfig().partition_size_warn_threshold_in_mb;
        DatabaseDescriptor.getGuardrailsConfig().partition_size_warn_threshold_in_mb = 1;
    }

    @AfterClass
    public static void tearDownClass()
    {
        DatabaseDescriptor.getGuardrailsConfig().partition_size_warn_threshold_in_mb = partitionSizeThreshold;
    }

    @Before
    public void setup()
    {
        createTable("CREATE TABLE IF NOT EXISTS %s (k INT, c INT, v TEXT, PRIMARY KEY(k, c))");
        warnTriggered.set(false);
        failTriggered.set(false);
    }

    @After
    public void tearDown()
    {
        dropTable("DROP TABLE %s");
    }

    private final Guardrails.Listener testListener = new Guardrails.Listener()
    {
        @Override
        public void onWarningTriggered(String guardrailName, String message)
        {
            assertThat("partition_size").isEqualTo(guardrailName);
            assertEquals(String.format("Detected partition 100 in %s of size 1.1MB is greater than the maximum recommended size (1MB)",
                                       currentTableMetadata()), message);
            warnTriggered.set(true);
        }

        @Override
        public void onFailureTriggered(String guardrailName, String message)
        {
            failTriggered.set(true);
        }
    };

    @Test
    public void testCompactLargePartition() throws Throwable
    {
        Guardrails.register(testListener);

        disableCompaction();
        // insert stuff into a single partition
        for (int i = 0; i < 23000; i++)
            execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", 100, i, "long string for large partition test");

        flush();
        enableCompaction();
        compact();

        assertThat(warnTriggered.get()).isTrue();
        assertThat(failTriggered.get()).isFalse();
        Guardrails.unregister(testListener);
    }
}