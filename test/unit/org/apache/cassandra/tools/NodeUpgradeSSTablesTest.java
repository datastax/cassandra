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

package org.apache.cassandra.tools;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.assertj.core.api.Assertions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(OrderedJUnit4ClassRunner.class)
public class NodeUpgradeSSTablesTest extends CQLTester
{
    private static NodeProbe probe;

    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
        probe = new NodeProbe(jmxHost, jmxPort);
    }

    @AfterClass
    public static void teardown() throws IOException
    {
        probe.close();
    }

    @Test
    public void testConcurrentCompactorsExceeded() throws Throwable
    {
        int concurrentCompactors = probe.getConcurrentCompactors() + 2;
        ToolResult toolResult = ToolRunner.invokeNodetool("upgradesstables", "-j", String.valueOf(concurrentCompactors));
        Assertions.assertThat(toolResult.getStdout()).containsPattern("jobs \\(\\d*\\) is bigger than configured concurrent_compactors \\(\\d*\\), using at most \\d* threads");
    }
    
    @Test
    public void testSetConcurrentCompactors() throws Throwable
    {
        // Increase the number of concurrent compactors
        int concurrentCompactors = probe.getConcurrentCompactors() + 2;
        assertToolResult(ToolRunner.invokeNodetool("setconcurrentcompactors", String.valueOf(concurrentCompactors)));

        // Verify that the number of concurrent compactors set by 'nodetool setconcurrentcompactors'
        // is used in subsequent nodetool upgradesstables
        assertToolResult(ToolRunner.invokeNodetool("upgradesstables", "-j", String.valueOf(concurrentCompactors)));
    }
    
    private void assertToolResult(ToolResult toolResult)
    {
        assertTrue(toolResult.getStdout().isEmpty());
        toolResult.assertOnCleanExit();
    }
}
