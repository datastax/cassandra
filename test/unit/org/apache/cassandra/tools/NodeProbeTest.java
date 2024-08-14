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
import java.io.PrintStream;
import java.util.concurrent.ExecutionException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tools.ToolRunner.ToolResult;

import static org.assertj.core.api.Assertions.assertThat;
import static java.lang.String.format;

public class NodeProbeTest extends CQLTester
{
    private static NodeProbe probe;

    @BeforeClass
    public static void setup() throws Exception
    {
        startJMXServer();
        probe = new NodeProbe(jmxHost, jmxPort);
    }

    @AfterClass
    public static void teardown() throws IOException
    {
        probe.close();
    }

    /**
     * Verify that nodetool -j/--jobs option warning is raised depending on the value of
     * {@link org.apache.cassandra.config.Config#concurrent_compactors} in the target node, independently of where the
     * tool is used.
     *
     * Before CASSANDRA-16104 the warning was based on the local value of {@code concurrent_compactors}, and not in the
     * value used in the target node, which is got through JMX.
     */
    @Test
    public void testCheckJobs()
    {
        int compactors = probe.getConcurrentCompactors();
        int jobs = compactors + 1;

        // Verify that trying to use more jobs than configured concurrent compactors prints a warning
        ToolResult toolResult = ToolRunner.invokeNodetool("upgradesstables", "-j", String.valueOf(jobs));
        toolResult.assertOnCleanExit();
        assertThat(toolResult.getStdout()).contains(format("jobs (%d) is bigger than configured concurrent_compactors (%d) on the host, using at most %d threads",
                                                           jobs, compactors, compactors));

        // Increase the number of concurrent compactors and verify that the new number of concurrent compactors is seen
        // by subsequent validations
        assertToolResult(ToolRunner.invokeNodetool("setconcurrentcompactors", String.valueOf(jobs)));
        assertToolResult(ToolRunner.invokeNodetool("upgradesstables", "-j", String.valueOf(jobs)));
    }

    @Test
    public void testVerifyWithoutValidateAllRows() throws IOException, ExecutionException, InterruptedException
    {
        ToolResult toolResult = ToolRunner.invokeNodetool("verify", "--extended-verify");
        toolResult.assertOnCleanExit();
        assertThat(toolResult.getStdout()).isEmpty();

        for (String keyspace : probe.getKeyspaces())
        {
            PrintStream out = probe.output().out;
            probe.verify(out, true, false, false, false, false, false, false, keyspace);
            probe.verify(true, false, false, false, false, false, false, keyspace);
        }
    }

    @Test
    public void testVerifyWithValidateAllRows() throws IOException, ExecutionException, InterruptedException
    {
        // new config is recognized
        ToolResult toolResult = ToolRunner.invokeNodetool("verify", "--extended-verify", "--validate-all-rows");
        toolResult.assertOnCleanExit();
        assertThat(toolResult.getStdout()).isEmpty();

        for (String keyspace : probe.getKeyspaces())
        {
            PrintStream out = probe.output().out;
            probe.verify(out, true, true, false, false, false, false, false, keyspace);
            probe.verify(true, true, false, false, false, false, false, keyspace);
        }
    }

    private static void assertToolResult(ToolResult toolResult)
    {
        assertThat(toolResult.getStdout()).isEmpty();
        toolResult.assertOnCleanExit();
    }
}
