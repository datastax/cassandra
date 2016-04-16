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

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;

@RunWith(OrderedJUnit4ClassRunner.class)
public class StandaloneSplitterTest extends ToolsTester
{
    @Test
    public void testStandaloneSplitter_NoArgs()
    {
        runTool(1, "org.apache.cassandra.tools.StandaloneSplitter");
        assertNoUnexpectedThreadsStarted(null, null);
        assertSchemaNotLoaded();
        assertCLSMNotLoaded();
        assertSystemKSNotLoaded();
        assertKeyspaceNotLoaded();
        assertServerNotLoaded();
    }

    @Test
    public void testStandaloneSplitter_WithArgs() throws Exception
    {
        // the legacy tables use a different partitioner :(
        // (Don't use ByteOrderedPartitioner.class.getName() as that would initialize the class and work
        // against the goal of this test to check classes and threads initialized by the tool.)
        System.setProperty("cassandra.partitioner", "org.apache.cassandra.dht.ByteOrderedPartitioner");

        File srcDir = sstableDir("legacy_sstables", "legacy_ma_simple");
        File sstable = srcDir.listFiles(f -> f.isFile() && f.getName().endsWith("-Data.db"))[0];

        // this one will fail - as StandaloneSplitter requires a valid schema (and we don't have
        // one yet at this stage).
        try
        {
            runTool(1, "org.apache.cassandra.tools.StandaloneSplitter", "--debug", sstable.getAbsolutePath());
        }
        catch (AssertionError ignored)
        {
            // java.lang.AssertionError: Unknown keyspace legacy_sstables
        }

        // Now take one of the sstables from the legacy-sstables and just put it into the directory for
        // system_schema.keyspaces. (Ugly, but it works for this test.)
        File dstDir = new File(srcDir, "../../system_schema").listFiles(f -> f.isDirectory() && f.getName().startsWith("keyspaces-"))[0];
        FileUtils.copyDirectory(srcDir, dstDir);
        sstable = dstDir.listFiles(f -> f.isFile() && f.getName().endsWith("-Data.db"))[0];
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run()
            {
                try
                {
                    FileUtils.deleteDirectory(dstDir);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
        });

        runTool(0, "org.apache.cassandra.tools.StandaloneSplitter", "--debug", sstable.getAbsolutePath());
        assertNoUnexpectedThreadsStarted(EXPECTED_THREADS_WITH_SCHEMA, OPTIONAL_THREADS_WITH_SCHEMA);
        assertSchemaLoaded();
        assertServerNotLoaded();
    }
}
