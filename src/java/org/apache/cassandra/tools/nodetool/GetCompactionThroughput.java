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
package org.apache.cassandra.tools.nodetool;

import java.util.Map;

import io.airlift.airline.Command;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "getcompactionthroughput", description = "Print the MiB/s throughput cap for compaction in the system")
public class GetCompactionThroughput extends NodeToolCmd
{
    @Override
    public void execute(NodeProbe probe)
    {
        probe.output().out.println("Current compaction throughput: " + probe.getCompactionThroughput() + " MiB/s");

        Map<String, String> currentCompactionThroughputMetricsMap = probe.getCurrentCompactionThroughputMiBPerSec();
        probe.output().out.println("Current compaction throughput (1 minute): " + currentCompactionThroughputMetricsMap.get("1minute") + " MiB/s");
        probe.output().out.println("Current compaction throughput (5 minute): " + currentCompactionThroughputMetricsMap.get("5minute") + " MiB/s");
        probe.output().out.println("Current compaction throughput (15 minute): " + currentCompactionThroughputMetricsMap.get("15minute") + " MiB/s");
    }
}
