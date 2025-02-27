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

import java.io.PrintStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.db.compaction.CompactionStrategyStatistics;
import org.apache.cassandra.db.compaction.TableOperation;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

import static java.lang.String.format;

@Command(name = "compactionstats", description = "Print statistics on compactions")
public class CompactionStats extends NodeToolCmd
{
    @Option(title = "human_readable",
            name = {"-H", "--human-readable"},
            description = "Display bytes in human readable form, i.e. KiB, MiB, GiB, TiB")
    private boolean humanReadable = false;

    @Option(title = "aggregate",
    name = {"-A", "--aggregate"},
    description = "Show the compaction aggregates for the compactions in progress, e.g. the levels for LCS or the buckets for STCS and TWCS.")
    private boolean aggregate = false;

    @Option(title = "overlap",
    name = {"-O", "--overlap"},
    description = "Show a map of the maximum sstable overlap per compaction region.\n" +
                  "Note: This map includes all sstables in the system, including ones that are currently being compacted, " +
                  "and also takes into account early opened sstables. Overlaps per level may be greater than the values " +
                  "the --aggregate option reports.")
    private boolean overlap = false;

    @Arguments(usage = "[<keyspace> <tables>...]", description = "With --aggregate or --overlap, optionally list only the data for the specified keyspace and tables.")
    private List<String> args = new ArrayList<>();

    @Override
    public void execute(NodeProbe probe)
    {
        PrintStream out = probe.output().out;
        CompactionManagerMBean cm = probe.getCompactionManagerProxy();
        Map<String, Map<String, Integer>> pendingTaskNumberByTable =
            (Map<String, Map<String, Integer>>) probe.getCompactionMetric("PendingTasksByTableName");
        Map<String, Map<String, Double>> writeAmplificationByTableName =
        (Map<String, Map<String, Double>>) probe.getCompactionMetric("WriteAmplificationByTableName");
        int numTotalPendingTask = 0;
        double totWriteAmplification = 0;
        for (Entry<String, Map<String, Integer>> ksEntry : pendingTaskNumberByTable.entrySet())
        {
            Map<String, Double> ksWriteAmplification = writeAmplificationByTableName.get(ksEntry.getKey());
            for (Entry<String, Integer> tableEntry : ksEntry.getValue().entrySet())
            {
                numTotalPendingTask += tableEntry.getValue();
                if (ksWriteAmplification != null)
                    totWriteAmplification += ksWriteAmplification.get(tableEntry.getKey());
            }
        }

        Set<String> keyspaces = new HashSet<>(parseOptionalKeyspace(args, probe));
        Set<String> tableNames = new HashSet<>(Arrays.asList(parseOptionalTables(args)));

        out.println("pending tasks: " + numTotalPendingTask);
        out.println(String.format("write amplification: %.2f", totWriteAmplification));
        for (Entry<String, Map<String, Integer>> ksEntry : pendingTaskNumberByTable.entrySet())
        {
            String ksName = ksEntry.getKey();
            Map<String, Double> ksWriteAmplification = writeAmplificationByTableName.get(ksName);
            for (Entry<String, Integer> tableEntry : ksEntry.getValue().entrySet())
            {
                String tableName = tableEntry.getKey();
                int pendingTaskCount = tableEntry.getValue();

                double wa = ksWriteAmplification == null ? 0 : ksWriteAmplification.get(tableName);
                out.println(String.format("- %s.%s: %d", ksName, tableName, pendingTaskCount));
                out.println(String.format("- %s.%s write amplification.: %.2f", ksName, tableName, wa));
            }
        }
        out.println();
        reportCompactionTable(cm.getCompactions(), probe.getCompactionThroughput(), humanReadable, out);

        if (aggregate)
        {
            reportAggregateCompactions(probe, keyspaces, tableNames, out);
        }

        if (overlap)
            reportOverlap((Map<String, Map<String, Map<String, String>>>) probe.getCompactionMetric("MaxOverlapsMap"), keyspaces, tableNames, out);
    }

    public static void reportCompactionTable(List<Map<String,String>> compactions, int compactionThroughput, boolean humanReadable, PrintStream out)
    {
        if (!compactions.isEmpty())
        {
            long remainingBytes = 0;
            TableBuilder table = new TableBuilder();

            table.add("id", "compaction type", "keyspace", "table", "completed", "total", "unit", "progress");
            for (Map<String, String> c : compactions)
            {
                long total = Long.parseLong(c.get(TableOperation.Progress.TOTAL));
                long completed = Long.parseLong(c.get(TableOperation.Progress.COMPLETED));
                String taskType = c.get(TableOperation.Progress.OPERATION_TYPE);
                String keyspace = c.get(TableOperation.Progress.KEYSPACE);
                String columnFamily = c.get(TableOperation.Progress.COLUMNFAMILY);
                String unit = c.get(TableOperation.Progress.UNIT);
                boolean toFileSize = humanReadable && TableOperation.Unit.isFileSize(unit);
                String completedStr = toFileSize ? FileUtils.stringifyFileSize(completed) : Long.toString(completed);
                String totalStr = toFileSize ? FileUtils.stringifyFileSize(total) : Long.toString(total);
                String percentComplete = total == 0 ? "n/a" : new DecimalFormat("0.00").format((double) completed / total * 100) + "%";
                String id = c.get(TableOperation.Progress.OPERATION_ID);
                table.add(id, taskType, keyspace, columnFamily, completedStr, totalStr, unit, percentComplete);
                remainingBytes += total - completed;
            }
            table.printTo(out);

            String remainingTime = "n/a";
            if (compactionThroughput != 0)
            {
                long remainingTimeInSecs = remainingBytes / (1024L * 1024L * compactionThroughput);
                remainingTime = format("%dh%02dm%02ds", remainingTimeInSecs / 3600, (remainingTimeInSecs % 3600) / 60, (remainingTimeInSecs % 60));
            }
            out.printf("%25s%10s%n", "Active compaction remaining time : ", remainingTime);
        }
    }

    private static void reportAggregateCompactions(NodeProbe probe, Set<String> keyspaces, Set<String> tableNames, PrintStream out)
    {
        List<CompactionStrategyStatistics> statistics = (List<CompactionStrategyStatistics>) probe.getCompactionMetric("AggregateCompactions");
        if (statistics.isEmpty())
            return;

        out.println("Aggregated view:");
        for (CompactionStrategyStatistics stat : statistics)
        {
            if (!keyspaces.contains(stat.keyspace()))
                continue;
            if (!tableNames.isEmpty() && !tableNames.contains(stat.table()))
                continue;
            out.println(stat.toString());
        }
    }

    private static void reportOverlap(Map<String, Map<String, Map<String, String>>> maxOverlap, Set<String> keyspaces, Set<String> tableNames, PrintStream out)
    {
        if (maxOverlap == null)
        {
            out.println("Overlap map is not available.");
            return;
        }

        for (Map.Entry<String, Map<String, Map<String, String>>> ksEntry : maxOverlap.entrySet())
        {
            String ksName = ksEntry.getKey();
            if (!keyspaces.contains(ksName))
                continue;
            for (Map.Entry<String, Map<String, String>> tableEntry : ksEntry.getValue().entrySet())
            {
                String tableName = tableEntry.getKey();
                if (!tableNames.isEmpty() && !tableNames.contains(tableName))
                    continue;
                out.println("Max overlap map for " + ksName + "." + tableName + ":");
                for (Map.Entry<String, String> compactionEntry : tableEntry.getValue().entrySet())
                {
                    out.println("  " + compactionEntry.getKey() + ": " + compactionEntry.getValue());
                }
            }
        }
    }
}
