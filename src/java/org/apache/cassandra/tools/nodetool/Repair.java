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

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.command.Option;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.service.TableInfo;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

import org.apache.commons.lang3.StringUtils;

@Command(name = "repair", description = "Repair one or more tables")
public class Repair extends NodeToolCmd
{
    public final static Set<String> ONLY_EXPLICITLY_REPAIRED = Sets.newHashSet(SystemDistributedKeyspace.NAME);

    @Arguments(usage = "[<keyspace> <tables>...]", description = "The keyspace followed by one or many tables")
    private List<String> args = new ArrayList<>();

    @Option(title = "seqential", name = {"-seq", "--sequential"}, description = "Use -seq to carry out a sequential repair")
    private boolean sequential = false;

    @Option(title = "dc parallel", name = {"-dcpar", "--dc-parallel"}, description = "Use -dcpar to repair data centers in parallel.")
    private boolean dcParallel = false;

    @Option(title = "local_dc", name = {"-local", "--in-local-dc"}, description = "Use -local to only repair against nodes in the same datacenter")
    private boolean localDC = false;

    @Option(title = "specific_dc", name = {"-dc", "--in-dc"}, description = "Use -dc to repair specific datacenters")
    private List<String> specificDataCenters = new ArrayList<>();;

    @Option(title = "specific_host", name = {"-hosts", "--in-hosts"}, description = "Use -hosts to repair specific hosts")
    private List<String> specificHosts = new ArrayList<>();

    @Option(title = "start_token", name = {"-st", "--start-token"}, description = "Use -st to specify a token at which the repair range starts")
    private String startToken = EMPTY;

    @Option(title = "end_token", name = {"-et", "--end-token"}, description = "Use -et to specify a token at which repair range ends")
    private String endToken = EMPTY;

    @Option(title = "primary_range", name = {"-pr", "--partitioner-range"}, description = "Use -pr to repair only the first range returned by the partitioner")
    private boolean primaryRange = false;

    @Option(title = "full", name = {"-full", "--full"}, description = "Use -full to issue a full repair.")
    private boolean fullOption = false;

    @Option(title = "incremental", name = {"-inc", "--inc"}, description = "Use -inc to issue an incremental repair.")
    private boolean incrementalOption = false;

    @Option(title = "runAntiCompaction", name = {"-run-anticompaction", "--run-anticompaction"}, description = "Use --run-anticompaction to run anticompaction when running full repair.")
    private boolean runAntiCompaction = false;

    @Option(title = "job_threads", name = {"-j", "--job-threads"}, description = "Number of threads to run repair jobs. " +
                                                                                 "Usually this means number of CFs to repair concurrently. " +
                                                                                 "WARNING: increasing this puts more load on repairing nodes, so be careful. (default: 1, max: 4)")
    private int numJobThreads = 1;

    @Option(title = "trace_repair", name = {"-tr", "--trace"}, description = "Use -tr to trace the repair. Traces are logged to system_traces.events.")
    private boolean trace = false;

    @Override
    public void execute(NodeProbe probe)
    {
        List<String> keyspaces = parseOptionalKeyspace(args, probe, KeyspaceSet.NON_LOCAL_STRATEGY);
        String[] cfnames = parseOptionalTables(args);

        if (primaryRange && (!specificDataCenters.isEmpty() || !specificHosts.isEmpty()))
            throw new RuntimeException("Primary range repair should be performed on all nodes in the cluster.");

        if (fullOption && incrementalOption)
        {
            throw new IllegalArgumentException("Cannot run both full and incremental repair, choose either --full or -inc option.");
        }

        for (String keyspace : keyspaces)
        {
            Map<String, TableInfo> tablesToRepair = probe.getTableInfos(keyspace, cfnames);

            // avoid repairing system_distributed by default (CASSANDRA-9621)
            if ((args == null || args.isEmpty()) && ONLY_EXPLICITLY_REPAIRED.contains(keyspace))
                continue;

            Set<String> tablesToRunFullRepair = getTablesToRunFullRepair(probe, keyspace, tablesToRepair);
            Set<String> tablesToRunIncRepair = Sets.difference(tablesToRepair.keySet(), tablesToRunFullRepair);

            if (tablesToRunFullRepair.isEmpty())
            {
                runRepair(probe, keyspace, cfnames, true);
            }
            else if (tablesToRunIncRepair.isEmpty())
            {
                runRepair(probe, keyspace, cfnames, false);
            }
            else
            {
                runRepair(probe, keyspace, tablesToRunIncRepair.toArray(new String[tablesToRunIncRepair.size()]), true);
                runRepair(probe, keyspace, tablesToRunFullRepair.toArray(new String[tablesToRunFullRepair.size()]), false);
            }
        }
    }

    private Set<String> getTablesToRunFullRepair(NodeProbe probe, String keyspace, Map<String, TableInfo> tablesToRepair)
    {
        if (fullOption)
        {
            if (!runAntiCompaction && hasEverIncrementallyRepaired(tablesToRepair))
            {
                System.out.println(String.format("INFO: Anticompaction is no longer run after full repair. Use the --run-anticompaction " +
                                                 "option to anticompact SSTables after full repair."));
            }
            return tablesToRepair.keySet();
        }

        Set<String> tablesWithViews = tablesToRepair.entrySet().stream().filter(e -> e.getValue().isOrHasView()).map(e -> e.getKey()).collect(Collectors.toSet());
        if (incrementalOption || tablesToRepair.keySet().equals(tablesWithViews))
        {
            if (!tablesWithViews.isEmpty())
            {
                System.out.println(String.format("WARN: Incremental repair is not supported on tables with Materialized Views. Running full repairs on table(s) %s.%s.",
                                                 keyspace, tablesWithViews));
            }
            return tablesWithViews;
        }

        Set<String> wasNotIncrementallyRepaired = tablesToRepair.entrySet()
                                                                .stream()
                                                                .filter(e -> !e.getValue().wasIncrementallyRepaired)
                                                                .map(e -> e.getKey()).collect(Collectors.toSet());
        Set<String> tablesToFullRepair = Sets.union(tablesWithViews, wasNotIncrementallyRepaired);


        if (!tablesToFullRepair.isEmpty() && probe.hasIncrementallyRepairedAnyTable())
        {
            //The idea is to never print this message on new clusters, just existing cluster that have ever ran incremental repair
            System.out.println(String.format("INFO: Neither --inc or --full repair options were provided. Running full repairs " +
                                             "on tables with MVs or that were never incrementally repaired: %s", tablesToFullRepair));
        }

        return tablesToFullRepair;
    }

    private boolean hasEverIncrementallyRepaired(Map<String, TableInfo> tablesToRepair)
    {
        return tablesToRepair.entrySet().stream().anyMatch(e -> e.getValue().wasIncrementallyRepaired);
    }

    private void runRepair(NodeProbe probe, String keyspace, String[] cfnames, boolean isIncremental)
    {
        Map<String, String> options = new HashMap<>();
        RepairParallelism parallelismDegree = RepairParallelism.PARALLEL;
        if (sequential)
            parallelismDegree = RepairParallelism.SEQUENTIAL;
        else if (dcParallel)
            parallelismDegree = RepairParallelism.DATACENTER_AWARE;
        options.put(RepairOption.PARALLELISM_KEY, parallelismDegree.getName());
        options.put(RepairOption.PRIMARY_RANGE_KEY, Boolean.toString(primaryRange));
        options.put(RepairOption.INCREMENTAL_KEY, Boolean.toString(isIncremental));
        options.put(RepairOption.JOB_THREADS_KEY, Integer.toString(numJobThreads));
        options.put(RepairOption.TRACE_KEY, Boolean.toString(trace));
        options.put(RepairOption.COLUMNFAMILIES_KEY, StringUtils.join(cfnames, ","));
        options.put(RepairOption.RUN_ANTI_COMPACTION_KEY, Boolean.toString(runAntiCompaction));
        if (!startToken.isEmpty() || !endToken.isEmpty())
        {
            options.put(RepairOption.RANGES_KEY, startToken + ":" + endToken);
        }
        if (localDC)
        {
            options.put(RepairOption.DATACENTERS_KEY, StringUtils.join(newArrayList(probe.getDataCenter()), ","));
        }
        else
        {
            options.put(RepairOption.DATACENTERS_KEY, StringUtils.join(specificDataCenters, ","));
        }
        options.put(RepairOption.HOSTS_KEY, StringUtils.join(specificHosts, ","));
        try
        {
            probe.repairAsync(System.out, keyspace, options);
        } catch (Exception e)
        {
            throw new RuntimeException("Error occurred during repair", e);
        }
    }
}