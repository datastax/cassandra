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

import io.airlift.command.Arguments;
import io.airlift.command.Command;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import io.airlift.command.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "rebuild", description = "Rebuild data by streaming from other nodes (similarly to bootstrap)")
public class Rebuild extends NodeToolCmd
{
    @Arguments(usage = "<src-dc-name>",
               description = "Name of DC from which to select sources for streaming. By default, pick any DC. Must not be used in combination with --dcs.")
    private String sourceDataCenterName = null;

    @Option(title = "specific_keyspace",
            name = {"-ks", "--keyspace"},
            description = "Use -ks to rebuild specific one or more keyspaces (comma separated list).")
    private String keyspace = null;

    @Option(title = "specific_tokens",
            name = {"-ts", "--tokens"},
            description = "Use -ts to rebuild specific token ranges, in the format of \"(start_token_1,end_token_1],(start_token_2,end_token_2],...(start_token_n,end_token_n]\".")
    private String tokens = null;

    @Option(title = "mode",
    allowedValues = {"normal", "refetch", "reset", "reset-no-shapshot"},
    name = {"-m", "--mode"},
    description = "normal: conventional behaviour, only streams ranges that are not already locally available (this is the default) - " +
                  "refetch: resets the locally available ranges, streams all ranges but leaves current data untouched - " +
                  "reset: resets the locally available ranges, removes all locally present data (like a TRUNCATE), streams all ranges - " +
                  "reset-no-snapshot: like 'reset', but prevents a snapshot if 'auto_snapshot' is enabled.")
    private String mode = "normal";

    @Option(title = "specific_sources",
    name = {"-s", "--sources"},
    description = "Use -s to specify hosts that this node should stream. Multiple hosts should be separated using commas (e.g. 127.0.0.1,127.0.0.2,...)")
    private String specificSources = null;

    @Option(title = "exclude_sources",
    name = {"-x", "--exclude-sources"},
    description = "Use -x to specify hosts that this node should not stream from. Multiple hosts should be separated using commas (e.g. 127.0.0.1,127.0.0.2,...)")
    private String excludeSources = null;

    @Option(title = "src_dc_names",
    name = {"-dc", "--dcs"},
    description = "List of DC names or DC+Rack to stream from. Multiple DCs should be separated using commas (e.g. dc-a,dc-b,...). To also incude a rack name, separate DC and rack name by a colon ':' (e.g. dc-a:rack1,dc-a:rack2).")
    private String sourceDCs = null;

    @Option(title = "exclude_dc_names",
    name = {"-xdc", "--exclude-dcs"},
    description = "List of DC names or DC+Rack to exclude from streaming. Multiple DCs should be separated using commas (e.g. dc-a,dc-b,...). To also incude a rack name, separate DC and rack name by a colon ':' (e.g. dc-a:rack1,dc-a:rack2).")
    private String excludeDCs = null;

    @Override
    public void execute(NodeProbe probe)
    {
        // check the arguments
        if (sourceDataCenterName != null && sourceDCs != null)
        {
            throw new IllegalArgumentException("Use either the name of the source DC (command argument) or a list of source DCs (-dc option) to include but not both");
        }

        List<String> keyspaces = commaSeparatedList(keyspace);

        List<String> whitelistDCs = sourceDataCenterName != null ?
                                    Collections.singletonList(sourceDataCenterName) :
                                    commaSeparatedList(sourceDCs);

        List<String> blacklistDcs = commaSeparatedList(excludeDCs);

        List<String> whitelistSources = commaSeparatedList(specificSources);
        List<String> blacklistSources = commaSeparatedList(excludeSources);

        String msg = probe.rebuild(keyspaces, tokens, mode,
                                   whitelistDCs, blacklistDcs, whitelistSources, blacklistSources);
        System.out.println(msg);
    }

    private static List<String> commaSeparatedList(String s)
    {
        if (s == null)
            return null;

        return Arrays.stream(s.split(",")).map(String::trim).collect(Collectors.toList());
    }
}
