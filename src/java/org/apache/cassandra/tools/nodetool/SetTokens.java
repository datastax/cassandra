/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.tools.nodetool;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import io.airlift.airline.Command;
import io.airlift.airline.Option;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "settokens", description = "Keep only a subset of the tokens of the node, streaming the ranges it gives up to their new replicas " +
                                           "(see tokenreductionplanner). Blocks until done; run 'nodetool flush' and 'nodetool cleanup' afterwards")
public class SetTokens extends NodeToolCmd
{
    @Option(title = "keep_file", name = { "--keep-file" }, description = "File with the tokens to keep, one per line (as written by tokenreductionplanner)")
    private String keepFile = null;

    @Option(title = "keep", name = { "--keep" }, description = "Tokens to keep, separated by commas")
    private String keep = null;

    @Override
    public void execute(NodeProbe probe)
    {
        if ((keepFile == null) == (keep == null))
            throw new IllegalArgumentException("Specify exactly one of --keep-file and --keep");

        List<String> tokens = new ArrayList<>();
        try
        {
            if (keepFile != null)
            {
                for (String line : Files.readAllLines(new File(keepFile).toPath(), StandardCharsets.UTF_8))
                    if (!line.trim().isEmpty())
                        tokens.add(line.trim());
            }
            else
            {
                for (String token : keep.split(","))
                    if (!token.trim().isEmpty())
                        tokens.add(token.trim());
            }
        }
        catch (IOException e)
        {
            throw new IllegalArgumentException("Cannot read " + keepFile + ": " + e.getMessage(), e);
        }
        if (tokens.isEmpty())
            throw new IllegalArgumentException("No tokens to keep");

        try
        {
            probe.shrinkTokens(tokens);
            probe.output().out.printf("The node now has %d tokens. Run 'nodetool flush' and 'nodetool cleanup' on it, and set num_tokens to %d in cassandra.yaml.%n",
                                      tokens.size(), tokens.size());
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error while shrinking the tokens of the node", e);
        }
    }
}
