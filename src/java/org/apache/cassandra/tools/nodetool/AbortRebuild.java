/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.tools.nodetool;

import io.airlift.command.Command;
import io.airlift.command.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "abortrebuild", description = "Abort a currently running rebuild operation. " +
                                              "Currently active streams will finish but no new streams will be started.")
public class AbortRebuild extends NodeToolCmd
{
    @Option(title = "reason",
            name = {"-r", "--reason"},
            description = "Specify a reason to be logged.")
    private String reason = null;

    @Override
    public void execute(NodeProbe probe)
    {
        probe.abortRebuild(reason);
    }
}
