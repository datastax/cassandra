package org.apache.cassandra.tools.nodetool;

import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;

import io.airlift.command.Arguments;
import io.airlift.command.Command;
import io.airlift.command.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

import static com.google.common.base.Preconditions.checkArgument;

@Command(name = "mark_unrepaired", description = "Mark all SSTables of a table or keyspace as unrepaired. Use when no " +
                                                 "longer running incremental repair on a table or keyspace.")
public class MarkUnrepaired extends NodeToolCmd
{
    @Arguments(usage = "<keyspace> [<tables>...]", description = "The keyspace followed by zero or many tables")
    private List<String> args = new ArrayList<>();

    @Option(title = "force", name = { "-force", "--force"}, description = "Use -force to confirm the operation")
    private boolean force = false;

    @Override
    protected void execute(NodeProbe probe)
    {
        checkArgument(args.size() >= 1, "mark_unrepaired requires at least one keyspace as argument");

        List<String> keyspaces = parseOptionalKeyspace(args, probe, KeyspaceSet.ALL);
        assert keyspaces.size() == 1;
        String[] tables = parseOptionalTables(args);
        String keyspace = keyspaces.iterator().next();

        if (!force)
        {
            String keyspaceOrTables = tables.length == 0? String.format("keyspace %s", keyspace) : String.format("table(s) %s.[%s]", keyspace,
                                                                                                                 Joiner.on(",").join(tables));
            throw new IllegalArgumentException(String.format("WARNING: This operation will mark all SSTables of %s as unrepaired, " +
                                                             "potentially creating new compaction tasks. Only use this when no longer running " +
                                                             "incremental repair on this node. Use --force option to confirm.",
                                                             keyspaceOrTables));
        }

        try
        {
            probe.markAllSSTablesAsUnrepaired(System.out, keyspace, tables);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }
}