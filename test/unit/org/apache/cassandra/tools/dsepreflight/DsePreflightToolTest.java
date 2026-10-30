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

package org.apache.cassandra.tools.dsepreflight;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.fasterxml.jackson.core.type.TypeReference;

import org.apache.cassandra.tools.OfflineToolUtils;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.apache.cassandra.utils.JsonUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The preflight inspection run as the operator runs it, through {@code main} in a JVM where nothing initialised the
 * configuration before (so with this software's default trie version, not DSE 6's): it initialises itself as a client,
 * exits with its exit code, and loads neither the schema, nor a keyspace, nor the commit log, nor the server.
 */
public class DsePreflightToolTest extends OfflineToolUtils
{
    /** The threads the tool may leave behind, most of them started by the read of the sstables. */
    private static final String[] OPTIONAL_THREADS = { "Reference-Reaper", "NonPeriodicTasks:[1-9]", "ScheduledTasks:[1-9]", "ScheduledFastTasks:[1-9]",
                                                       "OptionalTasks:[1-9]", "ForkJoinPool.commonPool-worker-[0-9]+",
                                                       "process reaper", "ObjectCleanerThread", "Strong-Reference-Leak-Detector:[1-9]",
                                                       "LocalPool-Cleaner(-networking|-chunk-cache)", "JNA Cleaner", "Attach Listener" };

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void theToolRunsAsAnOfflineClientThroughMain() throws IOException
    {
        DseNodeFixture node = DseNodeFixture.create(temporaryFolder.newFolder().toPath());
        Map<String, String> before = node.fingerprint();
        Path json = temporaryFolder.getRoot().toPath().resolve("report.json");
        ToolResult tool = ToolRunner.invokeClass(DsePreflight.class, node.args("--json", json.toString()));
        assertThat(tool.getExitCode()).as("%s%s", tool.getStdout(), tool.getStderr()).isEqualTo(DsePreflight.EXIT_READY);
        assertThat(tool.getStdout()).contains("Result: READY").contains("SAI on-disk version to pin: aa");
        assertThat(node.fingerprint()).isEqualTo(before);

        NodeReport report = ClusterAggregate.READER.readValue(Files.readAllBytes(json));
        assertThat(report.result).isEqualTo(NodeReport.READY);
        assertThat(report.findings).noneMatch(f -> f.code.equals("SSTABLE_FORMAT") || f.code.equals("UNREADABLE_SCHEMA"));
        assertThat(report.findings).anyMatch(f -> f.code.equals("VIEWS_HANDLED") && f.message.contains("lab.wide_by_v"));

        assertSchemaNotLoaded();
        assertKeyspaceNotLoaded();
        assertSystemKSNotLoaded();
        assertCLSMNotLoaded();
        assertServerNotLoaded();
        assertNoUnexpectedThreadsStarted(OPTIONAL_THREADS, false);
    }

    /**
     * The aggregate mode through {@code main}, on the report of one fixture node: it reads the report only, and blocks
     * for the DSE peers that have none.
     */
    @Test
    public void theAggregateRunsAsAnOfflineClientThroughMain() throws IOException
    {
        DseNodeFixture node = DseNodeFixture.create(temporaryFolder.newFolder().toPath());
        Path reports = temporaryFolder.newFolder("reports").toPath();
        ToolResult inspection = ToolRunner.invokeClass(DsePreflight.class, node.args("--json", reports.resolve("node1.json").toString()));
        assertThat(inspection.getExitCode()).as("%s%s", inspection.getStdout(), inspection.getStderr()).isEqualTo(DsePreflight.EXIT_READY);

        Path json = temporaryFolder.getRoot().toPath().resolve("aggregate.json");
        ToolResult tool = ToolRunner.invokeClass(DsePreflight.class, "--aggregate", reports.toString(), "--json", json.toString());
        assertThat(tool.getExitCode()).as("%s%s", tool.getStdout(), tool.getStderr()).isEqualTo(DsePreflight.EXIT_BLOCKED);
        assertThat(tool.getStdout()).contains("Reports read: 1, in 1 data center").contains("Result: BLOCKED").contains("[MISSING_REPORT]");
        Map<String, Object> aggregate = JsonUtils.JSON_OBJECT_MAPPER.readValue(Files.readAllBytes(json), new TypeReference<Map<String, Object>>() {});
        assertThat(aggregate).containsEntry("kind", NodeReport.AGGREGATE).containsEntry("result", NodeReport.BLOCKED).containsEntry("reports_read", 1);

        assertSchemaNotLoaded();
        assertKeyspaceNotLoaded();
        assertSystemKSNotLoaded();
        assertCLSMNotLoaded();
        assertServerNotLoaded();
    }

    @Test
    public void wrongUsageExitsWithTwoThroughMain()
    {
        ToolResult tool = ToolRunner.invokeClass(DsePreflight.class, "--no-such-option");
        assertThat(tool.getExitCode()).isEqualTo(DsePreflight.EXIT_ERROR);
        assertThat(tool.getStderr()).contains("no-such-option");
    }
}
