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

import org.apache.cassandra.tools.OfflineToolUtils;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.apache.cassandra.utils.JsonUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The preflight inspection run as the operator runs it, through {@code main} in a JVM where nothing initialised the
 * configuration before: it initialises itself as a client, exits with its exit code, and loads neither the schema, nor
 * a keyspace, nor the commit log, nor the server.
 */
public class DsePreflightToolTest extends OfflineToolUtils
{
    /** The threads the tool may leave behind. */
    private static final String[] OPTIONAL_THREADS = { "Attach Listener" };

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

        NodeReport report = JsonUtils.JSON_OBJECT_MAPPER.readValue(Files.readAllBytes(json), NodeReport.class);
        assertThat(report.result).isEqualTo(NodeReport.READY);
        assertThat(report.findings).noneMatch(f -> f.code.equals("SSTABLE_FORMAT"));

        assertSchemaNotLoaded();
        assertKeyspaceNotLoaded();
        assertSystemKSNotLoaded();
        assertCLSMNotLoaded();
        assertServerNotLoaded();
        assertNoUnexpectedThreadsStarted(OPTIONAL_THREADS, false);
    }

    @Test
    public void wrongUsageExitsWithTwoThroughMain()
    {
        ToolResult tool = ToolRunner.invokeClass(DsePreflight.class, "--no-such-option");
        assertThat(tool.getExitCode()).isEqualTo(DsePreflight.EXIT_ERROR);
        assertThat(tool.getStderr()).contains("no-such-option");
    }
}
