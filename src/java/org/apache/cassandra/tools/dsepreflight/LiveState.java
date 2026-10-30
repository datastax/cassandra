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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

/**
 * Runs the read-only {@code nodetool} commands of the live state of a running DSE node ({@code --nodetool}): the DSE
 * {@code nodetool} given, with the extra arguments given (host, port, credentials), one command at a time, each with a
 * time limit. A command that cannot be run, fails or times out is recorded as such; the inspection of the files does not
 * depend on it.
 */
final class LiveState
{
    /**
     * The commands run, in this order; each is read-only. {@code gossipinfo} and {@code tpstats} are only recorded in the
     * report, for the operator.
     */
    static final List<String> COMMANDS = List.of("version", "status", "describecluster", "nodesyncservice status", "compactionstats",
                                                 "netstats", "listpendinghints", "gossipinfo", "tpstats");
    /** The most output kept of one command. */
    static final int MAX_OUTPUT_BYTES = 256 * 1024;

    private LiveState()
    {
    }

    /** Runs every command of {@link #COMMANDS}; the result is keyed by command. */
    static Map<String, NodeReport.LiveCommand> collect(String nodetool, List<String> extraArguments, long timeoutSeconds)
    {
        Map<String, NodeReport.LiveCommand> results = new LinkedHashMap<>();
        for (String command : COMMANDS)
            results.put(command, run(nodetool, extraArguments, command, timeoutSeconds));
        return results;
    }

    static NodeReport.LiveCommand run(String nodetool, List<String> extraArguments, String command, long timeoutSeconds)
    {
        NodeReport.LiveCommand result = new NodeReport.LiveCommand();
        List<String> commandLine = new ArrayList<>();
        commandLine.add(nodetool);
        commandLine.addAll(extraArguments);
        commandLine.addAll(Arrays.asList(command.split(" ")));
        Process process;
        try
        {
            process = new ProcessBuilder(commandLine).redirectErrorStream(true).start();
        }
        catch (IOException e)
        {
            result.error = "cannot run " + nodetool + ": " + e.getMessage();
            return result;
        }
        try
        {
            process.getOutputStream().close();
            OutputReader reader = new OutputReader(process.getInputStream());
            Thread thread = executorFactory().startThread("dsepreflight-nodetool-output", reader);
            if (!process.waitFor(timeoutSeconds, TimeUnit.SECONDS))
            {
                destroy(process);
                thread.join(TimeUnit.SECONDS.toMillis(5));
                result.output = reader.output();
                result.error = "nodetool " + command + " did not finish within " + timeoutSeconds + " s";
                return result;
            }
            thread.join(TimeUnit.SECONDS.toMillis(5));
            result.exitCode = process.exitValue();
            result.output = reader.output();
            return result;
        }
        catch (InterruptedException e)
        {
            destroy(process);
            Thread.currentThread().interrupt();
            result.error = "interrupted while running nodetool " + command;
            return result;
        }
        catch (IOException e)
        {
            destroy(process);
            result.error = "cannot run nodetool " + command + ": " + e.getMessage();
            return result;
        }
    }

    /** Kills a command and every process it started: DSE's nodetool is a script that starts a JVM. */
    private static void destroy(Process process)
    {
        process.descendants().forEach(ProcessHandle::destroyForcibly);
        process.destroyForcibly();
    }

    /** Reads the output of a command, keeping at most {@link #MAX_OUTPUT_BYTES}. */
    private static final class OutputReader implements Runnable
    {
        private final InputStream in;
        private final ByteArrayOutputStream kept = new ByteArrayOutputStream();
        private boolean cut;

        OutputReader(InputStream in)
        {
            this.in = in;
        }

        @Override
        public void run()
        {
            byte[] buffer = new byte[8192];
            try
            {
                int read;
                while ((read = in.read(buffer)) >= 0)
                {
                    synchronized (this)
                    {
                        int room = MAX_OUTPUT_BYTES - kept.size();
                        if (room > 0)
                            kept.write(buffer, 0, Math.min(room, read));
                        if (read > room)
                            cut = true;
                    }
                }
            }
            catch (IOException e)
            {
                // the process went away: keep what was read
            }
        }

        synchronized String output()
        {
            String text = new String(kept.toByteArray(), StandardCharsets.UTF_8);
            return cut ? text + "\n... (output cut at " + MAX_OUTPUT_BYTES + " bytes)" : text;
        }
    }
}
