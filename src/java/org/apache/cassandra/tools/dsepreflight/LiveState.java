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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

/**
 * Runs the read-only {@code nodetool} commands of the live state of a running DSE node ({@code --nodetool}): the DSE
 * {@code nodetool} given, with the extra arguments given (host, port, credentials), one command at a time, each with a
 * time limit, in the environment of the tool without the variables of its own launch ({@link #TOOL_ENVIRONMENT}).
 * A command that cannot be run, fails or times out is recorded as such; the inspection of the files does not depend
 * on it.
 */
final class LiveState
{
    /**
     * The commands run, in this order; each is read-only. {@code gossipinfo} and {@code tpstats} are only recorded in the
     * report, for the operator.
     */
    static final List<String> COMMANDS = List.of("version", "status", "describecluster", "nodesyncservice status", "compactionstats",
                                                 "netstats", "listpendinghints", "gossipinfo", "tpstats");
    /**
     * The commands run on a DSE 5.1 node, in this order; each is read-only. DSE 5.1 has no NodeSync, names its command
     * of pending hints {@code listendpointspendinghints}, and still runs Thrift ({@code statusthrift}).
     */
    static final List<String> DSE51_COMMANDS = List.of("version", "status", "describecluster", "statusthrift",
                                                       "compactionstats", "netstats", "listendpointspendinghints",
                                                       "gossipinfo", "tpstats");
    /**
     * The commands the {@code nodetool} of some DSE release does not have (DSE 6.8.65 and 6.9.26 have no
     * {@code listpendinghints}; the hints directory is checked on disk): skipped when the {@code nodetool} given
     * rejects the command itself. Any other command that fails leaves the live state incomplete.
     */
    static final Set<String> OPTIONAL_COMMANDS = Set.of("listpendinghints");
    /** What DSE's {@code nodetool} answers for parameters it does not take, a command it does not have among them. */
    static final String UNEXPECTED_PARAMETERS = "Found unexpected parameters: ";
    /** The most output kept of one command. */
    static final int MAX_OUTPUT_BYTES = 256 * 1024;
    /**
     * The variables removed from the environment of DSE's {@code nodetool}: every variable that
     * {@code tools/bin/dsepreflight} and the {@code tools/bin/cassandra.in.sh} it sources set, which a caller that had
     * exported one of them passes on with the value of this tool's installation, but {@code MAX_HEAP_SIZE}; and
     * {@code CASSANDRA_INCLUDE} and {@code JAVA_OPTS}, which would name the include file and the JVM options of an
     * installation of this software, not DSE's. DSE's {@code nodetool} takes {@code CASSANDRA_CONF} when that directory
     * has a {@code cassandra.yaml}, and {@code CASSANDRA_HOME} when it has a {@code tools/lib}, as this installation
     * does, and then fails; it reads {@code CASSANDRA_INCLUDE}, {@code CLASSPATH}, {@code JVM_OPTS} and
     * {@code JAVA_AGENT} too; {@code JAVA_OPTS} is extended by DSE 6.8.65's {@code cassandra.in.sh}. Without them DSE's
     * {@code nodetool} finds its configuration from its own installation. {@code MAX_HEAP_SIZE} stays: the script sets
     * it only when it is empty, so an exported value is the caller's. {@code PATH} and {@code JAVA_HOME} stay, as
     * neither script sets them: DSE's {@code nodetool} runs on the JDK the caller gave the tool (those of DSE 6.8.65
     * and 6.9.26 run on JDK 17), unless {@code --nodetool-java-home} names another. Every other variable of the
     * caller stays, DSE's own ({@code DSE_HOME}, {@code DSE_CONF}) among them.
     */
    static final Set<String> TOOL_ENVIRONMENT = Set.of("CASSANDRA_HOME", "CASSANDRA_CONF", "CASSANDRA_INCLUDE",
                                                        "CLASSPATH", "JVM_OPTS", "JAVA_OPTS", "JAVA_AGENT", "JAVA",
                                                        "JVM_VERSION", "JAVA_VERSION", "JVM_VENDOR", "JVM_ARCH",
                                                        "JVM_OPTS_FILE", "JVM_DEP_OPTS_FILE", "USING_JDK",
                                                        "cassandra_storagedir", "jars_cnt", "dir", "dse_db_bin",
                                                        "jar", "java", "java_ver_output", "jvmver", "short", "jvm",
                                                        "opt");

    private LiveState()
    {
    }

    /**
     * Runs every command of {@code commands}, {@link #COMMANDS} or {@link #DSE51_COMMANDS}, keyed by command;
     * {@code javaHome}, when not null, is the {@code JAVA_HOME} of DSE's {@code nodetool}
     * ({@code --nodetool-java-home}).
     */
    static Map<String, NodeReport.LiveCommand> collect(String nodetool, List<String> extraArguments,
                                                       long timeoutSeconds, @Nullable String javaHome,
                                                       List<String> commands)
    {
        Map<String, NodeReport.LiveCommand> results = new LinkedHashMap<>();
        for (String command : commands)
            results.put(command, run(nodetool, extraArguments, command, timeoutSeconds, javaHome, null));
        return results;
    }

    /**
     * Runs one command; {@code environment}, when not null, takes the place of the environment of the tool, for the
     * tests.
     */
    @VisibleForTesting
    static NodeReport.LiveCommand run(String nodetool, List<String> extraArguments, String command, long timeoutSeconds,
                                      @Nullable String javaHome, @Nullable Map<String, String> environment)
    {
        NodeReport.LiveCommand result = new NodeReport.LiveCommand();
        List<String> commandLine = new ArrayList<>();
        commandLine.add(nodetool);
        commandLine.addAll(extraArguments);
        commandLine.addAll(Arrays.asList(command.split(" ")));
        Process process;
        try
        {
            ProcessBuilder builder = new ProcessBuilder(commandLine).redirectErrorStream(true);
            Map<String, String> nodetoolEnvironment = builder.environment();
            if (environment != null)
            {
                nodetoolEnvironment.clear();
                nodetoolEnvironment.putAll(environment);
            }
            nodetoolEnvironment.keySet().removeAll(TOOL_ENVIRONMENT);
            if (javaHome != null)
            {
                nodetoolEnvironment.put("JAVA_HOME", javaHome);
                // DSE's nodetool is a shell script, on a system whose PATH separates its entries with a colon
                String path = nodetoolEnvironment.get("PATH");
                String bin = Path.of(javaHome, "bin").toString();
                nodetoolEnvironment.put("PATH", path == null || path.isEmpty() ? bin : bin + ':' + path);
            }
            process = builder.start();
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

    /**
     * Whether {@code command} is one of {@link #OPTIONAL_COMMANDS} that the {@code nodetool} given does not have: it
     * failed, rejecting the command's own words and nothing else (an argument it rejects with them is a failure).
     */
    static boolean unknownCommand(String command, NodeReport.LiveCommand result)
    {
        return OPTIONAL_COMMANDS.contains(command) && result.error == null && result.exitCode != null &&
               result.exitCode != 0 && result.output != null &&
               result.output.contains(UNEXPECTED_PARAMETERS + '[' + String.join(", ", command.split(" ")) + ']');
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
