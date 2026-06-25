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

package org.apache.cassandra.inject;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.ServerSocket;

import com.google.common.collect.Lists;

import org.apache.commons.io.IOUtils;
import org.jboss.byteman.agent.install.Install;
import org.jboss.byteman.agent.submit.Submit;

import org.apache.cassandra.utils.Shared;

/**
 * Owns the single Byteman agent for the current JVM and hands out a {@link Submit} bound to it, so that every
 * place that installs Byteman rules in tests ({@link Injections},
 * {@code org.apache.cassandra.tools.SystemExitManager}, and any future caller) shares one agent on one
 * {@code (host, port)} instead of each attaching its own and guessing the other's port.
 * <p>
 * Only one Byteman agent can be attached per JVM, so the chosen port is a JVM-global value. The class is
 * {@link Shared} so that, in in-JVM dtests, every instance class loader sees the same {@link #port} rather
 * than re-deriving it.
 * <p>
 * The agent always listens on loopback ({@code 127.0.0.1}); that is where a self-attached in-process agent
 * is reachable. {@code setPolicy} is always {@code false}: Byteman's {@code installPolicy()} calls
 * {@code Policy.setPolicy()}, which throws {@code UnsupportedOperationException} on JDK 24+ (JEP 486).
 */
@Shared
public final class BytemanAgentSupport
{
    private static final String HOST = "127.0.0.1";

    // BMUnit attaches its own agent and listens on this port unless its system property overrides it.
    private static final String BMUNIT_PORT_PROP = "org.jboss.byteman.contrib.bmunit.agent.port";
    private static final int BMUNIT_DEFAULT_PORT = 9091;

    private static volatile int port = -1;
    private static volatile boolean installed = false;

    private BytemanAgentSupport()
    {
    }

    /** Attaches the Byteman agent to this JVM if it is not already attached. Idempotent and JVM-global. */
    public static synchronized void ensureInstalled()
    {
        if (installed)
            return;

        try
        {
            // ProcessHandle is available since JDK 9; Cassandra runs on JDK 11+.
            String pid = Long.toString(ProcessHandle.current().pid());
            if (Install.isAgentAttached(pid))
            {
                // The agent was attached by something other than us — in practice BMUnit.
                port = Integer.getInteger(BMUNIT_PORT_PROP, BMUNIT_DEFAULT_PORT); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
            }
            else
            {
                int chosen = freePort();
                // addToBoot (2nd arg) true + org.jboss.byteman.transform.all=true so bootstrap classes (e.g.
                // java.lang.Runtime, used by SystemExitManager) can be instrumented; setPolicy (3rd) false
                // (see class javadoc). Install chatter is suppressed so it cannot pollute a tool's captured
                // stdout/stderr if the attach happens inside a capture region.
                quietly(() -> Install.install(pid, true, false, HOST, chosen,
                                              new String[]{ "org.jboss.byteman.transform.all=true" }));
                port = chosen;
            }
            installed = true;
        }
        catch (Throwable t)
        {
            throw new RuntimeException("Failed to attach the Byteman agent", t);
        }
    }

    /** A {@link Submit} bound to this JVM's Byteman agent, attaching the agent first if necessary. */
    public static Submit submitter()
    {
        ensureInstalled();
        return new Submit(HOST, port);
    }

    /** The host the agent listens on. */
    public static String host()
    {
        return HOST;
    }

    /** The port the agent listens on (attaching the agent first if necessary). */
    public static int port()
    {
        ensureInstalled();
        return port;
    }

    /** Submits the given Byteman rule text to this JVM's agent (attaching it first if necessary). */
    public static void submitRules(String ruleText)
    {
        ensureInstalled();
        try
        {
            quietly(() -> new Submit(HOST, port).addRulesFromResources(Lists.newArrayList(IOUtils.toInputStream(ruleText, "UTF-8"))));
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to submit Byteman rules", e);
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable
    {
        void run() throws Exception;
    }

    /** Run {@code action} with System.out/err temporarily silenced (to hide Byteman's install chatter). */
    private static void quietly(ThrowingRunnable action) throws Exception
    {
        PrintStream out = System.out;
        PrintStream err = System.err;
        PrintStream nul = new PrintStream(OutputStream.nullOutputStream(), false, "UTF-8");
        System.setOut(nul);
        System.setErr(nul);
        try
        {
            action.run();
        }
        finally
        {
            System.setOut(out);
            System.setErr(err);
            nul.close();
        }
    }

    private static int freePort()
    {
        try (ServerSocket serverSocket = new ServerSocket(0))
        {
            return serverSocket.getLocalPort();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
