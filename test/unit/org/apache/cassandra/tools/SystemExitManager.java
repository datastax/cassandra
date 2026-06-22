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

package org.apache.cassandra.tools;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.inject.BytemanAgentSupport;
import org.apache.cassandra.utils.Shared;

/**
 * Test utility that intercepts {@code System.exit}/{@code Runtime.exit}/{@code Runtime.halt} and turns it
 * into a catchable {@link SystemExitException}, replacing the old {@link SecurityManager}-based mechanism
 * (which cannot be installed on JDK 24+).
 * <p>
 * Interception is performed with a Byteman rule on {@code java.lang.Runtime}. Byteman is used rather than a
 * plain agent because the rule action runs in the application class loader, so it can throw the
 * application-loaded {@link SystemExitException} that callers {@code catch} (a bootstrap-injected copy would
 * be a different {@code Class}). The rule is installed lazily once per JVM the first time exits are blocked.
 * <p>
 * Exit blocking is reference-counted and JVM-global (matching the previous SecurityManager, which blocked
 * exits on every thread while installed): wrap a scope with {@link #blockExit()} / {@link #unblockExit()}.
 * The class is {@link Shared} so that, in in-JVM dtests, every instance class loader sees the same counter.
 */
@Shared
public final class SystemExitManager
{
    private static final AtomicInteger blockedCount = new AtomicInteger(0);
    private static volatile boolean ruleInstalled = false;

    private SystemExitManager()
    {
    }

    /** Begin blocking {@code System.exit}/{@code Runtime.exit}/{@code Runtime.halt} JVM-wide (re-entrant). */
    public static void blockExit()
    {
        ensureRuleInstalled();
        blockedCount.incrementAndGet();
    }

    /** End one {@link #blockExit()} scope. */
    public static void unblockExit()
    {
        blockedCount.updateAndGet(n -> n > 0 ? n - 1 : 0);
    }

    /** Force-clear all exit blocking (used during cluster teardown to guarantee the JVM can exit). */
    public static void reset()
    {
        blockedCount.set(0);
    }

    /** Invoked by the Byteman rule: whether exits are currently being intercepted. */
    public static boolean isExitBlocked()
    {
        return blockedCount.get() > 0;
    }

    /**
     * Installs the Byteman interception rule eagerly (without blocking exits). Tests that assert on captured
     * tool stdout or on the set of running threads should call this in {@code @BeforeClass} BEFORE they
     * snapshot the baseline threads, so the Byteman listener thread and its install-time output do not show
     * up during a later tool run. Idempotent and safe to call repeatedly.
     */
    public static void ensureInstalled()
    {
        ensureRuleInstalled();
    }

    private static synchronized void ensureRuleInstalled()
    {
        if (ruleInstalled)
            return;

        // Use Byteman's 'throw' action directly so the SystemExitException propagates to the caller of
        // System.exit/Runtime.exit; calling a helper that throws would be wrapped in an ExecuteException.
        // The shared agent is attached with org.jboss.byteman.transform.all=true, which is required to
        // transform the bootstrap class java.lang.Runtime.
        String rule =
        "RULE cassandra-test intercept Runtime.exit\n" +
        "CLASS java.lang.Runtime\n" +
        "METHOD exit\n" +
        "AT ENTRY\n" +
        "IF org.apache.cassandra.tools.SystemExitManager.isExitBlocked()\n" +
        "DO throw new org.apache.cassandra.tools.SystemExitException($1)\n" +
        "ENDRULE\n" +
        "RULE cassandra-test intercept Runtime.halt\n" +
        "CLASS java.lang.Runtime\n" +
        "METHOD halt\n" +
        "AT ENTRY\n" +
        "IF org.apache.cassandra.tools.SystemExitManager.isExitBlocked()\n" +
        "DO throw new org.apache.cassandra.tools.SystemExitException($1)\n" +
        "ENDRULE\n";

        try
        {
            BytemanAgentSupport.submitRules(rule);
            ruleInstalled = true;
        }
        catch (Throwable t)
        {
            throw new RuntimeException("Failed to install Byteman System.exit interception rule", t);
        }
    }
}
