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

package org.apache.cassandra.concurrent;

import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A jstack-like dump of every live thread, for {@link ExecutorLivenessWatchdog}. Unlike {@link ThreadInfo#toString()},
 * which stops at 8 frames, it prints every frame, as the frames that explain a stall (who holds the barrier, the lock,
 * the permit) are usually deep.
 */
final class ThreadDump
{
    private ThreadDump()
    {
    }

    /**
     * Dumps every live thread, with the monitors each holds but not its ownable synchronizers: finding those makes
     * HotSpot walk the whole heap at a safepoint, a long pause on a large heap.
     *
     * @param stalled          what is stalled, named in the header
     * @param firstThreadNames threads to list first, in this order; then every other thread, in the JVM's order
     */
    static String dumpAllThreads(Collection<String> stalled, Collection<String> firstThreadNames)
    {
        return format(ManagementFactory.getThreadMXBean().dumpAllThreads(true, false), stalled, firstThreadNames);
    }

    static String format(ThreadInfo[] threads, Collection<String> stalled, Collection<String> firstThreadNames)
    {
        Map<String, List<ThreadInfo>> first = new LinkedHashMap<>();
        for (String name : firstThreadNames)
            first.put(name, new ArrayList<>(1));
        List<ThreadInfo> rest = new ArrayList<>(threads.length);
        int count = 0;
        for (ThreadInfo thread : threads)
        {
            if (thread == null)   // exited while being dumped
                continue;
            ++count;
            List<ThreadInfo> named = first.get(thread.getThreadName());
            if (named != null)
                named.add(thread);
            else
                rest.add(thread);
        }

        StringBuilder sb = new StringBuilder();
        sb.append("Executor liveness thread dump (").append(count).append(" threads)");
        if (!stalled.isEmpty())
            sb.append("; stalled: ").append(String.join(", ", stalled));
        sb.append('\n');
        for (List<ThreadInfo> named : first.values())
            for (ThreadInfo thread : named)
                append(sb, thread);
        for (ThreadInfo thread : rest)
            append(sb, thread);
        return sb.toString();
    }

    private static void append(StringBuilder sb, ThreadInfo thread)
    {
        sb.append("\n\"").append(thread.getThreadName()).append("\" #").append(thread.getThreadId());
        if (thread.isDaemon())
            sb.append(" daemon");
        sb.append(' ').append(thread.getThreadState());
        if (thread.getLockName() != null)
            sb.append(" on ").append(thread.getLockName());
        if (thread.getLockOwnerName() != null)
            sb.append(" owned by \"").append(thread.getLockOwnerName()).append("\" #").append(thread.getLockOwnerId());
        if (thread.isSuspended())
            sb.append(" (suspended)");
        if (thread.isInNative())
            sb.append(" (in native)");
        sb.append('\n');

        StackTraceElement[] stack = thread.getStackTrace();
        MonitorInfo[] monitors = thread.getLockedMonitors();
        for (int depth = 0; depth < stack.length; depth++)
        {
            appendFrame(sb, stack[depth]);
            if (depth == 0)
                appendBlockedOn(sb, thread);
            for (MonitorInfo monitor : monitors)
            {
                if (monitor.getLockedStackDepth() == depth)
                    sb.append("\t- locked ").append(monitor).append('\n');
            }
        }
        // monitors entered through JNI belong to no frame; jstack and ThreadInfo.toString() omit them, so list them last
        for (MonitorInfo monitor : monitors)
        {
            if (monitor.getLockedStackDepth() < 0)
                sb.append("\t- locked ").append(monitor).append(" (through JNI)\n");
        }
    }

    // as StackTraceElement.toString() did before Java 9, without the class loader and module prefixes
    private static void appendFrame(StringBuilder sb, StackTraceElement frame)
    {
        sb.append("\tat ").append(frame.getClassName()).append('.').append(frame.getMethodName()).append('(');
        if (frame.isNativeMethod())
            sb.append("Native Method");
        else if (frame.getFileName() == null)
            sb.append("Unknown Source");
        else if (frame.getLineNumber() >= 0)
            sb.append(frame.getFileName()).append(':').append(frame.getLineNumber());
        else
            sb.append(frame.getFileName());
        sb.append(")\n");
    }

    private static void appendBlockedOn(StringBuilder sb, ThreadInfo thread)
    {
        LockInfo lock = thread.getLockInfo();
        if (lock == null)
            return;
        switch (thread.getThreadState())
        {
            case BLOCKED:
                sb.append("\t- blocked on ").append(lock).append('\n');
                break;
            case WAITING:
            case TIMED_WAITING:
                sb.append("\t- waiting on ").append(lock).append('\n');
                break;
            default:
                break;
        }
    }
}
