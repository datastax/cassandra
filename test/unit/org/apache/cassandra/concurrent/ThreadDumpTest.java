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

import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.Util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ThreadDumpTest
{
    private static final int DEPTH = 20;
    private static final String STALLED = "ThreadDumpTest-stalled";
    private static final String HOLDER = "ThreadDumpTest-holder";

    private final Object lock = new Object();
    private final CountDownLatch release = new CountDownLatch(1);
    private Thread stalled;
    private Thread holder;

    @After
    public void releaseThreads() throws InterruptedException
    {
        release.countDown();
        if (stalled != null)
            stalled.join(10_000);
        if (holder != null)
            holder.join(10_000);
    }

    // a parked thread under a recursion deeper than ThreadInfo.toString() prints, holding a monitor half way down
    private void recurse(int depth)
    {
        if (depth == DEPTH / 2)
        {
            synchronized (lock)
            {
                recurse(depth - 1);
            }
        }
        else if (depth > 0)
        {
            recurse(depth - 1);
        }
        else
        {
            try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }

    private void startThreads()
    {
        holder = new Thread(() -> { try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } }, HOLDER);
        holder.setDaemon(true);
        holder.start();
        stalled = new Thread(() -> recurse(DEPTH), STALLED);
        stalled.setDaemon(true);
        stalled.start();
        Util.spinAssertEquals(Thread.State.WAITING, stalled::getState, 10);
        Util.spinAssertEquals(Thread.State.WAITING, holder::getState, 10);
    }

    private static int count(String s, String part)
    {
        int n = 0;
        for (int i = s.indexOf(part); i >= 0; i = s.indexOf(part, i + part.length()))
            n++;
        return n;
    }

    // the section of the dump for one thread, from its header to the next thread's header
    private static String section(String dump, String threadName)
    {
        int start = dump.indexOf("\n\"" + threadName + "\" ");
        assertTrue(threadName + " not in dump", start >= 0);
        int end = dump.indexOf("\n\"", start + 1);
        return end < 0 ? dump.substring(start) : dump.substring(start, end);
    }

    @Test
    public void testFormatEveryFrameAndLockedMonitor()
    {
        startThreads();
        ThreadInfo[] threads = ManagementFactory.getThreadMXBean().dumpAllThreads(true, false);
        String dump = ThreadDump.format(threads, List.of(), List.of());

        assertTrue(dump, dump.startsWith("Executor liveness thread dump (" + threads.length + " threads)\n"));
        String stalledSection = section(dump, STALLED);
        assertTrue(stalledSection, stalledSection.contains("\" #" + stalled.getId() + " daemon WAITING"));
        // every frame of the recursion, where ThreadInfo.toString() stops at 8 frames
        assertEquals(stalledSection, DEPTH + 1, count(stalledSection, "\tat " + ThreadDumpTest.class.getName() + ".recurse("));
        assertTrue(stalledSection, stalledSection.contains("\tat " + ThreadDumpTest.class.getName() + ".lambda$startThreads$"));
        String locked = "\t- locked " + Object.class.getName() + '@' + Integer.toHexString(System.identityHashCode(lock));
        assertEquals(stalledSection, 1, count(stalledSection, locked));
        // the monitor shows under the frame that took it: recurse() at depth DEPTH / 2, below the frames of depths 0 to DEPTH / 2
        List<String> lines = Arrays.asList(stalledSection.split("\n"));
        int lockedLine = lines.indexOf(locked);
        int firstRecurse = 0;
        while (!lines.get(firstRecurse).contains(".recurse("))
            firstRecurse++;
        assertEquals(stalledSection, DEPTH / 2 + 1, count(String.join("\n", lines.subList(firstRecurse, lockedLine)), ".recurse("));
    }

    @Test
    public void testMonitorLockedThroughJNI() throws Exception
    {
        startThreads();
        // no thread here holds a monitor entered through JNI, which has no frame, so make the stalled thread's monitor
        // look like one, in the form the platform MBean server reports a ThreadInfo
        CompositeData[] threads = (CompositeData[]) ManagementFactory.getPlatformMBeanServer().invoke(
            new ObjectName(ManagementFactory.THREAD_MXBEAN_NAME), "dumpAllThreads",
            new Object[]{ true, false }, new String[]{ boolean.class.getName(), boolean.class.getName() });
        ThreadInfo thread = null;
        for (CompositeData data : threads)
        {
            if (STALLED.equals(data.get("threadName")))
            {
                CompositeData[] monitors = (CompositeData[]) data.get("lockedMonitors");
                assertEquals(1, monitors.length);
                CompositeData jniMonitor = with(with(monitors[0], "lockedStackDepth", -1), "lockedStackFrame", null);
                CompositeData[] jniMonitors = Arrays.copyOf(monitors, 1);
                jniMonitors[0] = jniMonitor;
                thread = ThreadInfo.from(with(data, "lockedMonitors", jniMonitors));
            }
        }
        assertEquals(1, thread.getLockedMonitors().length);
        MonitorInfo monitor = thread.getLockedMonitors()[0];
        assertEquals(-1, monitor.getLockedStackDepth());

        String section = section(ThreadDump.format(new ThreadInfo[]{ thread }, List.of(), List.of()), STALLED);
        // listed once, after the last frame
        String locked = "\t- locked " + monitor + " (through JNI)";
        assertEquals(section, 1, count(section, "\t- locked "));
        List<String> lines = Arrays.asList(section.split("\n"));
        assertEquals(section, locked, lines.get(lines.size() - 1));
        assertTrue(section, lines.get(lines.size() - 2).startsWith("\tat "));
    }

    private static CompositeData with(CompositeData data, String key, Object value) throws Exception
    {
        Map<String, Object> items = new HashMap<>();
        for (String k : data.getCompositeType().keySet())
            items.put(k, data.get(k));
        items.put(key, value);
        return new CompositeDataSupport(data.getCompositeType(), items);
    }

    @Test
    public void testStalledThreadsFirst()
    {
        startThreads();
        String dump = ThreadDump.format(ManagementFactory.getThreadMXBean().dumpAllThreads(true, false), List.of(), List.of(STALLED));

        int firstThread = dump.indexOf("\n\"");
        assertEquals(dump, firstThread, dump.indexOf("\n\"" + STALLED + "\" "));
        assertTrue(dump, dump.indexOf("\n\"" + HOLDER + "\" ") > firstThread);
    }

    @Test
    public void testHeaderNamesWhatIsStalled()
    {
        startThreads();
        ThreadInfo[] threads = ManagementFactory.getThreadMXBean().dumpAllThreads(true, false);
        String dump = ThreadDump.format(threads, List.of("approximate clock refresher", "MemtableReclaimMemory"), List.of(STALLED));

        assertTrue(dump, dump.startsWith("Executor liveness thread dump (" + threads.length + " threads); stalled: " +
                                         "approximate clock refresher, MemtableReclaimMemory\n"));
        assertEquals(dump, dump.indexOf("\n\""), dump.indexOf("\n\"" + STALLED + "\" "));
    }

    @Test
    public void testDumpAllThreads()
    {
        startThreads();
        String dump = ThreadDump.dumpAllThreads(List.of("MemtableReclaimMemory"), List.of(HOLDER, STALLED));

        assertTrue(dump, dump.startsWith("Executor liveness thread dump ("));
        assertTrue(dump, dump.contains(" threads); stalled: MemtableReclaimMemory\n"));
        // in the order given, ahead of every other thread
        assertTrue(dump, dump.indexOf("\n\"" + HOLDER + "\" ") < dump.indexOf("\n\"" + STALLED + "\" "));
        assertEquals(dump, dump.indexOf("\n\""), dump.indexOf("\n\"" + HOLDER + "\" "));
        assertTrue(dump, dump.contains("\n\"" + Thread.currentThread().getName() + "\" "));
        assertEquals(DEPTH + 1, count(section(dump, STALLED), ".recurse("));
    }
}
