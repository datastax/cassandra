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

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.Permission;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;

import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Base unit test class for standalone tools
 */
public abstract class ToolsTester
{
    private static List<ThreadInfo> initialThreads;

    static final String[] EXPECTED_THREADS_WITH_SCHEMA = {
    "NativePoolCleaner",
    "COMMIT-LOG-ALLOCATOR",
    "COMMIT-LOG-WRITER",
    "PerDiskMemtableFlushWriter_0:1",
    "MemtablePostFlush:1",
    "MemtableFlushWriter:1",
    "MemtableReclaimMemory:1",
    };
    static final String[] OPTIONAL_THREADS_WITH_SCHEMA = {
    "ScheduledTasks:1",
    "OptionalTasks:1",
    "Reference-Reaper:1",
    "LocalPool-Cleaner:1",
    "CacheCleanupExecutor:1",
    "CompactionExecutor:1",
    "CompactionExecutor:2",
    "CompactionExecutor:3",
    "CompactionExecutor:4",
    "ValidationExecutor:1",
    "NonPeriodicTasks:1",
    "Sampler:1",
    "SecondaryIndexManagement:1",
    "Strong-Reference-Leak-Detector:1",
    "Background_Reporter:1",
    "EXPIRING-MAP-REAPER:1",
    };

    public void assertNoUnexpectedThreadsStarted(String[] expectedThreadNames, String[] optionalThreadNames)
    {
        ThreadMXBean threads = ManagementFactory.getThreadMXBean();

        Set<String> initial = initialThreads
                                    .stream()
                                    .map(ThreadInfo::getThreadName)
                                    .collect(Collectors.toSet());

        Set<String> current = Arrays.asList(threads.getThreadInfo(threads.getAllThreadIds()))
                                    .stream()
                                    .map(ThreadInfo::getThreadName)
                                    .collect(Collectors.toSet());

        List<String> expected = expectedThreadNames != null ? Arrays.asList(expectedThreadNames) : Collections.emptyList();
        Collections.sort(expected);

        List<String> optional = optionalThreadNames != null ? Arrays.asList(optionalThreadNames) : Collections.emptyList();
        Collections.sort(optional);

        current.removeAll(initial);

        List<String> notPresent = expected.stream().filter(threadName -> !current.contains(threadName)).collect(Collectors.toList());

        current.removeAll(expected);
        current.removeAll(optional);

        if (!current.isEmpty())
            System.err.println("Unexpected thread names: " + current);
        if (!notPresent.isEmpty())
            System.err.println("Mandatory thread missing: " + notPresent);

        assertTrue("Wrong thread status", current.isEmpty() && notPresent.isEmpty());
    }

    public void assertSchemaNotLoaded()
    {
        assertClassNotLoaded("org.apache.cassandra.config.Schema");
    }

    public void assertSchemaLoaded()
    {
        assertClassLoaded("org.apache.cassandra.config.Schema");
    }

    public void assertKeyspaceNotLoaded()
    {
        assertClassNotLoaded("org.apache.cassandra.db.Keyspace");
    }

    public void assertKeyspaceLoaded()
    {
        assertClassLoaded("org.apache.cassandra.db.Keyspace");
    }

    public void assertServerNotLoaded()
    {
        assertClassNotLoaded("org.apache.cassandra.transport.Server");
    }

    public void assertSystemKSNotLoaded()
    {
        assertClassNotLoaded("org.apache.cassandra.db.SystemKeyspace");
    }

    public void assertCLSMNotLoaded()
    {
        assertClassNotLoaded("org.apache.cassandra.db.commitlog.CommitLogSegmentManager");
    }

    public void assertClassLoaded(String clazz)
    {
        assertClassLoadedStatus(clazz, true);
    }

    public void assertClassNotLoaded(String clazz)
    {
        assertClassLoadedStatus(clazz, false);
    }

    private void assertClassLoadedStatus(String clazz, boolean expected)
    {
        for (ClassLoader cl = Thread.currentThread().getContextClassLoader(); cl != null; cl = cl.getParent())
        {
            try
            {
                Method mFindLoadedClass = ClassLoader.class.getDeclaredMethod("findLoadedClass", String.class);
                mFindLoadedClass.setAccessible(true);
                boolean loaded = mFindLoadedClass.invoke(cl, clazz) != null;

                if (expected)
                {
                    if (loaded)
                        return;
                }
                else
                    assertFalse(clazz + " has been loaded", loaded);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        if (expected)
            fail(clazz + " has not been loaded");
    }

    public void runTool(int expectedExitCode, String clazz, String... args)
    {
        try
        {
            // install security manager to get informed about the exit-code
            System.setSecurityManager(new SecurityManager() {
                public void checkExit(int status)
                {
                    throw new SystemExitException(status);
                }

                public void checkPermission(Permission perm)
                {
                }

                public void checkPermission(Permission perm, Object context)
                {
                }
            });

            try
            {
                Class.forName(clazz).getDeclaredMethod("main", String[].class).invoke(null, (Object) args);
            }
            catch (InvocationTargetException e)
            {
                Throwable cause = e.getCause();
                if (cause instanceof Error)
                    throw (Error) cause;
                if (cause instanceof RuntimeException)
                    throw (RuntimeException) cause;
                throw e;
            }

            assertEquals("Unexpected exit code", expectedExitCode, 0);
        }
        catch (SystemExitException e)
        {
            assertEquals("Unexpected exit code", expectedExitCode, e.status);
        }
        catch (InvocationTargetException e)
        {
            throw new RuntimeException(e.getTargetException());
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            // uninstall security manager
            System.setSecurityManager(null);
        }
    }

    @BeforeClass
    public static void setupTester()
    {
        System.setProperty("cassandra.partitioner", "org.apache.cassandra.dht.Murmur3Partitioner");

        // may start an async appender
        LoggerFactory.getLogger(ToolsTester.class);

        ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        initialThreads = Arrays.asList(threads.getThreadInfo(threads.getAllThreadIds()));
    }

    public static class SystemExitException extends Error
    {
        public final int status;

        public SystemExitException(int status)
        {
            this.status = status;
        }
    }

    public static String findOneSSTable(String ks, String cf) throws IOException
    {
        File cfDir = sstableDir(ks, cf);
        File[] sstableFiles = cfDir.listFiles((file) -> file.isFile() && file.getName().endsWith("-Data.db"));
        return sstableFiles[0].getAbsolutePath();
    }

    public static String sstableDirName(String ks, String cf) throws IOException
    {
        return sstableDir(ks, cf).getAbsolutePath();
    }

    public static File sstableDir(String ks, String cf) throws IOException
    {
        File dataDir = copySSTables();
        File ksDir = new File(dataDir, ks);
        File[] cfDirs = ksDir.listFiles((dir, name) -> cf.equals(name) || name.startsWith(cf + '-'));
        return cfDirs[0];
    }

    public static File copySSTables() throws IOException
    {
        File dataDir = new File("build/test/cassandra/data");
        File srcDir = new File("test/data/legacy-sstables/ma");
        FileUtils.copyDirectory(new File(srcDir, "legacy_tables"), new File(dataDir, "legacy_sstables"));
        return dataDir;
    }
}
