/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.index.sai;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.management.AttributeNotFoundException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.google.common.base.Predicates;
import org.junit.After;

import com.datastax.driver.core.QueryTrace;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.ReadFailureException;
import com.sun.management.UnixOperatingSystemMXBean;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sai.disk.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.utils.NamedMemoryLimiter;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;
import org.apache.lucene.codecs.CodecUtil;
import org.awaitility.Awaitility;

import static org.apache.cassandra.inject.ActionBuilder.newActionBuilder;
import static org.apache.cassandra.inject.Expression.quote;
import static org.apache.cassandra.inject.InvokePointBuilder.newInvokePoint;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SAITester extends CQLTester
{
    protected static final String CREATE_KEYSPACE_TEMPLATE = "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}";

    protected static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE %s (id1 TEXT PRIMARY KEY, v1 INT, v2 TEXT) WITH compaction = " +
            "{'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }";
    protected static final String CREATE_INDEX_TEMPLATE = "CREATE CUSTOM INDEX IF NOT EXISTS ON %%s(%s) USING 'StorageAttachedIndex'";

    protected static int ASSERTION_TIMEOUT_SECONDS = 15;

    protected static final Injections.Counter INDEX_BUILD_COUNTER = Injections.newCounter("IndexBuildCounter")
                                                                              .add(newInvokePoint().onClass(CompactionManager.class)
                                                                                                   .onMethod("submitIndexBuild", "SecondaryIndexBuilder", "ActiveCompactionsTracker"))
                                                                              .build();

    protected static ColumnIdentifier V1_COLUMN_IDENTIFIER = ColumnIdentifier.getInterned("v1", true);
    protected static ColumnIdentifier V2_COLUMN_IDENTIFIER = ColumnIdentifier.getInterned("v2", true);

    public enum CorruptionType
    {
        REMOVED
                {
                    @Override
                    public void corrupt(File file) throws IOException
                    {
                        if (!file.delete())
                            throw new IOException("Unable to delete file: " + file);
                    }
                },
        EMPTY_FILE
                {
                    @Override
                    public void corrupt(File file) throws IOException
                    {
                        FileChannel.open(file.toPath(), StandardOpenOption.WRITE).truncate(0).close();
                    }
                },
        TRUNCATED_HEADER
                {
                    @Override
                    public void corrupt(File file) throws IOException
                    {
                        FileChannel.open(file.toPath(), StandardOpenOption.WRITE).truncate(2).close();
                    }
                },
        TRUNCATED_DATA
                {
                    @Override
                    public void corrupt(File file) throws IOException
                    {
                        // header length is not fixed, use footer length to navigate a given data position
                        FileChannel.open(file.toPath(), StandardOpenOption.WRITE).truncate(file.length() - CodecUtil.footerLength() - 2).close();
                    }
                },
        TRUNCATED_FOOTER
                {
                    @Override
                    public void corrupt(File file) throws IOException
                    {
                        FileChannel.open(file.toPath(), StandardOpenOption.WRITE).truncate(file.length() - CodecUtil.footerLength() + 2).close();
                    }
                },
        APPENDED_DATA
                {
                    @Override
                    public void corrupt(File file) throws IOException
                    {
                        try (RandomAccessFile raf = new RandomAccessFile(file, "rw"))
                        {
                            raf.seek(file.length());

                            byte[] corruptedData = new byte[100];
                            new Random().nextBytes(corruptedData);
                            raf.write(corruptedData);
                        }
                    }
                };

        public abstract void corrupt(File file) throws IOException;
    }

    @After
    public void removeAllInjections()
    {
        Injections.deleteAll();
    }

    public static ColumnContext createColumnContext(String name, AbstractType<?> validator)
    {
        return new ColumnContext("test_ks",
                                 "test_cf",
                                 UTF8Type.instance,
                                 new ClusteringComparator(),
                                 ColumnMetadata.regularColumn("sai", "internal", name, validator),
                                 IndexMetadata.fromSchemaMetadata(name, IndexMetadata.Kind.CUSTOM, null),
                                 IndexWriterConfig.emptyConfig());
    }

    public static ColumnContext createColumnContext(String columnName, String indexName, AbstractType<?> validator)
    {
        return new ColumnContext("test_ks",
                                 "test_cf",
                                 UTF8Type.instance,
                                 new ClusteringComparator(),
                                 ColumnMetadata.regularColumn("sai", "internal", columnName, validator),
                                 IndexMetadata.fromSchemaMetadata(indexName, IndexMetadata.Kind.CUSTOM, null),
                                 IndexWriterConfig.emptyConfig());
    }

    protected void simulateNodeRestart()
    {
        simulateNodeRestart(true);
    }

    protected void simulateNodeRestart(boolean wait)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        cfs.indexManager.listIndexes().forEach(index -> {
            ((StorageAttachedIndexGroup)cfs.indexManager.getIndexGroup(index)).reset();
        });
        cfs.indexManager.listIndexes().forEach(index -> cfs.indexManager.buildIndex(index));
        cfs.indexManager.executePreJoinTasksBlocking(true);
        if (wait)
        {
            waitForIndexQueryable();
        }
    }

    protected void corruptNDIComponent(Component ndiComponent, CorruptionType corruptionType) throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());

        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            File file = sstable.descriptor.fileFor(ndiComponent);
            corruptionType.corrupt(file);
        }
    }

    protected void waitForAssert(Runnable runnableAssert, long timeout, TimeUnit unit)
    {
        Awaitility.await().dontCatchUncaughtExceptions().atMost(timeout, unit).untilAsserted(runnableAssert::run);
    }

    protected void waitForAssert(Runnable assertion)
    {
        waitForAssert(() -> {
            try
            {
                assertion.run();
            }
            catch (Throwable ex)
            {
                throw new RuntimeException(ex);
            }
        }, ASSERTION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    protected boolean indexNeedsFullRebuild(String index)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        return cfs.indexManager.needsFullRebuild(index);
    }

    protected boolean isIndexQueryable()
    {
        return isIndexQueryable(KEYSPACE, currentTable());
    }

    protected boolean isIndexQueryable(String keyspace, String table)
    {
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        for (Index index : cfs.indexManager.listIndexes())
        {
            if (!cfs.indexManager.isIndexQueryable(index))
                return false;
        }
        return true;
    }

    protected void verifyInitialIndexFailed(String indexName)
    {
        // Verify that the initial index build fails...
        waitForAssert(() -> {
            try
            {
                assertTrue(indexNeedsFullRebuild(indexName));
            }
            catch (Exception e)
            {
                throw Throwables.unchecked(e);
            }
        });
    }

    protected boolean verifyChecksum(ColumnContext context)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());

        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            IndexComponents components = IndexComponents.create(context.getIndexName(), sstable);
            if (!components.validatePerSSTableComponentsChecksum() || !components.validatePerColumnComponentsChecksum(context.isLiteral()))
                return false;
        }
        return true;
    }

    protected static void assertFailureReason(ReadFailureException e, RequestFailureReason reason)
    {
        int expected = reason.codeForNativeProtocol();
        int actual = e.getFailuresMap().get(FBUtilities.getBroadcastAddressAndPort().address);
        assertEquals(expected, actual);
    }

    protected Object getMBeanAttribute(ObjectName name, String attribute) throws Exception
    {
        return jmxConnection.getAttribute(name, attribute);
    }

    protected Object getMetricValue(ObjectName metricObjectName) throws Exception
    {
        // lets workaround the fact that gauges have Value, but counters have Count
        Object metricValue;
        try
        {
            metricValue = getMBeanAttribute(metricObjectName, "Value");
        }
        catch (AttributeNotFoundException ignored)
        {
            metricValue = getMBeanAttribute(metricObjectName, "Count");
        }
        return metricValue;
    }

    public void waitForIndexQueryable()
    {
        waitForIndexQueryable(KEYSPACE, currentTable());
    }

    public void waitForIndexQueryable(String keyspace, String table)
    {
        waitForAssert(() -> assertTrue(isIndexQueryable(keyspace, table)), 60, TimeUnit.SECONDS);
    }

    protected void startCompaction() throws Throwable
    {
        Iterable<ColumnFamilyStore> tables = StorageService.instance.getValidColumnFamilies(true, false, KEYSPACE, currentTable());
        tables.forEach(table ->
        {
            int gcBefore = CompactionManager.getDefaultGcBefore(table, FBUtilities.nowInSeconds());
            CompactionManager.instance.submitMaximal(table, gcBefore, false);
        });
    }

    public void waitForCompactions()
    {
        waitForAssert(() -> {
            try
            {
                assertFalse(CompactionManager.instance.isCompacting(ColumnFamilyStore.all(), Predicates.alwaysTrue()));
            }
            catch (Throwable ex)
            {
                throw Throwables.unchecked(ex);
            }
        }, 300, TimeUnit.SECONDS);

    }

    protected void waitForCompactionsFinished()
    {
        waitForAssert(() -> {
            try
            {
                assertEquals(0, getCompactionTasks());
            }
            catch (Throwable ex)
            {
                throw new RuntimeException(ex);
            }
        }, 60, TimeUnit.SECONDS);
    }

    protected void waitForEquals(ObjectName name, ObjectName name2)
    {
        waitForAssert(() -> {
            try
            {
                long jmxValue = ((Number) getMetricValue(name)).longValue();
                long jmxValue2 = ((Number) getMetricValue(name2)).longValue();

                jmxValue2 += 2; // add 2 for the first 2 queries in setupCluster

                assertEquals(jmxValue, jmxValue2);
            }
            catch (Throwable ex)
            {
                throw Throwables.unchecked(ex);
            }
        }, 160, TimeUnit.SECONDS);
    }

    protected void waitForEquals(ObjectName name, long value)
    {
        waitForAssert(() -> {
            try
            {
                long jmxValue = ((Number) getMetricValue(name)).longValue();
                assertEquals(value, jmxValue);
            }
            catch (Throwable ex)
            {
                throw Throwables.unchecked(ex);
            }
        }, 160, TimeUnit.SECONDS);
    }

    protected ObjectName objectName(String name, String keyspace, String table, String index, String type)
    {
        try
        {
            return new ObjectName(String.format("org.apache.cassandra.metrics:type=StorageAttachedIndex,keyspace=%s,table=%s,index=%s,scope=%s,name=%s",
                    keyspace, table, index, type, name));
        }
        catch (Throwable ex)
        {
            throw Throwables.unchecked(ex);
        }
    }

    protected ObjectName objectNameNoIndex(String name, String keyspace, String table, String type)
    {
        try
        {
            return new ObjectName(String.format("org.apache.cassandra.metrics:type=StorageAttachedIndex,keyspace=%s,table=%s,scope=%s,name=%s",
                    keyspace, table, type, name));
        }
        catch (Throwable ex)
        {
            throw Throwables.unchecked(ex);
        }
    }

    protected void verifySegments(String indexName, int count) throws Exception
    {
        int segments = getSegmentCount(indexName);
        assertEquals("Expect " + count +" segments, but got " + segments, count, segments);
    }

    protected int getSegmentCount(String indexName) throws Exception
    {
        return 1;
    }

    protected void upgradeSSTables()
    {
        try
        {
            StorageService.instance.upgradeSSTables(KEYSPACE, false, currentTable());
        }
        catch (Throwable e)
        {
            throw new RuntimeException(e);
        }
    }

    protected void releaseIndexFiles()
    {
        ColumnFamilyStore cfs = Schema.instance.getKeyspaceInstance(KEYSPACE).getColumnFamilyStore(currentTable());
        StorageAttachedIndexGroup.getIndexGroup(cfs).sstableContextManager().clear();
        for (Index index : cfs.indexManager.listIndexes())
        {
            StorageAttachedIndex sai = (StorageAttachedIndex) index;
            sai.getContext().drop(cfs.getLiveSSTables());
        }
    }

    protected int getOpenIndexFiles() throws Throwable
    {
        ColumnFamilyStore cfs = Schema.instance.getKeyspaceInstance(KEYSPACE).getColumnFamilyStore(currentTable());
        return StorageAttachedIndexGroup.getIndexGroup(cfs).openIndexFiles();
    }

    protected long getOpenVmFiles()
    {
        OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        if (os instanceof UnixOperatingSystemMXBean)
        {
            return ((UnixOperatingSystemMXBean) os).getOpenFileDescriptorCount();
        }
        throw new UnsupportedOperationException("getOpenFileDescriptorCount() is not supported on current os: " + os.getName());
    }

    protected void verifyIndexFiles(int numericFiles, int stringFiles)
    {
        verifyIndexFiles(Math.max(numericFiles, stringFiles), numericFiles, stringFiles, numericFiles + stringFiles);
    }

    protected void verifyIndexFiles(int perSSTableFiles, int numericFiles, int stringFiles, int completionFiles)
    {
        Set<File> indexFiles = indexFiles();

        for (Component component : IndexComponents.PER_SSTABLE_COMPONENTS)
        {
            Set<File> tableFiles = componentFiles(indexFiles, component);
            assertEquals(tableFiles.toString(), perSSTableFiles, tableFiles.size());
        }

        for (IndexComponents.NDIType type : IndexComponents.STRING_COMPONENTS)
        {
            Set<File> stringIndexFiles = componentFiles(indexFiles, type.name);
            assertEquals(stringIndexFiles.toString(), stringFiles, stringIndexFiles.size());
        }

        Set<File> kdTreeFiles = componentFiles(indexFiles, IndexComponents.NDIType.KD_TREE.name);
        assertEquals(kdTreeFiles.toString(), numericFiles, kdTreeFiles.size());

        Set<File> metaFiles = componentFiles(indexFiles, IndexComponents.NDIType.META.name);
        assertEquals(metaFiles.toString(), numericFiles + stringFiles, metaFiles.size());

        Set<File> completionMarkers = componentFiles(indexFiles, IndexComponents.NDIType.COLUMN_COMPLETION_MARKER.name);
        assertEquals(completionMarkers.toString(), completionFiles, completionMarkers.size());
    }

    protected Set<File> indexFiles()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        Set<Component> components = cfs.indexManager.listIndexGroups()
                                                    .stream()
                                                    .filter(g -> g instanceof StorageAttachedIndexGroup)
                                                    .map(Index.Group::getComponents)
                                                    .flatMap(Set::stream)
                                                    .collect(Collectors.toSet());

        Set<File> indexFiles = new HashSet<>();
        for (Component component : components)
        {
            List<File> files = cfs.getDirectories().getCFDirectories()
                    .stream()
                    .flatMap(dir -> Arrays.stream(dir.listFiles()))
                    .filter(File::isFile)
                    .filter(f -> f.getName().endsWith(component.name))
                    .collect(Collectors.toList());
            indexFiles.addAll(files);
        }
        return indexFiles;
    }

    protected ObjectName bufferSpaceObjectName(String name) throws MalformedObjectNameException
    {
        return new ObjectName(String.format("org.apache.cassandra.metrics:type=StorageAttachedIndex,name=%s", name));
    }

    protected long getSegmentBufferSpaceLimit() throws Exception
    {
        ObjectName limitBytesName = bufferSpaceObjectName("SegmentBufferSpaceLimitBytes");
        return (long) (Long) getMetricValue(limitBytesName);
    }

    protected Object getSegmentBufferUsedBytes() throws Exception
    {
        ObjectName usedBytesName = bufferSpaceObjectName("SegmentBufferSpaceUsedBytes");
        return getMetricValue(usedBytesName);
    }

    protected Object getColumnIndexBuildsInProgress() throws Exception
    {
        ObjectName buildersInProgressName = bufferSpaceObjectName("ColumnIndexBuildsInProgress");
        return getMetricValue(buildersInProgressName);
    }

    protected void verifySSTableIndexes(String indexName, int count)
    {
        try
        {
            verifySSTableIndexes(indexName, count, count);
        }
        catch (Exception e)
        {
            throw Throwables.unchecked(e);
        }
    }

    protected void verifySSTableIndexes(String indexName, int sstableContextCount, int sstableIndexCount)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        StorageAttachedIndexGroup indexGroup = StorageAttachedIndexGroup.getIndexGroup(cfs);
        int contextCount = indexGroup.sstableContextManager().size();
        assertEquals("Expected " + sstableContextCount +" SSTableContexts, but got " + contextCount, sstableContextCount, contextCount);

        StorageAttachedIndex sai = (StorageAttachedIndex) cfs.indexManager.getIndexByName(indexName);
        Collection<SSTableIndex> sstableIndexes = sai == null ? Collections.emptyList() : sai.getContext().getView().getIndexes();
        assertEquals("Expected " + sstableIndexCount +" SSTableIndexes, but got " + sstableIndexes.toString(), sstableIndexCount, sstableIndexes.size());
    }

    protected void truncate(boolean snapshot)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        cfs.truncateBlocking(snapshot);
    }

    protected void rebuildIndexes(String... indexes)
    {
        ColumnFamilyStore.rebuildSecondaryIndex(KEYSPACE, currentTable(), indexes);
    }

    protected void reloadSSTableIndex()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        StorageAttachedIndexGroup.getIndexGroup(cfs).unsafeReload();
    }

    protected void runInitializationTask() throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        for (Index i : cfs.indexManager.listIndexes())
        {
            assert i instanceof StorageAttachedIndex;
            cfs.indexManager.makeIndexNonQueryable(i, Index.Status.BUILD_FAILED);
            cfs.indexManager.buildIndex(i).get();
        }
    }

    protected int getCompactionTasks()
    {
        return CompactionManager.instance.getActiveCompactions() + CompactionManager.instance.getPendingTasks();
    }

    protected String getSingleTraceStatement(Session session, String query, String contains) throws Throwable
    {
        query = String.format(query, KEYSPACE + "." + currentTable());
        QueryTrace trace = session.execute(session.prepare(query).bind().enableTracing()).getExecutionInfo().getQueryTrace();
        waitForTracingEvents();

        for (QueryTrace.Event event : trace.getEvents())
        {
            if (event.getDescription().contains(contains))
                return event.getDescription();
        }
        return null;
    }

    protected void assertNumRows(int expected, String query, Object... args) throws Throwable
    {
        ResultSet rs = executeNet(String.format(query, args));
        assertEquals(expected, rs.all().size());
    }

    protected static Injection newFailureOnEntry(String name, Class<?> invokeClass, String method, Class<? extends Throwable> exception)
    {
        return Injections.newCustom(name)
                         .add(newInvokePoint().onClass(invokeClass).onMethod(method))
                         .add(newActionBuilder().actions().doThrow(exception, quote("Injected failure!")))
                         .build();
    }

    protected int snapshot(String snapshotName)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        Set<SSTableReader> snapshottedSSTables = cfs.snapshot(snapshotName);
        return snapshottedSSTables.size();
    }

    protected List<String> restoreSnapshot(String snapshot)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        Directories.SSTableLister lister = cfs.getDirectories().sstableLister(Directories.OnTxnErr.IGNORE).snapshots(snapshot);
        return restore(cfs, lister);
    }

    protected List<String> restoreBackup()
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        Directories.SSTableLister lister = cfs.getDirectories().sstableLister(Directories.OnTxnErr.IGNORE).includeBackups(true).onlyBackups(true);
        return restore(cfs, lister);
    }

    protected List<String> restore(ColumnFamilyStore cfs, Directories.SSTableLister lister)
    {
        File dataDirectory = cfs.getDirectories().getDirectoryForNewSSTables();

        List<String> fileNames = new ArrayList<>();
        for (File file : lister.listFiles())
        {
            if (file.renameTo(new File(dataDirectory.getAbsoluteFile() + File.separator + file.getName())))
            {
                fileNames.add(file.getName());
            }
        }
        cfs.loadNewSSTables();
        return fileNames;
    }

    protected long indexFilesLastModified()
    {
        return indexFiles().stream().map(File::lastModified).max(Long::compare).orElse(0L);
    }

    protected static void setSegmentWriteBufferSpace(final int segmentSize) throws Exception
    {
        NamedMemoryLimiter limiter = (NamedMemoryLimiter) StorageAttachedIndex.class.getDeclaredField("SEGMENT_BUILD_MEMORY_LIMITER").get(null);
        Field limitBytes = limiter.getClass().getDeclaredField("limitBytes");
        limitBytes.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(limitBytes, limitBytes.getModifiers() & ~Modifier.FINAL);
        limitBytes.set(limiter, segmentSize);
    }

    private Set<File> componentFiles(Collection<File> indexFiles, Component component)
    {
        return indexFiles.stream().filter(c -> c.getName().endsWith(component.name)).collect(Collectors.toSet());
    }

    private Set<File> componentFiles(Collection<File> indexFiles, String shortName)
    {
        String suffix = String.format("_%s.db", shortName);
        return indexFiles.stream().filter(c -> c.getName().endsWith(suffix)).collect(Collectors.toSet());
    }

    /**
     * Run repeated verification task concurrently with target test
     */
    protected static class TestWithConcurrentVerification
    {
        private final Runnable verificationTask;
        private final CountDownLatch verificationStarted = new CountDownLatch(1);

        private final Runnable targetTask;
        private final CountDownLatch taskCompleted = new CountDownLatch(1);

        private final int verificationIntervalInMs;
        private final int verificationMaxInMs = 300_000; // 300s

        public TestWithConcurrentVerification(Runnable verificationTask, Runnable targetTask)
        {
            this(verificationTask, targetTask, 10);
        }

        /**
         * @param verificationTask to be run concurrently with target task
         * @param targetTask task to be performed once
         * @param verificationIntervalInMs interval between each verification task, -1 to run verification task once
         */
        public TestWithConcurrentVerification(Runnable verificationTask, Runnable targetTask, int verificationIntervalInMs)
        {
            this.verificationTask = verificationTask;
            this.targetTask = targetTask;
            this.verificationIntervalInMs = verificationIntervalInMs;
        }

        public void start()
        {
            Thread verificationThread = new Thread(() -> {
                verificationStarted.countDown();

                while (true)
                {
                    try
                    {
                        verificationTask.run();

                        if (verificationIntervalInMs < 0 || taskCompleted.await(verificationIntervalInMs, TimeUnit.MILLISECONDS))
                            break;
                    }
                    catch (Throwable e)
                    {
                        throw Throwables.unchecked(e);
                    }
                }
            });

            try
            {
                verificationThread.start();
                verificationStarted.await();

                targetTask.run();
                taskCompleted.countDown();

                verificationThread.join(verificationMaxInMs);
            }
            catch (InterruptedException e)
            {
                throw Throwables.unchecked(e);
            }
        }
    }
}
