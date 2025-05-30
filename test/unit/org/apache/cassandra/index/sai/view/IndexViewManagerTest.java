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
package org.apache.cassandra.index.sai.view;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MonotonicClock;
import org.awaitility.Awaitility;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IndexViewManagerTest extends SAITester
{
    private static final int CONCURRENT_UPDATES = 100;

    @BeforeClass
    public static void setupVersionBarrier()
    {
        requireNetwork();
    }

    @Test
    public void testUpdateFromFlush()
    {
        createTable("CREATE TABLE %S (k INT PRIMARY KEY, v INT)");
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        IndexContext columnContext = columnIndex(getCurrentColumnFamilyStore(), indexName);
        View initialView = columnContext.getView();

        execute("INSERT INTO %s(k, v) VALUES (1, 10)");
        execute("INSERT INTO %s(k, v) VALUES (2, 20)");
        flush();

        View updatedView = columnContext.getView();
        assertNotEquals(initialView, updatedView);
        assertEquals(1, updatedView.getIndexes().size());
    }

    @Test
    public void testUpdateFromCompaction()
    {
        createTable("CREATE TABLE %S (k INT PRIMARY KEY, v INT)");
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        ColumnFamilyStore store = getCurrentColumnFamilyStore();
        IndexContext columnContext = columnIndex(store, indexName);
        store.disableAutoCompaction();

        execute("INSERT INTO %s(k, v) VALUES (1, 10)");
        execute("INSERT INTO %s(k, v) VALUES (2, 20)");
        execute("INSERT INTO %s(k, v) VALUES (3, 30)");
        flush();

        execute("INSERT INTO %s(k, v) VALUES (4, 40)");
        execute("INSERT INTO %s(k, v) VALUES (5, 50)");
        execute("INSERT INTO %s(k, v) VALUES (6, 60)");
        flush();

        View initialView = columnContext.getView();
        assertEquals(2, initialView.getIndexes().size());

        CompactionManager.instance.performMaximal(store, false);

        View updatedView = columnContext.getView();
        assertNotEquals(initialView, updatedView);
        assertEquals(1, updatedView.getIndexes().size());
    }

    /**
     * Tests concurrent sstable updates from flush and compaction, see CASSANDRA-14207.
     */
    @Test
    public void testConcurrentUpdate() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %S (k INT PRIMARY KEY, v INT)");
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        ColumnFamilyStore store = getCurrentColumnFamilyStore();
        IndexContext columnContext = columnIndex(store, indexName);
        Path tmpDir = Files.createTempDirectory("IndexViewManagerTest");
        store.disableAutoCompaction();

        List<Descriptor> descriptors = new ArrayList<>();

        // create sstable 1 from flush
        execute("INSERT INTO %s(k, v) VALUES (1, 10)");
        execute("INSERT INTO %s(k, v) VALUES (2, 20)");
        execute("INSERT INTO %s(k, v) VALUES (3, 30)");
        flush();

        // create sstable 2 from flush
        execute("INSERT INTO %s(k, v) VALUES (4, 40)");
        execute("INSERT INTO %s(k, v) VALUES (5, 50)");
        execute("INSERT INTO %s(k, v) VALUES (6, 60)");
        flush();

        // save sstables 1 and 2 and create sstable 3 from compaction
        assertEquals(2, store.getLiveSSTables().size());
        store.getLiveSSTables().forEach(reader -> copySSTable(reader, tmpDir));
        getCurrentColumnFamilyStore().getLiveSSTables().stream().map(t -> t.descriptor).forEach(descriptors::add);
        CompactionManager.instance.performMaximal(store, false);

        // create sstable 4 from flush
        execute("INSERT INTO %s(k, v) VALUES (5, 50)");
        execute("INSERT INTO %s(k, v) VALUES (6, 60)");
        flush();

        // save sstables 3 and 4
        store.getLiveSSTables().forEach(reader -> copySSTable(reader, tmpDir));
        getCurrentColumnFamilyStore().getLiveSSTables().stream().map(t -> t.descriptor).forEach(descriptors::add);

        List<SSTableReader> sstables = descriptors.stream()
                                                .map(desc -> new Descriptor(new File(tmpDir), KEYSPACE, tableName, desc.id))
                                                .map(desc -> desc.getFormat().getReaderFactory().open(desc))
                                                .collect(Collectors.toList());

        assertThat(sstables).hasSize(4);

        List<SSTableReader> none = Collections.emptyList();
        List<SSTableReader> initial = sstables.stream().limit(2).collect(Collectors.toList());

        ExecutorService executor = Executors.newFixedThreadPool(2);
        for (int i = 0; i < CONCURRENT_UPDATES; i++)
        {
            // mock the initial view indexes to track the number of releases
            List<SSTableContext> initialContexts = sstables.stream().limit(2).map(s -> SSTableContext.create(s, loadDescriptor(s, store).perSSTableComponents())).collect(Collectors.toList());
            List<SSTableIndex> initialIndexes = new ArrayList<>();

            for (SSTableContext initialContext : initialContexts)
            {
                MockSSTableIndex mockSSTableIndex = new MockSSTableIndex(initialContext, initialContext.usedPerSSTableComponents().indexDescriptor().perIndexComponents(columnContext));
                initialIndexes.add(mockSSTableIndex);
            }

            IndexViewManager tracker = new IndexViewManager(columnContext, initialIndexes);
            View initialView = tracker.getView();
            assertEquals(2, initialView.size());

            List<SSTableReader> compacted = List.of(sstables.get(2));
            List<SSTableReader> flushed = List.of(sstables.get(3));

            List<SSTableContext> compactedContexts = compacted.stream().map(s -> SSTableContext.create(s, loadDescriptor(s, store).perSSTableComponents())).collect(Collectors.toList());
            List<SSTableContext> flushedContexts = flushed.stream().map(s -> SSTableContext.create(s, loadDescriptor(s, store).perSSTableComponents())).collect(Collectors.toList());

            // concurrently update from both flush and compaction
            Future<?> compaction = executor.submit(() -> tracker.update(initial, compactedContexts, true));
            Future<?> flush = executor.submit(() -> tracker.update(none, flushedContexts, true));

            FBUtilities.waitOnFutures(Arrays.asList(compaction, flush));

            View updatedView = tracker.getView();
            assertNotEquals(initialView, updatedView);
            assertEquals(2, updatedView.getIndexes().size());

            for (SSTableIndex index : initialIndexes)
            {
                // Because of the race condition, it is released either once or twice. It won't be more
                // because there are only two updates. The only real requirement is that it is released.
                var releaseCount = ((MockSSTableIndex) index).releaseCount;
                assertTrue("releaseCount should be 1 or 2 but it is " + releaseCount,
                           releaseCount == 1 || releaseCount == 2);
                assertTrue(index.isReleased());
            }

            // release original SSTableContext objects.
            // shared copies are already released when compacted and flushed are added.
            initialContexts.forEach(SSTableContext::close);
            initialContexts.forEach(group -> assertTrue(group.isCleanedUp()));

            // release compacted and flushed SSTableContext original and shared copies
            compactedContexts.forEach(SSTableContext::close);
            flushedContexts.forEach(SSTableContext::close);
            tracker.getView().getIndexes().forEach(SSTableIndex::release);
            compactedContexts.forEach(group -> assertTrue(group.isCleanedUp()));
            flushedContexts.forEach(group -> assertTrue(group.isCleanedUp()));
        }
        sstables.forEach(sstable -> sstable.selfRef().release());
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);
    }


    @Test
    public void testMarkIndexWasDropped()
    {
        IndexContext mockContext = mock(IndexContext.class);
        when(mockContext.isVector()).thenReturn(true);
        SSTableReader sstable = mock(SSTableReader.class);
        SSTableIndex index = mock(SSTableIndex.class);
        when(index.reference()).thenReturn(true);
        when(index.getSSTable()).thenReturn(sstable);
        when(index.getIndexContext()).thenReturn(mockContext);

        IndexViewManager tracker = new IndexViewManager(mockContext, Collections.singleton(index));
        var view = tracker.getView();
        // Now we have 2 references to the view.
        assertTrue(view.reference());
        // Invalidate and trigger markIndexWasDropped in the view, but not in the index since we have an extra ref.
        tracker.invalidate(true);
        // Assert index hasn't had markIndexDropped called yet.
        verify(index, never()).markIndexDropped();
        // Release and trigger/validate markIndexDropped in the index.
        view.release();
        verify(index, times(1)).markIndexDropped();
        assertFalse(view.reference());
    }

    @Test
    public void testRetryGetReferencedView() throws Throwable
    {
        createTable("CREATE TABLE %S (k INT PRIMARY KEY, v INT)");
        String indexName = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");
        disableCompaction();

        ColumnFamilyStore store = getCurrentColumnFamilyStore();
        IndexContext columnContext = columnIndex(store, indexName);

        // Insert data and flush to create initial SSTable
        execute("INSERT INTO %s(k, v) VALUES (1, 10)");
        execute("INSERT INTO %s(k, v) VALUES (2, 20)");
        flush();

        // Create a barrier that will pause the getReferencedView method
        Injections.Barrier viewReferencePause =
            Injections.newBarrier("pause_get_referenced_view", 3, false)
                .add(InvokePointBuilder.newInvokePoint()
                                       .onClass(View.class)
                                       .onMethod("reference"))
                .build();

        // Create a counter to track how many times IndexViewManager.getView() is called
        Injections.Counter referenceCounter =
            Injections.newCounter("get_view_counter")
                .add(InvokePointBuilder.newInvokePoint()
                    .onClass(IndexViewManager.class)
                    .onMethod("getView"))
                .build();

        try
        {
            // Inject the barrier and counter
            Injections.inject(viewReferencePause, referenceCounter);

            // Start a thread that will try to get a referenced view. The deadline is high because we want to loop.
            Future<View> viewFutureExpectSuccess = CompletableFuture.supplyAsync(() -> {
                return columnContext.getReferencedView(TimeUnit.SECONDS.toNanos(100));
            });

            // Start a thread that will try to get a referenced view. The deadline is high because we want to loop.
            Future<View> viewFutureExpectNull = CompletableFuture.supplyAsync(() -> {
                return columnContext.getReferencedView(0);
            });

            //  Barrier should have 1 remaining await
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> viewReferencePause.getCount() == 1);

            // Sleep long enough for the next call to isAfter to return true
            Thread.sleep(TimeUnit.NANOSECONDS.toMillis(MonotonicClock.approxTime.error()));

            // While getReferencedView is paused, perform a compaction to change the view
            execute("INSERT INTO %s(k, v) VALUES (3, 30)");
            flush();

            // Confirm state before proceeding
            assertEquals("getView should have been called once so far", 2, referenceCounter.get());

            // Release just in case it hasn't been yet.
            viewReferencePause.countDown();

            // Get the result and verify it's not null
            View result = viewFutureExpectSuccess.get(5, TimeUnit.SECONDS);
            assertNotNull("Should have eventually gotten a referenced view", result);

            // Verify that the other thread got a null result
            assertNull("Should have eventually gotten a null view", viewFutureExpectNull.get());

            // Verify that reference() was called 3 times (indicating retry on the first but not the second)
            assertEquals("Reference should have been called 3 times", referenceCounter.get(), 3);

            // Clean up
            result.release();
        }
        finally
        {
            Injections.deleteAll();
        }
    }

    private IndexContext columnIndex(ColumnFamilyStore store, String indexName)
    {
        assert store.indexManager != null;
        StorageAttachedIndex sai = (StorageAttachedIndex) store.indexManager.getIndexByName(indexName);
        return sai.getIndexContext();
    }

    public static class MockSSTableIndex extends SSTableIndex
    {
        int releaseCount = 0;

        MockSSTableIndex(SSTableContext group, IndexComponents.ForRead perIndexComponents) throws IOException
        {
            super(group, perIndexComponents);
        }

        @Override
        public void release()
        {
            super.release();
            releaseCount++;
        }
    }

    private static void copySSTable(SSTableReader table, Path destDir)
    {
        for (Component component : SSTable.componentsFor(table.descriptor))
        {
            Path src = table.descriptor.fileFor(component).toPath();
            Path dst = destDir.resolve(src.getFileName());
            try
            {
                Files.copy(src, dst, StandardCopyOption.REPLACE_EXISTING);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}
