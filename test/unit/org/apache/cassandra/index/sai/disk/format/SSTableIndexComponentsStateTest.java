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
package org.apache.cassandra.index.sai.disk.format;

import java.io.IOException;
import java.util.Set;

import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.SSTableIndexComponentsState.UnapplicableDiffException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.mockito.Mockito;

import static org.apache.cassandra.index.sai.disk.format.IndexDescriptorTest.createFakeDataFile;
import static org.apache.cassandra.index.sai.disk.format.IndexDescriptorTest.createFakePerIndexComponents;
import static org.apache.cassandra.index.sai.disk.format.IndexDescriptorTest.createFakePerSSTableComponents;
import static org.apache.cassandra.index.sai.disk.format.IndexDescriptorTest.loadDescriptor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class SSTableIndexComponentsStateTest
{
    @Test
    public void indexWasUpdatedTest()
    {
        // Note: the `indexWasUpdated` method is not used in C* at the time of this writing, but it is in CNDB. Those
        // tests both make sure it works as expected, but also avoid the method be marked "unused" by code editors.

        var base = SSTableIndexComponentsState.builder()
                                               .addPerSSTable(Version.DB, 0, 1)
                                               .addPerIndex("index1", Version.DB, 0, 1)
                                               .addPerIndex("index2", Version.DB, 0, 1);

        // Additions are changes.
        assertTrue(base.copy().build().indexWasUpdated(SSTableIndexComponentsState.EMPTY, "index1"));
        assertTrue(base.copy().build().indexWasUpdated(SSTableIndexComponentsState.EMPTY, "index2"));

        // Modifying the per-sstable component is an update of all indexes.
        // version change:
        assertIndexUpdated(base, base.copy().addPerSSTable(Version.EB, 0, 1), "index1");
        assertIndexUpdated(base, base.copy().addPerSSTable(Version.EB, 0, 1), "index2");
        // generation change:
        assertIndexUpdated(base, base.copy().addPerSSTable(Version.DB, 1, 1), "index1");
        assertIndexUpdated(base, base.copy().addPerSSTable(Version.DB, 1, 1), "index2");

        // Modifying a per-index component only count as an update of that index.
        // version change:
        assertIndexUpdated(base, base.copy().addPerIndex("index1", Version.EB, 0, 1), "index1");
        assertIndexNotUpdated(base, base.copy().addPerIndex("index1", Version.EB, 0, 1), "index2");
        // generation change:
        assertIndexUpdated(base, base.copy().addPerIndex("index1", Version.DB, 1, 1), "index1");
        assertIndexNotUpdated(base, base.copy().addPerIndex("index1", Version.DB, 1, 1), "index2");

        // Same state means no change
        assertIndexNotUpdated(base, base, "index1");
        assertIndexNotUpdated(base, base, "index2");
    }

    private void assertIndexUpdated(SSTableIndexComponentsState.Builder before, SSTableIndexComponentsState.Builder after, String indexName)
    {
        assertTrue(after.copy().build().indexWasUpdated(before.copy().build(), indexName));
    }

    private void assertIndexNotUpdated(SSTableIndexComponentsState.Builder before, SSTableIndexComponentsState.Builder after, String indexName)
    {
        assertFalse(after.copy().build().indexWasUpdated(before.copy().build(), indexName));
    }

    @Test
    public void includedIndexTest()
    {
        assertEquals(SSTableIndexComponentsState.builder()
                                                .addPerSSTable(Version.DB, 0, 1)
                                                .addPerIndex("index1", Version.DB, 0, 1)
                                                .addPerIndex("index2", Version.DB, 0, 1)
                     .build()
                     .includedIndexes(),
                     Set.of("index1", "index2"));
    }

    @Test
    public void diffToStringTest()
    {
        var before = SSTableIndexComponentsState.builder()
                                                .addPerSSTable(Version.EB, 0, 10)
                                                .addPerIndex("index1", Version.DB, 0, 1)
                                                .build();

        var after = before.unbuild()
                          .removePerSSTable()
                          .addPerIndex("index2", Version.EB, 1, 4)
                          .build();

        var diff = after.diff(before);
        // The details of that string are not that important, but just making sure it looks reasonable.
        assertEquals("{<shared>: eb@0 (10MB), index1: db@0 (1MB)} -> {index1: db@0 (1MB), index2: eb@1 (4MB)} (-<shared> +index2)", diff.toString());
    }

    @Test
    public void diffNoConcurrentModificationTest()
    {
        var before = SSTableIndexComponentsState.builder()
                                                .addPerSSTable(Version.EB, 0, 1)
                                                .addPerIndex("index1", Version.DB, 0, 1)
                                                .addPerIndex("index2", Version.EB, 0, 1)
                                                .addPerIndex("index3", Version.EB, 0, 1)
                                                .addPerIndex("index4", Version.EB, 0, 1)
                                                .build();

        // Rebuilds the per-sstable and "index2" to a new generation, and rebuild "index1" but bumping to newer index
        // version. "index4" is removed, but "index3" is unmodified.
        var after = SSTableIndexComponentsState.builder()
                                                .addPerSSTable(Version.EB, 1, 1)
                                                .addPerIndex("index1", Version.EB, 0, 1)
                                                .addPerIndex("index2", Version.EB, 1, 1)
                                                .addPerIndex("index3", Version.EB, 0, 1)
                                               .build();


        SSTableIndexComponentsState.Diff diff = after.diff(before);
        assertFalse(diff.isEmpty());
        assertTrue(diff.perSSTableUpdated);
        assertEquals(2, diff.perIndexesUpdated.size());
        assertTrue(diff.perIndexesUpdated.contains("index1"));
        assertTrue(diff.perIndexesUpdated.contains("index2"));
        assertEquals(1, diff.perIndexesRemoved.size());
        assertTrue(diff.perIndexesRemoved.contains("index4"));

        assertTrue(diff.createsUnusedComponents());

        assertEquals(after, before.tryApplyDiff(diff));

    }

    @Test
    public void diffConcurrentModificationTest()
    {
        var before = SSTableIndexComponentsState.builder()
                                                .addPerSSTable(Version.EB, 0, 1)
                                                .addPerIndex("index1", Version.EB, 0, 1)
                                                .addPerIndex("index2", Version.EB, 0, 1)
                                                .addPerIndex("index3", Version.EB, 0, 1)
                                                .addPerIndex("index4", Version.EB, 0, 1)
                                                .build();

        // Rebuilds "index1" and "index3" to a new generation. The rest is unmodified.
        var after = SSTableIndexComponentsState.builder()
                                               .addPerSSTable(Version.EB, 1, 1)
                                               .addPerIndex("index1", Version.EB, 1, 1)
                                               .addPerIndex("index2", Version.EB, 0, 1)
                                               .addPerIndex("index3", Version.EB, 1, 1)
                                               .addPerIndex("index4", Version.EB, 0, 1)
                                               .build();

        SSTableIndexComponentsState.Diff diff = after.diff(before);
        assertFalse(diff.isEmpty());
        assertTrue(diff.perSSTableUpdated);
        assertEquals(2, diff.perIndexesUpdated.size());
        assertTrue(diff.perIndexesUpdated.contains("index1"));
        assertTrue(diff.perIndexesUpdated.contains("index3"));
        assertEquals(0, diff.perIndexesRemoved.size());

        assertTrue(diff.createsUnusedComponents());

        // The current state has modification compared to `current`, namely: "index2" has been removed and
        // "index4" has been updated.
        var current = SSTableIndexComponentsState.builder()
                                                 .addPerSSTable(Version.EB, 0, 1)
                                                 .addPerIndex("index1", Version.EB, 0, 1)
                                                 .addPerIndex("index3", Version.EB, 0, 1)
                                                 .addPerIndex("index4", Version.EB, 1, 1)
                                                 .build();

        // The diff should apply and be the combination of all changes
        var updated = current.tryApplyDiff(diff);

        var expected = SSTableIndexComponentsState.builder()
                                                 .addPerSSTable(Version.EB, 1, 1)
                                                 .addPerIndex("index1", Version.EB, 1, 1)
                                                 .addPerIndex("index3", Version.EB, 1, 1)
                                                 .addPerIndex("index4", Version.EB, 1, 1)
                                                 .build();

        assertEquals(expected, updated);
    }

    @Test
    public void diffNoModificationTest()
    {
        var state = SSTableIndexComponentsState.builder()
                                                .addPerSSTable(Version.EB, 0, 1)
                                                .addPerIndex("index1", Version.EB, 0, 1)
                                                .addPerIndex("index2", Version.EB, 0, 1)
                                                .addPerIndex("index3", Version.EB, 0, 1)
                                                .addPerIndex("index4", Version.EB, 0, 1)
                                                .build();

        SSTableIndexComponentsState.Diff diff = state.diff(state);
        assertTrue(diff.isEmpty());
        assertFalse(diff.createsUnusedComponents());

        assertEquals(state, state.tryApplyDiff(diff));

    }

    @Test
    public void diffIncompatibleModificationTest()
    {
        var before = SSTableIndexComponentsState.builder()
                                                .addPerSSTable(Version.EB, 0, 1)
                                                .addPerIndex("index1", Version.EB, 0, 1)
                                                .addPerIndex("index2", Version.EB, 0, 1)
                                                .build();

        // Updates per-sstable and "index1".
        var after = SSTableIndexComponentsState.builder()
                                               .addPerSSTable(Version.EB, 1, 1)
                                               .addPerIndex("index1", Version.EB, 1, 1)
                                               .addPerIndex("index2", Version.EB, 0, 1)
                                               .build();

        SSTableIndexComponentsState.Diff diff = after.diff(before);

        // Concurrent modification that modifides "index2" but also "index1"
        var current = SSTableIndexComponentsState.builder()
                                               .addPerSSTable(Version.EB, 0, 1)
                                               .addPerIndex("index1", Version.EB, 1, 1)
                                               .addPerIndex("index2", Version.EB, 1, 1)
                                               .build();

        assertThrows(UnapplicableDiffException.class, () -> current.tryApplyDiff(diff));
    }

    @Test
    public void diffWithConcurrentDropTest()
    {
        var before = SSTableIndexComponentsState.builder()
                                                .addPerSSTable(Version.EB, 0, 1)
                                                .addPerIndex("index1", Version.EB, 0, 1)
                                                .addPerIndex("index2", Version.EB, 0, 1)
                                                .build();

        // Updates all indexes.
        var after = SSTableIndexComponentsState.builder()
                                               .addPerSSTable(Version.EB, 1, 1)
                                               .addPerIndex("index1", Version.EB, 1, 1)
                                               .addPerIndex("index2", Version.EB, 1, 1)
                                               .build();

        SSTableIndexComponentsState.Diff diff = after.diff(before);
        assertTrue(diff.createsUnusedComponents());

        // Concurrent modification that modifides drop "index2"
        var current = SSTableIndexComponentsState.builder()
                                                 .addPerSSTable(Version.EB, 0, 1)
                                                 .addPerIndex("index1", Version.EB, 0, 1)
                                                 .build();

        // We expect that applying the diff work, but just that "index2" has been removed.
        var expected = SSTableIndexComponentsState.builder()
                                                  .addPerSSTable(Version.EB, 1, 1)
                                                  .addPerIndex("index1", Version.EB, 1, 1)
                                                  .build();
        assertEquals(expected, current.tryApplyDiff(diff));
    }

    @Test
    public void diffWithOriginEmptyTest()
    {
        var before = SSTableIndexComponentsState.EMPTY;

        // Creates a bunch of indexes.
        var after = SSTableIndexComponentsState.builder()
                                                .addPerSSTable(Version.EB, 0, 1)
                                                .addPerIndex("index1", Version.EB, 0, 1)
                                                .addPerIndex("index2", Version.EB, 0, 1)
                                                .build();

        SSTableIndexComponentsState.Diff diff = after.diff(before);
        assertFalse(diff.createsUnusedComponents());

        assertEquals(after, before.tryApplyDiff(diff));
    }

    @Test
    public void diffWithOnlyRemove()
    {
        var before = SSTableIndexComponentsState.builder()
                                               .addPerSSTable(Version.EB, 0, 1)
                                               .addPerIndex("index1", Version.EB, 0, 1)
                                               .addPerIndex("index2", Version.EB, 0, 1)
                                               .build();

        // Same but for the drop of index1
        var after = SSTableIndexComponentsState.builder()
                                                .addPerSSTable(Version.EB, 0, 1)
                                                .addPerIndex("index2", Version.EB, 0, 1)
                                                .build();

        SSTableIndexComponentsState.Diff diff = after.diff(before);
        assertTrue(diff.createsUnusedComponents());

        assertFalse(diff.perSSTableUpdated);
        assertTrue(diff.perIndexesUpdated.isEmpty());
        assertEquals(1, diff.perIndexesRemoved.size());
        assertTrue(diff.perIndexesRemoved.contains("index1"));

        assertEquals(after, before.tryApplyDiff(diff));
    }

    @Test
    public void buildFromDescriptorTest() throws IOException
    {
        TemporaryFolder temporaryFolder = new TemporaryFolder();
        temporaryFolder.create();
        try
        {
            IndexContext idx1 = SAITester.createIndexContext("test_index1", Int32Type.instance);
            IndexContext idx2 = SAITester.createIndexContext("test_index2", UTF8Type.instance);

            Descriptor descriptor = Descriptor.fromFilename(temporaryFolder.newFolder().getAbsolutePath() + "/ca-1-bti-Data.db");

            createFakeDataFile(descriptor);
            createFakePerSSTableComponents(descriptor, SAIUtil.currentVersion(), 0, 1 * 1024 * 1024); // 1mb per file
            createFakePerIndexComponents(descriptor, idx1, SAIUtil.currentVersion(), 1, 2 * 1024 * 1024); // 2mb per file
            createFakePerIndexComponents(descriptor, idx2, Version.DB, 0, 3 * 1024 * 1024); // 3mb per file

            SSTableReader sstable = Mockito.mock(SSTableReader.class);
            Mockito.when(sstable.getDescriptor()).thenReturn(descriptor);
            SSTableIndexComponentsState discovered = IndexComponentDiscovery.instance().discoverComponents(sstable);
            assertEquals(6, discovered.perSSTable().sizeInMB);
            assertEquals(8, discovered.perIndex(idx1.getIndexName()).sizeInMB);
            assertEquals(12, discovered.perIndex(idx2.getIndexName()).sizeInMB);
            assertEquals(26, discovered.totalSizeInMB());

            IndexDescriptor indexDescriptor = loadDescriptor(descriptor, idx1, idx2);

            SSTableIndexComponentsState state = SSTableIndexComponentsState.of(indexDescriptor);
            assertFalse(state.isEmpty());

            assertEquals(Set.of(idx1.getIndexName(), idx2.getIndexName()), state.includedIndexes());

            assertEquals(SAIUtil.currentVersion(), state.perSSTable().buildId.version());
            assertEquals(0, state.perSSTable().buildId.generation());
            assertEquals(6, state.perSSTable().sizeInMB);

            assertEquals(SAIUtil.currentVersion(), state.perIndex(idx1.getIndexName()).buildId.version());
            assertEquals(1, state.perIndex(idx1.getIndexName()).buildId.generation());
            assertEquals(8, state.perIndex(idx1.getIndexName()).sizeInMB);

            assertEquals(Version.DB, state.perIndex(idx2.getIndexName()).buildId.version());
            assertEquals(0, state.perIndex(idx2.getIndexName()).buildId.generation());
            assertEquals(12, state.perIndex(idx2.getIndexName()).sizeInMB);

            assertEquals(26, state.totalSizeInMB());
        }
        finally
        {
            temporaryFolder.delete();
        }
    }

    @Test
    public void unbuildTest()
    {
        var state1 = SSTableIndexComponentsState.builder()
                                                .addPerSSTable(Version.DC, 0, 1)
                                                .addPerIndex("index1", Version.EB, 0, 1)
                                                .addPerIndex("index2", Version.EB, 0, 1)
                                                .build();

        var state2 = state1.unbuild()
                           .addPerSSTable(Version.EB, 0, 1)
                           .addPerIndex("index3", Version.EB, 0, 1)
                           .build();


        assertEquals(Version.EB, state2.perSSTable().buildId.version());
        assertEquals(0, state2.perSSTable().buildId.generation());

        assertFalse(state1.includedIndexes().contains("index3"));
        assertTrue(state2.includedIndexes().contains("index3"));

        // Undoing the changes to state1
        var state3 = state2.unbuild()
                           .addPerSSTable(Version.DC, 0, 1)
                           .removePerIndex("index3")
                           .build();

        assertEquals(state1, state3);
    }

    @Test
    public void sizeInBytesTest()
    {
        var state1 = SSTableIndexComponentsState.builder()
                                                .addPerSSTable(Version.DC, 0, 100)
                                                .addPerIndex("index1", Version.EB, 0, 42)
                                                .addPerIndex("index2", Version.EB, 0, 220)
                                                .build();

        assertEquals(100 + 42 + 220, state1.totalSizeInMB());
    }
}
