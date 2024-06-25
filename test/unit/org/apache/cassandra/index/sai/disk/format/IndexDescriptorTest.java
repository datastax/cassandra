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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.io.sstable.Descriptor;

import static org.apache.cassandra.index.sai.SAIUtil.setLatestVersion;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * At the time of this writing, the test of this class mostly test the "fallback-scan-disk" mode of component discovery
 * from IndexDescriptor, because they don't create a proper sstable with a TOC, they just "touch" a bunch of the
 * component files. The "normal" discovery path that uses the TOC is however effectively tested by pretty much
 * every other SAI test, so it is a reasonable way to test that fallback. Besides, the test also test the parsing of
 * of the component filename at various versions, and that code is common to both paths.
 */
public class IndexDescriptorTest
{
    private TemporaryFolder temporaryFolder = new TemporaryFolder();
    private Descriptor descriptor;
    private Version latest;

    @BeforeClass
    public static void initialise()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup() throws Throwable
    {
        temporaryFolder.create();
        descriptor = Descriptor.fromFilename(temporaryFolder.newFolder().getAbsolutePath() + "/ca-1-bti-Data.db");
        latest = Version.latest();
    }

    @After
    public void teardown() throws Throwable
    {
        setLatestVersion(latest);
        temporaryFolder.delete();
    }

    private IndexDescriptor loadDescriptor(IndexContext... contexts)
    {
        IndexDescriptor indexDescriptor = IndexDescriptor.empty(descriptor);
        indexDescriptor.reload(new HashSet<>(Arrays.asList(contexts)));
        return indexDescriptor;
    }


    @Test
    public void versionAAPerSSTableComponentIsParsedCorrectly() throws Throwable
    {
        setLatestVersion(Version.AA);

        // As mentioned in the class javadoc, we rely on the no-TOC fallback path and that only kick in if there is a
        // data file. Otherwise, it assumes the SSTable simply does not exist at all.
        createFileOnDisk("-Data.db");
        createFileOnDisk("-SAI_GroupComplete.db");

        IndexDescriptor indexDescriptor = loadDescriptor();

        assertEquals(Version.AA, indexDescriptor.perSSTableComponents().version());
        assertTrue(indexDescriptor.perSSTableComponents().has(IndexComponentType.GROUP_COMPLETION_MARKER));
    }

    @Test
    public void versionAAPerIndexComponentIsParsedCorrectly() throws Throwable
    {
        setLatestVersion(Version.AA);

        createFileOnDisk("-Data.db");
        createFileOnDisk("-SAI_GroupComplete.db");
        createFileOnDisk("-SAI_test_index_ColumnComplete.db");

        IndexContext indexContext = SAITester.createIndexContext("test_index", UTF8Type.instance);
        IndexDescriptor indexDescriptor = loadDescriptor(indexContext);

        assertEquals(Version.AA, indexDescriptor.perSSTableComponents().version());
        assertTrue(indexDescriptor.perIndexComponents(indexContext).has(IndexComponentType.COLUMN_COMPLETION_MARKER));
    }

    @Test
    public void versionBAPerSSTableComponentIsParsedCorrectly() throws Throwable
    {
        setLatestVersion(Version.BA);

        createFileOnDisk("-Data.db");
        createFileOnDisk("-SAI+ba+GroupComplete.db");

        IndexDescriptor indexDescriptor = loadDescriptor();

        assertEquals(Version.BA, indexDescriptor.perSSTableComponents().version());
        assertTrue(indexDescriptor.perSSTableComponents().has(IndexComponentType.GROUP_COMPLETION_MARKER));
    }

    @Test
    public void versionBAPerIndexComponentIsParsedCorrectly() throws Throwable
    {
        setLatestVersion(Version.BA);

        createFileOnDisk("-Data.db");
        createFileOnDisk("-SAI+ba+test_index+ColumnComplete.db");

        IndexContext indexContext = SAITester.createIndexContext("test_index", UTF8Type.instance);
        IndexDescriptor indexDescriptor = loadDescriptor(indexContext);

        assertEquals(Version.BA, indexDescriptor.perSSTableComponents().version());
        assertTrue(indexDescriptor.perIndexComponents(indexContext).has(IndexComponentType.COLUMN_COMPLETION_MARKER));
    }

    @Test
    public void allVersionAAPerSSTableComponentsAreLoaded() throws Throwable
    {
        setLatestVersion(Version.AA);

        createFileOnDisk("-Data.db");
        createFileOnDisk("-SAI_GroupComplete.db");
        createFileOnDisk("-SAI_GroupMeta.db");
        createFileOnDisk("-SAI_TokenValues.db");
        createFileOnDisk("-SAI_OffsetsValues.db");

        IndexDescriptor result = loadDescriptor();

        assertTrue(result.perSSTableComponents().has(IndexComponentType.GROUP_COMPLETION_MARKER));
        assertTrue(result.perSSTableComponents().has(IndexComponentType.GROUP_META));
        assertTrue(result.perSSTableComponents().has(IndexComponentType.TOKEN_VALUES));
        assertTrue(result.perSSTableComponents().has(IndexComponentType.OFFSETS_VALUES));
    }

    @Test
    public void allVersionAAPerIndexLiteralComponentsAreLoaded() throws Throwable
    {
        setLatestVersion(Version.AA);

        createFileOnDisk("-Data.db");
        createFileOnDisk("-SAI_GroupComplete.db");
        createFileOnDisk("-SAI_test_index_ColumnComplete.db");
        createFileOnDisk("-SAI_test_index_Meta.db");
        createFileOnDisk("-SAI_test_index_TermsData.db");
        createFileOnDisk("-SAI_test_index_PostingLists.db");


        IndexContext indexContext = SAITester.createIndexContext("test_index", UTF8Type.instance);
        IndexDescriptor indexDescriptor = loadDescriptor(indexContext);

        IndexComponents.ForRead components = indexDescriptor.perIndexComponents(indexContext);
        assertTrue(components.has(IndexComponentType.COLUMN_COMPLETION_MARKER));
        assertTrue(components.has(IndexComponentType.META));
        assertTrue(components.has(IndexComponentType.TERMS_DATA));
        assertTrue(components.has(IndexComponentType.POSTING_LISTS));
    }

    @Test
    public void allVersionAAPerIndexNumericComponentsAreLoaded() throws Throwable
    {
        setLatestVersion(Version.AA);

        createFileOnDisk("-Data.db");
        createFileOnDisk("-SAI_GroupComplete.db");
        createFileOnDisk("-SAI_test_index_ColumnComplete.db");
        createFileOnDisk("-SAI_test_index_Meta.db");
        createFileOnDisk("-SAI_test_index_KDTree.db");
        createFileOnDisk("-SAI_test_index_KDTreePostingLists.db");

        IndexContext indexContext = SAITester.createIndexContext("test_index", Int32Type.instance);
        IndexDescriptor indexDescriptor = loadDescriptor(indexContext);

        IndexComponents.ForRead components = indexDescriptor.perIndexComponents(indexContext);
        assertTrue(components.has(IndexComponentType.COLUMN_COMPLETION_MARKER));
        assertTrue(components.has(IndexComponentType.META));
        assertTrue(components.has(IndexComponentType.KD_TREE));
        assertTrue(components.has(IndexComponentType.KD_TREE_POSTING_LISTS));
    }

    @Test
    public void testReload() throws Throwable
    {
        setLatestVersion(latest);

        // We create the descriptor first, with no files, so it should initially be empty.
        IndexContext indexContext = SAITester.createIndexContext("test_index", Int32Type.instance);
        IndexDescriptor indexDescriptor = loadDescriptor(indexContext);

        assertFalse(indexDescriptor.perSSTableComponents().isComplete());
        assertFalse(indexDescriptor.perIndexComponents(indexContext).isComplete());

        // We then create the proper files and call reload
        createFileOnDisk("-Data.db");
        createFileOnDisk("-SAI_GroupComplete.db");
        createFileOnDisk("-SAI_test_index_ColumnComplete.db");
        createFileOnDisk("-SAI_test_index_Meta.db");
        createFileOnDisk("-SAI_test_index_KDTree.db");
        createFileOnDisk("-SAI_test_index_KDTreePostingLists.db");
        indexDescriptor.reload(Set.of(indexContext));

        // Both the perSSTableComponents and perIndexComponents should now be complete and the components should be present

        assertTrue(indexDescriptor.perSSTableComponents().isComplete());

        IndexComponents.ForRead components = indexDescriptor.perIndexComponents(indexContext);
        assertTrue(components.isComplete());
        assertTrue(components.has(IndexComponentType.META));
        assertTrue(components.has(IndexComponentType.KD_TREE));
        assertTrue(components.has(IndexComponentType.KD_TREE_POSTING_LISTS));
    }

    private void createFileOnDisk(String filename) throws Throwable
    {
        descriptor.baseFile().parent().resolve(descriptor.baseFile().name() + filename).createFileIfNotExists();
    }
}
