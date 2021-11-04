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

import java.io.File;

import com.google.common.io.Files;
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
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IndexDescriptorTest
{
    private TemporaryFolder temporaryFolder = new TemporaryFolder();
    private Descriptor descriptor;

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
    }

    @After
    public void teardown()
    {
        temporaryFolder.delete();
    }

    @Test
    public void versionAAPerSSTableComponentIsParsedCorrectly() throws Throwable
    {
        Files.touch(new File(descriptor.baseFilename() + "-SAI_GroupComplete.db"));

        IndexDescriptor indexDescriptor = IndexDescriptor.create(descriptor);

        assertEquals(Version.AA, indexDescriptor.version);
        assertTrue(indexDescriptor.hasComponent(IndexComponent.GROUP_COMPLETION_MARKER));
    }

    @Test
    public void versionAAPerIndexComponentIsParsedCorrectly() throws Throwable
    {
        Files.touch(new File(descriptor.baseFilename() + "-SAI_GroupComplete.db"));
        Files.touch(new File(descriptor.baseFilename() + "-SAI_test_index_ColumnComplete.db"));

        IndexDescriptor indexDescriptor = IndexDescriptor.create(descriptor);
        IndexContext indexContext = SAITester.createIndexContext("test_index", UTF8Type.instance);

        assertEquals(Version.AA, indexDescriptor.version);
        assertTrue(indexDescriptor.hasComponent(IndexComponent.COLUMN_COMPLETION_MARKER, indexContext));
    }

    @Test
    public void allVersionAAPerSSTableComponentsAreLoaded() throws Throwable
    {
        Files.touch(new File(descriptor.baseFilename() + "-SAI_GroupComplete.db"));
        Files.touch(new File(descriptor.baseFilename() + "-SAI_GroupMeta.db"));
        Files.touch(new File(descriptor.baseFilename() + "-SAI_TokenValues.db"));
        Files.touch(new File(descriptor.baseFilename() + "-SAI_OffsetsValues.db"));

        IndexDescriptor result = IndexDescriptor.create(descriptor);

        assertTrue(result.hasComponent(IndexComponent.GROUP_COMPLETION_MARKER));
        assertTrue(result.hasComponent(IndexComponent.GROUP_META));
        assertTrue(result.hasComponent(IndexComponent.TOKEN_VALUES));
        assertTrue(result.hasComponent(IndexComponent.OFFSETS_VALUES));
    }

    @Test
    public void allVersionAAPerIndexLiteralComponentsAreLoaded() throws Throwable
    {
        Files.touch(new File(descriptor.baseFilename() + "-SAI_GroupComplete.db"));
        Files.touch(new File(descriptor.baseFilename() + "-SAI_test_index_ColumnComplete.db"));
        Files.touch(new File(descriptor.baseFilename() + "-SAI_test_index_Meta.db"));
        Files.touch(new File(descriptor.baseFilename() + "-SAI_test_index_TermsData.db"));
        Files.touch(new File(descriptor.baseFilename() + "-SAI_test_index_PostingLists.db"));


        IndexDescriptor indexDescriptor = IndexDescriptor.create(descriptor);
        IndexContext indexContext = SAITester.createIndexContext("test_index", UTF8Type.instance);

        assertTrue(indexDescriptor.hasComponent(IndexComponent.COLUMN_COMPLETION_MARKER, indexContext));
        assertTrue(indexDescriptor.hasComponent(IndexComponent.META, indexContext));
        assertTrue(indexDescriptor.hasComponent(IndexComponent.TERMS_DATA, indexContext));
        assertTrue(indexDescriptor.hasComponent(IndexComponent.POSTING_LISTS, indexContext));
    }

    @Test
    public void allVersionAAPerIndexNumericComponentsAreLoaded() throws Throwable
    {
        Files.touch(new File(descriptor.baseFilename() + "-SAI_GroupComplete.db"));
        Files.touch(new File(descriptor.baseFilename() + "-SAI_test_index_ColumnComplete.db"));
        Files.touch(new File(descriptor.baseFilename() + "-SAI_test_index_Meta.db"));
        Files.touch(new File(descriptor.baseFilename() + "-SAI_test_index_KDTree.db"));
        Files.touch(new File(descriptor.baseFilename() + "-SAI_test_index_KDTreePostingLists.db"));

        IndexDescriptor indexDescriptor = IndexDescriptor.create(descriptor);
        IndexContext indexContext = SAITester.createIndexContext("test_index", Int32Type.instance);

        assertTrue(indexDescriptor.hasComponent(IndexComponent.COLUMN_COMPLETION_MARKER, indexContext));
        assertTrue(indexDescriptor.hasComponent(IndexComponent.META, indexContext));
        assertTrue(indexDescriptor.hasComponent(IndexComponent.KD_TREE, indexContext));
        assertTrue(indexDescriptor.hasComponent(IndexComponent.KD_TREE_POSTING_LISTS, indexContext));
    }
}
