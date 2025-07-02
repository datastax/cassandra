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

package org.apache.cassandra.index.sai.functional;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.io.sstable.IVerifier;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class VerifyTest extends SAITester
{
    @Parameterized.Parameter
    public Version version;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        return Version.ALL.stream().map(v -> new Object[]{ v }).collect(Collectors.toList());
    }

    @Before
    public void setVersion()
    {
        SAIUtil.setCurrentVersion(version);
    }

    private static final String TABLE = "verify_test";
    private static final String INDEX_NAME = "value_idx";

    @Before
    public void setup()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, value int)");
        createIndex("CREATE CUSTOM INDEX " + INDEX_NAME + " ON %s(value) USING 'StorageAttachedIndex'");
        CompactionManager.instance.disableAutoCompaction();
    }

    @Test
    public void testVerifyCorrectSAIComponents()
    {
        // Insert data and flush to create an SSTable with SAI components
        execute("INSERT INTO %s (pk, value) VALUES (1, 100)");
        execute("INSERT INTO %s (pk, value) VALUES (2, 200)");
        execute("INSERT INTO %s (pk, value) VALUES (3, 300)");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        // Verify the SSTable with SAI components
        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().invokeDiskFailurePolicy(true).build()))
        {
            verifier.verify();
        }
        catch (CorruptSSTableException err)
        {
            fail("Unexpected CorruptSSTableException: " + err.getMessage());
        }
    }

    @Test
    public void testVerifyExtendedSAIComponents()
    {
        // Insert data and flush to create an SSTable with SAI components
        execute("INSERT INTO %s (pk, value) VALUES (1, 100)");
        execute("INSERT INTO %s (pk, value) VALUES (2, 200)");
        execute("INSERT INTO %s (pk, value) VALUES (3, 300)");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        // Verify the SSTable with extended verification
        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, 
                                             IVerifier.options()
                                                    .invokeDiskFailurePolicy(true)
                                                    .extendedVerification(true)
                                                    .build()))
        {
            verifier.verify();
        }
        catch (CorruptSSTableException err)
        {
            fail("Unexpected CorruptSSTableException: " + err.getMessage());
        }
    }

    @Test
    public void testVerifyCorruptSAIComponent() throws IOException
    {
        // Insert data and flush to create an SSTable with SAI components
        execute("INSERT INTO %s (pk, value) VALUES (1, 100)");
        execute("INSERT INTO %s (pk, value) VALUES (2, 200)");
        execute("INSERT INTO %s (pk, value) VALUES (3, 300)");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        // Get the index context for our index
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        StorageAttachedIndex index = group.getIndexes().iterator().next();
        IndexContext indexContext = getIndexContext(index);

        // Load the index descriptor
        IndexDescriptor indexDescriptor = IndexDescriptor.load(sstable, Collections.singleton(indexContext));

        // Corrupt a component file (META component)
        IndexComponentType componentToCorrupt = IndexComponentType.META;
        File fileToCorrupt = indexDescriptor.perIndexComponents(indexContext).get(componentToCorrupt).file().toJavaIOFile();

        try (RandomAccessFile file = new RandomAccessFile(fileToCorrupt, "rw"))
        {
            // Truncate the file to corrupt it
            file.setLength(3);
        }

        // Verify should detect the corruption
        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().invokeDiskFailurePolicy(true).build()))
        {
            verifier.verify();
            fail("Expected a CorruptSSTableException to be thrown");
        }
        catch (CorruptSSTableException err)
        {
            // Expected
        }
    }

    @Test
    public void testVerifyMissingCompletionMarker() throws IOException
    {
        // Insert data and flush to create an SSTable with SAI components
        execute("INSERT INTO %s (pk, value) VALUES (1, 100)");
        execute("INSERT INTO %s (pk, value) VALUES (2, 200)");
        execute("INSERT INTO %s (pk, value) VALUES (3, 300)");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        // Get the index context for our index
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        StorageAttachedIndex index = group.getIndexes().iterator().next();
        IndexContext indexContext = getIndexContext(index);

        // Load the index descriptor
        IndexDescriptor indexDescriptor = IndexDescriptor.load(sstable, Collections.singleton(indexContext));

        // Delete the completion marker
        boolean deleted = indexDescriptor.perIndexComponents(indexContext)
                                         .get(IndexComponentType.COLUMN_COMPLETION_MARKER)
                                         .file()
                                         .tryDelete();
        assertTrue("Failed to delete completion marker", deleted);

        // Verify should detect the missing completion marker
        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().invokeDiskFailurePolicy(true).build()))
        {
            verifier.verify();
            fail("Expected a CorruptSSTableException to be thrown");
        }
        catch (CorruptSSTableException err)
        {
            // Expected
        }
    }

    @Test
    public void testVerifyQuickSkipsChecksumValidation()
    {
        // Insert data and flush to create an SSTable with SAI components
        execute("INSERT INTO %s (pk, value) VALUES (1, 100)");
        execute("INSERT INTO %s (pk, value) VALUES (2, 200)");
        execute("INSERT INTO %s (pk, value) VALUES (3, 300)");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        // Verify with quick option should skip checksum validation
        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, 
                                             IVerifier.options()
                                                    .invokeDiskFailurePolicy(true)
                                                    .quick(true)
                                                    .build()))
        {
            verifier.verify();
        }
        catch (CorruptSSTableException err)
        {
            fail("Unexpected CorruptSSTableException: " + err.getMessage());
        }
    }

    @Test
    public void testVerifyAfterCompaction()
    {
        // Insert data and flush to create multiple SSTables
        execute("INSERT INTO %s (pk, value) VALUES (1, 100)");
        flush();
        execute("INSERT INTO %s (pk, value) VALUES (2, 200)");
        flush();
        execute("INSERT INTO %s (pk, value) VALUES (3, 300)");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();

        // Force compaction
        CompactionManager.instance.performMaximal(cfs, false);

        // Verify the compacted SSTable
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().invokeDiskFailurePolicy(true).build()))
        {
            verifier.verify();
        }
        catch (CorruptSSTableException err)
        {
            fail("Unexpected CorruptSSTableException: " + err.getMessage());
        }
    }

    @Test
    public void testVerifyMultipleIndices()
    {
        // Create a new table with multiple indices
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, value1 int, value2 text)");
        createIndex("CREATE CUSTOM INDEX value1_idx ON %s(value1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX value2_idx ON %s(value2) USING 'StorageAttachedIndex'");

        // Insert data and flush
        execute("INSERT INTO %s (pk, value1, value2) VALUES (1, 100, 'text1')");
        execute("INSERT INTO %s (pk, value1, value2) VALUES (2, 200, 'text2')");
        execute("INSERT INTO %s (pk, value1, value2) VALUES (3, 300, 'text3')");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        // Verify the SSTable with multiple indices
        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().invokeDiskFailurePolicy(true).build()))
        {
            verifier.verify();
        }
        catch (CorruptSSTableException err)
        {
            fail("Unexpected CorruptSSTableException: " + err.getMessage());
        }
    }

    @Test
    public void testVerifyLiteralIndex()
    {
        // Create a table with a literal (text) column
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, text_value text)");
        createIndex("CREATE CUSTOM INDEX text_idx ON %s(text_value) USING 'StorageAttachedIndex'");

        // Need to disable again because we created a new table
        CompactionManager.instance.disableAutoCompaction();

        // Insert data with text values
        execute("INSERT INTO %s (pk, text_value) VALUES (1, 'apple')");
        execute("INSERT INTO %s (pk, text_value) VALUES (2, 'banana')");
        execute("INSERT INTO %s (pk, text_value) VALUES (3, 'cherry')");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        // Verify the SSTable with literal index
        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().invokeDiskFailurePolicy(true).build()))
        {
            verifier.verify();
        }
        catch (CorruptSSTableException err)
        {
            fail("Unexpected CorruptSSTableException: " + err.getMessage());
        }
    }

    @Test
    public void testVerifyVectorIndex()
    {
        // Skip test if version doesn't support vector indexes
        if (!version.onOrAfter(Version.JVECTOR_EARLIEST))
            return;

        // Create a table with a vector column
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, vec vector<float, 3>)");
        createIndex("CREATE CUSTOM INDEX vector_idx ON %s(vec) USING 'StorageAttachedIndex'");
        CompactionManager.instance.disableAutoCompaction();

        // Insert data with vector values
        execute("INSERT INTO %s (pk, vec) VALUES (1, [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, vec) VALUES (2, [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, vec) VALUES (3, [3.0, 4.0, 5.0])");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        // Verify the SSTable with vector index
        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().invokeDiskFailurePolicy(true).build()))
        {
            verifier.verify();
        }
        catch (CorruptSSTableException err)
        {
            fail("Unexpected CorruptSSTableException: " + err.getMessage());
        }
    }

    @Test
    public void testVerifyCorruptVectorIndex() throws IOException
    {
        // Skip test if version doesn't support vector indexes
        if (!version.onOrAfter(Version.JVECTOR_EARLIEST))
            return;

        // Create a table with a vector column
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, vec vector<float, 3>)");
        createIndex("CREATE CUSTOM INDEX vector_idx ON %s(vec) USING 'StorageAttachedIndex'");
        CompactionManager.instance.disableAutoCompaction();

        // Insert data with vector values
        execute("INSERT INTO %s (pk, vec) VALUES (1, [1.0, 2.0, 3.0])");
        execute("INSERT INTO %s (pk, vec) VALUES (2, [2.0, 3.0, 4.0])");
        execute("INSERT INTO %s (pk, vec) VALUES (3, [3.0, 4.0, 5.0])");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        // Get the index context for our vector index
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        StorageAttachedIndex index = group.getIndexes().iterator().next();
        IndexContext indexContext = getIndexContext(index);

        // Load the index descriptor
        IndexDescriptor indexDescriptor = IndexDescriptor.load(sstable, Collections.singleton(indexContext));

        // Corrupt a component file
        IndexComponentType componentToCorrupt = IndexComponentType.TERMS_DATA;
        File fileToCorrupt = indexDescriptor.perIndexComponents(indexContext).get(componentToCorrupt).file().toJavaIOFile();

        try (RandomAccessFile file = new RandomAccessFile(fileToCorrupt, "rw"))
        {
            // Truncate the file to corrupt it
            file.setLength(3);
        }

        // We didn't start validating these files until version EC
        boolean verifyShouldFail = version.onOrAfter(Version.EC);
        // Verify should detect the corruption
        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().invokeDiskFailurePolicy(true).build()))
        {
            verifier.verify();
            if (verifyShouldFail)
                fail("Expected a CorruptSSTableException to be thrown");
        }
        catch (CorruptSSTableException err)
        {
            if (!verifyShouldFail)
                fail("Unexpected CorruptSSTableException: " + err.getMessage());
        }
    }

    @Test
    public void testVerifyAnalyzedTextIndex()
    {
        // Create a table with a text column and an analyzer
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, text_value text)");
        createIndex("CREATE CUSTOM INDEX analyzed_idx ON %s(text_value) USING 'StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'index_analyzer': 'standard' }");
        CompactionManager.instance.disableAutoCompaction();

        // Insert data with text values
        execute("INSERT INTO %s (pk, text_value) VALUES (1, 'The quick brown fox')");
        execute("INSERT INTO %s (pk, text_value) VALUES (2, 'jumps over the lazy dog')");
        execute("INSERT INTO %s (pk, text_value) VALUES (3, 'Lorem ipsum dolor sit amet')");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        SSTableReader sstable = cfs.getLiveSSTables().iterator().next();

        // Verify the SSTable with analyzed text index
        try (IVerifier verifier = sstable.getVerifier(cfs, new OutputHandler.LogOutput(), false, IVerifier.options().invokeDiskFailurePolicy(true).build()))
        {
            verifier.verify();
        }
        catch (CorruptSSTableException err)
        {
            fail("Unexpected CorruptSSTableException: " + err.getMessage());
        }
    }

    // Helper method to get the IndexContext from a StorageAttachedIndex
    private IndexContext getIndexContext(StorageAttachedIndex index) throws RuntimeException
    {
        try
        {
            // Use reflection to access the indexContext field
            java.lang.reflect.Field field = StorageAttachedIndex.class.getDeclaredField("indexContext");
            field.setAccessible(true);
            return (IndexContext) field.get(index);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to get IndexContext", e);
        }
    }
}
