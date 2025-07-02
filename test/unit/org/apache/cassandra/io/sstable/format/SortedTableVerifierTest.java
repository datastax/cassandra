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

package org.apache.cassandra.io.sstable.format;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.util.Collections;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.IVerifier;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.OutputHandler;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SortedTableVerifierTest
{
    private static final String KEYSPACE = "SortedTableVerifierTest";
    private static final String TABLE = "test";

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, TABLE));
    }

    @Before
    public void setup()
    {
        Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).truncateBlocking();
    }

    /**
     * Utility method to create a verifier and invoke the protected verifyStorageAttachedIndexes method
     * 
     * @param sstable The SSTable to verify
     * @param cfs The ColumnFamilyStore (can be a spy/mock)
     * @param isOffline Whether the verifier is in offline mode
     * @param options The verifier options
     * @return The verifier instance (for additional verification if needed)
     */
    private SortedTableVerifier<?> createVerifierAndInvokeMethod(SSTableReader sstable, 
                                                                  ColumnFamilyStore cfs,
                                                                  boolean isOffline,
                                                                  IVerifier.Options options) throws Exception
    {
        OutputHandler outputHandler = new OutputHandler.LogOutput();
        SortedTableVerifier<?> verifier = (SortedTableVerifier<?>) sstable.getVerifier(cfs, outputHandler, isOffline, options);
        
        invokeVerifyStorageAttachedIndexes(verifier);
        
        return verifier;
    }
    
    /**
     * Overloaded version for tests that need a custom OutputHandler
     */
    private SortedTableVerifier<?> createVerifierAndInvokeMethod(SSTableReader sstable,
                                                                  ColumnFamilyStore cfs,
                                                                  OutputHandler outputHandler,
                                                                  boolean isOffline,
                                                                  IVerifier.Options options) throws Exception
    {
        SortedTableVerifier<?> verifier = (SortedTableVerifier<?>) sstable.getVerifier(cfs, outputHandler, isOffline, options);
        
        invokeVerifyStorageAttachedIndexes(verifier);
        
        return verifier;
    }
    
    /**
     * Helper method to invoke the protected verifyStorageAttachedIndexes method via reflection
     */
    private static void invokeVerifyStorageAttachedIndexes(SortedTableVerifier<?> verifier) throws Exception
    {
        Method method = SortedTableVerifier.class.getDeclaredMethod("verifyStorageAttachedIndexes");
        method.setAccessible(true);
        method.invoke(verifier);
    }

    @Test
    public void testVerifyStorageAttachedIndexesWithNoRealm() throws Exception
    {
        // Test when realm is null - should return early without validation
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            IVerifier.Options options = IVerifier.options().invokeDiskFailurePolicy(false).build();
            
            try (SortedTableVerifier<?> verifier = createVerifierAndInvokeMethod(sstable, null, false, options))
            {
                // Should return early without throwing
            }
        }
    }

    @Test
    public void testVerifyStorageAttachedIndexesOffline() throws Exception
    {
        // Test when isOffline is true - should return early without validation
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            IVerifier.Options options = IVerifier.options().invokeDiskFailurePolicy(false).build();
            
            try (SortedTableVerifier<?> verifier = createVerifierAndInvokeMethod(sstable, cfs, true, options))
            {
                // Should return early without throwing
            }
        }
    }

    @Test
    public void testVerifyStorageAttachedIndexesWithNoIndexManager() throws Exception
    {
        // Test when index manager is null
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        
        ColumnFamilyStore spyCfs = spy(cfs);
        when(spyCfs.getIndexManager()).thenReturn(null);
        
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            IVerifier.Options options = IVerifier.options().invokeDiskFailurePolicy(false).build();
            
            try (SortedTableVerifier<?> verifier = createVerifierAndInvokeMethod(sstable, spyCfs, false, options))
            {
                // Should log but not throw
            }
        }
    }

    @Test
    public void testVerifyStorageAttachedIndexesSuccess() throws Exception
    {
        // Test successful validation
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        
        SecondaryIndexManager mockIndexManager = mock(SecondaryIndexManager.class);
        when(mockIndexManager.validateSSTableAttachedIndexes(any(), eq(true), anyBoolean())).thenReturn(true);
        
        ColumnFamilyStore spyCfs = spy(cfs);
        when(spyCfs.getIndexManager()).thenReturn(mockIndexManager);
        
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            IVerifier.Options options = IVerifier.options().invokeDiskFailurePolicy(false).build();
            
            try (SortedTableVerifier<?> verifier = createVerifierAndInvokeMethod(sstable, spyCfs, false, options))
            {
                verify(mockIndexManager).validateSSTableAttachedIndexes(any(), eq(true), eq(true));
            }
        }
    }

    @Test
    public void testVerifyStorageAttachedIndexesQuickMode() throws Exception
    {
        // Test quick mode - should skip checksum validation
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        
        SecondaryIndexManager mockIndexManager = mock(SecondaryIndexManager.class);
        when(mockIndexManager.validateSSTableAttachedIndexes(any(), eq(true), anyBoolean())).thenReturn(true);
        
        ColumnFamilyStore spyCfs = spy(cfs);
        when(spyCfs.getIndexManager()).thenReturn(mockIndexManager);
        
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            IVerifier.Options options = IVerifier.options().invokeDiskFailurePolicy(false).quick(true).build();
            
            try (SortedTableVerifier<?> verifier = createVerifierAndInvokeMethod(sstable, spyCfs, false, options))
            {
                verify(mockIndexManager).validateSSTableAttachedIndexes(any(), eq(true), eq(false));
            }
        }
    }

    @Test
    public void testVerifyStorageAttachedIndexesValidationFails() throws Exception
    {
        // Test when validation returns false (should throw IOException)
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        
        SecondaryIndexManager mockIndexManager = mock(SecondaryIndexManager.class);
        when(mockIndexManager.validateSSTableAttachedIndexes(any(), eq(true), anyBoolean())).thenReturn(false);
        
        ColumnFamilyStore spyCfs = spy(cfs);
        when(spyCfs.getIndexManager()).thenReturn(mockIndexManager);
        
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            IVerifier.Options options = IVerifier.options().invokeDiskFailurePolicy(true).build();
            
            try
            {
                try (SortedTableVerifier<?> verifier = createVerifierAndInvokeMethod(sstable, spyCfs, false, options))
                {
                    // Should not reach here
                }
                fail("Expected CorruptSSTableException");
            }
            catch (Exception e)
            {
                // InvocationTargetException wraps the actual exception
                Throwable cause = e.getCause();
                if (cause instanceof CorruptSSTableException)
                {
                    // Expected
                }
                else
                {
                    throw e;
                }
            }
        }
    }

    @Test
    public void testVerifyStorageAttachedIndexesIllegalStateException() throws Exception
    {
        // Test when IllegalStateException is thrown (incomplete indexes)
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        
        SecondaryIndexManager mockIndexManager = mock(SecondaryIndexManager.class);
        when(mockIndexManager.validateSSTableAttachedIndexes(any(), eq(true), anyBoolean()))
            .thenThrow(new IllegalStateException("Incomplete index"));
        
        ColumnFamilyStore spyCfs = spy(cfs);
        when(spyCfs.getIndexManager()).thenReturn(mockIndexManager);
        
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            OutputHandler outputHandler = mock(OutputHandler.class);
            IVerifier.Options options = IVerifier.options().invokeDiskFailurePolicy(true).build();
            
            try (SortedTableVerifier<?> verifier = (SortedTableVerifier<?>) sstable.getVerifier(spyCfs, outputHandler, false, options))
            {
                // Use reflection to call protected method
                try
                {
                    invokeVerifyStorageAttachedIndexes(verifier);
                    fail("Expected CorruptSSTableException");
                }
                catch (Exception e)
                {
                    // InvocationTargetException wraps the actual exception
                    Throwable cause = e.getCause();
                    if (cause instanceof CorruptSSTableException)
                    {
                        verify(outputHandler).warn(any(IllegalStateException.class));
                    }
                    else
                    {
                        throw e;
                    }
                }
            }
        }
    }

    @Test
    public void testVerifyStorageAttachedIndexesUncheckedIOException() throws Exception
    {
        // Test when UncheckedIOException is thrown (checksum validation failure)
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        
        SecondaryIndexManager mockIndexManager = mock(SecondaryIndexManager.class);
        when(mockIndexManager.validateSSTableAttachedIndexes(any(), eq(true), anyBoolean()))
            .thenThrow(new UncheckedIOException(new IOException("Checksum mismatch")));
        
        ColumnFamilyStore spyCfs = spy(cfs);
        when(spyCfs.getIndexManager()).thenReturn(mockIndexManager);
        
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            OutputHandler outputHandler = mock(OutputHandler.class);
            IVerifier.Options options = IVerifier.options().invokeDiskFailurePolicy(true).build();
            
            try (SortedTableVerifier<?> verifier = (SortedTableVerifier<?>) sstable.getVerifier(spyCfs, outputHandler, false, options))
            {
                // Use reflection to call protected method
                try
                {
                    invokeVerifyStorageAttachedIndexes(verifier);
                    fail("Expected CorruptSSTableException");
                }
                catch (Exception e)
                {
                    // InvocationTargetException wraps the actual exception
                    Throwable cause = e.getCause();
                    if (cause instanceof CorruptSSTableException)
                    {
                        verify(outputHandler).warn(any(UncheckedIOException.class));
                    }
                    else
                    {
                        throw e;
                    }
                }
            }
        }
    }

    @Test  
    public void testVerifyStorageAttachedIndexesGenericThrowable() throws Exception
    {
        // Test when generic Throwable is thrown
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        
        SecondaryIndexManager mockIndexManager = mock(SecondaryIndexManager.class);
        when(mockIndexManager.validateSSTableAttachedIndexes(any(), eq(true), anyBoolean()))
            .thenThrow(new RuntimeException("Unexpected error"));
        
        ColumnFamilyStore spyCfs = spy(cfs);
        when(spyCfs.getIndexManager()).thenReturn(mockIndexManager);
        
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            OutputHandler outputHandler = mock(OutputHandler.class);
            IVerifier.Options options = IVerifier.options().invokeDiskFailurePolicy(false).build();
            
            try (SortedTableVerifier<?> verifier = (SortedTableVerifier<?>) sstable.getVerifier(spyCfs, outputHandler, false, options))
            {
                // Use reflection to call protected method
                try
                {
                    invokeVerifyStorageAttachedIndexes(verifier);
                    fail("Expected RuntimeException");
                }
                catch (Exception e)
                {
                    // InvocationTargetException wraps the actual exception
                    Throwable cause = e.getCause();
                    if (cause instanceof RuntimeException)
                    {
                        verify(outputHandler).warn(any(RuntimeException.class));
                    }
                    else
                    {
                        throw e;
                    }
                }
            }
        }
    }
}