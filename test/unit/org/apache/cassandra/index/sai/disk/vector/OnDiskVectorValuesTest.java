/*
 * Copyright DataStax, Inc.
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

package org.apache.cassandra.index.sai.disk.vector;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.io.util.File;
import org.roaringbitmap.RoaringBitmap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class OnDiskVectorValuesTest extends SAITester
{
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();
    private File tempFile;

    @Before
    public void setUp() throws IOException
    {
        // Need network stack to initialize buffers
        requireNetwork();
        tempFile = new File(Files.createTempFile("on-disk-vector-values-test", ".tmp"));
    }

    @After
    public void tearDown()
    {
        if (tempFile != null && tempFile.exists())
            tempFile.delete();
    }

    @Test
    public void testReadSingleVector() throws IOException
    {
        int dimension = 3;
        float[] data = {1.0f, 2.0f, 3.0f};

        // Write vector
        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            writer.write(0, vts.createFloatVector(data));
        }

        // Read vector
        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, dimension))
        {
            assertEquals(1, reader.size());
            assertEquals(dimension, reader.dimension());
            
            VectorFloat<?> vector = reader.getVector(0);
            assertVectorEquals(data, vector);
        }
    }

    @Test
    public void testReadMultipleVectors() throws IOException
    {
        int dimension = 4;
        int numVectors = 10;
        float[][] vectors = new float[numVectors][dimension];

        // Write vectors
        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            for (int i = 0; i < numVectors; i++)
            {
                for (int j = 0; j < dimension; j++)
                    vectors[i][j] = i * dimension + j;
                writer.write(i, vts.createFloatVector(vectors[i]));
            }
        }

        // Read vectors
        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, dimension))
        {
            assertEquals(numVectors, reader.size());
            assertEquals(dimension, reader.dimension());
            
            for (int i = 0; i < numVectors; i++)
            {
                VectorFloat<?> vector = reader.getVector(i);
                assertVectorEquals(vectors[i], vector);
            }
        }
    }

    @Test
    public void testReadSparseVectors() throws IOException
    {
        int dimension = 3;
        int[] ordinals = {0, 2, 5};
        float[][] vectors = {
            {1.0f, 2.0f, 3.0f},
            {4.0f, 5.0f, 6.0f},
            {7.0f, 8.0f, 9.0f}
        };

        // Write sparse vectors
        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            for (int i = 0; i < ordinals.length; i++)
                writer.write(ordinals[i], vts.createFloatVector(vectors[i]));
        }

        // Read vectors
        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, dimension))
        {
            assertEquals(6, reader.size()); // 0-5 inclusive
            
            // Check written vectors
            for (int i = 0; i < ordinals.length; i++)
            {
                VectorFloat<?> vector = reader.getVector(ordinals[i]);
                assertVectorEquals(vectors[i], vector);
            }
            
            // Check gaps are zeros
            assertVectorEquals(new float[]{0.0f, 0.0f, 0.0f}, reader.getVector(1));
            assertVectorEquals(new float[]{0.0f, 0.0f, 0.0f}, reader.getVector(3));
            assertVectorEquals(new float[]{0.0f, 0.0f, 0.0f}, reader.getVector(4));
        }
    }

    @Test
    public void testReadLargeVectors() throws IOException
    {
        int dimension = 1536;
        float[] data = new float[dimension];
        for (int i = 0; i < dimension; i++)
            data[i] = (float) Math.sin(i * 0.1);

        // Write vector
        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            writer.write(0, vts.createFloatVector(data));
        }

        // Read vector
        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, dimension))
        {
            VectorFloat<?> vector = reader.getVector(0);
            assertVectorEquals(data, vector);
        }
    }

    @Test
    public void testRandomAccess() throws IOException
    {
        int dimension = 3;
        int numVectors = 20;
        float[][] vectors = new float[numVectors][dimension];

        // Write vectors
        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            for (int i = 0; i < numVectors; i++)
            {
                for (int j = 0; j < dimension; j++)
                    vectors[i][j] = i * 10 + j;
                writer.write(i, vts.createFloatVector(vectors[i]));
            }
        }

        // Read in random order
        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, dimension))
        {
            int[] readOrder = {15, 3, 19, 0, 7, 12, 5, 18, 1, 10};
            for (int ordinal : readOrder)
            {
                VectorFloat<?> vector = reader.getVector(ordinal);
                assertVectorEquals(vectors[ordinal], vector);
            }
        }
    }

    @Test
    public void testIsValueShared()
    {
        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, 3))
        {
            assertFalse("Vectors should be shared", reader.isValueShared());
        }
    }

    @Test
    public void testCopy() throws Exception
    {
        int dimension = 4;
        float[] data = {1.0f, 2.0f, 3.0f, 4.0f};

        // Write vector
        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            writer.write(0, vts.createFloatVector(data));
        }

        // Create reader and copy
        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, dimension))
        {
            RandomAccessVectorValues copy = reader.copy();
            assertNotNull(copy);
            assertSame("Copy should be the same instance instance", reader, copy);
            
            // Both should read the same data
            assertVectorEquals(data, reader.getVector(0));
            assertVectorEquals(data, copy.getVector(0));
        }
    }

    @Test
    public void testConcurrentReads() throws Exception
    {
        int dimension = 5;
        int numVectors = 100;
        float[][] vectors = new float[numVectors][dimension];

        // Write vectors
        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            for (int i = 0; i < numVectors; i++)
            {
                for (int j = 0; j < dimension; j++)
                    vectors[i][j] = i + j * 0.1f;
                writer.write(i, vts.createFloatVector(vectors[i]));
            }
        }

        // Concurrent reads using copies
        int numThreads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger errorCount = new AtomicInteger(0);
        List<Future<?>> futures = new ArrayList<>();

        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, dimension))
        {
            for (int t = 0; t < numThreads; t++)
            {
                futures.add(executor.submit(() -> {
                    try
                    {
                        startLatch.await();
                        
                        // Each thread reads all vectors
                        for (int i = 0; i < numVectors; i++)
                        {
                            VectorFloat<?> vector = reader.getVector(i);
                            for (int j = 0; j < dimension; j++)
                            {
                                float expected = i + j * 0.1f;
                                if (Math.abs(vector.get(j) - expected) > 0.0001f)
                                    errorCount.incrementAndGet();
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        errorCount.incrementAndGet();
                        throw new RuntimeException(e);
                    }
                }));
            }

            startLatch.countDown(); // Start all threads
            
            for (Future<?> future : futures)
                future.get(10, TimeUnit.SECONDS);
        }
        finally
        {
            executor.shutdown();
        }

        assertEquals("No errors should occur during concurrent reads", 0, errorCount.get());
    }

    @Test
    public void testGetFile() throws IOException
    {
        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, 3))
        {
            assertEquals(tempFile, reader.getFile());
        }
    }

    @Test
    public void testGetVectorSize()
    {
        int dimension = 128;
        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, dimension))
        {
            long expectedSize = dimension * Float.BYTES;
            assertEquals(expectedSize, reader.getVectorSize());
        }
    }

    @Test
    public void testSizeCalculation() throws IOException
    {
        int dimension = 4;
        int numVectors = 50;

        // Write vectors
        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            for (int i = 0; i < numVectors; i++)
            {
                float[] data = new float[dimension];
                for (int j = 0; j < dimension; j++)
                    data[j] = i + j;
                writer.write(i, vts.createFloatVector(data));
            }
        }

        // Verify size calculation
        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, dimension))
        {
            assertEquals(numVectors, reader.size());
            
            long fileSize = tempFile.length();
            long vectorSize = reader.getVectorSize();
            assertEquals(numVectors, fileSize / vectorSize);
        }
    }

    @Test
    public void testEmptyFile()
    {
        // Create empty file
        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, 3))
        {
            // Don't write anything
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        // Read from empty file
        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, 3))
        {
            assertEquals(0, reader.size());
        }
    }

    @Test
    public void testCloseIsIdempotent() throws IOException
    {
        int dimension = 3;
        
        // Write a vector
        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            writer.write(0, vts.createFloatVector(new float[]{1.0f, 2.0f, 3.0f}));
        }

        OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, dimension);
        reader.getVector(0); // Read something
        reader.close();
        reader.close(); // Should not throw
    }

    @Test
    public void testMultipleCopiesIndependent() throws Exception
    {
        int dimension = 3;
        int numVectors = 10;

        // Write vectors
        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            for (int i = 0; i < numVectors; i++)
            {
                float[] data = {i, i + 1, i + 2};
                writer.write(i, vts.createFloatVector(data));
            }
        }

        // Create multiple copies and verify they work independently
        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, dimension))
        {
            RandomAccessVectorValues copy1 = reader.copy();
            RandomAccessVectorValues copy2 = reader.copy();

            try
            {
                // Read different vectors from each
                VectorFloat<?> v1 = copy1.getVector(0);
                VectorFloat<?> v2 = copy2.getVector(5);
                VectorFloat<?> v3 = reader.getVector(9);

                assertVectorEquals(new float[]{0, 1, 2}, v1);
                assertVectorEquals(new float[]{5, 6, 7}, v2);
                assertVectorEquals(new float[]{9, 10, 11}, v3);

                // Verify they can still read independently
                v1 = copy1.getVector(3);
                v2 = copy2.getVector(7);
                
                assertVectorEquals(new float[]{3, 4, 5}, v1);
                assertVectorEquals(new float[]{7, 8, 9}, v2);
            }
            finally
            {
                if (copy1 instanceof AutoCloseable)
                    ((AutoCloseable) copy1).close();
                if (copy2 instanceof AutoCloseable)
                    ((AutoCloseable) copy2).close();
            }
        }
    }

    @Test
    public void testReadBoundaryVectors() throws IOException
    {
        int dimension = 4;
        int numVectors = 100;

        // Write vectors
        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            for (int i = 0; i < numVectors; i++)
            {
                float[] data = new float[dimension];
                for (int j = 0; j < dimension; j++)
                    data[j] = i * dimension + j;
                writer.write(i, vts.createFloatVector(data));
            }
        }

        // Read boundary vectors
        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, dimension))
        {
            // First vector
            VectorFloat<?> first = reader.getVector(0);
            assertVectorEquals(new float[]{0, 1, 2, 3}, first);

            // Last vector
            VectorFloat<?> last = reader.getVector(numVectors - 1);
            float[] expectedLast = new float[dimension];
            for (int j = 0; j < dimension; j++)
                expectedLast[j] = (numVectors - 1) * dimension + j;
            assertVectorEquals(expectedLast, last);
        }
    }

    private void assertVectorEquals(float[] expected, VectorFloat<?> actual)
    {
        assertEquals("Vector dimension mismatch", expected.length, actual.length());
        for (int i = 0; i < expected.length; i++)
        {
            assertEquals("Mismatch at index " + i, expected[i], actual.get(i), 0.0001f);
        }
    }

    @Test
    public void testPresentOrdinalsWithGetVector() throws IOException
    {
        int dimension = 3;
        int numVectors = 10;
        float[][] vectors = new float[numVectors][dimension];

        // Write vectors at all ordinals
        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            for (int i = 0; i < numVectors; i++)
            {
                for (int j = 0; j < dimension; j++)
                    vectors[i][j] = i * dimension + j;
                writer.write(i, vts.createFloatVector(vectors[i]));
            }
        }

        // Create bitmap with only some ordinals present
        RoaringBitmap presentOrdinals = new RoaringBitmap();
        presentOrdinals.add(0);
        presentOrdinals.add(2);
        presentOrdinals.add(5);
        presentOrdinals.add(9);

        // Read with presentOrdinals bitmap
        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, dimension, presentOrdinals))
        {
            assertEquals(numVectors, reader.size());
            
            // Present ordinals should return vectors
            assertNotNull(reader.getVector(0));
            assertVectorEquals(vectors[0], reader.getVector(0));
            assertNotNull(reader.getVector(2));
            assertVectorEquals(vectors[2], reader.getVector(2));
            assertNotNull(reader.getVector(5));
            assertVectorEquals(vectors[5], reader.getVector(5));
            assertNotNull(reader.getVector(9));
            assertVectorEquals(vectors[9], reader.getVector(9));
            
            // Non-present ordinals should return null
            assertNull(reader.getVector(1));
            assertNull(reader.getVector(3));
            assertNull(reader.getVector(4));
            assertNull(reader.getVector(6));
            assertNull(reader.getVector(7));
            assertNull(reader.getVector(8));
        }
    }

    @Test
    public void testPresentOrdinalsEmpty() throws IOException
    {
        int dimension = 3;
        
        // Write some vectors
        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            writer.write(0, vts.createFloatVector(new float[]{1.0f, 2.0f, 3.0f}));
            writer.write(1, vts.createFloatVector(new float[]{4.0f, 5.0f, 6.0f}));
        }

        // Create empty bitmap
        RoaringBitmap presentOrdinals = new RoaringBitmap();

        // All getVector calls should return null
        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, dimension, presentOrdinals))
        {
            assertNull(reader.getVector(0));
            assertNull(reader.getVector(1));
        }
    }

    @Test
    public void testPresentOrdinalsSingleElement() throws IOException
    {
        int dimension = 4;
        float[] data = {1.0f, 2.0f, 3.0f, 4.0f};
        
        // Write multiple vectors
        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            writer.write(0, vts.createFloatVector(new float[]{0.0f, 0.0f, 0.0f, 0.0f}));
            writer.write(1, vts.createFloatVector(data));
            writer.write(2, vts.createFloatVector(new float[]{8.0f, 9.0f, 10.0f, 11.0f}));
        }

        // Bitmap with only ordinal 1
        RoaringBitmap presentOrdinals = new RoaringBitmap();
        presentOrdinals.add(1);

        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, dimension, presentOrdinals))
        {
            assertNull(reader.getVector(0));
            assertNotNull(reader.getVector(1));
            assertVectorEquals(data, reader.getVector(1));
            assertNull(reader.getVector(2));
        }
    }

    @Test
    public void testRemoveHolesWithNullBitmap() throws IOException
    {
        int dimension = 3;
        int numVectors = 5;
        
        // Write dense vectors
        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            for (int i = 0; i < numVectors; i++)
            {
                float[] data = {i, i + 1, i + 2};
                writer.write(i, vts.createFloatVector(data));
            }
        }

        // No bitmap means no holes
        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, dimension, null))
        {
            RandomAccessVectorValues result = reader.removeHoles();
            assertSame("Should return self when no holes", reader, result);
        }
    }

    @Test
    public void testRemoveHolesWithDenseBitmap() throws IOException
    {
        int dimension = 3;
        int numVectors = 5;
        
        // Write vectors
        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            for (int i = 0; i < numVectors; i++)
            {
                float[] data = {i, i + 1, i + 2};
                writer.write(i, vts.createFloatVector(data));
            }
        }

        // Dense bitmap (all ordinals present)
        RoaringBitmap presentOrdinals = new RoaringBitmap();
        for (int i = 0; i < numVectors; i++)
            presentOrdinals.add(i);

        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, dimension, presentOrdinals))
        {
            RandomAccessVectorValues result = reader.removeHoles();
            assertSame("Should return self when bitmap is dense", reader, result);
        }
    }

    @Test
    public void testRemoveHolesWithSparseOrdinals() throws IOException
    {
        int dimension = 3;
        int numVectors = 10;
        float[][] vectors = new float[numVectors][dimension];
        
        // Write vectors
        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            for (int i = 0; i < numVectors; i++)
            {
                for (int j = 0; j < dimension; j++)
                    vectors[i][j] = i * 10 + j;
                writer.write(i, vts.createFloatVector(vectors[i]));
            }
        }

        // Sparse bitmap with holes
        RoaringBitmap presentOrdinals = new RoaringBitmap();
        presentOrdinals.add(1);
        presentOrdinals.add(3);
        presentOrdinals.add(7);
        presentOrdinals.add(9);

        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, dimension, presentOrdinals))
        {
            RandomAccessVectorValues result = reader.removeHoles();
            
            // Result should be a RemappedRandomAccessVectorValues
            assertTrue("Should return RemappedRandomAccessVectorValues when there are holes", 
                       result.getClass().getName().contains("Remapped"));
            
            // Should have exactly the number of present ordinals
            assertEquals(4, result.size());
            
            // Verify remapped vectors are correct
            // Old ordinal 1 -> new ordinal 0
            assertVectorEquals(vectors[1], result.getVector(0));
            // Old ordinal 3 -> new ordinal 1
            assertVectorEquals(vectors[3], result.getVector(1));
            // Old ordinal 7 -> new ordinal 2
            assertVectorEquals(vectors[7], result.getVector(2));
            // Old ordinal 9 -> new ordinal 3
            assertVectorEquals(vectors[9], result.getVector(3));
        }
    }

    @Test
    public void testRemoveHolesWithEdgeCases() throws IOException
    {
        int dimension = 4;
        int numVectors = 20;
        
        // Write vectors
        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            for (int i = 0; i < numVectors; i++)
            {
                float[] data = new float[dimension];
                for (int j = 0; j < dimension; j++)
                    data[j] = i + j * 0.1f;
                writer.write(i, vts.createFloatVector(data));
            }
        }

        // Bitmap with first and last ordinals only
        RoaringBitmap presentOrdinals = new RoaringBitmap();
        presentOrdinals.add(0);
        presentOrdinals.add(numVectors - 1);

        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, dimension, presentOrdinals))
        {
            RandomAccessVectorValues result = reader.removeHoles();
            
            assertEquals(2, result.size());
            
            // Verify boundary vectors
            VectorFloat<?> first = result.getVector(0);
            assertNotNull(first);
            assertEquals(0.0f, first.get(0), 0.0001f);
            
            VectorFloat<?> last = result.getVector(1);
            assertNotNull(last);
            assertEquals(numVectors - 1, last.get(0), 0.0001f);
        }
    }

    @Test
    public void testRemoveHolesConsecutiveOrdinals() throws IOException
    {
        int dimension = 3;
        int numVectors = 10;
        
        // Write vectors
        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            for (int i = 0; i < numVectors; i++)
            {
                float[] data = {i, i + 1, i + 2};
                writer.write(i, vts.createFloatVector(data));
            }
        }

        // Bitmap with consecutive ordinals in the middle
        RoaringBitmap presentOrdinals = new RoaringBitmap();
        presentOrdinals.add(3);
        presentOrdinals.add(4);
        presentOrdinals.add(5);
        presentOrdinals.add(6);

        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, dimension, presentOrdinals))
        {
            RandomAccessVectorValues result = reader.removeHoles();
            
            assertEquals(4, result.size());
            
            // Verify consecutive mapping
            for (int i = 0; i < 4; i++)
            {
                VectorFloat<?> vector = result.getVector(i);
                assertNotNull(vector);
                assertEquals(3 + i, vector.get(0), 0.0001f);
            }
        }
    }

    @Test
    public void testCopyWithPresentOrdinals() throws IOException
    {
        int dimension = 3;
        float[] data = {1.0f, 2.0f, 3.0f};
        
        // Write vectors
        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            writer.write(0, vts.createFloatVector(data));
            writer.write(1, vts.createFloatVector(new float[]{4.0f, 5.0f, 6.0f}));
        }

        RoaringBitmap presentOrdinals = new RoaringBitmap();
        presentOrdinals.add(0);

        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, dimension, presentOrdinals))
        {
            RandomAccessVectorValues copy = reader.copy();
            assertSame("Copy should be same instance", reader, copy);
            
            // Both should respect presentOrdinals
            assertNotNull(reader.getVector(0));
            assertNotNull(copy.getVector(0));
            assertVectorEquals(data, reader.getVector(0));
            assertVectorEquals(data, copy.getVector(0));
            
            assertNull(reader.getVector(1));
            assertNull(copy.getVector(1));
        }
    }

    @Test
    public void testConcurrentReadsWithPresentOrdinals() throws Exception
    {
        int dimension = 4;
        int numVectors = 50;
        float[][] vectors = new float[numVectors][dimension];

        // Write vectors
        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            for (int i = 0; i < numVectors; i++)
            {
                for (int j = 0; j < dimension; j++)
                    vectors[i][j] = i + j * 0.1f;
                writer.write(i, vts.createFloatVector(vectors[i]));
            }
        }

        // Create bitmap with every other ordinal
        RoaringBitmap presentOrdinals = new RoaringBitmap();
        for (int i = 0; i < numVectors; i += 2)
            presentOrdinals.add(i);

        int numThreads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger errorCount = new AtomicInteger(0);
        List<Future<?>> futures = new ArrayList<>();

        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, dimension, presentOrdinals))
        {
            for (int t = 0; t < numThreads; t++)
            {
                futures.add(executor.submit(() -> {
                    try
                    {
                        startLatch.await();
                        
                        // Each thread reads all ordinals
                        for (int i = 0; i < numVectors; i++)
                        {
                            VectorFloat<?> vector = reader.getVector(i);
                            if (i % 2 == 0)
                            {
                                // Should be present
                                if (vector == null)
                                    errorCount.incrementAndGet();
                                else
                                {
                                    for (int j = 0; j < dimension; j++)
                                    {
                                        float expected = i + j * 0.1f;
                                        if (Math.abs(vector.get(j) - expected) > 0.0001f)
                                            errorCount.incrementAndGet();
                                    }
                                }
                            }
                            else
                            {
                                // Should be null
                                if (vector != null)
                                    errorCount.incrementAndGet();
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        errorCount.incrementAndGet();
                        throw new RuntimeException(e);
                    }
                }));
            }

            startLatch.countDown();
            
            for (Future<?> future : futures)
                future.get(10, TimeUnit.SECONDS);
        }
        finally
        {
            executor.shutdown();
        }

        assertEquals("No errors should occur during concurrent reads with presentOrdinals", 0, errorCount.get());
    }

    @Test
    public void testPresentOrdinalsWithLargeGaps() throws IOException
    {
        int dimension = 3;
        int maxOrdinal = 1000;
        
        // Write vectors at sparse ordinals
        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            for (int i = 0; i <= maxOrdinal; i++)
            {
                float[] data = {i, i + 1, i + 2};
                writer.write(i, vts.createFloatVector(data));
            }
        }

        // Bitmap with very sparse ordinals
        RoaringBitmap presentOrdinals = new RoaringBitmap();
        presentOrdinals.add(0);
        presentOrdinals.add(100);
        presentOrdinals.add(500);
        presentOrdinals.add(1000);

        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, dimension, presentOrdinals))
        {
            // Verify present ordinals
            assertNotNull(reader.getVector(0));
            assertNotNull(reader.getVector(100));
            assertNotNull(reader.getVector(500));
            assertNotNull(reader.getVector(1000));
            
            // Verify some gaps
            assertNull(reader.getVector(1));
            assertNull(reader.getVector(50));
            assertNull(reader.getVector(250));
            assertNull(reader.getVector(999));
            
            // Test removeHoles with large gaps
            RandomAccessVectorValues result = reader.removeHoles();
            assertEquals(4, result.size());
            
            // Verify remapped vectors
            VectorFloat<?> v0 = result.getVector(0);
            assertEquals(0.0f, v0.get(0), 0.0001f);
            
            VectorFloat<?> v1 = result.getVector(1);
            assertEquals(100.0f, v1.get(0), 0.0001f);
            
            VectorFloat<?> v2 = result.getVector(2);
            assertEquals(500.0f, v2.get(0), 0.0001f);
            
            VectorFloat<?> v3 = result.getVector(3);
            assertEquals(1000.0f, v3.get(0), 0.0001f);
        }
    }

    @Test
    public void testSizeWithPresentOrdinals() throws IOException
    {
        int dimension = 3;
        int numVectors = 20;
        
        // Write vectors
        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            for (int i = 0; i < numVectors; i++)
            {
                float[] data = {i, i + 1, i + 2};
                writer.write(i, vts.createFloatVector(data));
            }
        }

        // Bitmap doesn't affect size() - it's based on file size
        RoaringBitmap presentOrdinals = new RoaringBitmap();
        presentOrdinals.add(0);
        presentOrdinals.add(5);
        presentOrdinals.add(10);

        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, dimension, presentOrdinals))
        {
            // size() returns file-based size, not bitmap cardinality
            assertEquals(numVectors, reader.size());
        }
    }
}

// Made with Bob
