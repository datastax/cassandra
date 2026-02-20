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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.github.jbellis.jvector.vector.ArrayVectorFloat;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.io.util.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OnDiskVectorValuesWriterTest extends SAITester
{
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();
    private File tempFile;

    @Before
    public void setUp() throws IOException
    {
        // Need network for the buffer initialization, not for actual network.
        requireNetwork();
        tempFile = new File(Files.createTempFile("vector-by-ordinal-test", ".tmp"));
    }

    @After
    public void tearDown()
    {
        if (tempFile != null && tempFile.exists())
            tempFile.delete();
    }

    @Test
    public void testWriteSingleVector() throws IOException
    {
        int dimension = 3;
        float[] data = { 1.0f, 2.0f, 3.0f };
        VectorFloat<?> vector = vts.createFloatVector(data);

        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            writer.write(0, vector);
            assertEquals(0, writer.getLastOrdinal());
            assertEquals(dimension, writer.getDimension());
        }

        // Verify file size
        long expectedSize = dimension * Float.BYTES;
        assertEquals(expectedSize, tempFile.length());

        // Verify content
        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, dimension))
        {
            assertEquals(1, reader.size());
            VectorFloat<?> readVector = reader.getVector(0);
            assertVectorEquals(data, readVector);
        }
    }

    @Test
    public void testWriteSequentialVectors() throws IOException
    {
        int dimension = 4;
        int numVectors = 5;

        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            for (int i = 0; i < numVectors; i++)
            {
                float[] data = new float[dimension];
                for (int j = 0; j < dimension; j++)
                    data[j] = i * dimension + j;

                VectorFloat<?> vector = vts.createFloatVector(data);
                writer.write(i, vector);
                assertEquals(i, writer.getLastOrdinal());
            }
        }

        // Verify all vectors
        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, dimension))
        {
            assertEquals(numVectors, reader.size());
            for (int i = 0; i < numVectors; i++)
            {
                float[] expected = new float[dimension];
                for (int j = 0; j < dimension; j++)
                    expected[j] = i * dimension + j;

                VectorFloat<?> readVector = reader.getVector(i);
                assertVectorEquals(expected, readVector);
            }
        }
    }

    @Test
    public void testWriteSparseVectors() throws IOException
    {
        int dimension = 3;
        int[] ordinals = { 0, 2, 5, 10 };
        float[][] vectors = {
        { 1.0f, 2.0f, 3.0f },
        { 4.0f, 5.0f, 6.0f },
        { 7.0f, 8.0f, 9.0f },
        { 10.0f, 11.0f, 12.0f }
        };

        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            for (int i = 0; i < ordinals.length; i++)
            {
                VectorFloat<?> vector = vts.createFloatVector(vectors[i]);
                writer.write(ordinals[i], vector);
            }
            assertEquals(ordinals[ordinals.length - 1], writer.getLastOrdinal());
        }

        // Verify written vectors and gaps
        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, dimension))
        {
            // Size should be based on the highest ordinal + 1
            assertEquals(11, reader.size());

            // Check written vectors
            for (int i = 0; i < ordinals.length; i++)
            {
                VectorFloat<?> readVector = reader.getVector(ordinals[i]);
                assertVectorEquals(vectors[i], readVector);
            }

            // Check gaps are zeros
            VectorFloat<?> gapVector = reader.getVector(1);
            assertVectorEquals(new float[]{ 0.0f, 0.0f, 0.0f }, gapVector);

            gapVector = reader.getVector(3);
            assertVectorEquals(new float[]{ 0.0f, 0.0f, 0.0f }, gapVector);
        }
    }

    @Test
    public void testWriteArrayVectorFloat() throws IOException
    {
        int dimension = 5;
        float[] data = { 1.5f, 2.5f, 3.5f, 4.5f, 5.5f };
        ArrayVectorFloat vector = (ArrayVectorFloat) vts.createFloatVector(data);

        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            writer.write(0, vector);
        }

        // Verify
        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, dimension))
        {
            VectorFloat<?> readVector = reader.getVector(0);
            assertVectorEquals(data, readVector);
        }
    }

    @Test
    public void testWriteLargeVectors() throws IOException
    {
        int dimension = 1536; // Common embedding dimension
        float[] data = new float[dimension];
        for (int i = 0; i < dimension; i++)
            data[i] = (float) Math.sin(i * 0.1);

        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            writer.write(0, vts.createFloatVector(data));
        }

        // Verify
        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, dimension))
        {
            VectorFloat<?> readVector = reader.getVector(0);
            assertVectorEquals(data, readVector);
        }
    }

    @Test
    public void testWriteMultipleLargeVectors() throws IOException
    {
        int dimension = 768;
        int numVectors = 100;

        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            for (int i = 0; i < numVectors; i++)
            {
                float[] data = new float[dimension];
                for (int j = 0; j < dimension; j++)
                    data[j] = (float) (i + Math.cos(j * 0.1));

                writer.write(i, vts.createFloatVector(data));
            }
        }

        // Verify file size
        long expectedSize = (long) numVectors * dimension * Float.BYTES;
        assertEquals(expectedSize, tempFile.length());

        // Spot check a few vectors
        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, dimension))
        {
            assertEquals(numVectors, reader.size());

            // Check first vector
            float[] expected = new float[dimension];
            for (int j = 0; j < dimension; j++)
                expected[j] = (float) Math.cos(j * 0.1);
            assertVectorEquals(expected, reader.getVector(0));

            // Check last vector
            expected = new float[dimension];
            for (int j = 0; j < dimension; j++)
                expected[j] = (float) (numVectors - 1 + Math.cos(j * 0.1));
            assertVectorEquals(expected, reader.getVector(numVectors - 1));
        }
    }

    @Test
    public void testWriteWithLargeGaps() throws IOException
    {
        int dimension = 3;

        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            writer.write(0, vts.createFloatVector(new float[]{ 1.0f, 2.0f, 3.0f }));
            writer.write(1000, vts.createFloatVector(new float[]{ 4.0f, 5.0f, 6.0f }));
        }

        // Verify file size accounts for the gap
        long expectedSize = 1001L * dimension * Float.BYTES;
        assertEquals(expectedSize, tempFile.length());

        try (OnDiskVectorValues reader = new OnDiskVectorValues(tempFile, dimension))
        {
            assertEquals(1001, reader.size());
            assertVectorEquals(new float[]{ 1.0f, 2.0f, 3.0f }, reader.getVector(0));
            assertVectorEquals(new float[]{ 4.0f, 5.0f, 6.0f }, reader.getVector(1000));
            assertVectorEquals(new float[]{ 0.0f, 0.0f, 0.0f }, reader.getVector(500));
        }
    }

    @Test
    public void testGetDimension() throws IOException
    {
        int dimension = 128;
        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            assertEquals(dimension, writer.getDimension());
        }
    }

    @Test
    public void testGetLastOrdinalInitialState() throws IOException
    {
        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, 3))
        {
            assertEquals(-1, writer.getLastOrdinal());
        }
    }

    @Test
    public void testPosition() throws IOException
    {
        int dimension = 4;

        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            assertEquals(0, writer.position());

            writer.write(0, vts.createFloatVector(new float[]{ 1.0f, 2.0f, 3.0f, 4.0f }));
            long expectedPosition = dimension * Float.BYTES;
            assertEquals(expectedPosition, writer.position());

            writer.write(1, vts.createFloatVector(new float[]{ 5.0f, 6.0f, 7.0f, 8.0f }));
            expectedPosition = 2L * dimension * Float.BYTES;
            assertEquals(expectedPosition, writer.position());
        }
    }

    @Test(expected = AssertionError.class)
    public void testWriteNonIncreasingOrdinalFails() throws IOException
    {
        int dimension = 3;

        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            writer.write(5, vts.createFloatVector(new float[]{ 1.0f, 2.0f, 3.0f }));
            // This should fail - ordinal must be greater than last
            writer.write(5, vts.createFloatVector(new float[]{ 4.0f, 5.0f, 6.0f }));
        }
    }

    @Test(expected = AssertionError.class)
    public void testWriteDecreasingOrdinalFails() throws IOException
    {
        int dimension = 3;

        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, dimension))
        {
            writer.write(5, vts.createFloatVector(new float[]{ 1.0f, 2.0f, 3.0f }));
            // This should fail - ordinal must be greater than last
            writer.write(3, vts.createFloatVector(new float[]{ 4.0f, 5.0f, 6.0f }));
        }
    }

    @Test
    public void testCloseIsIdempotent() throws IOException
    {
        OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, 3);
        writer.write(0, vts.createFloatVector(new float[]{ 1.0f, 2.0f, 3.0f }));
        writer.close();
        writer.close(); // Should not throw
    }

    @Test
    public void testEmptyFile() throws IOException
    {
        // Create and immediately close without writing
        try (OnDiskVectorValuesWriter writer = new OnDiskVectorValuesWriter(tempFile, 3))
        {
            assertEquals(-1, writer.getLastOrdinal());
        }

        // File should exist but be empty
        assertTrue(tempFile.exists());
        assertEquals(0, tempFile.length());
    }

    private void assertVectorEquals(float[] expected, VectorFloat<?> actual)
    {
        assertEquals("Vector dimension mismatch", expected.length, actual.length());
        for (int i = 0; i < expected.length; i++)
        {
            assertEquals("Mismatch at index " + i, expected[i], actual.get(i), 0.0001f);
        }
    }
}

// Made with Bob
