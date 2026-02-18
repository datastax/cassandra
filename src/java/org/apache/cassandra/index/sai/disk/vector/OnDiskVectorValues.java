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

import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;

/**
 * Reads vectors from a file indexed by ordinal position.
 * <p>
 * This class provides random access to vectors stored on disk by ordinal.
 * Vectors are expected to be stored at positions calculated as: ordinal * dimension * Float.BYTES.
 * <p>
 * The reader supports:
 * - Random access by ordinal via getVector(int)
 * - Determining the total number of vectors in the file
 * - Creating independent copies for concurrent access
 * <p>
 * This class is not thread-safe and to reduce the cost of allocations, there is a single shared vector array.
 * It should only be used within the vector index package.
 */
public class OnDiskVectorValues implements RandomAccessVectorValues, AutoCloseable
{
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    private final RandomAccessReader reader;
    private final float[] sharedVector;
    private final int dimension;
    private final long vectorSize;

    /**
     * Creates a new reader for vectors of the specified dimension.
     *
     * @param file the file containing vectors written by VectorByOrdinalWriter
     * @param dimension the dimension of vectors in the file
     */
    public OnDiskVectorValues(File file, int dimension)
    {
        this.reader = RandomAccessReader.open(file);
        this.sharedVector = new float[dimension];
        this.dimension = dimension;
        this.vectorSize = (long) dimension * Float.BYTES;
    }

    /**
     * Returns the total number of vectors in the file.
     * This is calculated based on the file size and vector dimension.
     */
    @Override
    public int size()
    {
        return (int) (reader.length() / vectorSize);
    }

    /**
     * Returns the dimension of vectors in this file.
     */
    @Override
    public int dimension()
    {
        return dimension;
    }

    /**
     * Reads and returns the vector at the specified ordinal position.
     *
     * @param ordinal the ordinal position to read from
     * @return the vector at the specified position
     * @throws RuntimeException if an I/O error occurs
     */
    @Override
    public VectorFloat<?> getVector(int ordinal)
    {
        try
        {
            reader.seek(ordinal * vectorSize);
            reader.readFully(sharedVector);
            return vts.createFloatVector(sharedVector);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns true, indicating that vectors are shared between calls to getVector.
     * Each call reuses the same vector instance.
     */
    @Override
    public boolean isValueShared()
    {
        return true;
    }

    /**
     * Creates an independent copy of this reader that can be used concurrently.
     * The copy shares the same file but has its own file position and its own shared vector.
     *
     * @return a new independent reader for the same file
     */
    @Override
    public RandomAccessVectorValues copy()
    {
        return new OnDiskVectorValues(reader.getFile(), dimension);
    }

    /**
     * Returns the underlying file being read.
     */
    File getFile()
    {
        return reader.getFile();
    }

    /**
     * Returns the size in bytes of each vector in the file.
     */
    long getVectorSize()
    {
        return vectorSize;
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(reader);
    }
}
