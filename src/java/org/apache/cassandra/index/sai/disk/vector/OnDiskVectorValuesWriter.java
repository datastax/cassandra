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

import java.io.Closeable;
import java.io.IOException;

import io.github.jbellis.jvector.disk.BufferedRandomAccessWriter;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import org.apache.cassandra.io.util.File;

/**
 * Writes vectors to a file indexed by ordinal position.
 * <p>
 * This class provides efficient sequential and sparse writing of vectors to disk.
 * Vectors are stored at positions calculated as: ordinal * dimension * Float.BYTES.
 * This allows for direct random access reading by ordinal.
 * <p>
 * The writer supports:
 * - Sequential writes (ordinal increases by 1 each time)
 * - Sparse writes (ordinals can have gaps, which are left as zeros)
 * - Efficient buffering via BufferedRandomAccessWriter
 * This class is not thread-safe and should only be used within the vector index package.
 */
class OnDiskVectorValuesWriter implements Closeable
{
    private final int dimension;
    private final BufferedRandomAccessWriter bufferedWriter;
    private int lastOrdinal;

    /**
     * Creates a new writer for vectors of the specified dimension.
     *
     * @param file the file to write vectors to
     * @param dimension the dimension of vectors to be written
     * @throws IOException if an I/O error occurs
     */
    OnDiskVectorValuesWriter(File file, int dimension) throws IOException
    {
        this.bufferedWriter = new BufferedRandomAccessWriter(file.toPath());
        this.dimension = dimension;
        this.lastOrdinal = -1;
    }

    /**
     * Writes a vector at the specified ordinal position.
     * <p>
     * Ordinals must be written in increasing order. If there are gaps between ordinals,
     * the file will contain zeros at those positions.
     *
     * @param ordinal the ordinal position for this vector (must be greater than the last written ordinal)
     * @param vector the vector to write (must have the same dimension as specified in constructor)
     * @throws IOException if an I/O error occurs
     * @throws AssertionError if ordinal is not greater than the last written ordinal, or if seeking backwards
     */
    void write(int ordinal, VectorFloat<?> vector) throws IOException
    {
        assert ordinal > lastOrdinal : "Unexpected ordinal " + ordinal + " must be greater than " + lastOrdinal;
        assert vector != null : "Vector is null";
        assert vector.length() == dimension : "Incorrect vector dimension " + vector.length() + " != " + dimension;

        // We are careful to only skip or call position() when necessary because the BufferedRandomAccessWriter always
        // flushes the buffer for each of those operations. See https://github.com/datastax/jvector/issues/562.
        if (ordinal != lastOrdinal + 1)
        {
            // Skip to the correct position (ensuring that we only skip forward). Note that if the ordinal
            // is the segmentRowId and there are duplicates, we will skip some positions. This works in conjunction
            // with the posting list logic.
            long targetPosition = ordinal * Float.BYTES * (long) dimension;
            assert bufferedWriter.position() <= targetPosition : "bufferedWriter.position()=" + bufferedWriter.position() + " > targetPosition=" + targetPosition;
            bufferedWriter.seek(targetPosition);
        }

        // Update the last ordinal
        lastOrdinal = ordinal;

        // Write the vector data
        vector.writeTo(bufferedWriter);
    }

    /**
     * Returns the dimension of vectors being written.
     */
    int getDimension()
    {
        return dimension;
    }

    /**
     * Returns the last ordinal that was written, or -1 if no vectors have been written yet.
     */
    int getLastOrdinal()
    {
        return lastOrdinal;
    }

    /**
     * Returns the current position in the file.
     */
    long position() throws IOException
    {
        return bufferedWriter.position();
    }

    @Override
    public void close() throws IOException
    {
        bufferedWriter.close();
    }
}
