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
import io.github.jbellis.jvector.graph.RemappedRandomAccessVectorValues;
import io.github.jbellis.jvector.util.ExplicitThreadLocal;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.roaringbitmap.RoaringBitmap;

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
 * This class is thread-safe.
 * It should only be used within the vector index package.
 */
public class OnDiskVectorValues implements RandomAccessVectorValues, AutoCloseable
{
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    // Because of the way RandomAccessVectorValues are used within jvector, this is the safest solution
    // at the moment. See https://github.com/datastax/jvector/issues/635.
    private final ExplicitThreadLocal<RandomAccessReader> threadLocalRandomAccessReader;
    private final int dimension;
    private final long vectorSize;
    private final RoaringBitmap presentOrdinals;

    /**
     * Creates a new reader for vectors of the specified dimension.
     *
     * @param file the file containing vectors written by VectorByOrdinalWriter
     * @param dimension the dimension of vectors in the file
     */
    public OnDiskVectorValues(File file, int dimension)
    {
        this(file, dimension, null);
    }

    /**
     * Creates a new reader for vectors of the specified dimension with a BitSet indicating which ordinals have vectors.
     *
     * @param file the file containing vectors written by VectorByOrdinalWriter
     * @param dimension the dimension of vectors in the file
     * @param presentOrdinals BitSet indicating which ordinals have vectors, or null if all ordinals have vectors
     */
    public OnDiskVectorValues(File file, int dimension, RoaringBitmap presentOrdinals)
    {
        this.threadLocalRandomAccessReader = ExplicitThreadLocal.withInitial(() -> RandomAccessReader.open(file));
        this.dimension = dimension;
        this.vectorSize = (long) dimension * Float.BYTES;
        this.presentOrdinals = presentOrdinals;
    }

    /**
     * Returns the total number of vectors in the file.
     * This is calculated based on the file size and vector dimension.
     */
    @Override
    public int size()
    {
        return (int) (threadLocalRandomAccessReader.get().length() / vectorSize);
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
     * @return the vector at the specified position, or null if no vector was written at this ordinal
     * @throws RuntimeException if an I/O error occurs
     */
    @Override
    public VectorFloat<?> getVector(int ordinal)
    {
        if (presentOrdinals != null && !presentOrdinals.contains(ordinal))
            return null;

        try
        {
            var reader = threadLocalRandomAccessReader.get();
            reader.seek(ordinal * vectorSize);
            return vts.readFloatVector(reader, dimension);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns false, indicating that vectors are not shared between calls to getVector.
     */
    @Override
    public boolean isValueShared()
    {
        return false;
    }

    /**
     * Returns an instance of self since the implementation is completely thread safe.
     *
     * @return self
     */
    @Override
    public RandomAccessVectorValues copy()
    {
        // The only shared state are thread local readers only used within this class, so it is safe to share them
        return this;
    }

    /**
     * Returns the underlying file being read.
     */
    File getFile()
    {
        return threadLocalRandomAccessReader.get().getFile();
    }

    /**
     * Returns the size in bytes of each vector in the file.
     */
    long getVectorSize()
    {
        return vectorSize;
    }

    /**
     * When computing the ProductQuantization, we need a representation of these elements without holes. We do
     * not care about the validity of the ordinal mapping in that case, so we simply remove the holes by either
     * returning self if the mapping is already dense or returning a {@link RemappedRandomAccessVectorValues} that uses
     * this backend internally. Both results are thread safe and both are lazy in that they leave vectors on disk until
     * fetched. The choice to defer loading of FP vectors minimizes the time vectors will be resident in memory and
     * allows for concurrent loading of vectors.
     *
     * @return a dense representation of this {@link RandomAccessVectorValues}. The resulting ordinal values are not
     * guarnateed to be valid.
     */
    RandomAccessVectorValues removeHoles()
    {
        // todo test size() here!
        // Range check on presentOrdinals is exclusive, so size() is the correct value.
        if (presentOrdinals == null || presentOrdinals.contains(0, size()))
            return this;

        // walk the on-disk Postings once to build (1) a dense list of vectors with no missing entries or zeros
        var ordinalIter = presentOrdinals.getIntIterator();

        // Because have holes in our ordinal mapping and refine assumes no holes, we cannot pass the
        // vectorValues within the refine method. Instead, we build a list of vectors here.
        var oldToNewMapping = new int[presentOrdinals.getCardinality()];
        int i = 0;
        while (ordinalIter.hasNext())
            oldToNewMapping[i++] = ordinalIter.next();

        assert i == oldToNewMapping.length : "Underfilled target array: " + i + " != " + oldToNewMapping.length;
        assert !ordinalIter.hasNext() : "ordinalIter had more elements";

        return new RemappedRandomAccessVectorValues(this, oldToNewMapping);
    }

    public void refreshReaders()
    {
        try
        {
            threadLocalRandomAccessReader.close();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close()
    {
        // Safely closes all readers, which is important because jvector doesn't handle closing them correctly
        // at the moment.
        FileUtils.closeQuietly(threadLocalRandomAccessReader);
    }
}
