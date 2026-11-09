/*
 * Copyright IBM Corp.
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

import com.google.common.annotations.VisibleForTesting;

import io.github.jbellis.jvector.graph.disk.OrdinalMapper;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;

/**
 * An {@link OrdinalMapper} for one source graph that keeps its mapping off the heap, in two memory-mapped files
 * that it creates, owns and deletes on {@link #close()}. Every lookup is a single mapped read; the heap holds only
 * a few scalars.
 *
 * <p>The arrays are {@link MappedArray.OfInt}s whose default element is {@link OrdinalMapper#OMITTED}, so no separate
 * encoding is needed:
 * <ul>
 *     <li>{@code oldToNew[oldOrdinal]} = {@code newOrdinal}, sized to the source's id upper bound;</li>
 *     <li>{@code newToOld[newOrdinal]} = {@code oldOrdinal}, sized to the number of merged ordinals.</li>
 * </ul>
 *
 * <p>Lookups outside the valid ranges, and {@link #oldToNew} of a node that was never assigned an ordinal, throw
 * {@link IllegalArgumentException}. The one exception is {@link #newToOld} of an in-range ordinal that this source
 * does not own, which returns {@link OrdinalMapper#OMITTED} as the interface requires: the compactor probes the
 * mappers of all sources with the same merged ordinal to find its owner.
 *
 * <p>Populate with {@link #assign} from a single thread, then hand the mapper over to readers (publication via
 * an executor or other happens-before edge is sufficient); after that it is read-only and safe to share.
 * Must not be used after {@link #close()}.
 */
public final class MappedOrdinalMapper implements OrdinalMapper, Closeable
{
    private final int idUpperBound;
    private final int ordinalCount;
    private final MappedArray.OfInt oldToNew;
    private final MappedArray.OfInt newToOld;
    private final File oldToNewFile;
    private final File newToOldFile;
    private int maxOrdinal = -1;

    /**
     * @param idUpperBound  exclusive upper bound of the source's node ids (valid range of old ordinals)
     * @param ordinalCount  number of merged ordinals (valid range of new ordinals is {@code [0, ordinalCount)})
     */
    MappedOrdinalMapper(int idUpperBound, int ordinalCount) throws IOException
    {
        this.idUpperBound = idUpperBound;
        this.ordinalCount = ordinalCount;
        this.oldToNewFile = FileUtils.createTempFile("ordinal_mapper_old_to_new", ".map");
        this.newToOldFile = FileUtils.createTempFile("ordinal_mapper_new_to_old", ".map");
        MappedArray.OfInt o2n = null;
        try
        {
            o2n = new MappedArray.OfInt(oldToNewFile.toJavaIOFile(), idUpperBound, OMITTED);
            this.newToOld = new MappedArray.OfInt(newToOldFile.toJavaIOFile(), ordinalCount, OMITTED);
            this.oldToNew = o2n;
        }
        catch (IOException | RuntimeException e)
        {
            FileUtils.closeQuietly(o2n);
            oldToNewFile.delete();
            newToOldFile.delete();
            throw e;
        }
    }

    /**
     * Maps source node {@code oldOrdinal} to merged ordinal {@code newOrdinal}. Each old and each new ordinal
     * may be assigned at most once.
     */
    void assign(int oldOrdinal, int newOrdinal)
    {
        checkOld(oldOrdinal);
        checkNew(newOrdinal);
        oldToNew.put(oldOrdinal, newOrdinal);
        newToOld.put(newOrdinal, oldOrdinal);
        maxOrdinal = Math.max(maxOrdinal, newOrdinal);
    }

    @Override
    public int maxOrdinal()
    {
        return maxOrdinal;
    }

    /** Whether {@code oldOrdinal} has been assigned a merged ordinal. */
    boolean isMapped(int oldOrdinal)
    {
        checkOld(oldOrdinal);
        return oldToNew.get(oldOrdinal) != OMITTED;
    }

    /**
     * @return the merged ordinal
     * @throws IllegalArgumentException if the ordinal is out of range or was not assigned a merged ordinal
     *                                  (i.e. the node did not survive)
     */
    @Override
    public int oldToNew(int oldOrdinal)
    {
        checkOld(oldOrdinal);
        int newOrdinal = oldToNew.get(oldOrdinal);
        if (newOrdinal == OMITTED)
            throw new IllegalArgumentException("old ordinal " + oldOrdinal + " has no merged ordinal");
        return newOrdinal;
    }

    /**
     * @return the old ordinal, or {@link OrdinalMapper#OMITTED} if the merged ordinal belongs to another source
     * @throws IllegalArgumentException if the ordinal is out of range
     */
    @Override
    public int newToOld(int newOrdinal)
    {
        checkNew(newOrdinal);
        return newToOld.get(newOrdinal);
    }

    private void checkOld(int oldOrdinal)
    {
        if (oldOrdinal < 0 || oldOrdinal >= idUpperBound)
            throw new IllegalArgumentException("old ordinal " + oldOrdinal + " outside [0, " + idUpperBound + ")");
    }

    private void checkNew(int newOrdinal)
    {
        if (newOrdinal < 0 || newOrdinal >= ordinalCount)
            throw new IllegalArgumentException("new ordinal " + newOrdinal + " outside [0, " + ordinalCount + ")");
    }

    /** Returns the temporary backing files owned by this instance; exposed for testing only. */
    @VisibleForTesting
    java.io.File[] backingFiles()
    {
        return new java.io.File[]{ oldToNewFile.toJavaIOFile(), newToOldFile.toJavaIOFile() };
    }

    /** Unmaps the arrays and deletes their backing files. */
    @Override
    public void close() throws IOException
    {
        FileUtils.closeQuietly(oldToNew, newToOld);
        oldToNewFile.delete();
        newToOldFile.delete();
    }
}
