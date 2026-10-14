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

package org.apache.cassandra.index.sai.disk.v5;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.function.IntFunction;

import com.google.common.annotations.VisibleForTesting;

import org.agrona.collections.Int2IntHashMap;
import org.agrona.collections.IntArrayList;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.io.util.SequentialWriter;

import static java.lang.Math.max;

public class V5VectorPostingsWriter
{
    /**
     * Write a one-to-many mapping if the number of "holes" in the resulting ordinal sequence
     * is less than this fraction of the total rows.  Holes have two effects that make us not
     * want to overuse them:
     * (1) We read the list of rowids associated with the holes into memory
     * (2) The holes make the terms component (the vector index) less cache-efficient
     * <br>
     * In the Cohere wikipedia dataset, we observe 0.014% vectors with multiple rows, so this
     * almost two orders of magnitude higher than the observed rate of holes in the same dataset.
     */
    @VisibleForTesting
    public static double GLOBAL_HOLES_ALLOWED = CassandraRelevantProperties.SAI_VECTOR_ORDINAL_HOLE_DENSITY_LIMIT.getDouble();

    public static int MAGIC = 0x90571265; // POSTINGS

    public enum Structure
    {
        /**
         * The mapping from vector ordinals to row ids is a bijection, i.e. each vector has exactly one row associated
         * with it and each row has exactly one vector associated with it.  No additional mappings need to be written,
         * and reads can happen without consulting disk.
         */
        ONE_TO_ONE,

        /**
         * Every row has a vector and at least one vector has multiple rows. The ratio of rows without a unique vector
         * to total rows is smaller than {@link #GLOBAL_HOLES_ALLOWED}.  Only special cases (where the row id
         * cannot be mapped to the same vector ordinal) are written; since this is a small fraction of total
         * rows, these special cases are read into memory and reads can happen without consulting disk.
         * When this format is used, level 0 of the graph contains a node for every row id, however, the only nodes
         * that are actually reachable are the first node for each vector.
         */
        ONE_TO_MANY,

        /**
         * Either:
         * 1. There is at least one row without a vector, or
         * 2. The mapping would be {@link #ONE_TO_MANY}, but the ratio of rows without a unique vector to total rows is larger
         *    than {@link #GLOBAL_HOLES_ALLOWED}.
         * Explicit mappings from each row id to vector ordinal and vice versa are written.  Reads must consult disk.
         */
        ZERO_OR_ONE_TO_MANY
    }

    /** Function from vector ordinal to its rowId list. A null return is treated as an empty list (a hole). */
    private final IntFunction<IntArrayList> rowIdsOf;
    /** Largest vector ordinal, inclusive. Negative means the relation is empty. */
    private final int maxVectorOrdinal;
    
    /** Encoding structure hint. Defaults to {@link Structure#ZERO_OR_ONE_TO_MANY}, which is always correct, but may not be optimal. */
    private Structure structure = Structure.ZERO_OR_ONE_TO_MANY;
    /**
     * Largest rowId in the segment.
     * <ul>
     *   <li>{@link Structure#ZERO_OR_ONE_TO_MANY}: <b>required</b>. Cannot be derived safely because
     *       gap rows (rows with no vector) never appear in any {@link #rowIdsOf} list, yet the reader's
     *       dense rowId→ordinal table must cover them. Omitting it throws from {@link #write}.</li>
     *   <li>{@link Structure#ONE_TO_ONE} and {@link Structure#ONE_TO_MANY}: must equal
     *       {@code maxVectorOrdinal} (every row has a vector, so there are no gaps or tail rows).
     *       Defaults to {@code maxVectorOrdinal} if not supplied; throws if supplied with a different value.</li>
     * </ul>
     * {@code -1} means not supplied.
     */
    private int maxRowId = -1;
    /** When true each ordinal's rowId list is assumed to be already ascending; sorting is skipped. */
    private boolean skipSorting = false;

    /**
     * Create a writer that encodes an ordinal→rowIds relation.
     * {@code rowIdsOf(i)} returns the rowIds for ordinal {@code i}; an empty list means a hole.
     * A {@code null} result is treated the same as an empty list (a hole), so callers whose lookup
     * returns null for an absent ordinal need not map it themselves.
     * Chain optional setters before calling {@link #write}.
     *
     * @param maxVectorOrdinal largest ordinal (inclusive); negative means the relation is empty
     * @param rowIdsOf         function from ordinal to rowId list; may return null, meaning empty,
     *                         must return rowIds in ascending order if {@link #skipRowIdSorting} was called
     */
    public static V5VectorPostingsWriter create(int maxVectorOrdinal, IntFunction<IntArrayList> rowIdsOf)
    {
        return new V5VectorPostingsWriter(maxVectorOrdinal, rowIdsOf);
    }

    private V5VectorPostingsWriter(int maxVectorOrdinal, IntFunction<IntArrayList> rowIdsOf)
    {
        this.maxVectorOrdinal = maxVectorOrdinal;
        this.rowIdsOf = rowIdsOf;
    }

    /** Shared stand-in for a null {@code rowIdsOf} result. Never mutated: the writer only reads these lists. */
    private static final IntArrayList NO_ROW_IDS = new IntArrayList(0, IntArrayList.DEFAULT_NULL_VALUE);

    /**
     * The rowIds of {@code ordinal}, in ascending order; a null from {@link #rowIdsOf} is treated as an empty list,
     * without allocating. This is the only place where rowIds are sorted: unless {@link #skipRowIdSorting()} was
     * called, an unsorted list is copied and sorted (the caller's list is never mutated). Sortedness of the result
     * is always asserted, whether or not sorting is enabled.
     */
    private IntArrayList rowIdsFor(int ordinal)
    {
        IntArrayList ids = rowIdsOf.apply(ordinal);
        if (ids == null)
            return NO_ROW_IDS;
        if (!skipSorting && !isSorted(ids))
        {
            int[] sorted = ids.toIntArray();
            Arrays.sort(sorted);
            ids = new IntArrayList(sorted, sorted.length, IntArrayList.DEFAULT_NULL_VALUE);
        }
        assert isSorted(ids) : "rowIds of ordinal " + ordinal + " are not sorted: " + ids;
        return ids;
    }

    private static boolean isSorted(IntArrayList ids)
    {
        for (int j = 1; j < ids.size(); j++)
            if (ids.getInt(j - 1) > ids.getInt(j))
                return false;
        return true;
    }

    /**
     * Encoding structure. Defaults to {@link Structure#ZERO_OR_ONE_TO_MANY} (always correct).
     * Supply a tighter structure for a more compact encoding. Returns {@code this}.
     */
    public V5VectorPostingsWriter structure(Structure structure)
    {
        this.structure = structure;
        return this;
    }

    /**
     * Largest rowId in the segment.
     * <p>Required for {@link Structure#ZERO_OR_ONE_TO_MANY} — omitting it throws from {@link #write}.
     * <p>For {@link Structure#ONE_TO_ONE} and {@link Structure#ONE_TO_MANY} it must equal
     * {@code maxVectorOrdinal}; may be omitted, in which case {@code maxVectorOrdinal} is used.
     * Returns {@code this}.
     */
    public V5VectorPostingsWriter maxRowId(int maxRowId)
    {
        this.maxRowId = maxRowId;
        return this;
    }

    /**
     * Assert that every ordinal's rowId list is already ascending; skip the defensive sort.
     * Returns {@code this}.
     */
    public V5VectorPostingsWriter skipRowIdSorting()
    {
        this.skipSorting = true;
        return this;
    }

    /**
     * Encode the ordinal→rowIds relation to {@code writer} and return the writer position after
     * the last byte written.
     */
    public long write(SequentialWriter writer) throws IOException
    {
        // ---- empty case ----
        if (maxVectorOrdinal < 0)
        {
            writer.writeInt(MAGIC);
            writer.writeInt(structure.ordinal());
            writer.writeInt(maxVectorOrdinal);
            writer.writeInt(-1);
            return writer.position();
        }

        // ---- validate maxRowId
        if ((structure == Structure.ONE_TO_ONE || structure == Structure.ONE_TO_MANY) && maxRowId != -1 && maxRowId != maxVectorOrdinal)
            throw new IllegalStateException("maxRowId = " + maxRowId + " must be unset or equal to maxVectorOrdinal = " + maxVectorOrdinal + " for " + structure.name());

        if (structure == Structure.ZERO_OR_ONE_TO_MANY && maxRowId == -1)
            throw new IllegalStateException(
            "maxRowId must be supplied for ZERO_OR_ONE_TO_MANY (gap rows at the tail of the "
            + "segment are not visible in rowIdsOf and would be silently omitted)");

        // For anything other than ZERO_OR_ONE_TO_MANY we have a 1:1 between vector odinals and their primary row ids
        if (maxRowId == -1)
            maxRowId = maxVectorOrdinal;

        // ---- ONE_TO_ONE: maxRowId == maxVectorOrdinal by definition; no scan needed ----
        if (structure == Structure.ONE_TO_ONE)
        {
            writer.writeInt(MAGIC);
            writer.writeInt(Structure.ONE_TO_ONE.ordinal());
            writer.writeInt(maxVectorOrdinal);
            writer.writeInt(maxVectorOrdinal);
            return writer.position();
        }


        // ---- write header ----
        writer.writeInt(MAGIC);
        writer.writeInt(structure.ordinal());
        writer.writeInt(maxVectorOrdinal);
        writer.writeInt(maxRowId);

        // ---- write body ----
        if (structure == Structure.ONE_TO_MANY)
        {
            writeOneToManyOrdinalMapping(writer);
            writeOneToManyRowIdMapping(writer);
        }
        else
        {
            writeGenericOrdinalToRowIdMapping(writer);
            writeGenericRowIdMapping(writer);
        }

        return writer.position();
    }

    // ---- ONE_TO_MANY encoders ----

    private void writeOneToManyOrdinalMapping(SequentialWriter writer) throws IOException
    {
        // entryCount must be written before entries; collect entries first.
        // Count: holes (size==0) + multi-row ordinals (size>1).
        // Calling rowIdsOf directly (no sort needed — only sizes matter for counting).
        int entryCount = 0;
        for (int i = 0; i <= maxVectorOrdinal; i++)
        {
            IntArrayList ids = rowIdsOf.apply(i);
            int sz = ids == null ? 0 : ids.size();
            if (sz == 0 || sz > 1) entryCount++;
        }
        writer.writeInt(entryCount);

        for (int i = 0; i <= maxVectorOrdinal; i++)
        {
            IntArrayList ids = rowIdsFor(i);
            int sz = ids.size();
            if (sz == 0)
            {
                writer.writeInt(i);
                writer.writeInt(0);
            }
            else if (sz > 1)
            {
                int[] extras = extrasForOneToMany(i, ids);
                writer.writeInt(i);
                writer.writeInt(extras.length);
                for (int extra : extras)
                    writer.writeInt(extra);
            }
            // sz == 1 && primary rowId == i: implicit, nothing written
        }
    }

    /**
     * Returns extra rowIds for {@code ordinal} (everything except the primary row equal to
     * {@code ordinal}), sorted ascending. Validates the ONE_TO_MANY invariant: min(ids) == ordinal.
     */
    private int[] extrasForOneToMany(int ordinal, IntArrayList ids)
    {
        int minVal = ids.getInt(0);
        for (int j = 1; j < ids.size(); j++)
            if (ids.getInt(j) < minVal) minVal = ids.getInt(j);
        if (minVal != ordinal)
            throw new IllegalStateException("ONE_TO_MANY violated: ordinal " + ordinal
                                            + " has min rowId " + minVal + " (expected == ordinal)");
        IntArrayList extras = new IntArrayList();
        for (int j = 0; j < ids.size(); j++)
            if (ids.getInt(j) != ordinal) extras.add(ids.getInt(j));
        return extras.toIntArray(); // ids are sorted (see rowIdsFor), so extras are too
    }

    private void writeOneToManyRowIdMapping(SequentialWriter writer) throws IOException
    {
        long startOffset = writer.position();

        // Collect extra (non-primary) rowId→ordinal pairs.
        // Primary rowId of ordinal i is i itself; the reader infers those without consulting disk.
        IntArrayList extraPairs = new IntArrayList(); // interleaved: rowId, ordinal, rowId, ordinal, ...
        for (int i = 0; i <= maxVectorOrdinal; i++)
        {
            IntArrayList ids = rowIdsFor(i);
            for (int j = 0; j < ids.size(); j++)
            {
                int v = ids.getInt(j);
                if (v != i) { extraPairs.add(v); extraPairs.add(i); }
            }
        }

        int pairCount = extraPairs.size() / 2;

        // Sort the pairs by rowId so the reader's Arrays.binarySearch lookup works correctly.
        Integer[] indices = new Integer[pairCount];
        for (int k = 0; k < pairCount; k++) indices[k] = k;
        Arrays.sort(indices, Comparator.comparingInt(k -> extraPairs.getInt(k * 2)));

        for (int k = 0; k < pairCount; k++)
        {
            writer.writeInt(extraPairs.getInt(indices[k] * 2));
            writer.writeInt(extraPairs.getInt(indices[k] * 2 + 1));
        }
        writer.writeLong(startOffset);
    }

    // ---- ZERO_OR_ONE_TO_MANY encoders ----

    private void writeGenericOrdinalToRowIdMapping(SequentialWriter writer) throws IOException
    {
        long ordToRowOffset = writer.getOnDiskFilePointer();
        int ordinalCount = maxVectorOrdinal + 1;

        // Resolve all rowId lists once; reuse them for both the offset table and the data write.
        IntArrayList[] allIds = new IntArrayList[ordinalCount];
        for (int i = 0; i < ordinalCount; i++)
            allIds[i] = rowIdsFor(i);

        long nextOffset = ordToRowOffset + 8L * ordinalCount;
        for (int i = 0; i < ordinalCount; i++)
        {
            writer.writeLong(nextOffset);
            nextOffset += 4 + (allIds[i].size() * 4L);
        }

        for (int i = 0; i < ordinalCount; i++)
        {
            IntArrayList ids = allIds[i];
            writer.writeInt(ids.size());
            for (int j = 0; j < ids.size(); j++)
                writer.writeInt(ids.getInt(j));
        }
    }

    private void writeGenericRowIdMapping(SequentialWriter writer)
            throws IOException
    {
        long startOffset = writer.position();

        Int2IntHashMap rowIdToOrdinal = new Int2IntHashMap(max(maxVectorOrdinal, 1), 0.65f, -1);
        for (int i = 0; i <= maxVectorOrdinal; i++)
        {
            IntArrayList ids = rowIdsFor(i);
            for (int j = 0; j < ids.size(); j++)
                rowIdToOrdinal.put(ids.getInt(j), i);
        }
        for (int r = 0; r <= maxRowId; r++)
        {
            writer.writeInt(r);
            writer.writeInt(rowIdToOrdinal.getOrDefault(r, -1));
        }
        writer.writeLong(startOffset);
    }

    /**
     * Given the number of vectors and rows indexed, determine if there are too many holes for a one to many mapping.
     * @param totalVectorsIndexed the number of unique vectors in the index segment
     * @param totalRowsIndexed the number of rows in the index segment
     * @return true if there are too many holes, false otherwise
     */
    public static boolean tooManyOrdinalMappingHoles(int totalVectorsIndexed, int totalRowsIndexed)
    {
        return totalRowsIndexed - totalVectorsIndexed > GLOBAL_HOLES_ALLOWED * totalRowsIndexed;
    }
}
