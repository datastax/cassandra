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
import java.util.Map;
import java.util.Set;
import java.util.function.IntPredicate;
import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.disk.OrdinalMapper;
import io.github.jbellis.jvector.util.FixedBitSet;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import org.agrona.collections.Int2IntHashMap;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntArrayList;
import org.apache.cassandra.index.sai.disk.vector.VectorPostings;
import org.apache.cassandra.io.util.SequentialWriter;

import static java.lang.Math.max;
import static java.lang.Math.min;

public class V5VectorPostingsWriter<T>
{
    private static final Logger logger = LoggerFactory.getLogger(V5VectorPostingsWriter.class);

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
    public static double GLOBAL_HOLES_ALLOWED = 0.01;

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

    private final RemappedPostings remappedPostings;

    /**
     * If Structure is ONE_TO_MANY then extraPostings should be the rowid -> ordinal map for the "extra" rows
     * as determined by CassandraOnHeapGraph::buildOrdinalMap; otherwise it should be null
     */
    public V5VectorPostingsWriter(RemappedPostings remappedPostings)
    {
        this.remappedPostings = remappedPostings;
    }

    /**
     * This method describes the mapping done during construction of the graph so that we can easily create
     * an appropriate V5VectorPostingsWriter.  No ordinal remapping is performed because (V5) compaction writes
     * vectors to disk as they are added to the graph, so there is no opportunity to reorder the way there is
     * in a Memtable index.
     */
    public static RemappedPostings describeForCompaction(Structure structure, int graphSize, Map<VectorFloat<?>, VectorPostings.CompactionVectorPostings> postingsMap)
    {
        assert !postingsMap.isEmpty(); // flush+compact should skip writing an index component in this case

        if (structure == Structure.ONE_TO_ONE)
        {
            return new RemappedPostings(Structure.ONE_TO_ONE,
                                        graphSize - 1,
                                        graphSize - 1,
                                        null,
                                        null,
                                        new OrdinalMapper.IdentityMapper(graphSize - 1));
        }

        if (structure == Structure.ONE_TO_MANY)
        {
            // compute maxOldOrdinal, maxRow, and extraOrdinals from the postingsMap
            int maxOldOrdinal = Integer.MIN_VALUE;
            int maxRow = Integer.MIN_VALUE;
            var extraOrdinals = new Int2IntHashMap(Integer.MIN_VALUE);
            for (var entry : postingsMap.entrySet())
            {
                var postings = entry.getValue();
                int ordinal = postings.getOrdinal();

                maxOldOrdinal = Math.max(maxOldOrdinal, ordinal);
                var rowIds = postings.getRowIds();
                assert ordinal == rowIds.getInt(0); // synthetic ordinals not allowed in ONE_TO_MANY
                for (int i = 0; i < rowIds.size(); i++)
                {
                    int rowId = rowIds.getInt(i);
                    maxRow = Math.max(maxRow, rowId);
                    if (i > 0)
                        extraOrdinals.put(rowId, ordinal);
                }
            }

            var skippedOrdinals = extraOrdinals.keySet();
            return new RemappedPostings(Structure.ONE_TO_MANY,
                                        maxOldOrdinal,
                                        maxRow,
                                        null,
                                        extraOrdinals,
                                        new OmissionAwareIdentityMapper(maxOldOrdinal, skippedOrdinals::contains));
        }

        assert structure == Structure.ZERO_OR_ONE_TO_MANY : structure;
        return createGenericIdentityMapping(postingsMap);
    }

    public long writePostings(SequentialWriter writer,
                              RandomAccessVectorValues vectorValues,
                              Map<? extends VectorFloat<?>, ? extends VectorPostings<T>> postingsMap) throws IOException
    {
        var structure = remappedPostings.structure;

        writer.writeInt(MAGIC);
        writer.writeInt(structure.ordinal());
        writer.writeInt(remappedPostings.maxNewOrdinal);
        writer.writeInt(remappedPostings.maxRowId);

        if (structure == Structure.ONE_TO_ONE || remappedPostings.maxNewOrdinal < 0)
        {
            // nothing more to do
        }
        else if (structure == Structure.ONE_TO_MANY)
        {
            writeOneToManyOrdinalMapping(writer);
            writeOneToManyRowIdMapping(writer);
        }
        else
        {
            assert structure == Structure.ZERO_OR_ONE_TO_MANY;
            writeGenericOrdinalToRowIdMapping(writer, vectorValues, postingsMap);
            writeGenericRowIdMapping(writer, vectorValues, postingsMap);
        }

        return writer.position();
    }

    private void writeOneToManyOrdinalMapping(SequentialWriter writer) throws IOException
    {
        // make sure we're in the right branch
        assert !remappedPostings.extraPostings.isEmpty();

        // Create a map of (original) ordinals to their extra rowids
        var ordinalToExtraRowIds = new Int2ObjectHashMap<IntArrayList>();
        for (var entry : remappedPostings.extraPostings.entrySet()) {
            int rowId = entry.getKey();
            int ordinal = entry.getValue();
            ordinalToExtraRowIds.computeIfAbsent(ordinal, k -> new IntArrayList()).add(rowId);
        }

        // Write the ordinals and their extra rowids
        int holeCount = (int) IntStream.range(0, remappedPostings.maxNewOrdinal + 1)
                                       .map(remappedPostings.ordinalMapper::newToOld)
                                       .filter(i -> i == OrdinalMapper.OMITTED)
                                       .count();
        writer.writeInt(holeCount + ordinalToExtraRowIds.size());
        int entries = 0;
        for (int newOrdinal = 0; newOrdinal <= remappedPostings.maxNewOrdinal; newOrdinal++) {
            // write the "holes" so they are not incorrectly associated with the corresponding rowId
            int oldOrdinal = remappedPostings.ordinalMapper.newToOld(newOrdinal);
            if (oldOrdinal == OrdinalMapper.OMITTED)
            {
                writer.writeInt(newOrdinal);
                writer.writeInt(0);
                entries++;
                continue;
            }

            // write the ordinals with multiple rows
            var extraRowIds = ordinalToExtraRowIds.get(oldOrdinal);
            if (extraRowIds != null)
            {
                writer.writeInt(newOrdinal);
                writer.writeInt(extraRowIds.size());
                for (int rowId : extraRowIds) {
                    writer.writeInt(rowId);
                }
                entries++;
            }
        }
        assert entries == holeCount + ordinalToExtraRowIds.size();
    }

    private void writeOneToManyRowIdMapping(SequentialWriter writer) throws IOException
    {
        long startOffset = writer.position();

        // make sure we're in the right branch
        assert !remappedPostings.extraPostings.isEmpty();

        // sort the extra rowids.  this boxes, but there isn't a good way to avoid that
        var extraRowIds = remappedPostings.extraPostings.keySet().stream().sorted().mapToInt(i -> i).toArray();
        // only write the extra postings, everything else can be determined from those
        int lastExtraRowId = -1;
        for (int i = 0; i < extraRowIds.length; i++)
        {
            int rowId = extraRowIds[i];
            int originalOrdinal = remappedPostings.extraPostings.get(rowId);
            writer.writeInt(rowId);
            writer.writeInt(remappedPostings.ordinalMapper.oldToNew(originalOrdinal));
            // validate that we do in fact have contiguous rowids in the non-extra mapping
            assert IntStream.range(lastExtraRowId + 1, rowId)
                            .allMatch(j -> remappedPostings.ordinalMapper.newToOld(j) != OrdinalMapper.OMITTED) : "Non-contiguous rowids found in non-extra mapping";
            lastExtraRowId = rowId;
        }

        // Write the position of the beginning of rowid -> ordinals mappings to the end
        writer.writeLong(startOffset);
    }

    // VSTODO add missing row information to remapping so we don't have to go through the vectorValues again
    public void writeGenericOrdinalToRowIdMapping(SequentialWriter writer,
                                                  RandomAccessVectorValues vectorValues,
                                                  Map<? extends VectorFloat<?>, ? extends VectorPostings<T>> postingsMap) throws IOException
    {
        long ordToRowOffset = writer.getOnDiskFilePointer();

        var newToOldMapper = (IntUnaryOperator) remappedPostings.ordinalMapper::newToOld;
        int ordinalCount = remappedPostings.maxNewOrdinal + 1; // may include unmapped ordinals
        // Write the offsets of the postings for each ordinal
        var offsetsStartAt = ordToRowOffset + 8L * ordinalCount;
        var nextOffset = offsetsStartAt;
        for (var i = 0; i < ordinalCount; i++) {
            // (ordinal is implied; don't need to write it)
            writer.writeLong(nextOffset);
            int originalOrdinal = newToOldMapper.applyAsInt(i);
            int postingListSize;
            if (originalOrdinal == OrdinalMapper.OMITTED)
            {
                assert remappedPostings.structure == Structure.ONE_TO_MANY;
                postingListSize = 0;
            }
            else
            {
                var rowIds = postingsMap.get(vectorValues.getVector(originalOrdinal)).getRowIds();
                postingListSize = rowIds.size();
            }
            nextOffset += 4 + (postingListSize * 4L); // 4 bytes for size and 4 bytes for each integer in the list
        }
        assert writer.position() == offsetsStartAt : "writer.position()=" + writer.position() + " offsetsStartAt=" + offsetsStartAt;

        // Write postings lists
        for (var i = 0; i < ordinalCount; i++) {
            int originalOrdinal = newToOldMapper.applyAsInt(i);
            if (originalOrdinal == OrdinalMapper.OMITTED)
            {
                assert remappedPostings.structure == Structure.ONE_TO_MANY;
                writer.writeInt(0);
                continue;
            }
            var rowIds = postingsMap.get(vectorValues.getVector(originalOrdinal)).getRowIds();
            writer.writeInt(rowIds.size());
            for (int r = 0; r < rowIds.size(); r++)
                writer.writeInt(rowIds.getInt(r));
        }
        assert writer.position() == nextOffset;
    }

    public void writeGenericRowIdMapping(SequentialWriter writer,
                                         RandomAccessVectorValues vectorValues,
                                         Map<? extends VectorFloat<?>, ? extends VectorPostings<T>> postingsMap) throws IOException
    {
        long startOffset = writer.position();

        // Create a Map of rowId -> ordinal
        int maxRowId = -1;
        var rowIdToOrdinalMap = new Int2IntHashMap(remappedPostings.maxNewOrdinal, 0.65f, OrdinalMapper.OMITTED);
        for (int i = 0; i <= remappedPostings.maxNewOrdinal; i++) {
            int ord = remappedPostings.ordinalMapper.newToOld(i);
            var rowIds = postingsMap.get(vectorValues.getVector(ord)).getRowIds();
            for (int r = 0; r < rowIds.size(); r++)
            {
                var rowId = rowIds.getInt(r);
                rowIdToOrdinalMap.put(rowId, i);
                maxRowId = max(maxRowId, rowId);
            }
        }

        // Write rowId -> ordinal mappings, filling in missing rowIds with -1
        for (int currentRowId = 0; currentRowId <= maxRowId; currentRowId++) {
            writer.writeInt(currentRowId);
            if (rowIdToOrdinalMap.containsKey(currentRowId))
                writer.writeInt(rowIdToOrdinalMap.get(currentRowId));
            else
                writer.writeInt(-1); // no corresponding ordinal
        }

        // write the position of the beginning of rowid -> ordinals mappings to the end
        writer.writeLong(startOffset);
    }

    /**
     * RemappedPostings is a
     *   - BiMap of original vector ordinal to the first row id it is associated with
     *   - Map of row id to original vector ordinal for rows that are NOT the first row associated with their vector
     * <p>
     * Example, using digits as ordianls and letters as row ids.  Postings map contains
     * 0 -> B, C
     * 1 -> A
     * 2 -> D
     * <p>
     * The returned ordinalMap would be {0 <-> B, 1 <-> A, 2 <-> D} and the extraPostings would be {C -> 0}
     */
    public static class RemappedPostings
    {
        /** relationship of vector ordinals to row ids */
        public final Structure structure;
        /** the largest vector ordinal in the postings (inclusive) */
        public final int maxNewOrdinal;
        /** the largest rowId in the postings (inclusive) */
        public final int maxRowId;
        /** map from rowId to [original] vector ordinal */
        @Nullable
        private final Int2IntHashMap extraPostings;
        /** public api */
        public final OrdinalMapper ordinalMapper;

        /** visible for V2VectorPostingsWriter.remapPostings, everyone else should use factory methods */
        public RemappedPostings(Structure structure, int maxNewOrdinal, int maxRowId, BiMap<Integer, Integer> ordinalMap, Int2IntHashMap extraPostings, OrdinalMapper ordinalMapper)
        {
            this.structure = structure;
            this.maxNewOrdinal = maxNewOrdinal;
            this.maxRowId = maxRowId;
            this.extraPostings = extraPostings;
            this.ordinalMapper = ordinalMapper;
        }
    }

    /**
     * @see RemappedPostings
     */
    public static <T> RemappedPostings remapForMemtable(Map<VectorFloat<?>, ? extends VectorPostings<T>> postingsMap)
    {
        assert V5OnDiskFormat.writeV5VectorPostings();

        BiMap<Integer, Integer> ordinalMap = HashBiMap.create();
        Int2IntHashMap extraPostings = new Int2IntHashMap(Integer.MIN_VALUE);
        int minRow = Integer.MAX_VALUE;
        int maxRow = Integer.MIN_VALUE;
        int maxNewOrdinal = Integer.MIN_VALUE;
        int maxOldOrdinal = Integer.MIN_VALUE;
        int totalRowsAssigned = 0;

        // build the ordinalMap and extraPostings
        for (var vectorPostings : postingsMap.values())
        {
            assert !vectorPostings.isEmpty(); // deleted vectors should be cleaned out before remapping
            var a = vectorPostings.getRowIds().toIntArray();
            Arrays.sort(a);
            int rowId = a[0];
            int oldOrdinal = vectorPostings.getOrdinal();
            maxOldOrdinal = max(maxOldOrdinal, oldOrdinal);
            minRow = min(minRow, rowId);
            maxRow = max(maxRow, a[a.length - 1]);
            assert !ordinalMap.containsKey(oldOrdinal); // vector <-> ordinal should be unique
            ordinalMap.put(oldOrdinal, rowId);
            maxNewOrdinal = max(maxNewOrdinal, rowId);
            totalRowsAssigned += a.length; // all row ids should also be unique, but we can't easily check that
            if (a.length > 1)
            {
                for (int i = 1; i < a.length; i++)
                    extraPostings.put(a[i], oldOrdinal);
            }
        }
        assert totalRowsAssigned == 0 || totalRowsAssigned <= maxRow + 1: "rowids are not unique -- " + totalRowsAssigned + " >= " + maxRow;

        // derive the correct structure
        Structure structure;
        if (totalRowsAssigned > 0 && (minRow != 0 || totalRowsAssigned < maxRow + 1))
        {
            logger.debug("Not all rows are assigned vectors, cannot remap one-to-many");
            structure = Structure.ZERO_OR_ONE_TO_MANY;
        }
        else
        {
            logger.debug("Remapped postings include {} unique vectors and {} 'extra' rows sharing them", ordinalMap.size(), extraPostings.size());
            structure = extraPostings.isEmpty()
                      ? Structure.ONE_TO_ONE
                      : Structure.ONE_TO_MANY;
            // override one-to-many to generic if there are too many holes
            if (structure == Structure.ONE_TO_MANY && extraPostings.size() > max(1, GLOBAL_HOLES_ALLOWED * maxRow))
                structure = Structure.ZERO_OR_ONE_TO_MANY;
        }

        // create the mapping
        if (structure == Structure.ZERO_OR_ONE_TO_MANY)
            return createGenericRenumberedMapping(ordinalMap.keySet(), maxOldOrdinal, maxRow);
        var ordinalMapper = new BiMapMapper(maxNewOrdinal, ordinalMap);
        return new RemappedPostings(structure, maxNewOrdinal, maxRow, ordinalMap, extraPostings, ordinalMapper);
    }

    /**
     * return an exhaustive zero-to-many mapping with the live ordinals renumbered sequentially
     */
    private static RemappedPostings createGenericRenumberedMapping(Set<Integer> liveOrdinals, int maxOldOrdinal, int maxRow)
    {
        var oldToNew = new Int2IntHashMap(maxOldOrdinal, 0.65f, Integer.MIN_VALUE);
        int nextOrdinal = 0;
        for (int i = 0; i <= maxOldOrdinal; i++) {
            if (liveOrdinals.contains(i))
                oldToNew.put(i, nextOrdinal++);
        }
        return new RemappedPostings(Structure.ZERO_OR_ONE_TO_MANY,
                                    nextOrdinal - 1,
                                    maxRow,
                                    null,
                                    null,
                                    new OrdinalMapper.MapMapper(oldToNew));
    }

    /**
     * return an exhaustive zero-to-many mapping with no renumbering
     */
    public static <T> RemappedPostings createGenericIdentityMapping(Map<VectorFloat<?>, ? extends VectorPostings<T>> postingsMap)
    {
        var maxOldOrdinal = postingsMap.values().stream().mapToInt(VectorPostings::getOrdinal).max().orElseThrow();
        int maxRow = postingsMap.values().stream().flatMap(p -> p.getRowIds().stream()).mapToInt(i -> i).max().orElseThrow();
        var presentOrdinals = new FixedBitSet(maxOldOrdinal + 1);
        for (var entry : postingsMap.entrySet())
            presentOrdinals.set(entry.getValue().getOrdinal());
        return new RemappedPostings(Structure.ZERO_OR_ONE_TO_MANY,
                                    maxOldOrdinal,
                                    maxRow,
                                    null,
                                    null,
                                    new OmissionAwareIdentityMapper(maxOldOrdinal, i -> !presentOrdinals.get(i)));
    }

    public static class BiMapMapper implements OrdinalMapper
    {
        private final int maxOrdinal;
        private final BiMap<Integer, Integer> ordinalMap;

        public BiMapMapper(int maxNewOrdinal, BiMap<Integer, Integer> ordinalMap)
        {
            this.maxOrdinal = maxNewOrdinal;
            this.ordinalMap = ordinalMap;
        }

        @Override
        public int maxOrdinal()
        {
            return maxOrdinal;
        }

        @Override
        public int oldToNew(int i)
        {
            return ordinalMap.get(i);
        }

        @Override
        public int newToOld(int i)
        {
            return ordinalMap.inverse().getOrDefault(i, OMITTED);
        }
    }

    private static class OmissionAwareIdentityMapper implements OrdinalMapper
    {
        private final int maxVectorOrdinal;
        private final IntPredicate toSkip;

        public OmissionAwareIdentityMapper(int maxVectorOrdinal, IntPredicate toSkip)
        {
            this.maxVectorOrdinal = maxVectorOrdinal;
            this.toSkip = toSkip;
        }

        @Override
        public int maxOrdinal()
        {
            return maxVectorOrdinal;
        }

        @Override
        public int oldToNew(int i)
        {
            return i;
        }

        @Override
        public int newToOld(int i)
        {
            return toSkip.test(i) ? OrdinalMapper.OMITTED : i;
        }
    }
}
