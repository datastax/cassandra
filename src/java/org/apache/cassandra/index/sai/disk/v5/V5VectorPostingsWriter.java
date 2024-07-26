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
import java.util.function.IntUnaryOperator;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.disk.OrdinalMapper;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import org.agrona.collections.Int2IntHashMap;
import org.apache.cassandra.index.sai.disk.vector.VectorPostings;
import org.apache.cassandra.io.util.SequentialWriter;

import static java.lang.Math.max;
import static java.lang.Math.min;

public class V5VectorPostingsWriter<T>
{
    private static final Logger logger = LoggerFactory.getLogger(V5VectorPostingsWriter.class);

    public static int MAGIC = 0x90571265; // POSTINGS

    public enum Structure
    {
        ONE_TO_ONE,
        ONE_TO_MANY,
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

    public V5VectorPostingsWriter(boolean postingsOneToOne, int graphSize, Map<VectorFloat<?>, VectorPostings.CompactionVectorPostings> postingsMap)
    {
        if (postingsOneToOne)
        {
            remappedPostings = new RemappedPostings(Structure.ONE_TO_ONE, graphSize - 1, graphSize - 1, null, null);
        }
        else
        {
            remappedPostings = remapPostings(postingsMap);
        }
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
            // FIXME add size restriction to this branch
            // remappedPostings.extraPostings.size() <= 0.01 * remappedPostings.maxRowId
            writeNodeOrdinalToRowIdMapping(writer, vectorValues, postingsMap);
            writeOneToManyRowIdMapping(writer);
        }
        else
        {
            assert structure == Structure.ZERO_OR_ONE_TO_MANY;
            writeNodeOrdinalToRowIdMapping(writer, vectorValues, postingsMap);
            writeGenericRowIdMapping(writer, vectorValues, postingsMap);
        }

        return writer.position();
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
            for (int j = lastExtraRowId + 1; j < rowId; j++)
                assert remappedPostings.ordinalMap.inverse().containsKey(j);
            lastExtraRowId = rowId;
        }

        // Write the position of the beginning of rowid -> ordinals mappings to the end
        writer.writeLong(startOffset);
    }

    public void writeNodeOrdinalToRowIdMapping(SequentialWriter writer,
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
            int originalOrdinal;
            try
            {
                originalOrdinal = newToOldMapper.applyAsInt(i);
            }
            catch (NullPointerException e)
            {
                // no vector assigned to this ordinal (hole in one-to-many mapping from multiple rows assigned to one vector somewhere)
                // VSTODO can we avoid using exceptions for control flow here?
                assert remappedPostings.structure == Structure.ONE_TO_MANY;
                originalOrdinal = -1;
            }
            int postingListSize;
            if (originalOrdinal >= 0)
            {
                var rowIds = postingsMap.get(vectorValues.getVector(originalOrdinal)).getRowIds();
                postingListSize = rowIds.size();
            }
            else
            {
                postingListSize = 0;
            }
            nextOffset += 4 + (postingListSize * 4L); // 4 bytes for size and 4 bytes for each integer in the list
        }
        assert writer.position() == offsetsStartAt : "writer.position()=" + writer.position() + " offsetsStartAt=" + offsetsStartAt;

        // Write postings lists
        for (var i = 0; i < ordinalCount; i++) {
            int originalOrdinal;
            try
            {
                originalOrdinal = newToOldMapper.applyAsInt(i);
            }
            catch (NullPointerException e)
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
        var rowIdToOrdinalMap = new Int2IntHashMap(remappedPostings.maxNewOrdinal, 0.65f, Integer.MIN_VALUE);
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
        /** map from original vector ordinal to rowId that will be its new, remapped ordinal */
        private final BiMap<Integer, Integer> ordinalMap;
        /** map from rowId to [original] vector ordinal */
        private final Int2IntHashMap extraPostings;
        /** public api */
        public final OrdinalMapper ordinalMapper;

        public RemappedPostings(Structure structure, int maxNewOrdinal, int maxRowId, BiMap<Integer, Integer> ordinalMap, Int2IntHashMap extraPostings)
        {
            assert structure == Structure.ONE_TO_ONE || structure == Structure.ONE_TO_MANY;
            this.structure = structure;
            this.maxNewOrdinal = maxNewOrdinal;
            this.maxRowId = maxRowId;
            this.ordinalMap = ordinalMap;
            this.extraPostings = extraPostings;
            ordinalMapper = new OrdinalMapper()
            {
                @Override
                public int oldToNew(int i)
                {
                    return ordinalMap.get(i);
                }

                @Override
                public int newToOld(int i)
                {
                    return ordinalMap.inverse().get(i);
                }
            };
        }

        public RemappedPostings(int maxNewOrdinal, int maxRowId, Int2IntHashMap sequentialMap)
        {
            this.structure = Structure.ZERO_OR_ONE_TO_MANY;
            this.maxNewOrdinal = maxNewOrdinal;
            this.maxRowId = maxRowId;
            this.ordinalMap = null;
            this.extraPostings = null;
            ordinalMapper = new OrdinalMapper.MapMapper(sequentialMap);
        }
    }

    public static <T> RemappedPostings remapPostings(Map<VectorFloat<?>, ? extends VectorPostings<T>> postingsMap)
    {
        BiMap<Integer, Integer> ordinalMap = HashBiMap.create();
        Int2IntHashMap extraPostings = new Int2IntHashMap(-1);
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

        if (totalRowsAssigned > 0 && (minRow != 0 || totalRowsAssigned != maxRow + 1))
        {
            // not every row had a vector associated with it
            logger.debug("Postings are not one:many");
            return createGenericMapping(ordinalMap.keySet(), maxOldOrdinal, maxRow);
        }

        logger.debug("Remapped postings include {} unique vectors and {} 'extra' rows sharing them", ordinalMap.size(), extraPostings.size());
        var structure = extraPostings.isEmpty() ? Structure.ONE_TO_ONE : Structure.ONE_TO_MANY;
        if (structure == Structure.ONE_TO_MANY && !V5OnDiskFormat.WRITE_V5_VECTOR_POSTINGS)
            return createGenericMapping(ordinalMap.keySet(), maxOldOrdinal, maxRow);

        return new RemappedPostings(structure, maxNewOrdinal, maxRow, ordinalMap, extraPostings);
    }

    private static RemappedPostings createGenericMapping(Set<Integer> liveOrdinals, int maxOldOrdinal, int maxRow)
    {
        var sequentialMap = new Int2IntHashMap(maxOldOrdinal, 0.65f, Integer.MIN_VALUE);
        int nextOrdinal = 0;
        for (int i = 0; i <= maxOldOrdinal; i++) {
            if (liveOrdinals.contains(i))
                sequentialMap.put(i, nextOrdinal++);
        }
        return new RemappedPostings(nextOrdinal - 1, maxRow, sequentialMap);
    }
}
