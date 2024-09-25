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

package org.apache.cassandra.index.sai.disk.v2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.IntUnaryOperator;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import org.agrona.collections.Int2IntHashMap;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter;
import org.apache.cassandra.index.sai.disk.vector.VectorPostings;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.Pair;

public class V2VectorPostingsWriter<T>
{
    // true if vectors rows are 1:1 (all vectors are associated with exactly 1 row, and each row has a non-null vector)
    private final boolean oneToOne;
    // the size of the post-cleanup graph (so NOT necessarily the same as the VectorValues size, which contains entries for obsoleted ordinals)
    private final int graphSize;
    // given a "new" ordinal (0..size), return the ordinal it corresponds to in the original graph and VectorValues
    private final IntUnaryOperator newToOldMapper;

    public V2VectorPostingsWriter(boolean oneToOne, int graphSize, IntUnaryOperator mapper) {
        this.oneToOne = oneToOne;
        this.graphSize = graphSize;
        this.newToOldMapper = mapper;
    }

    public long writePostings(SequentialWriter writer,
                              RandomAccessVectorValues vectorValues,
                              Map<? extends VectorFloat<?>, ? extends VectorPostings<T>> postingsMap,
                              Set<Integer> deletedOrdinals) throws IOException
    {
        writeDeletedOrdinals(writer, deletedOrdinals);
        writeNodeOrdinalToRowIdMapping(writer, vectorValues, postingsMap);
        writeRowIdToNodeOrdinalMapping(writer, vectorValues, postingsMap);

        return writer.position();
    }

    private void writeDeletedOrdinals(SequentialWriter writer, Set<Integer> deletedOrdinals) throws IOException
    {
        if (oneToOne) {
            assert deletedOrdinals.isEmpty();
            // -1 indicates that fast mapping of ordinal to rowId can be used
            writer.writeInt(-1);
            return;
        }

        writer.writeInt(deletedOrdinals.size());
        for (var ordinal : deletedOrdinals) {
            writer.writeInt(ordinal);
        }
    }

    public void writeNodeOrdinalToRowIdMapping(SequentialWriter writer,
                                               RandomAccessVectorValues vectorValues,
                                               Map<? extends VectorFloat<?>, ? extends VectorPostings<T>> postingsMap) throws IOException
    {
        long ordToRowOffset = writer.getOnDiskFilePointer();

        // total number of vectors
        writer.writeInt(graphSize);

        // Write the offsets of the postings for each ordinal
        var offsetsStartAt = ordToRowOffset + 4L + 8L * graphSize;
        var nextOffset = offsetsStartAt;
        for (var i = 0; i < graphSize; i++) {
            // (ordinal is implied; don't need to write it)
            writer.writeLong(nextOffset);
            int postingListSize;
            if (oneToOne)
            {
                postingListSize = 1;
            }
            else
            {
                var originalOrdinal = newToOldMapper.applyAsInt(i);
                var rowIds = postingsMap.get(vectorValues.getVector(originalOrdinal)).getRowIds();
                postingListSize = rowIds.size();
            }
            nextOffset += 4 + (postingListSize * 4L); // 4 bytes for size and 4 bytes for each integer in the list
        }
        assert writer.position() == offsetsStartAt : "writer.position()=" + writer.position() + " offsetsStartAt=" + offsetsStartAt;

        // Write postings lists
        for (var i = 0; i < graphSize; i++) {
            if (oneToOne)
            {
                writer.writeInt(1);
                writer.writeInt(i);
            }
            else
            {
                var originalOrdinal = newToOldMapper.applyAsInt(i);
                var rowIds = postingsMap.get(vectorValues.getVector(originalOrdinal)).getRowIds();
                writer.writeInt(rowIds.size());
                for (int r = 0; r < rowIds.size(); r++)
                    writer.writeInt(rowIds.getInt(r));
            }
        }
        assert writer.position() == nextOffset;
    }

    public void writeRowIdToNodeOrdinalMapping(SequentialWriter writer,
                                               RandomAccessVectorValues vectorValues,
                                               Map<? extends VectorFloat<?>, ? extends VectorPostings<T>> postingsMap) throws IOException
    {
        long startOffset = writer.position();

        if (oneToOne)
        {
            for (var i = 0; i < graphSize; i++)
            {
                writer.writeInt(i);
                writer.writeInt(i);
            }
        }
        else
        {
            // Collect all (rowId, vectorOrdinal) pairs
            List<Pair<Integer, Integer>> pairs = new ArrayList<>();
            for (var newOrdinal = 0; newOrdinal < graphSize; newOrdinal++) {
                int oldOrdinal = newToOldMapper.applyAsInt(newOrdinal);
                // if it's an on-disk Map then this is an expensive assert, only do it when in memory
                if (postingsMap instanceof ConcurrentSkipListMap)
                    assert postingsMap.get(vectorValues.getVector(oldOrdinal)).getOrdinal() == oldOrdinal;

                var rowIds = postingsMap.get(vectorValues.getVector(oldOrdinal)).getRowIds();
                for (int r = 0; r < rowIds.size(); r++)
                    pairs.add(Pair.create(rowIds.getInt(r), newOrdinal));
            }

            // Sort the pairs by rowId
            pairs.sort(Comparator.comparingInt(Pair::left));

            // Write the pairs to the file
            for (var pair : pairs) {
                writer.writeInt(pair.left);
                writer.writeInt(pair.right);
            }
        }

        // write the position of the beginning of rowid -> ordinals mappings to the end
        writer.writeLong(startOffset);
    }

    /**
     * @return a map of vector ordinal to row id and the largest rowid, or null if the vectors are not 1:1 with rows
     */
    private static <T> Pair<BiMap<Integer, Integer>, Integer> buildOrdinalMap(Map<VectorFloat<?>, ? extends VectorPostings<T>> postingsMap)
    {
        BiMap<Integer, Integer> ordinalMap = HashBiMap.create();
        int minRow = Integer.MAX_VALUE;
        int maxRow = Integer.MIN_VALUE;
        for (VectorPostings<T> vectorPostings : postingsMap.values())
        {
            if (vectorPostings.getRowIds().size() != 1)
            {
                // multiple rows associated with this vector
                return null;
            }
            int rowId = vectorPostings.getRowIds().getInt(0);
            int ordinal = vectorPostings.getOrdinal();
            minRow = Math.min(minRow, rowId);
            maxRow = Math.max(maxRow, rowId);
            assert !ordinalMap.containsKey(ordinal); // vector <-> ordinal should be unique
            ordinalMap.put(ordinal, rowId);
        }

        if (minRow != 0 || maxRow != postingsMap.values().size() - 1)
        {
            // not every row had a vector associated with it
            return null;
        }
        return Pair.create(ordinalMap, maxRow);
    }

    public static <T> V5VectorPostingsWriter.RemappedPostings remapForMemtable(Map<VectorFloat<?>, ? extends VectorPostings<T>> postingsMap,
                                                                               boolean containsDeletes)
    {
        var p = buildOrdinalMap(postingsMap);
        int maxNewOrdinal = postingsMap.size() - 1; // no in-graph deletes in v2
        if (p == null || containsDeletes)
            return V5VectorPostingsWriter.createGenericIdentityMapping(postingsMap);

        var ordinalMap = p.left;
        var maxRow = p.right;
        return new V5VectorPostingsWriter.RemappedPostings(V5VectorPostingsWriter.Structure.ONE_TO_ONE,
                                                           maxNewOrdinal,
                                                           maxRow,
                                                           ordinalMap,
                                                           new Int2IntHashMap(Integer.MIN_VALUE),
                                                           new V5VectorPostingsWriter.BiMapMapper(maxNewOrdinal, ordinalMap));
    }
}
