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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.IntUnaryOperator;

import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import org.apache.cassandra.index.sai.disk.vector.VectorPostings;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.Pair;

public class V5VectorPostingsWriter<T>
{
    public static int MAGIC = 0x90571265; // POSTINGS

    public enum Structure
    {
        ONE_TO_ONE,
        ZERO_OR_ONE_TO_MANY
    }

    private final Structure structure;
    private final int graphSize;
    private final IntUnaryOperator newToOldMapper;

    public V5VectorPostingsWriter(Structure structure, int graphSize, IntUnaryOperator mapper) {
        this.structure = structure;
        this.graphSize = graphSize;
        this.newToOldMapper = mapper;
    }

    public long writePostings(SequentialWriter writer,
                              RandomAccessVectorValues vectorValues,
                              Map<? extends VectorFloat<?>, ? extends VectorPostings<T>> postingsMap) throws IOException
    {
        writer.writeInt(MAGIC);
        writer.writeInt(structure.ordinal());
        
        if (structure == Structure.ONE_TO_ONE) {
            writer.writeInt(graphSize);
        } else {
            assert graphSize > 0;
            writeNodeOrdinalToRowIdMapping(writer, vectorValues, postingsMap);
            writeRowIdToNodeOrdinalMapping(writer, vectorValues, postingsMap);
        }

        return writer.position();
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
            var originalOrdinal = newToOldMapper.applyAsInt(i);
            var rowIds = postingsMap.get(vectorValues.getVector(originalOrdinal)).getRowIds();
            postingListSize = rowIds.size();
            nextOffset += 4 + (postingListSize * 4L); // 4 bytes for size and 4 bytes for each integer in the list
        }
        assert writer.position() == offsetsStartAt : "writer.position()=" + writer.position() + " offsetsStartAt=" + offsetsStartAt;

        // Write postings lists
        for (var i = 0; i < graphSize; i++) {
            var originalOrdinal = newToOldMapper.applyAsInt(i);
            var rowIds = postingsMap.get(vectorValues.getVector(originalOrdinal)).getRowIds();
            writer.writeInt(rowIds.size());
            for (int r = 0; r < rowIds.size(); r++)
                writer.writeInt(rowIds.getInt(r));
        }
        assert writer.position() == nextOffset;
    }

    public void writeRowIdToNodeOrdinalMapping(SequentialWriter writer,
                                               RandomAccessVectorValues vectorValues,
                                               Map<? extends VectorFloat<?>, ? extends VectorPostings<T>> postingsMap) throws IOException
    {
        long startOffset = writer.position();

        // Collect all (rowId, vectorOrdinal) pairs
        List<Pair<Integer, Integer>> pairs = new ArrayList<>();
        for (var i = 0; i < graphSize; i++) {
            // if it's an on-disk Map then this is an expensive assert, only do it when in memory
            if (postingsMap instanceof ConcurrentSkipListMap)
                assert postingsMap.get(vectorValues.getVector(i)).getOrdinal() == i;

            int ord = newToOldMapper.applyAsInt(i);
            var rowIds = postingsMap.get(vectorValues.getVector(ord)).getRowIds();
            for (int r = 0; r < rowIds.size(); r++)
                pairs.add(Pair.create(rowIds.getInt(r), i));
        }

        // Sort the pairs by rowId
        pairs.sort(Comparator.comparingInt(Pair::left));

        // Write the pairs to the file
        for (var pair : pairs) {
            writer.writeInt(pair.left);
            writer.writeInt(pair.right);
        }

        // write the position of the beginning of rowid -> ordinals mappings to the end
        writer.writeLong(startOffset);
    }
}
