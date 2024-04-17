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

package org.apache.cassandra.index.sai.disk.vector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntUnaryOperator;

import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.Pair;

public class VectorPostingsWriter<T>
{

    private final boolean canFastFindRows;

    private final IntUnaryOperator reverseOrdinalsMapper;

    public VectorPostingsWriter(boolean canFastFindRows, IntUnaryOperator mapper) {
        this.canFastFindRows = canFastFindRows;
        this.reverseOrdinalsMapper = mapper;
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
        if (canFastFindRows) {
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
        writer.writeInt(vectorValues.size());

        // Write the offsets of the postings for each ordinal
        var offsetsStartAt = ordToRowOffset + 4L + 8L * vectorValues.size();
        var nextOffset = offsetsStartAt;
        for (var i = 0; i < vectorValues.size(); i++) {
            // (ordinal is implied; don't need to write it)
            writer.writeLong(nextOffset);

            var originalOrdinal = reverseOrdinalsMapper.applyAsInt(i);

            var rowIds = postingsMap.get(vectorValues.getVector(originalOrdinal)).getRowIds();
            nextOffset += 4 + (rowIds.size() * 4L); // 4 bytes for size and 4 bytes for each integer in the list
        }
        assert writer.position() == offsetsStartAt : "writer.position()=" + writer.position() + " offsetsStartAt=" + offsetsStartAt;

        // Write postings lists
        for (var i = 0; i < vectorValues.size(); i++) {
            var originalOrdinal = reverseOrdinalsMapper.applyAsInt(i);
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
        List<Pair<Integer, Integer>> pairs = new ArrayList<>();

        // Collect all (rowId, vectorOrdinal) pairs
        for (var i = 0; i < vectorValues.size(); i++) {
            int ord = postingsMap.get(vectorValues.getVector(i)).getOrdinal();
            assert ord == i;

            ord = reverseOrdinalsMapper.applyAsInt(ord);
            var rowIds = postingsMap.get(vectorValues.getVector(ord)).getRowIds();
            for (int r = 0; r < rowIds.size(); r++)
                pairs.add(Pair.create(rowIds.getInt(r), i));
        }

        // Sort the pairs by rowId
        pairs.sort(Comparator.comparingInt(Pair::left));

        // Write the pairs to the file
        long startOffset = writer.position();
        for (var pair : pairs) {
            writer.writeInt(pair.left);
            writer.writeInt(pair.right);
        }

        // write the position of the beginning of rowid -> ordinals mappings to the end
        writer.writeLong(startOffset);
    }
}
