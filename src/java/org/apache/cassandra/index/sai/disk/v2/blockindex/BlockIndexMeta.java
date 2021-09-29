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

package org.apache.cassandra.index.sai.disk.v2.blockindex;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.cassandra.index.sai.disk.IndexOnDiskMetadata;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;

public class BlockIndexMeta implements IndexOnDiskMetadata
{
    public long orderMapFP;
    public long indexFP;
    public long leafFilePointersFP;
    public long numLeaves;
    public long nodeIDToLeafOrdinalFP;
    public long multiBlockLeafRangesFP;
    public long nodeIDToMultilevelPostingsFP_FP;
    public long zstdDictionaryFP;
    public long leafValuesSameFP;
    public long leafValuesSamePostingsFP;
    public long nodeIDPostingsFP_FP;
    public long compressedLeafFPs_FP;
    public long numRows;
    public long minRowID;
    public long maxRowID;
    public BytesRef minTerm, maxTerm;
    public long leafIDPostingsFP_FP;
    public BytesRef fileInfoMapBytes;
    public long rowPointMap_FP;

    public BlockIndexMeta(IndexInput input) throws Exception
    {
        Map<String, Field> map = Arrays.stream(BlockIndexMeta.class.getDeclaredFields()).collect(
        Collectors.toMap(Field::getName, (field) -> field));

        int size = input.readVInt();
        for (int x = 0; x < size; x++)
        {
            String fieldName = input.readString();

            Field field = map.get(fieldName);
            if (field == null)
            {
                throw new Exception("field " + fieldName + " not found.");
            }
            if (field.getType().isAssignableFrom(BytesRef.class))
            {
                int len = input.readVInt();
                byte[] bytes = new byte[len];
                input.readBytes(bytes, 0, len);
                field.set(this, new BytesRef(bytes));
            }
            else
            {
                long value = input.readZLong();
                field.setLong(this, value);
            }
        }
        System.out.println("BlockIndexMeta load minTerm="+Arrays.toString(minTerm.bytes));
    }

    public BlockIndexMeta(long orderMapFP,
                          long indexFP,
                          long leafFilePointersFP,
                          long numLeaves,
                          long nodeIDToLeafOrdinalFP,
                          long multiBlockLeafRangesFP,
                          long nodeIDToMultilevelPostingsFP_FP,
                          long zstdDictionaryFP,
                          long leafValuesSameFP,
                          long leafValuesSamePostingsFP,
                          long nodeIDPostingsFP_FP,
                          long compressedLeafFPs_FP,
                          long numRows,
                          long minRowID,
                          long maxRowID,
                          BytesRef minTerm,
                          BytesRef maxTerm,
                          long leafIDPostingsFP_FP,
                          BytesRef fileInfoMapBytes,
                          long rowPointMap_FP)
    {
        this.orderMapFP = orderMapFP;
        this.indexFP = indexFP;
        this.leafFilePointersFP = leafFilePointersFP;
        this.numLeaves = numLeaves;
        this.nodeIDToLeafOrdinalFP = nodeIDToLeafOrdinalFP;
        this.multiBlockLeafRangesFP = multiBlockLeafRangesFP;
        this.nodeIDToMultilevelPostingsFP_FP = nodeIDToMultilevelPostingsFP_FP;
        this.zstdDictionaryFP = zstdDictionaryFP;
        this.leafValuesSameFP = leafValuesSameFP;
        this.leafValuesSamePostingsFP = leafValuesSamePostingsFP;
        this.nodeIDPostingsFP_FP = nodeIDPostingsFP_FP;
        this.compressedLeafFPs_FP = compressedLeafFPs_FP;
        this.numRows = numRows;
        this.minRowID = minRowID;
        this.maxRowID = maxRowID;
        this.minTerm = minTerm;
        this.maxTerm = maxTerm;
        this.leafIDPostingsFP_FP = leafIDPostingsFP_FP;
        this.fileInfoMapBytes = fileInfoMapBytes;
        this.rowPointMap_FP = rowPointMap_FP;
    }

    public void write(final IndexOutput out) throws Exception
    {
        final Field[] fields = BlockIndexMeta.class.getDeclaredFields();
        out.writeVInt(fields.length);
        for (Field field : BlockIndexMeta.class.getDeclaredFields())
        {
            final Class type = field.getType();
            if (type.isAssignableFrom(long.class))
            {
                long value = field.getLong(this);
                out.writeString(field.getName());
                out.writeZLong(value);
            }
            else if (type.isAssignableFrom(BytesRef.class))
            {
                BytesRef value = (BytesRef)field.get(this);
                out.writeString(field.getName());
                out.writeVInt(value.length);
                out.writeBytes(value.bytes, value.offset, value.length);
            }
        }
    }
}
