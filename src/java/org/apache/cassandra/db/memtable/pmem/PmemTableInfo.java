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

package org.apache.cassandra.db.memtable.pmem;

import java.util.UUID;

import com.intel.pmem.llpl.TransactionalHeap;
import com.intel.pmem.llpl.TransactionalMemoryBlock;
import org.apache.cassandra.schema.TableId;

//Contains TableID & Cart pointer
// TableID - 36bytes
// Cart pointer - 8 bytes

public class PmemTableInfo
{
    private static final int TABLE_ID_OFFSET = 0;
    private static final int TABLE_ID_SIZE = 36;
    private TransactionalMemoryBlock block;

    public PmemTableInfo(TransactionalHeap heap, TableId tableID, long cartAddress)
    {
        block = heap.allocateMemoryBlock(TABLE_ID_SIZE + Long.SIZE);
        byte[] tableUUIDBytes = tableID.toString().getBytes();
        block.copyFromArray(tableUUIDBytes, 0, TABLE_ID_OFFSET, tableUUIDBytes.length);
        block.setLong(TABLE_ID_OFFSET + TABLE_ID_SIZE , cartAddress);
    }

    public PmemTableInfo(TransactionalMemoryBlock block)
    {
        this.block = block;
    }

    public static PmemTableInfo fromHandle(TransactionalHeap heap, long tableHandle)
    {
        return new PmemTableInfo(heap.memoryBlockFromHandle(tableHandle));
    }

    public long handle()
    {
        return block.handle();
    }
    public TableId getTableID()
    {
        int len = TABLE_ID_SIZE;

        byte[] b = new byte[len];
        block.copyToArray(TABLE_ID_OFFSET, b, 0, len);
        String str = new String(b);
        return TableId.fromUUID(UUID.fromString(str));
    }
    public long getCartAddress()
    {
        return block.getLong(TABLE_ID_OFFSET + TABLE_ID_SIZE);
    }

}
