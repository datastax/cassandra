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
package org.apache.cassandra.db;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.db.composites.SimpleDenseCellNameType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import org.junit.Assert;
import org.junit.Test;

public class RowIndexEntryTest extends SchemaLoader
{
    @Test
    public void testSerializedSize() throws IOException
    {
        testSerializedSize(false);
    }

    /**
     * Check that column index is properly generated when there are
     * multiple range tombstones
     */
    @Test
    public void testSerializedSizeWithRangeTombstones() throws IOException
    {
        testSerializedSize(true);
    }

    public void testSerializedSize(final boolean rangeTombstones) throws IOException
    {
        final RowIndexEntry simple = new RowIndexEntry(123);

        DataOutputBuffer buffer = new DataOutputBuffer();
        RowIndexEntry.Serializer serializer = new RowIndexEntry.Serializer(new SimpleDenseCellNameType(UTF8Type.instance));

        serializer.serialize(simple, buffer);

        Assert.assertEquals(buffer.getLength(), serializer.serializedSize(simple));

        final int INDEX_ENTRIES = 3;

        buffer = new DataOutputBuffer();
        final ColumnFamily cf = ArrayBackedSortedColumns.factory.create("Keyspace1", "Standard1");
        final AtomicLong totalWrittenBytes = new AtomicLong();
        ColumnIndex columnIndex = new ColumnIndex.Builder(cf, ByteBufferUtil.bytes("a"), new DataOutputBuffer())
        {{
            int col = 0;
            long size = 0;
            do
            {
                CellName cellName = CellNames.simpleDense(ByteBufferUtil.bytes(String.format("%05d", col++)));
                OnDiskAtom atom = new BufferCell(cellName, ByteBufferUtil.bytes("v"), FBUtilities.timestampMicros());
                size += addAtom(atom);
                if (rangeTombstones)
                {
                    for (int i = 0; i < 3; i++)
                    {
                        cellName = CellNames.simpleDense(ByteBufferUtil.bytes(String.format("%05d", col++)));
                        atom = new RangeTombstone(cellName.start(), cellName.end(), new DeletionTime(System.currentTimeMillis(), col));
                        size += addAtom(atom);
                    }
                }
            }
            while (size < DatabaseDescriptor.getColumnIndexSize() * INDEX_ENTRIES);
            finishAddingAtoms();
            totalWrittenBytes.set(size);
        }

            private long addAtom(OnDiskAtom atom) throws IOException
            {
                add(atom);
                return cf.getComparator().onDiskAtomSerializer().serializedSizeForSSTable(atom);
            }
        }.build();

        RowIndexEntry withIndex = RowIndexEntry.create(0xdeadbeef, DeletionTime.LIVE, columnIndex);

        serializer.serialize(withIndex, buffer);
        Assert.assertEquals(buffer.getLength(), serializer.serializedSize(withIndex));

        Assert.assertEquals(INDEX_ENTRIES, columnIndex.columnsIndex.size());
        long totalWidth = 0;
        for (int i = 0; i < INDEX_ENTRIES; i++)
        {
            IndexHelper.IndexInfo info = columnIndex.columnsIndex.get(i);
            Assert.assertTrue((info.offset >= DatabaseDescriptor.getColumnIndexSize() * i) && (info.offset <= DatabaseDescriptor.getColumnIndexSize() * (i+1)));
            totalWidth += info.width;
        }
        Assert.assertEquals(totalWrittenBytes.get(), totalWidth);
    }
}
