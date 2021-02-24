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
package org.apache.cassandra.io.sstable.format.trieindex;

import java.io.IOException;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.format.PartitionIndexIterator;
import org.apache.cassandra.io.sstable.format.RowIndexEntry;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Throwables;

class PartitionIterator extends PartitionIndex.IndexPosIterator implements PartitionIndexIterator
{
    private final PartitionIndex partitionIndex;
    private final IPartitioner partitioner;
    private final PartitionPosition limit;
    private final int exclusiveLimit;
    private final FileHandle dataFile;
    private final FileHandle rowIndexFile;

    private FileDataInput dataInput;
    private FileDataInput indexInput;

    private DecoratedKey currentKey;
    private RowIndexEntry currentEntry;
    private DecoratedKey nextKey;
    private RowIndexEntry nextEntry;
    private boolean closeHandles = false;

    /**
     * Note: For performance reasons this class does not request a reference of the files it uses.
     * If it is the only reference to the data, caller must request shared copies and apply closeHandles().
     * See {@link TrieIndexFormat.ReaderFactory#keyIterator(org.apache.cassandra.io.sstable.Descriptor, org.apache.cassandra.schema.TableMetadata)}
     */
    PartitionIterator(PartitionIndex partitionIndex, IPartitioner partitioner, FileHandle rowIndexFile, FileHandle dataFile,
                      PartitionPosition left, int inclusiveLeft, PartitionPosition right, int exclusiveRight, Rebufferer.ReaderConstraint rc) throws IOException
    {
        super(partitionIndex, left, right, rc);
        this.partitionIndex = partitionIndex;
        this.partitioner = partitioner;
        this.limit = right;
        this.exclusiveLimit = exclusiveRight;
        this.rowIndexFile = rowIndexFile;
        this.dataFile = dataFile;

        readNext();
        // first value can be off
        if (nextKey != null && !(nextKey.compareTo(left) > inclusiveLeft))
        {
            readNext();
        }
        advance();
    }

    /**
     * Note: For performance reasons this class does not request a reference of the files it uses.
     * If it is the only reference to the data, caller must request shared copies and apply closeHandles().
     * See {@link TrieIndexFormat.ReaderFactory#keyIterator(org.apache.cassandra.io.sstable.Descriptor, org.apache.cassandra.schema.TableMetadata)}
     */
    PartitionIterator(PartitionIndex partitionIndex, IPartitioner partitioner, FileHandle rowIndexFile, FileHandle dataFile, Rebufferer.ReaderConstraint rc) throws IOException
    {
        this(partitionIndex, partitioner, rowIndexFile, dataFile, partitionIndex.firstKey(), -1, partitionIndex.lastKey(), 0, rc);
    }

    private PartitionIterator(PartitionIndex partitionIndex, Rebufferer.ReaderConstraint rc)
    {
        super(partitionIndex, partitionIndex.firstKey(), partitionIndex.firstKey(), rc);
        this.partitionIndex = partitionIndex;
        this.partitioner = null;
        this.limit = partitionIndex.firstKey();
        this.exclusiveLimit = -1;
        this.rowIndexFile = null;
        this.dataFile = null;

        this.currentEntry = null;
        this.currentKey = null;
        this.nextEntry = null;
        this.nextKey = null;
    }

    static PartitionIterator empty(PartitionIndex partitionIndex, Rebufferer.ReaderConstraint rc)
    {
        return new PartitionIterator(partitionIndex, rc);
    }

    public PartitionIterator closeHandles()
    {
        this.closeHandles = true;
        return this;
    }

    @Override
    public void close()
    {
        Throwable accum = null;
        if (closeHandles)
        {
            accum = Throwables.close(accum, partitionIndex, dataFile, rowIndexFile);
        }
        accum = Throwables.close(accum, dataInput, indexInput);
        accum = Throwables.perform(accum, super::close);
        Throwables.maybeFail(accum);
    }

    public DecoratedKey key()
    {
        return currentKey;
    }

    public long dataPosition()
    {
        return currentEntry != null ? currentEntry.position : -1;
    }

    public RowIndexEntry entry()
    {
        return currentEntry;
    }

    public void advance() throws IOException
    {
        currentKey = nextKey;
        currentEntry = nextEntry;
        if (currentKey != null)
        {
            readNext();
            // if nextKey is null, then currentKey is the last key to be published, therefore check against any limit
            // and suppress the partition if it is beyond the limit
            if (nextKey == null && limit != null && currentKey.compareTo(limit) > exclusiveLimit)
            {   // exclude last partition outside range
                currentKey = null;
                currentEntry = null;
            }
        }
    }

    private void readNext() throws IOException
    {
        long pos = nextIndexPos();
        if (pos != PartitionIndex.NOT_FOUND)
        {
            if (pos >= 0)
            {
                FileDataInput in = indexInput(pos);
                nextKey = partitioner.decorateKey(ByteBufferUtil.readWithShortLength(in));
                nextEntry = TrieIndexEntry.deserialize(in, in.getSeekPosition());
            }
            else
            {
                pos = ~pos;
                FileDataInput in = dataInput(pos);
                nextKey = partitioner.decorateKey(ByteBufferUtil.readWithShortLength(in));
                nextEntry = new RowIndexEntry(pos);
            }
        }
        else
        {
            nextKey = null;
            nextEntry = null;
        }
    }

    private FileDataInput indexInput(long pos) throws IOException
    {
        FileDataInput in = indexInput;
        if (in == null)
            in = indexInput = rowIndexFile.createReader(pos, rc);
        else
            in.seek(pos);
        return in;
    }

    private FileDataInput dataInput(long pos) throws IOException
    {
        FileDataInput in = dataInput;
        if (in == null)
            in = dataInput = dataFile.createReader(pos, rc);
        else
            in.seek(pos);
        return in;
    }
}
