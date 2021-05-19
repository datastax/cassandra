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

import java.io.IOError;
import java.io.IOException;

import com.intel.pmem.llpl.util.AutoCloseableIterator;
import com.intel.pmem.llpl.util.LongART;
import com.intel.pmem.llpl.TransactionalHeap;
import com.intel.pmem.llpl.TransactionalMemoryBlock;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PmemRowMapIterator extends AbstractIterator<Unfiltered> implements UnfilteredRowIterator
{
    protected final TableMetadata metadata;
    private final TransactionalHeap heap;
    private  Iterator pmemRowTreeIterator;
    private DecoratedKey dkey;
    private boolean reversed = false;
    SerializationHeader header;
    DeletionTime deletionTime;
    private AutoCloseableIterator cartIterator;
    private Row staticRow = Rows.EMPTY_STATIC_ROW;
    private static final Logger logger = LoggerFactory.getLogger(PmemRowMapIterator.class);

    private PmemRowMapIterator(TableMetadata metadata,
                               LongART pmemRowMapTree, TransactionalHeap heap, DecoratedKey key,
                               DeletionTime deletionTime, AutoCloseableIterator cartIterator)
    {
        this.metadata = metadata;
        this.heap = heap;
        this.header =  SerializationHeader.makeWithoutStats(metadata);
        this.dkey = key;
        this.deletionTime = deletionTime;
        this.pmemRowTreeIterator =  pmemRowMapTree.getEntryIterator();
    	this.cartIterator = cartIterator;
   }

   private PmemRowMapIterator(TableMetadata metadata,
                              LongART pmemRowMapTree, TransactionalHeap heap, DecoratedKey key,
                              DeletionTime deletionTime, Slice slice, AutoCloseableIterator cartIterator)
   {
       this(metadata,pmemRowMapTree,heap, key, deletionTime, cartIterator);
       boolean includeStart = slice.start().isInclusive();
       boolean includeEnd = slice.end().isInclusive();
       ClusteringBound start = slice.start() == ClusteringBound.BOTTOM ? null : slice.start();
       ClusteringBound end = slice.end() == ClusteringBound.TOP ? null : slice.end();

       if ((start != null && start.size() != 0) && (end != null && end.size() != 0))
       {
           ByteSource clusteringByteSource = metadata.comparator.asByteComparable(start).asComparableBytes(ByteComparable.Version.OSS41);
           byte[] clusteringStartBytes= ByteSourceInverse.readBytes(clusteringByteSource);
           clusteringByteSource = metadata.comparator.asByteComparable(end).asComparableBytes(ByteComparable.Version.OSS41);
           byte[] clusteringEndBytes= ByteSourceInverse.readBytes(clusteringByteSource);
           this.pmemRowTreeIterator = pmemRowMapTree.getEntryIterator(clusteringStartBytes, includeStart, clusteringEndBytes, includeEnd);
       }
       else if ((start != null) && (start.size() != 0) )
       {
           ByteSource clusteringByteSource = metadata.comparator.asByteComparable(start).asComparableBytes(ByteComparable.Version.OSS41);
           byte[] clusteringBytes= ByteSourceInverse.readBytes(clusteringByteSource);
           this.pmemRowTreeIterator = pmemRowMapTree.getTailEntryIterator(clusteringBytes, includeStart);
       }
       else if ((end != null) && (end.size() != 0))
       {
           ByteSource clusteringByteSource = metadata.comparator.asByteComparable(end).asComparableBytes(ByteComparable.Version.OSS41);
           byte[] clusteringBytes= ByteSourceInverse.readBytes(clusteringByteSource);
           this.pmemRowTreeIterator = pmemRowMapTree.getHeadEntryIterator(clusteringBytes, includeEnd);
       }
   }

    public static UnfilteredRowIterator create(TableMetadata metadata,
                                               LongART pmemRowMapTree, TransactionalHeap heap, DecoratedKey key,
                                               DeletionTime deletionTime, boolean reversed, AutoCloseableIterator cartIterator)
    {
        PmemRowMapIterator pmemRowMapIterator = new PmemRowMapIterator(metadata, pmemRowMapTree, heap, key, deletionTime, cartIterator);
        pmemRowMapIterator.reversed = reversed;
        return pmemRowMapIterator;
    }
    public static UnfilteredRowIterator create(TableMetadata metadata,
                                               LongART pmemRowMapTree, TransactionalHeap heap, DecoratedKey key,
                                               DeletionTime deletionTime, Slice slice, boolean reversed, Row staticRow,
                                               AutoCloseableIterator cartIterator)//,SerializationHeader header,SerializationHelper helper)
    {
        PmemRowMapIterator pmemRowMapIterator = new PmemRowMapIterator(metadata, pmemRowMapTree, heap, key, deletionTime, slice,cartIterator);
        pmemRowMapIterator.staticRow = staticRow;
        pmemRowMapIterator.reversed = reversed;
        return pmemRowMapIterator;
    }

    protected Unfiltered computeNext()
    {
        Row.Builder builder = BTreeRow.sortedBuilder();
        if (pmemRowTreeIterator.hasNext())
        {
            LongART.Entry nextEntry = (LongART.Entry) pmemRowTreeIterator.next();
            ByteComparable clusteringByteComparable = ByteComparable.fixedLength(nextEntry.getKey());
            Clustering clustering = metadata.comparator.clusteringFromByteComparable(ByteArrayAccessor.instance, clusteringByteComparable);
            SerializationHeader serializationHeader ;
            serializationHeader = new SerializationHeader(false,
                                                          metadata,
                                                          metadata.regularAndStaticColumns(),
                                                          EncodingStats.NO_STATS);
            DeserializationHelper helper = new DeserializationHelper(metadata, -1, DeserializationHelper.Flag.LOCAL);//TODO: Check if version needs to be set
            TransactionalMemoryBlock cellMemoryBlock = heap.memoryBlockFromHandle(nextEntry.getValue());
            MemoryBlockDataInputPlus memoryBlockDataInputPlus = new MemoryBlockDataInputPlus(cellMemoryBlock, heap);
            try
            {
                builder.newRow(clustering);
                Unfiltered unfiltered = PmemRowSerializer.serializer.deserialize(memoryBlockDataInputPlus, serializationHeader, helper, builder);
                return unfiltered == null ? endOfData() : unfiltered;
            }
            catch (IOException e)
            {
                closeCartIterator();
                throw new IOError(e);
            }
        }
        return endOfData();
    }

    public TableMetadata metadata()
    {
        return metadata;
    }

    public boolean isReverseOrder()
    {
        return reversed;
    }

    public RegularAndStaticColumns columns()
    {
        return metadata().regularAndStaticColumns();
    }

    public DecoratedKey partitionKey()
    {
        return dkey;
    }

    public Row staticRow()
    {
        return staticRow;
    }

    public DeletionTime partitionLevelDeletion()
    {
        return this.deletionTime;
    }

    public EncodingStats stats()
    {
        return EncodingStats.NO_STATS;
    }

    @Override
    public void close()
    {
      closeCartIterator();
    }

    private void closeCartIterator()
    {
        try
        {
            if(cartIterator != null)
            {
                cartIterator.close();
                cartIterator = null;
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
