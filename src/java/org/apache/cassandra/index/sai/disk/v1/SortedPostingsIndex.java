/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.store.IndexInput;
import org.github.jamm.MemoryLayoutSpecification;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.cassandra.index.sai.utils.SAICodecUtils.validate;

/**
 * Mapping between node ID and an offset to its auxiliary posting list (containing every row id from all leaves
 * reachable from that node. See {@link OneDimBKDPostingsWriter}).
 */
class SortedPostingsIndex
{
    private final int size;
    public final Map<Integer, SortedPostingsWriter.SortedNode> index = new HashMap();

    SortedPostingsIndex(IndexInput input, long filePosition) throws IOException
    {
        validate(input);
        input.seek(filePosition);

        size = input.readVInt();

        for (int x = 0; x < size; x++)
        {
            final int node = input.readVInt();
            final long postingsFilePointer = input.readVLong();
            final long orderMapFilePointer = input.readZLong();
            final long minPointID = input.readVLong();;

            index.put(node, new SortedPostingsWriter.SortedNode(postingsFilePointer, orderMapFilePointer, minPointID));
        }
        input.close();
    }
    
    public long memoryUsage()
    {
        // IntLongHashMap uses two arrays: one for keys, one for values.
        return MemoryLayoutSpecification.sizeOfArray(index.size(), 4L)
                + MemoryLayoutSpecification.sizeOfArray(index.size(), 8L);
    }

    /**
     * Returns <tt>true</tt> if given node ID has an auxiliary posting list.
     */
    boolean exists(int nodeID)
    {
        checkArgument(nodeID > 0);
        return index.containsKey(nodeID);
    }

    /**
     * Returns an offset within the bkd postings file to the begining of the blocks summary of given node's auxiliary
     * posting list.
     *
     * @throws IllegalArgumentException when given nodeID doesn't have an auxiliary posting list. Check first with
     * {@link #exists(int)}
     */
    SortedPostingsWriter.SortedNode getNode(int nodeID)
    {
        checkArgument(exists(nodeID));
        return index.get(nodeID);
    }

    int size()
    {
        return size;
    }
}
