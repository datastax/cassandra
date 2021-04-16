/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.base.MoreObjects;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.utils.ArrayPostingList;
import org.apache.cassandra.index.sai.utils.SharedIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import static org.apache.cassandra.index.sai.disk.QueryEventListeners.NO_OP_POSTINGS_LISTENER;

public class SortedPostingsWriter
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final BKDReader reader;

    SortedPostingsWriter(BKDReader reader)
    {
        this.reader = reader;
    }

    static class SortedPoint implements Comparable<SortedPoint>
    {
        int origIndex;
        long pointID;
        long rowID;

        @Override
        public int compareTo(SortedPostingsWriter.SortedPoint other)
        {
            return Long.compare(pointID, other.pointID);
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                              .add("origIndex", origIndex)
                              .add("pointID", pointID)
                              .add("rowID", rowID)
                              .toString();
        }
    }

    public long finish(Supplier<IndexOutputWriter> postingsOutSupplier,
                       IndexOutput orderMapOut,
                       Supplier<IndexInput> postingsInputSupplier,
                       BKDReader.DocMapper rowIDToPointID) throws IOException
    {
        final IndexOutputWriter postingsOut = postingsOutSupplier.get();
        final PostingsWriter postingsWriter = new PostingsWriter(postingsOut);

        final BKDReader.IteratorState iterator = reader.iteratorState();
        final SortedPoint[] sortedPoints = new SortedPoint[reader.maxPointsInLeafNode];
        for (int x=0; x < reader.maxPointsInLeafNode; x++)
        {
            sortedPoints[x] = new SortedPoint();
        }
        final long[] leafPointIDs = new long[reader.maxPointsInLeafNode];
        final int[] orderMap = new int[reader.maxPointsInLeafNode];
        final Map<Integer,SortedNode> sortedNodes = new HashMap(); // key is the kdtree node id

        long total = 0;

        long leafMinPointID = Integer.MAX_VALUE;

        iterator.peek(); // init the leafPointCount?

        for (int leaf = 0; leaf < reader.numLeaves; leaf++)
        {
            System.out.println("iterator.leafPointCount="+iterator.leafPointCount+" leaf="+leaf+" reader.numLeaves="+reader.numLeaves);
            for (int leafIdx = 0; leafIdx < iterator.leafPointCount; leafIdx++)
            {
                final Long rowID = iterator.next();
                final long pointID = rowIDToPointID.oldToNew(rowID);

                sortedPoints[leafIdx].origIndex = leafIdx;
                sortedPoints[leafIdx].pointID = pointID;
                sortedPoints[leafIdx].rowID = rowID;

                leafMinPointID = Math.min(leafMinPointID, pointID);

                total++;
            }

            // sort by pointID
            Arrays.sort(sortedPoints, 0, iterator.leafPointCount);

            for (int leafIdx = 0; leafIdx < iterator.leafPointCount; leafIdx++)
            {
                leafPointIDs[leafIdx] = sortedPoints[leafIdx].pointID;
                orderMap[sortedPoints[leafIdx].origIndex] = (short)leafIdx;
            }
            System.out.println("orderMap="+Arrays.toString(Arrays.copyOf(orderMap, iterator.leafPointCount)));
            System.out.println("sortedPoints=" + Arrays.toString(Arrays.copyOf(sortedPoints, iterator.leafPointCount)));

            final ArrayPostingList postingList = new ArrayPostingList(leafPointIDs, iterator.leafPointCount);
            final long leafPostingsFilePointer = postingsWriter.write(postingList);
            final long orderMapFilePointer = orderMapOut.getFilePointer();

            LeafOrderMap.write(orderMap, iterator.leafPointCount, reader.maxPointsInLeafNode - 1, orderMapOut);

            final SortedNode sortedNode = new SortedNode(leafPostingsFilePointer, orderMapFilePointer, leafMinPointID);

            System.out.println("sorted nodeID="+iterator.currentNodeID+" leafPostingsFilePointer="+leafPostingsFilePointer);

            sortedNodes.put(iterator.currentNodeID, sortedNode);

            leafMinPointID = Integer.MAX_VALUE;
        }

        postingsWriter.complete();
        postingsWriter.close();

        final TreeMap<Long,Integer> leafOffsets = reader.getLeafOffsets();

        final Set<Integer> allNodeIDs = new HashSet(reader.postingsIndex.index.keySet());
        final Set<Integer> leafNodeIDs = new HashSet(leafOffsets.values());
        final Set<Integer> nonLeafNodeIDs = new HashSet();

        // TODO: can use some Sets method here
        for (int node : allNodeIDs)
        {
            if (!leafNodeIDs.contains(node))
            {
                nonLeafNodeIDs.add(node);
            }
        }

        final SharedIndexInput postingsInput = new SharedIndexInput(postingsInputSupplier.get());

        final IndexOutputWriter postingsOut2 = postingsOutSupplier.get();

        postingsOut2.skipToEnd();

        System.out.println("postingsOut2 end of file pointer="+postingsOut2.getFilePointer());

        final PostingsWriter postingsWriter2 = new PostingsWriter(postingsOut2);

        for (int nonLeafNodeID : nonLeafNodeIDs)
        {
            final OneDimBKDPostingsWriter.NodeEntry nodeEntry = reader.postingsIndex.index.get(nonLeafNodeID);

            final PriorityQueue<PostingList.PeekablePostingList> leafPostingsReaders = new PriorityQueue<>(100, Comparator.comparingLong(PostingList.PeekablePostingList::peek));

            List<PostingsReader> readersToClose = new ArrayList();

            long minPointID = Integer.MAX_VALUE;

            for (int leafNodeID : nodeEntry.leafNodes)
            {
                final SortedNode leafNode = sortedNodes.get(leafNodeID);
                System.out.println("nonLeafNodeID=" + nonLeafNodeID + " leafNodeID=" + leafNodeID + " leafNode=" + leafNode);

                minPointID = Math.min(leafNode.minPointID, minPointID);

                final PostingsReader postingsReader = new PostingsReader(postingsInput.sharedCopy(), leafNode.postingsFilePointer, NO_OP_POSTINGS_LISTENER);
                leafPostingsReaders.add(postingsReader.peekable());
                readersToClose.add(postingsReader);
            }

            final PostingList nonLeafNodePointIDs = MergePostingList.merge(leafPostingsReaders);

            final long writePostingsFilePointer = postingsWriter2.write(nonLeafNodePointIDs);

            for (PostingsReader postingsReader : readersToClose)
            {
                postingsReader.close();
            }

            final SortedNode sortedNode = new SortedNode(writePostingsFilePointer, -1, minPointID);

            if (sortedNodes.containsKey(nonLeafNodeID))
            {
                throw new IllegalStateException("nonLeafNodeID="+nonLeafNodeID);
            }

            sortedNodes.put(nonLeafNodeID, sortedNode);
        }

        if (reader.pointCount != total)
        {
            throw new IllegalStateException();
        }

        iterator.close();

        postingsInput.close();

        final Stopwatch flushTime = Stopwatch.createStarted();

        flushTime.stop();

//        logger.debug(components.logMessage("Flushed {} of posting lists for kd-tree nodes in {} ms."),
//                     FBUtilities.prettyPrintMemory(out.getFilePointer() - startFP),
//                     flushTime.elapsed(TimeUnit.MILLISECONDS));

        final long indexFilePointer = postingsOut2.getFilePointer();
        writeMap(sortedNodes, postingsOut2);
        postingsWriter2.complete();
        postingsWriter2.close();
        return indexFilePointer;
    }

    public static class SortedNode
    {
        public final long postingsFilePointer;
        public final long orderMapFilePointer;
        public final long minPointID;

        public SortedNode(long postingsFilePointer, long orderMapFilePointer, long minPointID)
        {
            this.postingsFilePointer = postingsFilePointer;
            this.orderMapFilePointer = orderMapFilePointer;
            this.minPointID = minPointID;
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                              .add("postingsFilePointer", postingsFilePointer)
                              .add("orderMapFilePointer", orderMapFilePointer)
                              .add("minPointID", minPointID)
                              .toString();
        }
    }

    private void writeMap(Map<Integer, SortedNode> map, IndexOutput out) throws IOException
    {
        out.writeVInt(map.size());

        for (Map.Entry<Integer, SortedNode> e : map.entrySet())
        {
            out.writeVInt(e.getKey());
            out.writeVLong(e.getValue().postingsFilePointer);
            out.writeZLong(e.getValue().orderMapFilePointer);
            out.writeVLong(e.getValue().minPointID);
        }
    }
}
