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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeMultimap;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntIntHashMap;
import com.carrotsearch.hppc.IntLongHashMap;
import com.carrotsearch.hppc.LongArrayList;
import org.apache.cassandra.index.sai.disk.MergePostingList;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.v1.DirectReaders;
import org.apache.cassandra.index.sai.disk.v1.LeafOrderMap;
import org.apache.cassandra.index.sai.disk.v2.FilteringPostingList;
import org.apache.cassandra.index.sai.disk.v2.postings.PForDeltaPostingsReader;
import org.apache.cassandra.index.sai.disk.v2.postings.PostingsReader;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.SeekingRandomAccessInput;
import org.apache.cassandra.index.sai.utils.SharedIndexInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.packed.DirectWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

import static org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexWriter.LEAF_SIZE;

public class BlockIndexReader implements AutoCloseable
{
    final FileHandle indexFile;
    final PackedLongValues leafFilePointers;
    final IntIntHashMap nodeIDToLeaf = new IntIntHashMap();
    final IntLongHashMap leafToOrderMapFP = new IntLongHashMap();
    final BlockIndexMeta meta;
    final ByteMapper byteMapper;
    final IntLongHashMap nodeIDToPostingsFP = new IntLongHashMap();
    final IndexInput orderMapInput;
    final SeekingRandomAccessInput orderMapRandoInput;
    private final DirectReaders.Reader orderMapReader;
    final RangeSet<Integer> multiBlockLeafRanges;
    final FixedBitSet leafValuesSame;
    final Multimap<Integer, Long> multiNodeIDToPostingsFP = TreeMultimap.create();
    final IntLongHashMap leafIDToPostingsFP = new IntLongHashMap();
    final BlockIndexFileProvider fileProvider;
    final boolean temporary;

    public BlockIndexReader(BlockIndexFileProvider fileProvider,
                            boolean temporary,
                            BlockIndexMeta meta) throws IOException
    {
        this(fileProvider, temporary, meta, ByteMapper.DEFAULT);
    }

    public BlockIndexReader(BlockIndexFileProvider fileProvider,
                            boolean temporary,
                            BlockIndexMeta meta,
                            ByteMapper byteMapper) throws IOException
    {
        this.fileProvider = fileProvider;
        this.temporary = temporary;
        this.meta = meta;
        this.byteMapper = byteMapper;

        // Can't validate temporary file because the file could contain multiple segments
//        if (!temporary)
//            this.fileProvider.validate(SerializationUtils.deserialize(meta.fileInfoMapBytes.bytes));

        SharedIndexInput bytesInput = fileProvider.openValuesInput(temporary);
        this.indexFile = fileProvider.getIndexFileHandle(temporary);
        SharedIndexInput leafLevelPostingsInput = fileProvider.openLeafPostingsInput(temporary);
        this.orderMapInput = fileProvider.openOrderMapInput(temporary);
        this.orderMapRandoInput = new SeekingRandomAccessInput(orderMapInput);
        SharedIndexInput multiPostingsInput = fileProvider.openMultiPostingsInput(temporary);

        orderMapReader = DirectReaders.getReaderForBitsPerValue((byte) DirectWriter.unsignedBitsRequired(LEAF_SIZE - 1));

        final PackedLongValues.Builder leafFPBuilder = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);

        bytesInput.seek(meta.leafFilePointersFP);
        for (int x = 0; x < meta.numLeaves; x++)
        {
            final long leafFP = bytesInput.readVLong();
            leafFPBuilder.add(leafFP);
        }
        leafFilePointers = leafFPBuilder.build();

        leafLevelPostingsInput.seek(meta.leafIDPostingsFP_FP);
        final int leafIDPostingsFPSize = leafLevelPostingsInput.readVInt();
        for (int x = 0; x < leafIDPostingsFPSize; x++)
        {
            int leafID = leafLevelPostingsInput.readVInt();
            long postingsFP = leafLevelPostingsInput.readVLong();
            leafIDToPostingsFP.put(leafID, postingsFP);
        }

        leafLevelPostingsInput.seek(meta.multiBlockLeafRangesFP);
        multiBlockLeafRanges = IntRangeSetSerializer.deserialize(leafLevelPostingsInput);

        if (meta.leafValuesSameFP != -1)
        {
            leafLevelPostingsInput.seek(meta.leafValuesSameFP);
            this.leafValuesSame = BitSetSerializer.deserialize(meta.leafValuesSamePostingsFP, leafLevelPostingsInput);
        }
        else
        {
            this.leafValuesSame = null;
        }

        leafLevelPostingsInput.seek(meta.nodeIDPostingsFP_FP);
        final int leafLevelPostingsSize = leafLevelPostingsInput.readVInt();
        for (int x = 0; x < leafLevelPostingsSize; x++)
        {
            int nodeID = leafLevelPostingsInput.readVInt();
            long postingsFP = leafLevelPostingsInput.readVLong();
            this.nodeIDToPostingsFP.put(nodeID, postingsFP);
        }

        multiPostingsInput.seek(meta.nodeIDToMultilevelPostingsFP_FP);
        int numBigPostings = multiPostingsInput.readVInt();
        for (int x = 0; x < numBigPostings; x++)
        {
            int nodeID = multiPostingsInput.readVInt();
            long postingsFP = multiPostingsInput.readZLong();
            multiNodeIDToPostingsFP.put(nodeID, postingsFP);
        }

        final PackedLongValues.Builder nodeIDToLeafOrdinalFPBuilder = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
        nodeIDToLeafOrdinalFPBuilder.add(0);

        bytesInput.seek(meta.nodeIDToLeafOrdinalFP);
        final int numNodes = bytesInput.readVInt();
        for (int x = 1; x <= numNodes; x++)
        {
            int nodeID = bytesInput.readVInt(); // TODO: en/decoding the node ID isn't necessary since it's in order
            int leafOrdinal = bytesInput.readVInt();

            assert nodeID == x : "nodeid=" + nodeID + " x=" + x;

            nodeIDToLeaf.put(nodeID, leafOrdinal);
        }

        orderMapInput.seek(meta.orderMapFP);
        final int numOrderMaps = orderMapInput.readVInt();
        for (int x = 0; x < numOrderMaps; x++)
        {
            int leaf = orderMapInput.readVInt(); // TODO: en/decoding the node ID isn't necessary since it's in order
            long fp = orderMapInput.readVLong();
            leafToOrderMapFP.put(leaf, fp);
        }
        FileUtils.closeQuietly(bytesInput, leafLevelPostingsInput, multiPostingsInput);
    }

    // TODO: return accurate heap used or move heap using data structures to disk
    public long heapMemoryUsed()
    {
        return 0;
    }

    public static class BlockIndexReaderContext implements Closeable
    {
        DirectReaders.Reader lengthsReader, prefixesReader;
        int lengthsBytesLen;
        int prefixBytesLen;
        byte lengthsBits;
        byte prefixBits;
        long arraysFilePointer;
        private SeekingRandomAccessInput seekingInput;
        int leafSize;
        private long leafBytesFP; // current file pointer in the bytes part of the leaf
        private long leafBytesStartFP; // file pointer where the overall bytes start for a leaf
        int bytesLength = 0;
        int lastLen = 0;
        int lastPrefix = 0;
        byte[] firstTerm;
        byte[] bytes;
        int firstTermLen = -1;
        int bytesLen = -1;
        int leaf;
        int leafIndex;
        int lastLeafIndex;
        long currentLeafFP = -1;
        boolean readBlock = false;
        BytesRefBuilder builder = new BytesRefBuilder();

        SharedIndexInput leafLevelPostingsInput, multiPostingsInput, bytesInput;

        @Override
        public void close() throws IOException
        {
            FileUtils.closeQuietly(seekingInput, leafLevelPostingsInput, multiPostingsInput, bytesInput);
        }
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.close(indexFile, orderMapInput);
    }

    static class NodeIDLeafFP
    {
        public final int nodeID;
        public final int leaf;
        public final long filePointer;

        public NodeIDLeafFP(int nodeID, int leaf, long filePointer)
        {
            this.nodeID = nodeID;
            this.leaf = leaf;
            this.filePointer = filePointer;
        }

        @Override
        public String toString()
        {
            return "NodeIDLeafFP{" +
                   "nodeID=" + nodeID +
                   ", leaf=" + leaf +
                   ", filePointer=" + filePointer +
                   '}';
        }
    }

    public static PostingList toOnePostingList(List<PostingList.PeekablePostingList> postingLists)
    {
        return MergePostingList.merge(postingLists);
    }

    public List<PostingList.PeekablePostingList> traverse(ByteComparable start,
                                                          boolean startExclusive,
                                                          ByteComparable end,
                                                          boolean endExclusive) throws IOException
    {
        ByteComparable realStart = start;
        ByteComparable realEnd = end;

        // TODO: probably a better way to get the length
        if (startExclusive && start != null)
        {
            byte[] startBytes = ByteSourceInverse.readBytes(start.asComparableBytes(ByteComparable.Version.OSS41));
            realStart = BytesUtil.nudge(start, startBytes.length - 1);
        }
        if (endExclusive && end != null)
        {
            byte[] endBytes = ByteSourceInverse.readBytes(end.asComparableBytes(ByteComparable.Version.OSS41));
            realEnd = BytesUtil.nudgeReverse(end, endBytes.length - 1);
        }
        return traverse(realStart, realEnd);
    }

    public IndexIterator iterator(long segmentRowIdOffset)
    {
        return new IndexIteratorImpl(segmentRowIdOffset);
    }

    public IndexIterator iterator()
    {
        return iterator(0);
    }

    public static class IndexState
    {
        public BytesRef term;
        public long rowid;

        public IndexState()
        {
        }

        @Override
        public String toString()
        {
            return "IndexState{" +
                   "term=" + term.utf8ToString() +
                   ", rowid=" + rowid +
                   '}';
        }

        public IndexState(BytesRef term, long rowid)
        {
            this.term = term;
            this.rowid = rowid;
        }
    }

    public interface IndexIterator extends Closeable
    {
        public IndexState next() throws IOException;

        public IndexState current();
    }

    public class IndexIteratorImpl implements IndexIterator
    {
        private final long segmentRowIdOffset;
        private long pointId = 0;
        final BlockIndexReaderContext context;
        private long currentPostingLeaf = -1;
        private long[] postings = null;
        private PostingsReader postingsReader = null;
        private long orderMapFP = -1;
        final IndexState indexState = new IndexState();

        public IndexIteratorImpl(long segmentRowIdOffset)
        {
            context = initContext();
            this.segmentRowIdOffset = segmentRowIdOffset;
        }

        @Override
        public void close() throws IOException
        {
            FileUtils.closeQuietly(postingsReader);
            context.close();
        }

        @Override
        public IndexState current()
        {
            return indexState;
        }

        @Override
        public IndexState next() throws IOException
        {
            if (pointId >= meta.numRows)
            {
                return null;
            }
            indexState.term = BlockIndexReader.this.seekTo(pointId++, context, false);
            indexState.rowid = posting() + segmentRowIdOffset;
            return indexState;
        }

        private int multiBlockUpperEndPoint = -1;

        private long posting() throws IOException
        {
            Range<Integer> multiBlockRange = null;
            if (currentPostingLeaf != context.leaf &&
                (multiBlockUpperEndPoint == -1 ||
                 currentPostingLeaf != multiBlockUpperEndPoint ||
                 context.leaf > multiBlockUpperEndPoint))
            {
                multiBlockRange = BlockIndexReader.this.multiBlockLeafRanges.rangeContaining(context.leaf);
                int leafPostings = context.leaf;
                if (multiBlockRange != null)
                {
                    // postings are in leafIDToPostingsFP at the max leaf id of a multi-block range
                    leafPostings = multiBlockUpperEndPoint = multiBlockRange.upperEndpoint();
                    currentPostingLeaf = leafPostings;
                }
                else
                {
                    currentPostingLeaf = context.leaf;
                    multiBlockUpperEndPoint = -1;
                }
                if (leafIDToPostingsFP.containsKey(leafPostings))
                {
                    long postingsFP = leafIDToPostingsFP.get(leafPostings);

                    if (postingsReader != null)
                    {
                        postingsReader.close();
                        postingsReader = null;
                    }

                    postingsReader = new PostingsReader(context.leafLevelPostingsInput.sharedCopy(), postingsFP, QueryEventListener.PostingListEventListener.NO_OP);

                    // if there's an order map the leaf size is normal
                    // use the postings array
                    if (leafToOrderMapFP.containsKey(context.leaf))
                    {
                        assert postingsReader.size() <= LEAF_SIZE;

                        orderMapFP = leafToOrderMapFP.get(context.leaf);

                        if (postings == null)
                        {
                            postings = new long[LEAF_SIZE];
                        }

                        int i = 0;
                        while (true)
                        {
                            final long rowid = postingsReader.nextPosting();
                            if (rowid == PostingList.END_OF_STREAM)
                            {
                                break;
                            }
                            final int postingsOrdinal = (int) orderMapReader.get(orderMapRandoInput, orderMapFP, i);
                            postings[postingsOrdinal] = rowid;
                            i++;
                        }
                    }
                    else
                    {
                        // multi-block posting list
                        orderMapFP = -1;
                    }
                }
                else
                {
                    assert orderMapFP == -1;
                }
            }

            if (orderMapFP != -1)
            {
                return postings[context.lastLeafIndex];
            }
            else
            {
                final long posting = postingsReader.nextPosting();

                if (posting == PostingList.END_OF_STREAM)
                {
                    throw new IllegalStateException("posting == PostingList.END_OF_STREAM");
                }
                return posting;
            }
        }
    }

    public List<PostingList.PeekablePostingList> traverse(final ByteComparable start,
                                                          final ByteComparable end) throws IOException
    {
        final List<PostingList.PeekablePostingList> postingLists = new ArrayList<>();

        try (final BlockIndexReaderContext context = initContext())
        {
            final TraverseTreeResult traverseTreeResult = traverseForNodeIDs(start, end);

            // if there's only 1 leaf in the index, filter on it
            if (traverseTreeResult.nodeIDs.size() == 0 && meta.numLeaves == 1)
            {
                traverseTreeResult.nodeIDs.add(this.nodeIDToLeaf.keys().iterator().next().value);
            }

            //TODO Is this strictly correct?
            if (traverseTreeResult.nodeIDs.size() == 0)
                return postingLists;

            // TODO: conversion also done in the method above
            BytesRef startBytes = start == null ? null : byteMapper.fromByteComparable(start);
            BytesRef endBytes = end == null ? null : byteMapper.fromByteComparable(end);

            final List<NodeIDLeafFP> leafNodeIDToLeafOrd = new ArrayList<>();

            for (int nodeID : traverseTreeResult.nodeIDs)
            {
                final Collection<Long> multiPostingFPs = this.multiNodeIDToPostingsFP.get(nodeID);
                if (multiPostingFPs != null && multiPostingFPs.size() > 0)
                {
                    final int leaf = this.nodeIDToLeaf.get(nodeID);
                    for (final long fp : multiPostingFPs)
                    {
                        leafNodeIDToLeafOrd.add(new NodeIDLeafFP(nodeID, leaf, fp));
                    }
                }
                else
                {
                    if (nodeIDToPostingsFP.containsKey(nodeID))
                    {
                        leafNodeIDToLeafOrd.add(new NodeIDLeafFP(nodeID, nodeIDToLeaf.get(nodeID), nodeIDToPostingsFP.get(nodeID)));
                    }
                }
            }
            // sort by leaf id
            Collections.sort(leafNodeIDToLeafOrd, (o1, o2) -> Integer.compare(o1.leaf, o2.leaf));
            int minNodeID = leafNodeIDToLeafOrd.get(0).nodeID;
            int minLeafOrd = leafNodeIDToLeafOrd.get(0).leaf;
            int maxNodeID = leafNodeIDToLeafOrd.get(leafNodeIDToLeafOrd.size() - 1).nodeID;
            int maxLeafOrd = leafNodeIDToLeafOrd.get(leafNodeIDToLeafOrd.size() - 1).leaf;

            // TODO: the leafNodeIDToLeafOrd list may have a big postings list at the end
            //       since leafNodeIDToLeafOrd is sorted by leaf and there may be the same leaf


            final boolean minRangeExists = multiBlockLeafRanges.contains(minLeafOrd);

            int startOrd = 1;
            int endOrd = leafNodeIDToLeafOrd.size() - 1;

            if (!minRangeExists && minLeafOrd == maxLeafOrd)
            {
                // TODO: if the minNode is all same values or multi-block there's
                //       no need to filter
                return Lists.newArrayList(filterLeaf(minNodeID, startBytes, endBytes, context).peekable());
            }

            Integer firstFilterNodeID = null;

            if (minRangeExists || start == null)
            {
                startOrd = 0;
            }
            else
            {
                firstFilterNodeID = minNodeID;
                PostingList firstList = filterLeaf(minNodeID, startBytes, endBytes, context);
                if (firstList != null)
                {
                    postingLists.add(firstList.peekable());
                }
            }

            final boolean maxRangeExists = this.multiBlockLeafRanges.contains(maxLeafOrd);
            final boolean allSameValues = leafValuesSame != null ? leafValuesSame.get(maxLeafOrd) : false;

            if (end == null || maxRangeExists || allSameValues)
            {
                endOrd = leafNodeIDToLeafOrd.size();
                NodeIDLeafFP pair = leafNodeIDToLeafOrd.get(endOrd - 1);

                if (allSameValues)
                {
                    // there is no order map for blocks with all the same value
                    assert !leafToOrderMapFP.containsKey(pair.leaf);
                }
            }
            else
            {
                if (firstFilterNodeID == null ||
                    (firstFilterNodeID != null && firstFilterNodeID.intValue() != maxNodeID))
                {
                    PostingList lastList = filterLeaf(maxNodeID,
                                                      startBytes,
                                                      endBytes,
                                                      context
                    );
                    if (lastList != null)
                    {
                        postingLists.add(lastList.peekable());
                    }
                }
            }

            // make sure to iterate over the posting lists in leaf id order
            // TODO: the postings are not in leaf id order
            for (int x = startOrd; x < endOrd; x++)
            {
                final NodeIDLeafFP nodeIDLeafOrd = leafNodeIDToLeafOrd.get(x);

                // negative file pointer means an upper level big posting list so use multiPostingsInput
                if (nodeIDLeafOrd.filePointer < 0)
                {
                    long fp = nodeIDLeafOrd.filePointer * -1;
                    PForDeltaPostingsReader postings = new PForDeltaPostingsReader(context.multiPostingsInput.sharedCopy(), fp, QueryEventListener.PostingListEventListener.NO_OP);
                    postingLists.add(postings.peekable());
                }
                else
                {
                    final long postingsFP = nodeIDToPostingsFP.get(nodeIDLeafOrd.nodeID);
                    PostingsReader postings = new PostingsReader(context.leafLevelPostingsInput.sharedCopy(), postingsFP, QueryEventListener.PostingListEventListener.NO_OP);
                    postingLists.add(postings.peekable());
                }
            }
        }
        return postingLists;
    }

    public static class TraverseTreeResult
    {
        final SortedSet<Integer> nodeIDs;
        final int maxLeaf, minLeaf;

        public TraverseTreeResult(SortedSet<Integer> nodeIDs, int maxLeaf, int minLeaf)
        {
            this.nodeIDs = nodeIDs;
            this.maxLeaf = maxLeaf;
            this.minLeaf = minLeaf;
        }
    }

    public TraverseTreeResult traverseForNodeIDs(ByteComparable start,
                                                 ByteComparable end) throws IOException
    {
        Pair<Integer, Integer> pair = traverseForMinMaxLeafOrdinals(start, end);
        int min = pair.left;
        int max = pair.right;
        if (pair.right == -1)
        {
            max = (int) meta.numLeaves;
        }
        if (pair.left == -1)
        {
            min = 0;
        }
        Range<Integer> multiBlockRange = multiBlockLeafRanges.rangeContaining(max);
        if (multiBlockRange != null)
        {
            max = multiBlockRange.upperEndpoint();
        }

        if (min > 0)
        {
            int prevMin = min - 1;

            boolean prevSameValues = leafValuesSame != null ? leafValuesSame.get(prevMin) : false;//leafValuesSame.get(prevMin);
            if (!prevSameValues)
            {
                min--;
            }
        }

        TreeSet<Integer> nodeIDs = traverseIndex(min, max);
        return new TraverseTreeResult(nodeIDs, min, max);
    }

    public PostingList filterLeaf(int nodeID,
                                  BytesRef start,
                                  BytesRef end,
                                  BlockIndexReaderContext context) throws IOException
    {
        assert nodeID >= meta.numLeaves; // assert that it's a leaf node id

        final int leaf = this.nodeIDToLeaf.get(nodeID);

        // TODO: check if the leaf is all the same value
        //       if true, there's no need to filter
        final Long orderMapFP;
        if (leafToOrderMapFP.containsKey(leaf))
        {
            orderMapFP = leafToOrderMapFP.get(leaf);
        }
        else
        {
            orderMapFP = null;
        }

        final long leafFP = leafFilePointers.get(leaf);
        readBlock(leafFP, context);
        //this.readCompressedBlock(leaf, context);

        int idx = 0;
        int startIdx = -1;

        int endIdx = context.leafSize;

        for (idx = 0; idx < context.leafSize; idx++)
        {
            final BytesRef term = seekInBlock(idx, context, true);

            if (startIdx == -1 && (start == null || term.compareTo(start) >= 0))
            {
                startIdx = idx;
            }

            if (end != null && term.compareTo(end) > 0)
            {
                endIdx = idx - 1;
                break;
            }
        }

        int cardinality = context.leafSize - startIdx;

        if (cardinality <= 0) return null;

        if (startIdx == -1) startIdx = context.leafSize;

        final int startIdxFinal = startIdx;
        final int endIdxFinal = endIdx;

        final long postingsFP = nodeIDToPostingsFP.get(nodeID);
        final PostingsReader postings = new PostingsReader(context.leafLevelPostingsInput.sharedCopy(), postingsFP, QueryEventListener.PostingListEventListener.NO_OP);
        FilteringPostingList filterPostings = new FilteringPostingList(
        cardinality,
        // get the row id's term ordinal to compare against the startOrdinal
        (postingsOrd, rowID) -> {
            int ord = postingsOrd;

            // if there's no order map use the postings order
            if (orderMapFP != null)
            {
                ord = (int) this.orderMapReader.get(this.orderMapRandoInput, orderMapFP, postingsOrd);
            }
            return ord >= startIdxFinal && ord <= endIdxFinal;
        },
        postings);
        return filterPostings;
    }

    public BinaryTreeIndex binaryTreeIndex()
    {
        return new BinaryTreeIndex((int) meta.numLeaves);
    }

    // using the given min and max leaf id's, traverse the binary tree, return node id's with postings
    // atm the that's only leaf node id's
    public TreeSet<Integer> traverseIndex(int minLeaf, int maxLeaf) throws IOException
    {
        BinaryTreeIndex.BinaryTreeRangeVisitor visitor = new BinaryTreeIndex.BinaryTreeRangeVisitor(new BinaryTreeIndex.BinaryTreeRangeVisitor.Bound(minLeaf, true),
                                                                                                    new BinaryTreeIndex.BinaryTreeRangeVisitor.Bound(maxLeaf, false));
        TreeSet<Integer> resultNodeIDs = new TreeSet();

        BinaryTreeIndex index = binaryTreeIndex();

        collectPostingLists(0,
                            nodeIDToLeaf.size() - 1,
                            index,
                            visitor,
                            resultNodeIDs);

        return resultNodeIDs;
    }

    protected void collectPostingLists(int cellMinLeafOrdinal,
                                       int cellMaxLeafOrdinal,
                                       BinaryTreeIndex index,
                                       BinaryTreeIndex.BinaryTreeVisitor visitor,
                                       Set<Integer> resultNodeIDs) throws IOException
    {
        final int nodeID = index.getNodeID();
        final PointValues.Relation r = visitor.compare(cellMinLeafOrdinal, cellMaxLeafOrdinal);

        if (r == PointValues.Relation.CELL_OUTSIDE_QUERY)
        {
            // This cell is fully outside of the query shape: stop recursing
            return;
        }

        if (r == PointValues.Relation.CELL_INSIDE_QUERY)
        {
            // if there is pre-built posting list for the entire subtree
            if (nodeIDToPostingsFP.containsKey(nodeID))
            {
                resultNodeIDs.add(nodeID);
                return;
            }

            // TODO: assert that the node is part of a multi-block postings
            //Preconditions.checkState(!index.isLeafNode(), "Leaf node %s does not have kd-tree postings.", index.getNodeID());

            visitNode(cellMinLeafOrdinal,
                      cellMaxLeafOrdinal,
                      index,
                      visitor,
                      resultNodeIDs);
            return;
        }

        if (index.isLeafNode())
        {
            if (index.nodeExists())
            {
                resultNodeIDs.add(nodeID);
            }
            return;
        }

        visitNode(cellMinLeafOrdinal,
                  cellMaxLeafOrdinal,
                  index,
                  visitor,
                  resultNodeIDs);
    }

    void visitNode(int cellMinPacked,
                   int cellMaxPacked,
                   BinaryTreeIndex index,
                   BinaryTreeIndex.BinaryTreeVisitor visitor,
                   Set<Integer> resultNodeIDs) throws IOException
    {
        int nodeID = index.getNodeID();
        int splitLeafOrdinal = nodeIDToLeaf.get(nodeID);

        index.pushLeft();
        collectPostingLists(cellMinPacked, splitLeafOrdinal, index, visitor, resultNodeIDs);
        index.pop();

        index.pushRight();
        collectPostingLists(splitLeafOrdinal, cellMaxPacked, index, visitor, resultNodeIDs);
        index.pop();
    }

    // do a start range query, then an end range query and return the min and max leaf id's
    public Pair<Integer, Integer> traverseForMinMaxLeafOrdinals(ByteComparable start, ByteComparable end) throws IOException
    {
        int minLeafOrdinal = 0, maxLeafOrdinal = (int) this.meta.numLeaves - 1;

        if (start != null)
        {
            try (TrieRangeIterator reader = new TrieRangeIterator(indexFile.instantiateRebufferer(),
                                                                  meta.indexFP,
                                                                  start,
                                                                  null,
                                                                  true,
                                                                  true))
            {
                Iterator<Pair<ByteSource, Long>> iterator = reader.iterator();
                if (iterator.hasNext())
                {
                    Pair<ByteSource, Long> pair = iterator.next();
                    long value = pair.right.longValue();
                    int minLeaf = (int) (value >> 32);
                    int maxLeaf = (int) value;
                    minLeafOrdinal = minLeaf;
                }
                else
                {
                    minLeafOrdinal = (int) this.meta.numLeaves;
                }
            }
        }

        if (end != null)
        {
            // TODO: could reuse the result of instantiateRebufferer?
            try (TrieRangeIterator reader = new TrieRangeIterator(indexFile.instantiateRebufferer(),
                                                                  meta.indexFP,
                                                                  end,
                                                                  null,
                                                                  true,
                                                                  true))
            {
                Iterator<Pair<ByteSource, Long>> iterator = reader.iterator();
                if (iterator.hasNext())
                {
                    Pair<ByteSource, Long> pair = iterator.next();

                    long value = pair.right.longValue();
                    int minLeaf = (int) (value >> 32);
                    int maxLeaf = (int) value;

                    byte[] bytes = ByteSourceInverse.readBytes(pair.left);
                    if (ByteComparable.compare(ByteComparable.fixedLength(bytes), end, ByteComparable.Version.OSS41) > 0)
                    {
                        // if the term found is greater than what we're looking for, use the previous leaf
                        maxLeafOrdinal = minLeaf - 1;
                    }
                    else
                    {
                        maxLeafOrdinal = maxLeaf;
                    }
                }
                else
                {
//                    System.out.println("traverseForMinMaxLeafOrdinals no max term ");
                }
            }
        }

        return Pair.create(minLeafOrdinal, maxLeafOrdinal);
    }

    public BytesRef seekTo(long pointID,
                           BlockIndexReaderContext context,
                           boolean incLeafIndex) throws IOException
    {
        final long leaf = pointID / LEAF_SIZE;
        final int leafIdx = (int) (pointID % LEAF_SIZE);

        if (context.readBlock && context.leaf == leaf && context.lastLeafIndex == leafIdx)
            return context.builder.get();

        final long leafFP = leafFilePointers.get(leaf);

        if (context.currentLeafFP != leafFP)
        {
            readBlock(leafFP, context);
            context.currentLeafFP = leafFP;
            context.leaf = (int) leaf;
            context.leafIndex = 0;
        }
        context.leaf = (int) leaf;
        return seekInBlock(leafIdx, context, incLeafIndex);
    }

    public BlockIndexReaderContext initContext()
    {
        // TODO: use initContext everywhere and lazily init the input streams
        BlockIndexReaderContext context = new BlockIndexReaderContext();
        context.bytesInput = fileProvider.openValuesInput(temporary);
        context.leafLevelPostingsInput = fileProvider.openLeafPostingsInput(temporary);
        context.multiPostingsInput = fileProvider.openMultiPostingsInput(temporary);
        return context;
    }

    public long seekTo(final BytesRef target, final BlockIndexReaderContext context) throws IOException
    {
        if (target.compareTo(meta.minTerm) <= 0)
            return meta.minRowID;
        if (target.compareTo(meta.maxTerm) >= 0)
            return meta.maxRowID;

        // TODO: do min/max term checking here to avoid extra IO
        try (TrieRangeIterator reader = new TrieRangeIterator(indexFile.instantiateRebufferer(),
                                                              meta.indexFP,
                                                              BytesUtil.fixedLength(target),
                                                              null,
                                                              true,
                                                              true))
        {
            final Iterator<Pair<ByteSource, Long>> iterator = reader.iterator();
            int leafId = -1;
            if (iterator.hasNext())
            {
                final Pair<ByteSource, Long> pair = iterator.next();
                leafId = (int) (pair.right.longValue() >> 32);
                // the term may be in a previous block
                if (leafId > 0)
                {
                    leafId--;
                    context.readBlock = false;
                }
            }
            else
            {
                leafId = (int)meta.numLeaves - 1;
            }

            while (leafId < meta.numLeaves)
            {
                if (leafId != context.leaf || !context.readBlock)
                {
                    final long leafFP = leafFilePointers.get(leafId);
                    readBlock(leafFP, context);
                    context.leaf = leafId;
                    context.leafIndex = 0;
                    context.readBlock = true;
                }

                for (context.leafIndex = 0; context.leafIndex < context.leafSize; context.leafIndex++)
                {
                    final BytesRef term = seekInBlock(context.leafIndex, context, false);
                    if (target.compareTo(term) <= 0)
                    {
                        final long pointId = leafId * LEAF_SIZE + context.leafIndex++;
                        return pointId;
                    }
                }
                leafId++;
            }
            return -1;
        }
    }

    private void readBlock(long filePointer, BlockIndexReaderContext context) throws IOException
    {
        context.bytesInput.seek(filePointer);
        context.currentLeafFP = filePointer;
        context.leafSize = context.bytesInput.readInt();
        context.lengthsBytesLen = context.bytesInput.readInt();
        context.prefixBytesLen = context.bytesInput.readInt();
        context.lengthsBits = context.bytesInput.readByte();
        context.prefixBits = context.bytesInput.readByte();

        context.arraysFilePointer = context.bytesInput.getFilePointer();

        context.lengthsReader = DirectReaders.getReaderForBitsPerValue(context.lengthsBits);
        context.prefixesReader = DirectReaders.getReaderForBitsPerValue(context.prefixBits);

        context.bytesInput.seek(context.arraysFilePointer + context.lengthsBytesLen + context.prefixBytesLen);

        context.leafBytesStartFP = context.leafBytesFP = context.bytesInput.getFilePointer();

        context.seekingInput = new SeekingRandomAccessInput(context.bytesInput);

        context.leafIndex = 0;

        context.readBlock = true;
    }

    public BytesRef seekInBlock(int seekIndex,
                                BlockIndexReaderContext context,
                                boolean incLeafIndex) throws IOException
    {
        if (seekIndex >= context.leafSize)
        {
            throw new IllegalArgumentException("seekIndex="+seekIndex+" must be less than the leaf size="+context.leafSize);
        }

        int len = 0;
        int prefix = 0;

        // TODO: this part can go back from the current
        //       position rather than start from the beginning each time

        int start = 0;

        // start from where we left off
        if (seekIndex >= context.leafIndex)
        {
            start = context.leafIndex;
        }

        for (int x = start; x <= seekIndex; x++)
        {
            len = LeafOrderMap.getValue(context.seekingInput, context.arraysFilePointer, x, context.lengthsReader);
            prefix = LeafOrderMap.getValue(context.seekingInput, context.arraysFilePointer + context.lengthsBytesLen, x, context.prefixesReader);

            if (x == 0)
            {
                if (context.firstTerm == null)
                {
                    context.firstTerm = new byte[len];
                }
                else
                {
                    context.firstTerm = ArrayUtil.grow(context.firstTerm, len);
                }
                context.firstTermLen = len;

                context.leafBytesFP = context.leafBytesStartFP;
                context.bytesInput.seek(context.leafBytesStartFP);
                context.bytesInput.readBytes(context.firstTerm, 0, len);
                context.lastPrefix = len;
                context.lastLen = len;
                context.bytesLength = 0;
                context.leafBytesFP += len;
            }
            else if (len > 0)
            {
                context.bytesLength = len - prefix;
                context.lastPrefix = prefix;
                context.lastLen = len;
                if (x < seekIndex)
                    context.leafBytesFP += context.bytesLength;

            }
            else
            {
                context.bytesLength = 0;
            }
        }

        // TODO: don't do this with an IndexIterator
        if (incLeafIndex)
        {
            context.leafIndex = seekIndex + 1;
        }

        if (!(len == 0 && prefix == 0))
        {
            context.builder.clear();

            if (context.bytesLength > 0)
            {
                if (context.bytes == null)
                {
                    context.bytes = new byte[context.bytesLength];
                }
                else
                {
                    context.bytes = ArrayUtil.grow(context.bytes, context.bytesLength);
                }
                context.bytesInput.seek(context.leafBytesFP);
                context.bytesInput.readBytes(context.bytes, 0, context.bytesLength);
                context.leafBytesFP += context.bytesLength;
            }
            if (context.lastPrefix > 0 && context.firstTerm != null)
                context.builder.append(context.firstTerm, 0, context.lastPrefix);

            if (context.bytesLength > 0 )
            {
                context.builder.append(context.bytes, 0, context.bytesLength);
            }
        }
        context.lastLeafIndex = seekIndex;
        return context.builder.get();
    }
}
