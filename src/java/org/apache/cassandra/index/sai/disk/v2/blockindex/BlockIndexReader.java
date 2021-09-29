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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeMultimap;
import org.apache.commons.lang3.SerializationUtils;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntIntHashMap;
import com.carrotsearch.hppc.IntLongHashMap;
import com.carrotsearch.hppc.LongArrayList;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdDictDecompress;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.disk.MergePostingList;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.ByteArrayIndexInput;
import org.apache.cassandra.index.sai.disk.v1.DirectReaders;
import org.apache.cassandra.index.sai.disk.v1.LeafOrderMap;
import org.apache.cassandra.index.sai.disk.v2.FilteringPostingList;
import org.apache.cassandra.index.sai.disk.v2.V2PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v2.V2PrimaryKeyMap;
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

import static org.apache.cassandra.index.sai.disk.format.IndexComponent.TERMS_DATA;
import static org.apache.cassandra.index.sai.disk.format.IndexComponent.TERMS_INDEX;
import static org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexWriter.LEAF_SIZE;

public class BlockIndexReader implements Closeable
{
    final FileHandle indexFile;
    final PackedLongValues leafFilePointers;
    final LongArrayList compressedLeafFPs = new LongArrayList();
    final IntArrayList compressedLeafLengths = new IntArrayList();
    final IntArrayList leafLengths = new IntArrayList();
    final IntIntHashMap nodeIDToLeaf = new IntIntHashMap();
    final IntLongHashMap leafToOrderMapFP = new IntLongHashMap();
    final BytesRefBuilder builder = new BytesRefBuilder(); // TODO: move the reader context, change name
    final BlockIndexMeta meta;
    final IntLongHashMap nodeIDToPostingsFP = new IntLongHashMap();
    final IndexInput orderMapInput;
    final SeekingRandomAccessInput orderMapRandoInput;
    private final DirectReaders.Reader orderMapReader;
    final RangeSet<Integer> multiBlockLeafRanges;
    final FixedBitSet leafValuesSame;
    final Multimap<Integer,Long> multiNodeIDToPostingsFP = TreeMultimap.create();
    final ZstdDictDecompress zstdDictDecompress;
    final IndexDescriptor indexDescriptor;
    final String indexName;
    final IntLongHashMap leafIDToPostingsFP = new IntLongHashMap();
    final V2PerIndexFiles perIndexFiles;
    final V2PrimaryKeyMap.V2PrimaryKeyMapFactory primaryKeyMapFactory;

    public BlockIndexReader(IndexDescriptor indexDescriptor,
                            String indexName,
                            BlockIndexMeta meta,
                            V2PerIndexFiles perIndexFiles,
                            V2PrimaryKeyMap.V2PrimaryKeyMapFactory primaryKeyMapFactory) throws IOException
    {
        this.indexDescriptor = indexDescriptor;
        this.indexName = indexName;
        this.meta = meta;
        this.perIndexFiles = perIndexFiles;
        this.primaryKeyMapFactory = primaryKeyMapFactory;

        HashMap<IndexComponent, FileValidator.FileInfo> fileInfoMap = SerializationUtils.deserialize(meta.fileInfoMapBytes.bytes);
        for (Map.Entry<IndexComponent,FileValidator.FileInfo> entry : fileInfoMap.entrySet())
        {
            FileValidator.FileInfo fileInfo = FileValidator.generate(entry.getKey(), perIndexFiles);
            if (!fileInfo.equals(entry.getValue()))
            {
                throw new IOException("CRC check on component "+entry.getKey()+" failed.");
            }
        }

        SharedIndexInput bytesInput = new SharedIndexInput(perIndexFiles.openInput(TERMS_DATA));
        this.indexFile = perIndexFiles.get(TERMS_INDEX);
        SharedIndexInput leafLevelPostingsInput = new SharedIndexInput(perIndexFiles.openInput(IndexComponent.POSTING_LISTS));
        this.orderMapInput = perIndexFiles.openInput(IndexComponent.ORDER_MAP);
        this.orderMapRandoInput = new SeekingRandomAccessInput(orderMapInput);
        SharedIndexInput multiPostingsInput = new SharedIndexInput(perIndexFiles.openInput(IndexComponent.KD_TREE_POSTING_LISTS));
        SharedIndexInput bytesCompressedInput = new SharedIndexInput(perIndexFiles.openInput(IndexComponent.COMPRESSED_TERMS_DATA));

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
        if (meta.zstdDictionaryFP != -1)
        {
            bytesCompressedInput.seek(meta.zstdDictionaryFP);
            int len = bytesCompressedInput.readVInt();
            byte[] dictBytes = new byte[len];
            bytesCompressedInput.readBytes(dictBytes, 0, dictBytes.length);
            zstdDictDecompress = new ZstdDictDecompress(dictBytes);
        }
        else
        {
            zstdDictDecompress = null;
        }

        if (meta.compressedLeafFPs_FP != -1)
        {
            bytesCompressedInput.seek(meta.compressedLeafFPs_FP);
            int numCompEntries = bytesCompressedInput.readVInt();
            assert meta.numLeaves == numCompEntries;
            for (int x = 0; x < meta.numLeaves; x++)
            {
                final long leafFP = bytesCompressedInput.readVLong();
                final int len = bytesCompressedInput.readVInt();
                final int origLen = bytesCompressedInput.readVInt();
                compressedLeafFPs.add(leafFP);
                compressedLeafLengths.add(len);
                leafLengths.add(origLen);
            }
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

            assert nodeID == x : "nodeid="+nodeID+" x="+x;

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
        long currentLeafFP = -1;

        private IndexInput compBytesInput = null;
        private byte[] compBytes = new byte[10];
        private byte[] uncompBytes = new byte[10];
        SharedIndexInput leafLevelPostingsInput, multiPostingsInput, bytesCompressedInput, bytesInput;
        PrimaryKeyMap primaryKeyMap;

        @Override
        public void close() throws IOException
        {
            FileUtils.closeQuietly(primaryKeyMap, seekingInput, compBytesInput, leafLevelPostingsInput, multiPostingsInput, bytesCompressedInput, bytesInput);
        }
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.close(indexFile, orderMapInput, zstdDictDecompress);
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

    public IndexIterator iterator()
    {
        return new IndexIteratorImpl();
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
        private long pointId = 0;
        final BlockIndexReaderContext context = new BlockIndexReaderContext();
        private long currentPostingLeaf = -1;
        private long[] postings = null;
        private PostingsReader postingsReader = null;
        private long orderMapFP = -1;
        final IndexState indexState = new IndexState();

        public IndexIteratorImpl()
        {
            context.bytesInput = new SharedIndexInput(perIndexFiles.openInput(TERMS_DATA));
            context.bytesCompressedInput = new SharedIndexInput(perIndexFiles.openInput(IndexComponent.COMPRESSED_TERMS_DATA));
            context.leafLevelPostingsInput = new SharedIndexInput(perIndexFiles.openInput(IndexComponent.POSTING_LISTS));
            context.multiPostingsInput = new SharedIndexInput(perIndexFiles.openInput(IndexComponent.KD_TREE_POSTING_LISTS));
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
            indexState.rowid = posting();
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
                return postings[context.leafIndex];
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
        final BlockIndexReaderContext context = new BlockIndexReaderContext();

        final TraverseTreeResult traverseTreeResult = traverseForNodeIDs(start, end);

        final int leafDiff = traverseTreeResult.maxLeaf - traverseTreeResult.minLeaf;

        double leafRatio = leafDiff / meta.numLeaves;

        System.out.println("leafRatio="+leafRatio);

        // if the leaf diff is 50% of the index then filter instead of
        // creating a posting list
//        if (leafRatio > 0.50d)
//        {
//            OrdinalPostingList matchAll = new OrdinalPostingList()
//            {
//                int i = 0;
//
//                @Override
//                public long getOrdinal()
//                {
//                    return i;
//                }
//
//                @Override
//                public long nextPosting() throws IOException
//                {
//                    return i++;
//                }
//
//                @Override
//                public long size()
//                {
//                    return meta.numRows;
//                }
//
//                @Override
//                public long advance(PrimaryKey nextPrimaryKey) throws IOException
//                {
//                    return 0;
//                }
//
//                @Override
//                public PrimaryKey mapRowId(long rowId) throws IOException
//                {
//                    return null;
//                }
//            };
//            FilteringPostingList.Filter filter = new FilteringPostingList.Filter()
//            {
//                @Override
//                public boolean matches(int ordinal, long rowID)
//                {
//                    return false;
//                }
//            };
//            return new FilteringPostingList(int cardinality, filter, matchAll);
//        }

        context.bytesInput = new SharedIndexInput(perIndexFiles.openInput(TERMS_DATA));
        context.bytesCompressedInput = new SharedIndexInput(perIndexFiles.openInput(IndexComponent.COMPRESSED_TERMS_DATA));
        context.leafLevelPostingsInput = new SharedIndexInput(perIndexFiles.openInput(IndexComponent.POSTING_LISTS));
        context.multiPostingsInput = new SharedIndexInput(perIndexFiles.openInput(IndexComponent.KD_TREE_POSTING_LISTS));
        context.primaryKeyMap = this.primaryKeyMapFactory.newPerSSTablePrimaryKeyMap(new SSTableQueryContext(new QueryContext()));

        // if there's only 1 leaf in the index, filter on it
        if (traverseTreeResult.nodeIDs.size() == 0 && meta.numLeaves == 1)
        {
            traverseTreeResult.nodeIDs.add(this.nodeIDToLeaf.keys().iterator().next().value);
        }

        // TODO: conversion also done in the method above
        BytesRef startBytes = null;
        if (start != null)
        {
            startBytes = new BytesRef(ByteSourceInverse.readBytes(start.asComparableBytes(ByteComparable.Version.OSS41)));
        }
        BytesRef endBytes = null;
        if (end != null)
        {
            endBytes = new BytesRef(ByteSourceInverse.readBytes(end.asComparableBytes(ByteComparable.Version.OSS41)));
        }

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
                final int leafOrdinal = nodeIDToLeaf.get(nodeID);
                Long postingsFP = null;
                if (nodeIDToPostingsFP.containsKey(nodeID))
                {
                    postingsFP = nodeIDToPostingsFP.get(nodeID);
                }

                if (postingsFP != null)
                {
                    leafNodeIDToLeafOrd.add(new NodeIDLeafFP(nodeID, leafOrdinal, postingsFP));
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

        final List<PostingList.PeekablePostingList> postingLists = new ArrayList<>();

        final boolean minRangeExists = multiBlockLeafRanges.contains(minLeafOrd);

        int startOrd = 1;
        int endOrd = leafNodeIDToLeafOrd.size() - 1;

        if (minLeafOrd == maxLeafOrd)
        {
            // TODO: if the minNode is all same values or multi-block there's
            //       no need to filter
            return Lists.newArrayList(filterLeaf(minNodeID,
                                                 startBytes,
                                                 endBytes,
                                                 context).peekable()
            );
        }

        Integer firstFilterNodeID = null;

        if (minRangeExists || start == null)
        {
            startOrd = 0;
        }
        else
        {
            firstFilterNodeID = minNodeID;
            PostingList firstList = filterLeaf(minNodeID,
                                               startBytes,
                                               endBytes,
                                               context
            );
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
                PForDeltaPostingsReader postings = new PForDeltaPostingsReader(context.multiPostingsInput, fp, QueryEventListener.PostingListEventListener.NO_OP);
                postingLists.add(postings.peekable());
            }
            else
            {
                final long postingsFP = nodeIDToPostingsFP.get(nodeIDLeafOrd.nodeID);
                PostingsReader postings = new PostingsReader(context.leafLevelPostingsInput, postingsFP, QueryEventListener.PostingListEventListener.NO_OP);
                postingLists.add(postings.peekable());
            }
        }
        return postingLists;
    }

    public static PostingList toOnePostingList(List<PostingList.PeekablePostingList> postingLists)
    {
        PriorityQueue postingsQueue = new PriorityQueue(postingLists.size(), Comparator.comparingLong(PostingList.PeekablePostingList::peek));
        postingsQueue.addAll(postingLists);
        return MergePostingList.merge(postingsQueue);
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
            max = (int)meta.numLeaves;
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
        final PostingsReader postings = new PostingsReader(context.leafLevelPostingsInput, postingsFP, QueryEventListener.PostingListEventListener.NO_OP);
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
        return new BinaryTreeIndex((int)meta.numLeaves);
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
    public Pair<Integer,Integer> traverseForMinMaxLeafOrdinals(ByteComparable start, ByteComparable end) throws IOException
    {
        int minLeafOrdinal = 0, maxLeafOrdinal = (int)this.meta.numLeaves - 1;

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
                    minLeafOrdinal = (int)this.meta.numLeaves;
                }
            }
        }

        if (end != null)
        {
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
                    System.out.println("traverseForMinMaxLeafOrdinals no max term ");
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

        final long leafFP = leafFilePointers.get(leaf);

        if (context.currentLeafFP != leafFP)
        {
            long filePointer = this.leafFilePointers.get(leaf);
            readBlock(filePointer, context);
            context.currentLeafFP = leafFP;
            context.leaf = (int)leaf;
            context.leafIndex = 0;
        }
        context.leaf = (int)leaf;
        context.leafIndex = leafIdx;
        return seekInBlock(leafIdx, context, incLeafIndex);
    }

    public BytesRef seekTo(BytesRef target,
                           BlockIndexReaderContext context) throws IOException
    {
        try (TrieRangeIterator reader = new TrieRangeIterator(indexFile.instantiateRebufferer(),
                                                              meta.indexFP,
                                                              BytesUtil.fixedLength(target),
                                                              null,
                                                              false,
                                                              true))
        {
            Iterator<Pair<ByteSource, Long>> iterator = reader.iterator();
            Pair<ByteSource, Long> pair = iterator.next();
            int leafOrdinal = pair.right.intValue();

            if (leafOrdinal != context.leaf)
            {
                readBlock(leafOrdinal, context);
            }

            for (int x = 0; x < context.leafSize; x++)
            {
                BytesRef term = seekInBlock(x, context, true);
                if (target.compareTo(term) <= 0)
                {
                    return term;
                }
            }

            return null;
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

        //System.out.println("arraysFilePointer="+arraysFilePointer+" lengthsBytesLen="+lengthsBytesLen+" prefixBytesLen="+prefixBytesLen+" lengthsBits="+lengthsBits+" prefixBits="+prefixBits);

        context.lengthsReader = DirectReaders.getReaderForBitsPerValue(context.lengthsBits);
        context.prefixesReader = DirectReaders.getReaderForBitsPerValue(context.prefixBits);

        context.bytesInput.seek(context.arraysFilePointer + context.lengthsBytesLen + context.prefixBytesLen);

        context.leafBytesStartFP = context.leafBytesFP = context.bytesInput.getFilePointer();

        context.seekingInput = new SeekingRandomAccessInput(context.bytesInput);

        context.leafIndex = 0;
    }

    private void readCompressedBlock(int leafID, BlockIndexReaderContext context) throws IOException
    {
        long filePointer = this.leafFilePointers.get(leafID);
        long compressedFP = this.compressedLeafFPs.get(leafID);

        context.bytesCompressedInput.seek(compressedFP);

        int compLen = compressedLeafLengths.get(leafID);
        int originalSize = leafLengths.get(leafID);

        context.compBytes = ArrayUtil.grow(context.compBytes, compLen);
        context.uncompBytes = ArrayUtil.grow(context.uncompBytes, originalSize);

        context.bytesCompressedInput.readBytes(context.compBytes, 0, compLen);

        long uncompLen = Zstd.decompressFastDict(context.uncompBytes, 0, context.compBytes, 0, compLen, this.zstdDictDecompress);

        if (uncompLen < 0)
        {
            throw new IllegalStateException("uncompLen="+uncompLen);
        }

        context.compBytesInput = new ByteArrayIndexInput("", context.uncompBytes);

        context.currentLeafFP = filePointer;
        context.leafSize = context.compBytesInput.readInt();
        context.lengthsBytesLen = context.compBytesInput.readInt();
        context.prefixBytesLen = context.compBytesInput.readInt();
        context.lengthsBits = context.compBytesInput.readByte();
        context.prefixBits = context.compBytesInput.readByte();

        context.arraysFilePointer = context.compBytesInput.getFilePointer();

        //System.out.println("arraysFilePointer="+arraysFilePointer+" lengthsBytesLen="+lengthsBytesLen+" prefixBytesLen="+prefixBytesLen+" lengthsBits="+lengthsBits+" prefixBits="+prefixBits);

        context.lengthsReader = DirectReaders.getReaderForBitsPerValue(context.lengthsBits);
        context.prefixesReader = DirectReaders.getReaderForBitsPerValue(context.prefixBits);

        context.compBytesInput.seek(context.arraysFilePointer + context.lengthsBytesLen + context.prefixBytesLen);

        context.seekingInput = new SeekingRandomAccessInput(context.compBytesInput);

        context.leafIndex = 0;
    }

    // TODO: seekInBlock has alloc changes 
//    public BytesRef seekInBlockCompressed(int seekIndex,
//                                          BlockIndexReaderContext context) throws IOException
//    {
//        if (seekIndex >= context.leafSize)
//        {
//            throw new IllegalArgumentException("seekIndex="+seekIndex+" must be less than the leaf size="+context.leafSize);
//        }
//
//        int len = 0;
//        int prefix = 0;
//
//        // TODO: this part can go back from the current
//        //       position rather than start from the beginning each time
//
//        int start = 0;
//
//        // start from where we left off
//        if (seekIndex >= context.leafIndex)
//        {
//            start = context.leafIndex;
//        }
//
//        for (int x = start; x <= seekIndex; x++)
//        {
//            len = LeafOrderMap.getValue(context.seekingInput, context.arraysFilePointer, x, context.lengthsReader);
//            prefix = LeafOrderMap.getValue(context.seekingInput, context.arraysFilePointer + context.lengthsBytesLen, x, context.prefixesReader);
//
//            if (x == 0)
//            {
//                context.firstTerm = new byte[len];
//                context.compBytesInput.seek(context.leafBytesStartFP);
//                context.compBytesInput.readBytes(context.firstTerm, 0, len);
//                context.lastPrefix = len;
//                context.bytesLength = 0;
//                context.leafBytesFP += len;
//            }
//
//            if (len > 0 && x > 0)
//            {
//                context.bytesLength = len - prefix;
//                context.lastLen = len;
//                context.lastPrefix = prefix;
//            }
//            else
//            {
//                context.bytesLength = 0;
//            }
//        }
//
//        context.leafIndex = seekIndex + 1;
//
//        if (!(len == 0 && prefix == 0))
//        {
//            builder.clear();
//
//            // TODO: fix this allocation by reading directly into builder
//            final byte[] bytes = new byte[context.bytesLength];
//            context.compBytesInput.seek(context.leafBytesFP);
//            context.compBytesInput.readBytes(bytes, 0, context.bytesLength);
//
//            context.leafBytesFP += context.bytesLength;
//
//            builder.append(context.firstTerm, 0, context.lastPrefix);
//            builder.append(bytes, 0, bytes.length);
//        }
//
//        return builder.get();
//    }

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

            //System.out.println("x="+x+" len="+len+" prefix="+prefix);

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

                context.bytesInput.seek(context.leafBytesStartFP);
                context.bytesInput.readBytes(context.firstTerm, 0, len);
                context.lastPrefix = len;
                //System.out.println("firstTerm="+new BytesRef(firstTerm).utf8ToString());
                context.bytesLength = 0;
                context.leafBytesFP += len;
            }

            if (len > 0 && x > 0)
            {
                context.bytesLength = len - prefix;
                context.lastLen = len;
                context.lastPrefix = prefix;
                //System.out.println("x=" + x + " bytesLength=" + bytesLength + " len=" + len + " prefix=" + prefix);
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
            builder.clear();

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
            builder.append(context.firstTerm, 0, context.lastPrefix);
            if (context.bytesLength > 0 )
            {
                builder.append(context.bytes, 0, context.bytesLength);
            }
        }
        return builder.get();
    }
}
