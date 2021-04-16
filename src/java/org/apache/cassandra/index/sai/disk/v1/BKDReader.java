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
package org.apache.cassandra.index.sai.disk.v1;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.agrona.collections.LongArrayList;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.io.CryptoUtils;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.index.sai.utils.AbstractIterator;
import org.apache.cassandra.index.sai.utils.SeekingRandomAccessInput;
import org.apache.cassandra.index.sai.utils.SharedIndexInput;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.packed.DirectWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

/**
 * Handles intersection of a multi-dimensional shape in byte[] space with a block KD-tree previously written with
 * {@link BKDWriter}.
 */
public class BKDReader extends TraversingBKDReader implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    final BKDPostingsIndex postingsIndex;

    private final FileHandle postingsFile, kdtreeFile;
    private final ICompressor compressor;
    private final DirectReaders.Reader leafOrderMapReader;
    private final TreeMap<Long,Integer> leafFPToLeafNode;
    private final PrimaryKeyMap primaryKeyMap;
    private final PackedLongValues filePointers;

    public static final String DEFAULT_POSTING_INDEX = "DEFAULT_POSTING_INDEX";
    final Map<String,SortedPostingsIndex> postingIndexMap = new HashMap();

    /**
     * Performs a blocking read.
     */

    public BKDReader(IndexComponents indexComponents,
                     FileHandle kdtreeFile,
                     long bkdIndexRoot) throws IOException
    {
        this(indexComponents, kdtreeFile, bkdIndexRoot, null, 0, null);
    }

    public BKDReader(IndexComponents indexComponents,
                     FileHandle kdtreeFile,
                     long bkdIndexRoot,
                     FileHandle postingsFile,
                     long bkdPostingsRoot,
                     PrimaryKeyMap primaryKeyMap) throws IOException
    {
        super(indexComponents, kdtreeFile, bkdIndexRoot);
        this.postingsFile = postingsFile;
        this.kdtreeFile = kdtreeFile;
        this.primaryKeyMap = primaryKeyMap;
        this.postingsIndex = postingsFile == null ? null : new BKDPostingsIndex(postingsFile, bkdPostingsRoot);
        this.compressor = null;
        final byte bits = (byte) DirectWriter.unsignedBitsRequired(maxPointsInLeafNode - 1);
        leafOrderMapReader = DirectReaders.getReaderForBitsPerValue(bits);

        leafFPToLeafNode = new TreeMap();
        final PackedIndexTree index = new PackedIndexTree();
        getLeafOffsets(index, leafFPToLeafNode);

        final PackedLongValues.Builder filePointerBuilder = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);

        for (Map.Entry<Long,Integer> entry : leafFPToLeafNode.entrySet())
        {
            final Long filePosition = entry.getKey();
            filePointerBuilder.add(filePosition);
        }
        filePointers = filePointerBuilder.build();
    }

    public List<PostingList.PeekablePostingList> intersect(IntersectVisitor visitor,
                                                           QueryEventListener.BKDIndexEventListener listener,
                                                           QueryContext context)
    {
        return intersect(visitor,
                         listener,
                         context,
                         DEFAULT_POSTING_INDEX,
                         null);
    }

    public List<RowIDAndPointIDIterator> rowIDIterators() throws IOException
    {
        List<RowIDAndPointIDIterator> list = new ArrayList<>();
        int count = 0;
        for (Map.Entry<Long, Integer> entry : leafFPToLeafNode.entrySet())
        {
            IndexInput bkdInput = indexComponents.openInput(kdtreeFile);
            SharedIndexInput bkdPostingsInput = new SharedIndexInput(indexComponents.openInput(postingsFile));
            RowIDAndPointIDIterator iterator = leafRowIDIterator(count++,
                                                                 entry.getValue(),
                                                                 entry.getKey(),
                                                                 bkdInput,
                                                                 bkdPostingsInput);
            list.add(iterator);
        }
        return list;
    }

    public static abstract class RowIDAndPointIDIterator extends AbstractIterator<BKDReader.RowIDAndPointID> implements Closeable
    {
    }

    public interface SortedPostingInputs
    {
        IndexInput openPostingsInput() throws IOException;

        IndexInput openOrderMapInput() throws IOException;
    }

    public RowIDAndPointIDIterator leafRowIDIterator(int leafIdx,
                                                     int leafNodeID,
                                                     long filePointer,
                                                     IndexInput bkdInput,
                                                     SharedIndexInput bkdPostingsInput) throws IOException
    {
        bkdInput.seek(filePointer);
        final int count = bkdInput.readVInt();
        // loading doc ids occurred here prior
        final int orderMapLength = bkdInput.readVInt(); // read and unused
        final long orderMapPointer = bkdInput.getFilePointer();

        // TODO: need to change the order map stored on disk to avoid the origIndex array creation
        final short[] origIndex = new short[maxPointsInLeafNode];

        final SeekingRandomAccessInput randoInput = new SeekingRandomAccessInput(bkdInput);
        for (int x = 0; x < count; x++)
        {
            final short idx = (short) LeafOrderMap.getValue(randoInput, orderMapPointer, x, leafOrderMapReader);
            origIndex[idx] = (short)x; // TODO: this can be avoided with the above comment
        }

        final long start = leafIdx * this.maxPointsInLeafNode;

        final RowIDAndPointID obj = new RowIDAndPointID();

        final PostingsReader postingsReader;

        if (postingsIndex.exists(leafNodeID))
        {
            final long pointer = postingsIndex.getPostingsFilePointer(leafNodeID);
            final PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(bkdPostingsInput, pointer);
            postingsReader = new PostingsReader(bkdPostingsInput.sharedCopy(), summary, QueryEventListener.PostingListEventListener.NO_OP, primaryKeyMap);
        }
        else
        {
            throw new IllegalStateException();
        }

        return new RowIDAndPointIDIterator()
        {
            int count = 0;

            @Override
            public void close() throws IOException
            {
                postingsReader.close();
                bkdInput.close();
                bkdPostingsInput.close();
            }

            @Override
            protected RowIDAndPointID computeNext()
            {
                try
                {
                    final long rowID = postingsReader.nextPosting();

                    if (rowID == PostingList.END_OF_STREAM) return endOfData();

                    int orderIdx = origIndex[count++];

                    // TODO: use the LeafOrderMap.getValue here
                    //final int orderIdx = LeafOrderMap.getValue(randoInput, orderMapPointer, count++, leafOrderMapReader);
                    final long pointID = start + orderIdx;

                    obj.pointID = pointID;
                    obj.rowID = rowID;

                    return obj;
                }
                catch (IOException ioex)
                {
                    throw new RuntimeException(ioex);
                }
            }
        };
    }

    public static class RowIDAndPointID implements Comparable<RowIDAndPointID>
    {
        public long rowID;
        public long pointID;

        @Override
        public int compareTo(BKDReader.RowIDAndPointID other)
        {
            final int cmp = Long.compare(this.rowID, other.rowID);
            if (cmp == 0)
            {
                return Long.compare(this.pointID, other.pointID);
            }
            return cmp;
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                              .add("rowID", rowID)
                              .add("pointID", pointID)
                              .toString();
        }
    }

    public interface DocMapper
    {
        long oldToNew(long rowID);
    }

    public TreeMap<Long,Integer> getLeafOffsets()
    {
        // TODO: make sure this is called only once and cached?
        final TreeMap<Long,Integer> map = new TreeMap();
        final PackedIndexTree index = new PackedIndexTree();
        getLeafOffsets(index, map);
        return map;
    }

    private void getLeafOffsets(final IndexTree index, TreeMap<Long, Integer> map)
    {
        if (index.isLeafNode())
        {
            if (index.nodeExists())
            {
                map.put(index.getLeafBlockFP(), index.getNodeID());
            }
        }
        else
        {
            index.pushLeft();
            getLeafOffsets(index, map);
            index.pop();

            index.pushRight();
            getLeafOffsets(index, map);
            index.pop();
        }
    }

    @VisibleForTesting
    public IteratorState iteratorState() throws IOException
    {
        return new IteratorState((rowID) -> rowID);
    }

    public IteratorState iteratorState(DocMapper docMapper) throws IOException
    {
        return new IteratorState(docMapper);
    }

    public class IteratorState extends AbstractIterator<Long> implements Comparable<IteratorState>, Closeable
    {
        final IndexInput bkdInput;
        final SharedIndexInput sharedInput;
        final byte[] packedValues = new byte[maxPointsInLeafNode * packedBytesLength];
        int leaf = -1;
        int leafPointCount = -1;
        int leafPointIndex = -1;
        final LongArrayList tempPostings = new LongArrayList();
        final long[] postings = new long[maxPointsInLeafNode];
        final DocMapper docMapper;
        private final byte[] scratch;
        private final byte[] scratchShort;
        long lastPointId = Long.MIN_VALUE;
        int currentNodeID = -1;

        public IteratorState(DocMapper docMapper) throws IOException
        {
            this.docMapper = docMapper;

            scratch = new byte[packedBytesLength];
            scratchShort = new byte[packedBytesLength - 1];

            final long firstLeafFilePointer = getMinLeafBlockFP();
            bkdInput = indexComponents.openInput(kdtreeFile);
            sharedInput = postingsFile == null ? null : new SharedIndexInput(indexComponents.openInput(postingsFile));
            bkdInput.seek(firstLeafFilePointer);
        }

        public byte[] getTermValue()
        {
            System.arraycopy(scratch, 1, scratchShort, 0, scratchShort.length);
            return scratchShort;
        }

        public byte[] getRawTermValue()
        {
            return scratch;
        }

        public int packedBytesLength()
        {
            return packedBytesLength;
        }

        public void seekTo(long pointId)
        {
            if (pointId >= pointCount)
            {
                throw new IllegalArgumentException("pointID="+pointId+" >= pointCount="+pointCount);
            }

            final int leafIndex = (int) (pointId / maxPointsInLeafNode);

            final int iterations;
            // If we are seeking forwards then we don't need to reset the leafPointIndex
            leafPointIndex = pointId > lastPointId ? leafPointIndex : -1;
            lastPointId = pointId;
            if (leafIndex != leaf)
            {   // load a new leaf
                leaf = leafIndex;
                newLeaf();
                iterations = (int) (pointId % maxPointsInLeafNode);
            }
            else
            {   // reuse the current leaf
                final int mod = (int) (pointId % maxPointsInLeafNode);
                iterations = mod - leafPointIndex - 1;
            }

            for (int iteration = 0; iteration <= iterations; iteration++)
            {
                if (leafPointIndex == leafPointCount - 1)
                {
                    leaf++;
                    if (leaf == numLeaves && leafPointIndex == leafPointCount - 1)
                    {
                        throw new IllegalStateException();
                    }
                    newLeaf();
                }

                leafPointIndex++;

            }
            System.arraycopy(packedValues, leafPointIndex * packedBytesLength, scratch, 0, packedBytesLength);
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(bkdInput, sharedInput);
        }

        @Override
        public int compareTo(final IteratorState other)
        {
            // For literal datatypes the scratch sizes may be different if we are currently in a merge operation
            final int cmp = FutureArrays.compareUnsigned(scratch, 0, packedBytesLength(), other.scratch, 0, other.packedBytesLength());
            if (cmp == 0)
            {
                final long rowid1 = next;
                final long rowid2 = other.next;
                return Long.compare(rowid1, rowid2);
            }
            return cmp;
        }

        public ByteComparable asByteComparable()
        {
            //return v -> ByteSource.fixedLength(scratch);
            return v -> ByteSource.fixedLength(getTermValue());
        }

        public long rowID()
        {
            return docMapper.oldToNew(postings[leafPointIndex]);
        }

        @Override
        protected Long computeNext()
        {
            if (leaf < 0)
            {
                leaf++;
                newLeaf();
            }
            while (true)
            {
                if (leafPointIndex == leafPointCount - 1)
                {
                    leaf++;
                    if (leaf == numLeaves && leafPointIndex == leafPointCount - 1)
                    {
                        return endOfData();
                    }
                    newLeaf();
                }

                leafPointIndex++;

                System.arraycopy(packedValues, leafPointIndex * packedBytesLength, scratch, 0, packedBytesLength);
                return docMapper.oldToNew(postings[leafPointIndex]);
            }
        }

        private void newLeaf()
        {
            final long filePointer = filePointers.get(leaf);
            final int leafNode = leafFPToLeafNode.get(filePointer);
            try
            {
                leafPointCount = readLeaf(filePointer, leafNode, bkdInput, packedValues, sharedInput, postings, tempPostings);
                currentNodeID = leafNode;
            }
            catch (IOException e)
            {
                logger.error("Failed to read leaf during BKDTree merger", e);
                throw new RuntimeException("Failed to read leaf during BKDTree merger", e);
            }
            leafPointIndex = -1;
        }
    }

    @SuppressWarnings("resource")
    public int readLeaf(long filePointer,
                        int nodeID,
                        final IndexInput bkdInput,
                        final byte[] packedValues,
                        final SharedIndexInput sharedInput,
                        long[] postings,
                        LongArrayList tempPostings) throws IOException
    {
        final IntersectVisitor visitor = new IntersectVisitor() {
            int i = 0;

            @Override
            public boolean visit(byte[] packedValue)
            {
                System.arraycopy(packedValue, 0, packedValues, i * packedBytesLength, packedBytesLength);
                i++;
                return true;
            }

            @Override
            public Relation compare(byte[] minPackedValue, byte[] maxPackedValue)
            {
                return Relation.CELL_CROSSES_QUERY;
            }
        };
        return readLeaf(filePointer, nodeID, bkdInput, sharedInput, postings, tempPostings, visitor);
    }

    public int readLeaf(long filePointer,
                        int nodeID,
                        final IndexInput bkdInput,
                        final SharedIndexInput sharedInput,
                        long[] postings,
                        LongArrayList tempPostings,
                        IntersectVisitor visitor) throws IOException
    {
        bkdInput.seek(filePointer);
        final int count = bkdInput.readVInt();
        // loading doc ids occurred here prior
        final int orderMapLength = bkdInput.readVInt();
        final long orderMapPointer = bkdInput.getFilePointer();

        // order of the values in the posting list

        // TODO: move these arrays into a reusable object
        final short[] origIndex = new short[maxPointsInLeafNode];

        final int[] commonPrefixLengths = new int[numDims];
        final byte[] scratchPackedValue1 = new byte[packedBytesLength];

        final SeekingRandomAccessInput randoInput = new SeekingRandomAccessInput(bkdInput);
        for (int x = 0; x < count; x++)
        {
            final short idx = (short) LeafOrderMap.getValue(randoInput, orderMapPointer, x, leafOrderMapReader);
            origIndex[x] = idx;
        }

        IndexInput leafInput = bkdInput;

        // reused byte arrays for the decompression of leaf values
        // TODO: move these BytesRef's into reusable objects
        final BytesRef uncompBytes = new BytesRef(new byte[16]);
        final BytesRef compBytes = new BytesRef(new byte[16]);

        // seek beyond the ordermap
        leafInput.seek(orderMapPointer + orderMapLength);

        if (compressor != null)
        {
            // This should not throw WouldBlockException, even though we're on a TPC thread, because the
            // secret key used by the underlying encryptor should be loaded at reader construction time.
            leafInput = CryptoUtils.uncompress(bkdInput, compressor, compBytes, uncompBytes);
        }

        // TODO: use the sorted postings order map
        visitDocValues(commonPrefixLengths, scratchPackedValue1, leafInput, count, visitor, null, origIndex);

        if (postingsIndex != null)
        {
            if (postingsIndex.exists(nodeID))
            {
                final long pointer = postingsIndex.getPostingsFilePointer(nodeID);
                final PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(sharedInput, pointer);
                final PostingsReader postingsReader = new PostingsReader(sharedInput,
                                                                         summary,
                                                                         QueryEventListener.PostingListEventListener.NO_OP,
                                                                         primaryKeyMap);

                tempPostings.clear();

                // gather the postings into tempPostings
                while (true)
                {
                    final long rowid = postingsReader.nextPosting();
                    if (rowid == PostingList.END_OF_STREAM) break;
                    tempPostings.add(rowid);
                }

                // put the postings into the array according the origIndex
                for (int x = 0; x < tempPostings.size(); x++)
                {
                    int idx = origIndex[x];
                    final long rowid = tempPostings.get(idx);

                    postings[x] = rowid;
                }
            }
            else
            {
                throw new IllegalStateException();
            }
        }
        return count;
    }

    public static int openPerIndexFiles()
    {
        // kd-tree, posting lists file
        return 2;
    }

    @Override
    public void close()
    {
        try
        {
            super.close();
        }
        finally
        {
            FileUtils.closeQuietly(postingsFile);
        }
    }

    private class ExtendedBytesIntersectVisitor implements IntersectVisitor
    {
        final IntersectVisitor visitor;
        private byte[] scratch = new byte[bytesPerDim - 1];
        private byte[] scratch2 = new byte[bytesPerDim - 1];

        public ExtendedBytesIntersectVisitor(IntersectVisitor visitor)
        {
            this.visitor = visitor;
        }

        @Override
        public boolean visit(byte[] packedValue)
        {
            System.arraycopy(packedValue, 1, scratch, 0, scratch.length);
            return visitor.visit(scratch);
        }

        @Override
        public Relation compare(byte[] minPackedValue, byte[] maxPackedValue)
        {
            System.arraycopy(minPackedValue, 1, scratch, 0, scratch.length);
            System.arraycopy(maxPackedValue, 1, scratch2, 0, scratch.length);
            return visitor.compare(scratch, scratch2);
        }
    }

    public List<PostingList.PeekablePostingList> intersect(IntersectVisitor visitor,
                                                           QueryEventListener.BKDIndexEventListener listener,
                                                           QueryContext context,
                                                           final String postingIndexName,
                                                           SortedPostingInputs sortedPostingInputs)
    {
        visitor = new ExtendedBytesIntersectVisitor(visitor);

        Relation relation = visitor.compare(minPackedValue, maxPackedValue);

        if (relation == Relation.CELL_OUTSIDE_QUERY)
        {
            listener.onIntersectionEarlyExit();
            return null;
        }

        listener.onSegmentHit();

        final IndexInput bkdInput = indexComponents.openInput(indexFile);
        final SharedIndexInput sharedInput;
        final IndexInput orderMapInput;
        if (postingIndexName.equals(DEFAULT_POSTING_INDEX))
        {
            sharedInput = new SharedIndexInput(indexComponents.openInput(postingsFile));
            orderMapInput = null;
        }
        else
        {
            try
            {
                sharedInput = new SharedIndexInput(sortedPostingInputs.openPostingsInput());
                orderMapInput = sortedPostingInputs.openOrderMapInput();
            }
            catch (IOException ioex)
            {
                throw new RuntimeException(ioex);
            }
        }
        final PackedIndexTree index = new PackedIndexTree();

        final Intersection completable =
        relation == Relation.CELL_INSIDE_QUERY ?
        new Intersection(bkdInput, sharedInput, index, listener, context, postingIndexName) :
        new FilteringIntersection(bkdInput, sharedInput, orderMapInput, index, visitor, listener, context, postingIndexName);

        try
        {
            return completable.execute();
        }
        finally
        {
            FileUtils.closeQuietly(sharedInput);
        }
    }

    /**
     * Synchronous intersection of an multi-dimensional shape in byte[] space with a block KD-tree
     * previously written with {@link BKDWriter}.
     */
    class Intersection
    {
        final QueryContext context;

        final IndexInput bkdInput;
        final SharedIndexInput sharedInput;
        final IndexTree index;
        final QueryEventListener.BKDIndexEventListener listener;
        final String postingIndexName;

        Intersection(IndexInput bkdInput,
                     SharedIndexInput sharedInput,
                     IndexTree index,
                     QueryEventListener.BKDIndexEventListener listener,
                     QueryContext context,
                     String postingIndexName)
        {
            this.bkdInput = bkdInput;
            this.sharedInput = sharedInput;
            this.index = index;
            this.listener = listener;
            this.context = context;
            this.postingIndexName = postingIndexName;
        }

        protected void closeAll()
        {
            FileUtils.closeQuietly(sharedInput, bkdInput);
        }

        public List<PostingList.PeekablePostingList> execute()
        {
            try
            {
                List<PostingList.PeekablePostingList> postingLists = new ArrayList<>();
                executeInternal(postingLists, postingIndexName);

                closeAll();

                return postingLists;
            }
            catch (Throwable t)
            {
                if (!(t instanceof AbortedOperationException))
                    logger.error(indexComponents.logMessage("kd-tree intersection failed on {}"), indexFile.path(), t);

                closeOnException();
                throw Throwables.cleaned(t);
            }
        }

        protected void executeInternal(final List<PostingList.PeekablePostingList> postingLists,
                                       String postingIndexName) throws IOException
        {
            collectPostingLists(postingLists, postingIndexName);
        }

        protected void closeOnException()
        {
            closeAll();
        }

        public void collectPostingLists(List<PostingList.PeekablePostingList> postingLists,
                                        String postingIndexName) throws IOException
        {
            context.checkpoint();

            final int nodeID = index.getNodeID();

            if (postingIndexName.equals(DEFAULT_POSTING_INDEX))
            {
                if (postingsIndex.exists(nodeID))
                {
                    final long postingsFilePointer = postingsIndex.getPostingsFilePointer(nodeID);
                    final PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(sharedInput.sharedCopy(),
                                                                                                  postingsFilePointer);
                    PostingsReader postingsReader = new PostingsReader(sharedInput.sharedCopy(),
                                                                       summary,
                                                                       listener.postingListEventListener(),
                                                                       primaryKeyMap);
                    postingLists.add(postingsReader.peekable());
                    return;
                }
            }
            else
            {
                final SortedPostingsIndex postingsIndex = postingIndexMap.get(postingIndexName);

                // if there is pre-built posting for entire subtree
                if (postingsIndex.exists(nodeID))
                {
                    final SortedPostingsWriter.SortedNode node = postingsIndex.getNode(nodeID);

                    final PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(sharedInput.sharedCopy(),
                                                                                                  node.postingsFilePointer);
                    PostingsReader postingsReader = new PostingsReader(sharedInput.sharedCopy(),
                                                                       summary,
                                                                       listener.postingListEventListener(),
                                                                       primaryKeyMap);
                    postingLists.add(postingsReader.peekable());
                    return;
                }
            }
            Preconditions.checkState(!index.isLeafNode(), "Leaf node %s does not have kd-tree postings.", index.getNodeID());

            // Recurse on left sub-tree:
            index.pushLeft();
            collectPostingLists(postingLists, postingIndexName);
            index.pop();

            // Recurse on right sub-tree:
            index.pushRight();
            collectPostingLists(postingLists, postingIndexName);
            index.pop();
        }
    }

    /**
     * Modified copy of BKDReader#visitDocValues()
     */
    private int visitDocValues(int[] commonPrefixLengths,
                               byte[] scratchPackedValue1,
                               IndexInput in,
                               int count,
                               IntersectVisitor visitor,
                               FixedBitSet[] holder,
                               final short[] origIndex) throws IOException
    {
        readCommonPrefixes(commonPrefixLengths, scratchPackedValue1, in);

        int compressedDim = readCompressedDim(in);
        if (compressedDim == -1)
        {
            return visitRawDocValues(commonPrefixLengths, scratchPackedValue1, in, count, visitor, holder, origIndex);
        }
        else
        {
            return visitCompressedDocValues(commonPrefixLengths, scratchPackedValue1, in, count, visitor, compressedDim, holder, origIndex);
        }
    }

    /**
     * Modified copy of {@link org.apache.lucene.util.bkd.BKDReader#readCompressedDim(IndexInput)}
     */
    @SuppressWarnings("JavadocReference")
    private int readCompressedDim(IndexInput in) throws IOException
    {
        int compressedDim = in.readByte();
        if (compressedDim < -1 || compressedDim >= numDims)
        {
            throw new CorruptIndexException(String.format("Dimension should be in the range [-1, %d), but was %d.", numDims, compressedDim), in);
        }
        return compressedDim;
    }

    /**
     * Modified copy of BKDReader#visitCompressedDocValues()
     */
    private int visitCompressedDocValues(int[] commonPrefixLengths,
                                         byte[] scratchPackedValue,
                                         IndexInput in,
                                         int count,
                                         IntersectVisitor visitor,
                                         int compressedDim,
                                         FixedBitSet[] holder,
                                         final short[] origIndex) throws IOException
    {
        // the byte at `compressedByteOffset` is compressed using run-length compression,
        // other suffix bytes are stored verbatim
        final int compressedByteOffset = compressedDim * bytesPerDim + commonPrefixLengths[compressedDim];
        commonPrefixLengths[compressedDim]++;
        int i, collected = 0;

        final FixedBitSet bitSet;
        if (holder != null)
        {
            bitSet = new FixedBitSet(maxPointsInLeafNode);
        }
        else
        {
            bitSet = null;
        }

        for (i = 0; i < count; )
        {
            scratchPackedValue[compressedByteOffset] = in.readByte();
            final int runLen = Byte.toUnsignedInt(in.readByte());
            for (int j = 0; j < runLen; ++j)
            {
                for (int dim = 0; dim < numDims; dim++)
                {
                    int prefix = commonPrefixLengths[dim];
                    in.readBytes(scratchPackedValue, dim * bytesPerDim + prefix, bytesPerDim - prefix);
                }
                final int rowIDIndex = origIndex[i + j];
                if (visitor.visit(scratchPackedValue))
                {
                    if (bitSet != null) bitSet.set(rowIDIndex);
                    collected++;
                }
            }
            i += runLen;
        }
        if (i != count)
        {
            throw new CorruptIndexException(String.format("Expected %d sub-blocks but read %d.", count, i), in);
        }

        if (holder != null)
        {
            holder[0] = bitSet;
        }

        return collected;
    }

    /**
     * Modified copy of BKDReader#visitRawDocValues()
     */
    private int visitRawDocValues(int[] commonPrefixLengths,
                                  byte[] scratchPackedValue,
                                  IndexInput in,
                                  int count,
                                  IntersectVisitor visitor,
                                  FixedBitSet[] holder,
                                  final short[] origIndex) throws IOException
    {
        final FixedBitSet bitSet;
        if (holder != null)
        {
            bitSet = new FixedBitSet(maxPointsInLeafNode);
        }
        else
        {
            bitSet = null;
        }

        int collected = 0;
        for (int i = 0; i < count; ++i)
        {
            for (int dim = 0; dim < numDims; dim++)
            {
                int prefix = commonPrefixLengths[dim];
                in.readBytes(scratchPackedValue, dim * bytesPerDim + prefix, bytesPerDim - prefix);
            }
            final int rowIDIndex = origIndex[i];
            if (visitor.visit(scratchPackedValue))
            {
                if (bitSet != null) bitSet.set(rowIDIndex);

                collected++;
            }
        }
        if (holder != null)
        {
            holder[0] = bitSet;
        }
        return collected;
    }

    /**
     * Copy of BKDReader#readCommonPrefixes()
     */
    private void readCommonPrefixes(int[] commonPrefixLengths, byte[] scratchPackedValue, IndexInput in) throws IOException
    {
        for (int dim = 0; dim < numDims; dim++)
        {
            int prefix = in.readVInt();
            commonPrefixLengths[dim] = prefix;
            if (prefix > 0)
            {
//                System.out.println("dim * bytesPerDim="+(dim * bytesPerDim)+" prefix="+prefix+" numDims="+numDims);
                in.readBytes(scratchPackedValue, dim * bytesPerDim, prefix);
            }
        }
    }

    private class FilteringIntersection extends Intersection
    {
        private final IntersectVisitor visitor;
        private final byte[] scratchPackedValue1;
        private final int[] commonPrefixLengths;
        private final short[] origIndex;

        // reused byte arrays for the decompression of leaf values
        private final BytesRef uncompBytes = new BytesRef(new byte[16]);
        private final BytesRef compBytes = new BytesRef(new byte[16]);
        private final IndexInput orderMapInput;

        FilteringIntersection(IndexInput bkdInput,
                              SharedIndexInput sharedInput,
                              IndexInput orderMapInput,
                              IndexTree index,
                              IntersectVisitor visitor,
                              QueryEventListener.BKDIndexEventListener listener,
                              QueryContext context,
                              String postingIndexName)
        {
            super(bkdInput, sharedInput, index, listener, context, postingIndexName);
            this.orderMapInput = orderMapInput;
            this.visitor = visitor;
            this.commonPrefixLengths = new int[numDims];
            this.scratchPackedValue1 = new byte[packedBytesLength];
            this.origIndex = new short[maxPointsInLeafNode];
        }

        @Override
        protected void closeAll()
        {
            super.closeAll();
            IOUtils.closeQuietly(orderMapInput);
        }

        @Override
        public void executeInternal(final List<PostingList.PeekablePostingList> postingLists, String postingIndexName) throws IOException
        {
            collectPostingLists(postingLists, minPackedValue, maxPackedValue, postingIndexName);
        }

        public void collectPostingLists(List<PostingList.PeekablePostingList> postingLists,
                                        byte[] cellMinPacked,
                                        byte[] cellMaxPacked,
                                        String postingIndexName) throws IOException
        {
            context.checkpoint();

            final Relation r = visitor.compare(cellMinPacked, cellMaxPacked);

            if (r == Relation.CELL_OUTSIDE_QUERY)
            {
                // This cell is fully outside of the query shape: stop recursing
                return;
            }

            if (r == Relation.CELL_INSIDE_QUERY)
            {
                // This cell is fully inside of the query shape: recursively add all points in this cell without filtering
                super.collectPostingLists(postingLists, postingIndexName);
                return;
            }

            if (index.isLeafNode())
            {
                if (index.nodeExists())
                    filterLeaf(postingLists);
                return;
            }

            visitNode(postingLists, cellMinPacked, cellMaxPacked, postingIndexName);
        }

        @SuppressWarnings("resource")
        void filterLeaf(List<PostingList.PeekablePostingList> postingLists) throws IOException
        {
            bkdInput.seek(index.getLeafBlockFP());

            final int count = bkdInput.readVInt();

            // loading doc ids occurred here prior
            final FixedBitSet[] holder = new FixedBitSet[1];

            final int orderMapLength = bkdInput.readVInt();

            final long orderMapPointer = bkdInput.getFilePointer();

            final SeekingRandomAccessInput randoInput = new SeekingRandomAccessInput(bkdInput);
            for (int x = 0; x < count; x++)
            {
                origIndex[x] = (short) LeafOrderMap.getValue(randoInput, orderMapPointer, x, leafOrderMapReader);
            }

            // seek beyond the ordermap
            bkdInput.seek(orderMapPointer + orderMapLength);

            IndexInput leafInput = bkdInput;

            if (compressor != null)
            {
                // This should not throw WouldBlockException, even though we're on a TPC thread, because the
                // secret key used by the underlying encryptor should be loaded at reader construction time.
                leafInput = CryptoUtils.uncompress(bkdInput, compressor, compBytes, uncompBytes);
            }

            if (postingIndexName.equals(DEFAULT_POSTING_INDEX))
            {
                final int nodeID = index.getNodeID();

                visitDocValues(commonPrefixLengths, scratchPackedValue1, leafInput, count, visitor, holder, origIndex);

                if (postingsIndex.exists(nodeID) && holder[0].cardinality() > 0)
                {
                    final long pointer = postingsIndex.getPostingsFilePointer(nodeID);
                    postingLists.add(initFilteringPostingReader(pointer, holder[0]).peekable());
                }
            }
            else
            {
                final SortedPostingsIndex postingsIndex = postingIndexMap.get(postingIndexName);

                final int nodeID = index.getNodeID();

                // if there is pre-built posting for entire subtree
                if (postingsIndex.exists(nodeID))
                {
                    final SortedPostingsWriter.SortedNode node = postingsIndex.getNode(nodeID);
                    final SeekingRandomAccessInput sortedOrderMapRandoInput = new SeekingRandomAccessInput(orderMapInput);

                    // reuse origIndex2?
                    final short[] origIndex2 = new short[maxPointsInLeafNode];
                    for (int x = 0; x < count; x++)
                    {
                        origIndex2[x] = (short) LeafOrderMap.getValue(sortedOrderMapRandoInput, node.orderMapFilePointer, x, leafOrderMapReader);
                    }

                    visitDocValues(commonPrefixLengths, scratchPackedValue1, leafInput, count, visitor, holder, origIndex2);

                    final FixedBitSet filteredValues = holder[0];

                    System.out.println("filterLeaf filteredValues.cardinality=" + filteredValues.cardinality() + " origIndex3=" + Arrays.toString(Arrays.copyOf(origIndex2, count)));

                    if (filteredValues.cardinality() > 0)
                    {
                        final PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(sharedInput.sharedCopy(), node.postingsFilePointer);
                        final PostingsReader postingsReader = new PostingsReader(sharedInput.sharedCopy(), summary, listener.postingListEventListener(), primaryKeyMap);
                        final PostingList.PeekablePostingList postingList = new FilteringPostingList(filteredValues, postingsReader).peekable();
                        postingLists.add(postingList);
                    }
                }
            }
        }

        void visitNode(List<PostingList.PeekablePostingList> postingLists,
                       byte[] cellMinPacked,
                       byte[] cellMaxPacked,
                       String postingIndexName) throws IOException
        {
            int splitDim = index.getSplitDim();
            assert splitDim >= 0 : "splitDim=" + splitDim;
            assert splitDim < numDims;

            byte[] splitPackedValue = index.getSplitPackedValue();
            BytesRef splitDimValue = index.getSplitDimValue();
            assert splitDimValue.length == bytesPerDim;

            // make sure cellMin <= splitValue <= cellMax:
            assert FutureArrays.compareUnsigned(cellMinPacked, splitDim * bytesPerDim, splitDim * bytesPerDim + bytesPerDim, splitDimValue.bytes, splitDimValue.offset, splitDimValue.offset + bytesPerDim) <= 0 : "bytesPerDim=" + bytesPerDim + " splitDim=" + splitDim + " numDims=" + numDims;
            assert FutureArrays.compareUnsigned(cellMaxPacked, splitDim * bytesPerDim, splitDim * bytesPerDim + bytesPerDim, splitDimValue.bytes, splitDimValue.offset, splitDimValue.offset + bytesPerDim) >= 0 : "bytesPerDim=" + bytesPerDim + " splitDim=" + splitDim + " numDims=" + numDims;

            // Recurse on left sub-tree:
            System.arraycopy(cellMaxPacked, 0, splitPackedValue, 0, packedBytesLength);
            System.arraycopy(splitDimValue.bytes, splitDimValue.offset, splitPackedValue, splitDim * bytesPerDim, bytesPerDim);

            index.pushLeft();
            collectPostingLists(postingLists, cellMinPacked, splitPackedValue, postingIndexName);
            index.pop();

            // Restore the split dim value since it may have been overwritten while recursing:
            System.arraycopy(splitPackedValue, splitDim * bytesPerDim, splitDimValue.bytes, splitDimValue.offset, bytesPerDim);
            // Recurse on right sub-tree:
            System.arraycopy(cellMinPacked, 0, splitPackedValue, 0, packedBytesLength);
            System.arraycopy(splitDimValue.bytes, splitDimValue.offset, splitPackedValue, splitDim * bytesPerDim, bytesPerDim);
            index.pushRight();
            collectPostingLists(postingLists, splitPackedValue, cellMaxPacked, postingIndexName);
            index.pop();
        }

        private PostingList initFilteringPostingReader(long offset, FixedBitSet filter) throws IOException
        {
            final PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(sharedInput.sharedCopy(), offset);
            PostingsReader postingsReader = new PostingsReader(sharedInput.sharedCopy(),
                                                               summary,
                                                               listener.postingListEventListener(),
                                                               primaryKeyMap);
            return new FilteringPostingList(filter, postingsReader);
        }
    }

    public int getNumDimensions()
    {
        return numDims;
    }

    public int getBytesPerDimension()
    {
        return bytesPerDim;
    }

    public long getPointCount()
    {
        return pointCount;
    }

    /**
     * We recurse the BKD tree, using a provided instance of this to guide the recursion.
     */
    public interface IntersectVisitor
    {
        /**
         * Called for all values in a leaf cell that crosses the query.  The consumer
         * should scrutinize the packedValue to decide whether to accept it.  In the 1D case,
         * values are visited in increasing order, and in the case of ties, in increasing order
         * by segment row ID.
         */
        boolean visit(byte[] packedValue);

        /**
         * Called for non-leaf cells to test how the cell relates to the query, to
         * determine how to further recurse down the tree.
         */
        Relation compare(byte[] minPackedValue, byte[] maxPackedValue);
    }
}
