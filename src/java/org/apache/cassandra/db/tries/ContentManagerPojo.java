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

package org.apache.cassandra.db.tries;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.db.tries.InMemoryBaseTrie.REFERENCE_ARRAY_ON_HEAP_SIZE;
import static org.apache.cassandra.db.tries.InMemoryReadTrie.getBufferIdx;
import static org.apache.cassandra.db.tries.InMemoryReadTrie.inBufferOffset;
import static org.github.jamm.MemoryMeterStrategy.MEMORY_LAYOUT;

/// Content manager storing data in lists of java objects. Encoded objects are put in the maintained lists and are
/// mapped to negative content ids that encode the position in the list. This avoids taking up data cells for content,
/// but has to maintain the list of references.
///
/// Like [BufferManagerMultibuf], we use multiple lists that grow in size and can optionally recycle indexes.
public class ContentManagerPojo<T> implements ContentManager<T>
{
    // This is chosen to fit the largest number of content pointers we can have in a trie.
    // It takes at least 4 bytes to write pointer to one content thus the largest content index we can have is 1/2 G.
    static final int CONTENT_FLAGS_SHIFT = 29;
    static final int CONTENT_INDEX_MASK = (1 << CONTENT_FLAGS_SHIFT) - 1;

    static final int CONTENT_AFTER_BRANCH = 1 << CONTENT_FLAGS_SHIFT;

    static final int CONTENTS_START_SHIFT = 4;
    static final int CONTENTS_START_SIZE = 1 << CONTENTS_START_SHIFT;

    private int reservedCount = 0;
    private int valuesCount = 0;
    final AtomicReferenceArray<T>[] contentArrays;
    final Predicate<T> shouldPreserveWithoutChildren;
    final MemoryAllocationStrategy objectAllocator;

    /// Creates a new content manager with the given expected lifetime.
    /// Short-lived managers will not recycle cells as it is simpler to throw the whole thing away at the end of its
    /// lifecycle, while long-lived will track freed cells and will reuse them after the given opOrder indicates that
    /// all operations that may be using them have finished.
    ///
    /// @param shouldPreserveWithoutChildren Predicate used to check whether a given object should be preserved when
    /// its branch becomes empty. See [ContentManager#shouldPreserveWithoutChildren].
    public interface PojoSerializer<T> {
        void serialize(T content, DataOutputPlus out) throws IOException;
        T deserialize(DataInputPlus in) throws IOException;
        long serializedSize(T content);
    }

    public long serializedSize(PojoSerializer<T> serializer)
    {
        int count = reservedCount;
        long size = 4L; // 32-bit count of total reserved slots
        int bitsetLongs = (count + 63) >>> 6;
        size += (long) bitsetLongs * 8L; // 64-bit packed bitset footprint
        for (int w = 0; w < bitsetLongs; w++)
        {
            int base = w << 6;
            int limit = Math.min(base + 64, count);
            for (int i = base; i < limit; i++)
            {
                T content = getContent(i);
                if (content != null)
                    size += serializer.serializedSize(content);
            }
        }
        return size;
    }

    /**
     * Thread-local buffer to avoid heap allocations when serializing larger multi-word bitsets.
     */
    private static final ThreadLocal<long[]> BITSETS_THREAD_LOCAL = ThreadLocal.withInitial(() -> new long[64]);

    /**
     * Serializes the content manager using a sparse packed-bitset encoding.
     * To keep wire footprint small and serialization fast:
     * 1. We write total reserved slots count followed by 64-bit mask words indicating present items.
     * 2. Non-null content items are serialized sequentially using trailing zero bit manipulation
     *    to jump directly over empty slots without array scan overhead.
     */
    public void serialize(DataOutputPlus out, PojoSerializer<T> serializer) throws IOException
    {
        int count = reservedCount;
        out.writeInt(count);
        int bitsetLongs = (count + 63) >>> 6;

        // Fast-path for single 64-bit word bitsets (fits up to 64 items without heap allocations)
        if (bitsetLongs == 1)
        {
            long bits = 0;
            for (int i = 0; i < count; i++)
            {
                if (getContent(i) != null)
                    bits |= (1L << i);
            }
            out.writeLong(bits);

            long b = bits;
            while (b != 0)
            {
                int bitIdx = Long.numberOfTrailingZeros(b);
                T content = getContent(bitIdx);
                serializer.serialize(content, out);
                b &= (b - 1);
            }
            return;
        }

        // Multi-word bitset encoding using thread-local buffer to prevent per-call array allocations
        long[] bitsets = (bitsetLongs <= 64) ? BITSETS_THREAD_LOCAL.get() : new long[bitsetLongs];
        for (int w = 0; w < bitsetLongs; w++)
        {
            long bits = 0;
            int base = w << 6;
            int limit = Math.min(base + 64, count);
            for (int i = base; i < limit; i++)
            {
                if (getContent(i) != null)
                    bits |= (1L << (i - base));
            }
            bitsets[w] = bits;
            out.writeLong(bits);
        }

        // Write non-null content payloads using bit-scan jumps
        for (int w = 0; w < bitsetLongs; w++)
        {
            long bits = bitsets[w];
            int base = w << 6;
            while (bits != 0)
            {
                int bitIdx = Long.numberOfTrailingZeros(bits);
                int targetIndex = base + bitIdx;
                T content = getContent(targetIndex);
                serializer.serialize(content, out);
                bits &= (bits - 1);
            }
        }
    }

    /**
     * Deserializes a ContentManagerPojo from the input stream.
     * Allocates all required backing AtomicReferenceArrays up front to eliminate reallocation checks,
     * then decodes non-null content items via bitset trailing-zero scanning.
     */
    public static <T> ContentManagerPojo<T> deserialize(DataInputPlus in, Predicate<T> shouldPreserveWithoutChildren, InMemoryBaseTrie.ExpectedLifetime lifetime, OpOrder opOrder, PojoSerializer<T> serializer) throws IOException, TrieSpaceExhaustedException
    {
        ContentManagerPojo<T> cm = new ContentManagerPojo<>(shouldPreserveWithoutChildren, lifetime, opOrder);
        int count = in.readInt();
        if (count < 0 || count > 10_000_000)
            throw new IOException("Corrupt ContentManagerPojo reservedCount: " + count);

        cm.reservedCount = count;
        if (count > 0)
        {
            // Pre-allocate backing reference arrays up front based on target size
            int maxLeadBit = getBufferIdx(count - 1, CONTENTS_START_SHIFT, CONTENTS_START_SIZE);
            for (int b = 0; b <= maxLeadBit; b++)
            {
                cm.contentArrays[b] = new AtomicReferenceArray<>(CONTENTS_START_SIZE << b);
            }
        }

        // Read packed bitset mask
        int bitsetLongs = (count + 63) >>> 6;
        long[] bitset = new long[bitsetLongs];
        for (int w = 0; w < bitsetLongs; w++)
            bitset[w] = in.readLong();

        // Fast-path decode non-null elements using trailing zero bit counts
        for (int w = 0; w < bitsetLongs; w++)
        {
            long bits = bitset[w];
            int base = w << 6;
            while (bits != 0)
            {
                int bitIdx = Long.numberOfTrailingZeros(bits);
                int targetIndex = base + bitIdx;
                T content = serializer.deserialize(in);
                cm.valuesCount++;
                int leadBit = getBufferIdx(targetIndex, CONTENTS_START_SHIFT, CONTENTS_START_SIZE);
                int ofs = inBufferOffset(targetIndex, leadBit, CONTENTS_START_SIZE);
                cm.contentArrays[leadBit].setPlain(ofs, content);
                bits &= (bits - 1);
            }
        }
        return cm;
    }

    // Suppress warning required for generic array creation of contentArrays (AtomicReferenceArray<T>[])
    @SuppressWarnings("unchecked")
    public ContentManagerPojo(Predicate<T> shouldPreserveWithoutChildren,
                              InMemoryBaseTrie.ExpectedLifetime lifetime,
                              OpOrder opOrder)
    {
        this.contentArrays = new AtomicReferenceArray[CONTENT_FLAGS_SHIFT - CONTENTS_START_SHIFT];
        this.shouldPreserveWithoutChildren = shouldPreserveWithoutChildren;
        switch (lifetime)
        {
            case SHORT:
                objectAllocator = new MemoryAllocationStrategy.NoReuseStrategy(this::allocateNewObject);
                break;
            case LONG:
                objectAllocator = new MemoryAllocationStrategy.OpOrderReuseWithClearingStrategy(this::allocateNewObject, this::clearIndex, opOrder);
                break;
            default:
                throw new AssertionError();
        }
    }

    @Override
    public T getContent(int id)
    {
        int leadBit = getBufferIdx(id & CONTENT_INDEX_MASK, CONTENTS_START_SHIFT, CONTENTS_START_SIZE);
        int ofs = inBufferOffset(id & CONTENT_INDEX_MASK, leadBit, CONTENTS_START_SIZE);
        AtomicReferenceArray<T> array = contentArrays[leadBit];
        return array.get(ofs);
    }

    @Override
    public boolean shouldPresentAfterBranch(int contentId)
    {
        return (contentId & CONTENT_AFTER_BRANCH) != 0;
    }

    @Override
    public boolean shouldPreserveWithoutChildren(int contentId)
    {
        if (shouldPreserveWithoutChildren == null)
            return true;

        return shouldPreserveWithoutChildren.test(getContent(contentId));
    }

    @Override
    public String dumpContentId(int id)
    {
        return "~" + (id & CONTENT_INDEX_MASK) + ((id & CONTENT_AFTER_BRANCH) != 0 ? "↑" : "");
    }

    @Override
    public int cellOrObjectSlotUsed(int id)
    {
        return ~(id & CONTENT_INDEX_MASK);
    }

    /// Allocate a new position in the object array. Called by the memory allocation strategy to allocate a content spot
    /// when it runs out of recycled positions.
    private int allocateNewObject() throws TrieSpaceExhaustedException
    {
        int index = reservedCount++;
        if ((index & CONTENT_INDEX_MASK) != index)
            throw new TrieSpaceExhaustedException();

        int leadBit = getBufferIdx(index, CONTENTS_START_SHIFT, CONTENTS_START_SIZE);
        AtomicReferenceArray<T> array = contentArrays[leadBit];
        if (array == null)
        {
            assert inBufferOffset(index, leadBit, CONTENTS_START_SIZE) == 0 : "Error in content arrays configuration.";
            contentArrays[leadBit] = new AtomicReferenceArray<>(CONTENTS_START_SIZE << leadBit);
        }
        return index;
    }

    private void clearIndex(int index)
    {
        int leadBit = getBufferIdx(index, CONTENTS_START_SHIFT, CONTENTS_START_SIZE);
        int ofs = inBufferOffset(index, leadBit, CONTENTS_START_SIZE);
        AtomicReferenceArray<T> array = contentArrays[leadBit];
        array.lazySet(ofs, null);
    }

    @Override
    public int addContent(T value, boolean contentAfterBranch) throws TrieSpaceExhaustedException
    {
        ++valuesCount;
        int index = objectAllocator.allocate();
        int leadBit = getBufferIdx(index, CONTENTS_START_SHIFT, CONTENTS_START_SIZE);
        int ofs = inBufferOffset(index, leadBit, CONTENTS_START_SIZE);
        AtomicReferenceArray<T> array = contentArrays[leadBit];
        // no need for a volatile set here; at this point the item is not referenced
        // by any node in the trie, and a volatile set will be made to reference it.
        array.setPlain(ofs, value);
        return formContentId(index, contentAfterBranch);
    }

    private int formContentId(int index, boolean contentAfterBranch)
    {
        return index | (1 << 31) | (contentAfterBranch ? CONTENT_AFTER_BRANCH : 0);
    }

    @Override
    public int setContent(int id, T value)
    {
        int leadBit = getBufferIdx(id & CONTENT_INDEX_MASK, CONTENTS_START_SHIFT, CONTENTS_START_SIZE);
        int ofs = inBufferOffset(id & CONTENT_INDEX_MASK, leadBit, CONTENTS_START_SIZE);
        AtomicReferenceArray<T> array = contentArrays[leadBit];
        array.set(ofs, value);
        return id;
    }

    @Override
    public void releaseContent(int id)
    {
        --valuesCount;
        objectAllocator.recycle(id & CONTENT_INDEX_MASK);
    }

    @Override
    public void completeMutation()
    {
        objectAllocator.completeMutation();
    }

    @Override
    public void abortMutation()
    {
        objectAllocator.abortMutation();
    }

    @Override
    public long usedSizeOffHeap()
    {
        return 0;
    }

    @Override
    public long usedSizeOnHeap()
    {
        return usedObjectSpace() +
               REFERENCE_ARRAY_ON_HEAP_SIZE * getBufferIdx(reservedCount, CONTENTS_START_SHIFT, CONTENTS_START_SIZE);
    }

    @VisibleForTesting
    long usedObjectSpace()
    {
        return valuesCount() * MEMORY_LAYOUT.getReferenceSize();
    }

    @Override
    @VisibleForTesting
    public long unusedReservedOnHeapMemory()
    {
        long bufferOverhead = 0;

        int index = reservedCount;
        int leadBit = getBufferIdx(index, CONTENTS_START_SHIFT, CONTENTS_START_SIZE);
        int ofs = inBufferOffset(index, leadBit, CONTENTS_START_SIZE);
        AtomicReferenceArray<T> contentArray = contentArrays[leadBit];
        long contentOverhead = ((contentArray != null ? contentArray.length() : 0) - ofs);
        contentOverhead += reservedCount - valuesCount;
        contentOverhead *= MEMORY_LAYOUT.getReferenceSize();

        return bufferOverhead + contentOverhead;
    }

    @Override
    @VisibleForTesting
    public void releaseReferencesUnsafe()
    {
        for (int idx : objectAllocator.indexesInPipeline())
            setContent(formContentId(idx, false), null);
    }

    @Override
    public int valuesCount()
    {
        return valuesCount;
    }

    @VisibleForTesting
    IntArrayList collectReleasedUnclearedContentIndexes()
    {
        objectAllocator.forceReferenceClearing();

        IntArrayList list = objectAllocator.indexesInPipeline();
        IntArrayList result = new IntArrayList();
        for (int idx : list)
        {
            if (getContent(formContentId(idx, false)) != null)
                result.addInt(idx);
        }
        return result;
    }

    @VisibleForTesting
    int getAllocatedPos()
    {
        return reservedCount;
    }
}
