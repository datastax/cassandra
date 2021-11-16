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

package org.apache.cassandra.index.sai.disk.v2.sortedterms;

import java.io.IOException;
import java.util.Iterator;

import org.apache.cassandra.index.sai.disk.io.IndexInputReader;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.lucene.store.ByteArrayIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectMonotonicReader;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Preconditions;

import static org.apache.cassandra.index.sai.disk.v2.sortedterms.SortedTermsWriter.DIRECT_MONOTONIC_BLOCK_SHIFT;
import static org.apache.cassandra.index.sai.disk.v2.sortedterms.SortedTermsWriter.TERMS_DICT_BLOCK_MASK;
import static org.apache.cassandra.index.sai.disk.v2.sortedterms.SortedTermsWriter.TERMS_DICT_BLOCK_SHIFT;

/**
 * Provides read access to a sorted on-disk sequence of terms.
 * <p>
 * Offers the following features:
 * <ul>
 *     <li>forward iterating over all terms sequentially with a cursor</li>
 *     <li>constant-time look up of the term at a given point id</li>
 *     <li>log-time lookup of the point id of a term</li>
 * </ul>
 * <p>
 * Care has been taken to make this structure as efficient as possible.
 * Reading terms does not require allocating data heap buffers per each read operation.
 * Only one term at a time is loaded to memory.
 * Low complexity algorithms are used – a lookup of the term by point id is constant time,
 * and a lookup of the point id by the term is logarithmic.
 *
 * <p>
 * Because the blocks are prefix compressed, random access applies only to the locating the whole block.
 * In order to jump to a concrete term inside the block, the block terms are iterated from the block beginning.
 * Expect random access by {@link Cursor#seekToPointId(long)} to be slower
 * than just moving to the next term with {@link Cursor#advance()}.
 * <p>
 * For documentation of the underlying on-disk data structures, see the package documentation.
 *
 * @see SortedTermsWriter
 * @see org.apache.cassandra.index.sai.disk.v2.sortedterms
 */
@ThreadSafe
public class SortedTermsReader
{
    private final FileHandle termsData;
    private final SortedTermsMeta meta;
    private final FileHandle termsTrie;
    private final LongValues blockOffsets;

    /**
     * Creates a new reader based on its data components.
     * <p>
     * It does not own the components, so you must close them separately after you're done with the reader.
     * @param termsData handle to the file with a sequence of prefix-compressed blocks
     *                  each storing a fixed number of terms
     * @param termsDataBlockOffsets file containing an encoded sequence of the file offsets pointing to the blocks
     * @param termsTrie handle to the file storing the trie with the term-to-point-id mapping
     * @param meta metadata object created earlier by the writer
     */
    public SortedTermsReader(@Nonnull FileHandle termsData,
                             @Nonnull IndexInput termsDataBlockOffsets,
                             @Nonnull FileHandle termsTrie,
                             @Nonnull SortedTermsMeta meta) throws IOException
    {
        this.termsData = termsData;
        this.termsTrie = termsTrie;
        this.meta = meta;

        ByteArrayIndexInput offsestMetaInput = new ByteArrayIndexInput("", meta.offsetMetaBytes);

        DirectMonotonicReader.Meta offsetsMeta =
                DirectMonotonicReader.loadMeta(offsestMetaInput, meta.offsetBlockCount, DIRECT_MONOTONIC_BLOCK_SHIFT);
        RandomAccessInput offsetSlice = termsDataBlockOffsets.randomAccessSlice(0, termsDataBlockOffsets.length());
        this.blockOffsets = DirectMonotonicReader.getInstance(offsetsMeta, offsetSlice);
    }

    /**
     * Returns the point id (ordinal) of the target term or the next greater if no exact match found.
     * If reached the end of the terms file, returns <code>Long.MAX_VALUE</code>.
     * Complexity of this operation is O(log n).
     *
     * @param term target term to lookup
     */
    public long getPointId(@Nonnull ByteComparable term)
    {
        Preconditions.checkNotNull(term, "term null");

        try (TrieRangeIterator reader = new TrieRangeIterator(termsTrie.instantiateRebufferer(),
                                                              meta.trieFP,
                                                              term,
                                                              null,
                                                              true,
                                                              true))
        {
            final Iterator<Pair<ByteSource, Long>> iterator = reader.iterator();
            return iterator.hasNext() ? iterator.next().right : Long.MAX_VALUE;
        }
    }

    /**
     * Returns the total number of terms.
     */
    public long count()
    {
        return meta.count;
    }

    /**
     * Opens a cursor over the terms stored in the terms file.
     * <p>
     * This does not read any data yet.
     * The cursor is initially positioned before the first item.
     * <p>
     * The cursor is to be used in a single thread.
     * The cursor is valid as long this object hasn't been closed.
     * You must close the cursor when you no longer need it.
     */
    public @Nonnull Cursor openCursor() throws IOException
    {
        return new Cursor(termsData);
    }

    /**
     * Allows reading the terms from the terms file.
     * Can quickly seek to a random term by <code>pointId</code>.
     * <p>
     * This object is stateful and not thread safe.
     * It maintains a position to the current term as well as a buffer that can hold one term.
     */
    @NotThreadSafe
    public class Cursor implements AutoCloseable
    {
        private final IndexInputReader termsData;

        // The term the cursor currently points to. Initially empty.
        private final BytesRef currentTerm;

        // The point id the cursor currently points to. -1 means before the first item.
        private long pointId = -1;

        Cursor(FileHandle termsData)
        {
            this.termsData = IndexInputReader.create(termsData);
            this.currentTerm = new BytesRef(meta.maxTermLength);
        }

        /**
         * Returns the number of terms
         */
        public long count()
        {
            return SortedTermsReader.this.count();
        }

        /**
         * Returns the current position of the cursor.
         * Initially, before the first call to {@link Cursor#advance}, the cursor is positioned at -1.
         * After reading all the items, the cursor is positioned at index one
         * greater than the position of the last item.
         */
        public long pointId()
        {
            return pointId;
        }

        /**
         * Returns the current term data as <code>ByteComparable</code> referencing the internal term buffer.
         * The term data stored behind that reference is valid only until the next call to
         * {@link Cursor#advance} or {@link Cursor#seekToPointId(long)}.
         */
        public @Nonnull ByteComparable term()
        {
            return ByteComparable.fixedLength(currentTerm.bytes, currentTerm.offset, currentTerm.length);
        }

        /**
         * Advances the cursor to the next term and reads it into the current term buffer.
         * <p>
         * If there are no more available terms, clears the term buffer and the cursor's position will point to the
         * one behind the last item.
         * <p>
         * This method has constant time complexity.
         *
         * @return true if the cursor was advanced successfully, false if the end of file was reached
         * @throws IOException if a read from the terms file fails
         */
        public boolean advance() throws IOException
        {
            if (pointId >= meta.count || ++pointId >= meta.count)
            {
                currentTerm.length = 0;
                return false;
            }

            int prefixLength;
            int suffixLength;
            if ((pointId & TERMS_DICT_BLOCK_MASK) == 0L)
            {
                prefixLength = 0;
                suffixLength = termsData.readVInt();
            }
            else
            {
                final int token = Byte.toUnsignedInt(termsData.readByte());
                prefixLength = token & 0x0F;
                suffixLength = 1 + (token >>> 4);
                if (prefixLength == 15)
                    prefixLength += termsData.readVInt();
                if (suffixLength == 16)
                    suffixLength += termsData.readVInt();
            }

            assert prefixLength + suffixLength <= meta.maxTermLength;
            currentTerm.length = prefixLength + suffixLength;
            termsData.readBytes(currentTerm.bytes, prefixLength, suffixLength);
            return true;
        }

        /**
         * Positions the cursor on the target point id and reads the term at target to the current term buffer.
         * <p>
         * It is allowed to position the cursor before the first item or after the last item;
         * in these cases the internal buffer is cleared.
         * <p>
         * This method has constant complexity.
         *
         * @param target point id to lookup
         * @throws IOException if a seek and read from the terms file fails
         * @throws IndexOutOfBoundsException if the target point id is less than -1 or greater than {@link Cursor#count}.
         */
        public void seekToPointId(long target) throws IOException
        {
            if (target < -1 || target > meta.count)
                throw new IndexOutOfBoundsException();

            if (target == -1 || target == meta.count)
            {
                termsData.seek(0);   // matters only if target is -1
                pointId = target;
                currentTerm.length = 0;
            }
            else
            {
                final long blockIndex = target >>> TERMS_DICT_BLOCK_SHIFT;
                final long blockAddress = blockOffsets.get(blockIndex);
                termsData.seek(blockAddress);
                pointId = (blockIndex << TERMS_DICT_BLOCK_SHIFT) - 1;
                while (pointId < target)
                {
                    boolean advanced = advance();
                    assert advanced : "unexpected eof";   // must return true because target is in range
                }
            }
        }

        /**
         * Resets the cursor to its initial position before the first item.
         */
        public void reset() throws IOException
        {
            seekToPointId(-1);
        }

        @Override
        public void close() throws IOException
        {
            this.termsData.close();
        }

    }
}
