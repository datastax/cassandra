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
package org.apache.cassandra.index.sai.disk.v1.bitpack;

import com.carrotsearch.hppc.IntObjectHashMap;
import org.apache.cassandra.index.sai.disk.io.IndexInput;
import org.apache.cassandra.index.sai.disk.oldlucene.LuceneCompat;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.utils.SeekingRandomAccessInput;
import org.apache.lucene.util.LongValues;

public abstract class AbstractBlockPackedReader implements LongArray
{
    private final int blockShift;
    private final int blockMask;
    private final int blockSize;
    private final long valueCount;
    final byte[] blockBitsPerValue; // package protected for test access
    private final SeekingRandomAccessInput input;
    private final IntObjectHashMap<LongValues> readers;

    private long prevTokenValue = Long.MIN_VALUE;
    private long lastIndex; // the last index visited by token -> row ID searches

    AbstractBlockPackedReader(IndexInput indexInput, byte[] blockBitsPerValue, int blockShift, int blockMask, long sstableRowId, long valueCount)
    {
        this.blockShift = blockShift;
        this.blockMask = blockMask;
        this.blockSize = blockMask + 1;
        this.valueCount = valueCount;
        this.input = new SeekingRandomAccessInput(indexInput);
        this.blockBitsPerValue = blockBitsPerValue;
        this.readers = new IntObjectHashMap<>();
        // start searching tokens from current index segment
        this.lastIndex = sstableRowId;
    }

    protected abstract long blockOffsetAt(int block);

    @Override
    public long get(final long index)
    {
        if (index < 0 || index >= valueCount)
        {
            throw new IndexOutOfBoundsException(String.format("Index should be between [0, %d), but was %d.", valueCount, index));
        }

        final int block = (int) (index >>> blockShift);
        final int idx = (int) (index & blockMask);
        return delta(block, idx) + getReader(block).get(idx);
    }

    private LongValues getReader(int block)
    {
        LongValues reader = readers.get(block);
        if (reader == null)
        {
            reader = blockBitsPerValue[block] == 0 ? LongValues.ZEROES
                                                   : LuceneCompat.directReaderGetInstance(input, blockBitsPerValue[block], blockOffsetAt(block));
            readers.put(block, reader);
        }
        return reader;
    }

    @Override
    public long ceilingRowId(long targetValue)
    {
        // already out of range
        if (isOutOfRangeState())
            return -1;

        long rowId = findBlockRowId(targetValue);
        lastIndex = rowId >= 0 ? rowId : -rowId - 1;
        return isOutOfRangeState() ? -1 : lastIndex;
    }

    @Override
    public long indexOf(long targetValue)
    {
        // already out of range
        if (isOutOfRangeState())
            return Long.MIN_VALUE;

        long rowId = findBlockRowId(targetValue);
        lastIndex = rowId >= 0 ? rowId : -rowId - 1;
        return isOutOfRangeState() ? Long.MIN_VALUE : rowId;
    }

    private boolean isOutOfRangeState()
    {
        return lastIndex >= valueCount;
    }

    /**
     * Find the row ID of the largest value less than or equal to the target value.
     * This is the floor operation, complementary to ceilingRowId.
     *
     * @param targetValue Value to search for
     * @return The row ID of the floor value, or -1 if the target is smaller than all values
     */
    public long floorRowId(long targetValue)
    {
        // Check if we're before the start of the array
        if (targetValue < get(0))
            return -1;

        return findBlockRowIdForFloor(targetValue);
    }

    /**
     * Find the block and row ID for floor operation.
     * Similar to findBlockRowId but searches for the largest value <= target.
     */
    private long findBlockRowIdForFloor(long targetValue)
    {
        int blockIndex = binarySearchBlockMaxValues(targetValue);

        // blockIndex is now the block that might contain our floor value
        // We need to search within this block (and potentially the next if exact match on block boundary)

        if (blockIndex < 0)
        {
            // Convert negative index to actual block position
            blockIndex = -blockIndex - 1;
        }

        // Search for the floor value within the identified block
        return findBlockRowIDForFloor(targetValue, blockIndex);
    }

    /**
     * Binary search block max values to find the block containing the floor.
     * Searches the last value of each block (block max) to determine which block
     * could contain the largest value <= target.
     *
     * @return positive block index for exact match on block max, negative for non-exact match
     */
    private int binarySearchBlockMaxValues(long targetValue)
    {
        int low = 0;
        int high = Math.toIntExact(blockBitsPerValue.length) - 1;

        while (low <= high)
        {
            int mid = low + ((high - low) >> 1);

            // Get the last value in this block (block max)
            long blockOffset = (long) mid << blockShift;
            long lastIdxInBlock = Math.min(blockOffset + blockSize - 1, valueCount - 1);
            long maxVal = get(lastIdxInBlock);

            if (maxVal < targetValue)
            {
                low = mid + 1;
            }
            else if (maxVal > targetValue)
            {
                high = mid - 1;
            }
            else
            {
                // Exact match on block max - but there might be duplicates in the next block
                if (mid < blockBitsPerValue.length - 1)
                {
                    long nextBlockOffset = (long) (mid + 1) << blockShift;
                    if (nextBlockOffset < valueCount && get(nextBlockOffset) == targetValue)
                    {
                        // Duplicates exist in the next block, search there
                        low = mid + 1;
                        continue;
                    }
                }
                return mid;
            }
        }

        // Return the block where floor value should be
        // high is now the last block with max value < target
        return -(high + 1);
    }

    /**
     * Find the floor row ID within a specific block.
     */
    private long findBlockRowIDForFloor(long targetValue, int blockIdx)
    {
        if (blockIdx < 0)
            return -1; // Target is smaller than all values

        // Calculate the global offset for the selected block
        long offset = (long) blockIdx << blockShift;

        // Search from start of block to the end of block
        long high = Math.min(offset + blockSize - 1, valueCount - 1);
        return binarySearchBlockForFloor(targetValue, offset, high);
    }

    /**
     * Binary search for floor value between low and high indices.
     *
     * @return index of the largest value <= target, or -1 if all values > target
     */
    private long binarySearchBlockForFloor(long target, long low, long high)
    {
        long result = -1; // Track the best floor candidate found so far

        while (low <= high)
        {
            long mid = low + ((high - low) >> 1);
            long midVal = get(mid);

            if (midVal <= target)
            {
                // This could be our floor, but there might be a larger one further right
                result = mid;
                low = mid + 1;
            }
            else
            {
                high = mid - 1;
            }
        }

        return result;
    }

    private long findBlockRowId(long targetValue)
    {
        // We keep track previous returned value in lastIndex, so searching backward will not return correct result.
        // Also it's logically wrong to search backward during token iteration in PostingListKeyRangeIterator.
        if (targetValue < prevTokenValue)
            throw new IllegalArgumentException(String.format("%d is smaller than prev token value %d", targetValue, prevTokenValue));
        prevTokenValue = targetValue;

        int blockIndex = binarySearchBlockMinValues(targetValue);

        // We need to check next block's min value on an exact match.
        boolean exactMatch = blockIndex >= 0;

        if (blockIndex < 0)
        {
            // A non-exact match, which is the negative index of the first value greater than the target.
            // For example, searching for 4 against min values [3,3,5,7] produces -2, which we convert to 2.
            blockIndex = -blockIndex;
        }

        if (blockIndex > 0)
        {
            // Start at the previous block, because there could be duplicate values in the previous block.
            // For example, with block 1: [1,2,3,3] & block 2: [3,3,5,7], binary search for 3 would find
            // block 2, but we need to start from block 1 and search both.
            // In case non-exact match, we need to pivot left as target is less than next block's min.
            blockIndex--;
        }

        // Find the global (not block-specific) index of the target token, which is equivalent to its row ID:
        return findBlockRowID(targetValue, blockIndex, exactMatch);
    }

    /**
     *
     * @return a positive block index for an exact match, or a negative one for a non-exact match
     */
    private int binarySearchBlockMinValues(long targetValue)
    {
        int high = Math.toIntExact(blockBitsPerValue.length) - 1;

        // Assume here that we'll never move backward through the blocks:
        int low = Math.toIntExact(lastIndex >> blockShift);

        // Short-circuit the search if the target is in current block:
        if (low + 1 <= high)
        {
            long cmp = Long.compare(targetValue, delta(low + 1, 0));

            if (cmp == 0)
            {
                // We have an exact match, so return the index of the next block, which means we'll start
                // searching from the current one and also inspect the first value of the next block.
                return low + 1;
            }
            else if (cmp < 0)
            {
                // We're in the same block. Indicate a non-exact match, and this value will be both
                // negated and then decremented to wind up at the current value of "low" here.
                return -low - 1;
            }

            // The target is greater than the next block's min value, so advance to that
            // block before starting the usual search...
            low++;
        }

        while (low <= high)
        {
            int mid = low + ((high - low) >> 1);

            long midVal = delta(mid, 0);

            if (midVal < targetValue)
            {
                low = mid + 1;
            }
            else if (midVal > targetValue)
            {
                high = mid - 1;
            }
            else
            {
                // target found, but we need to check for duplicates
                if (mid > 0 && delta(mid - 1, 0) == targetValue)
                {
                    // there are duplicates, pivot left
                    high = mid - 1;
                }
                else
                {
                    // no duplicates
                    return mid;
                }
            }
        }

        return -low; // no exact match found
    }

    private long findBlockRowID(long targetValue, long blockIdx, boolean exactMatch)
    {
        // Calculate the global offset for the selected block:
        long offset = blockIdx << blockShift;

        // Resume from previous index if it's larger than offset
        long low = Math.max(lastIndex, offset);

        // The high is either the last local index in the block, or something smaller if the block isn't full:
        long high = Math.min(offset + blockSize - 1 + (exactMatch ? 1 : 0), valueCount - 1);

        return binarySearchBlock(targetValue, low, high);
    }

    /**
     * binary search target value between low and high.
     *
     * @return index if exact match is found, or `-(insertion point) - 1` if no exact match is found.
     */
    private long binarySearchBlock(long target, long low, long high)
    {
        while (low <= high)
        {
            long mid = low + ((high - low) >> 1);

            long midVal = get(mid);

            if (midVal < target)
            {
                low = mid + 1;
                // future rowId cannot be smaller than mid as long as next token not smaller than current token.
                lastIndex = mid;
            }
            else if (midVal > target)
            {
                high = mid - 1;
            }
            else
            {
                // target found, but we need to check for duplicates
                if (mid > 0 && get(mid - 1) == target)
                {
                    // there are duplicates, pivot left
                    high = mid - 1;
                }
                else
                {
                    // exact match and no duplicates
                    return mid;
                }
            }
        }

        // target not found
        return -(low + 1);
    }

    @Override
    public long length()
    {
        return valueCount;
    }

    abstract long delta(int block, int idx);
}
