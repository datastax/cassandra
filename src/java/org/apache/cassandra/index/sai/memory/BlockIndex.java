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

package org.apache.cassandra.index.sai.memory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.primitives.UnsignedBytes;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.index.sasi.memory.SkipListMemIndex;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;

/**
 * A block sorted primary key -> values map that implements
 * @see MemoryIndex
 *
 * Added rows are tokenized and BytesRef objects added to a BytesRefArray byte block buffer.
 *
 * Primary keys are maintined in sorted order.
 *
 */
@NotThreadSafe // #add is not locked, BlockIndex should be used by MultiBlockIndex only
class BlockIndex extends MemoryIndex
{
    private static final Logger logger = LoggerFactory.getLogger(BlockIndex.class);

    // offheap bytes primary key -> offheap bytes attached values
    private final ConcurrentSkipListMap<OffheapBytes,Values> map = new ConcurrentSkipListMap<>();
    private final AbstractAnalyzer.AnalyzerFactory analyzerFactory;
    private final AbstractType<?> validator;
    private final boolean isLiteral;
    private final MultiBlockIndex parent;
    private final AtomicInteger maxTermLength = new AtomicInteger();
    private final OffheapBytesArray bytesArray = new OffheapBytesArray();
    private final BytesRef addBytesBuffer = new BytesRef(new byte[50]);
    private final AtomicReference<PrimaryKey> minKeyRef = new AtomicReference<>();
    private final AtomicReference<PrimaryKey> maxKeyRef = new AtomicReference<>();
    private final AtomicInteger count = new AtomicInteger(0);

    public BlockIndex(IndexContext indexContext, MultiBlockIndex parent)
    {
        super(indexContext);
        this.analyzerFactory = indexContext.getAnalyzerFactory();
        this.validator = indexContext.getValidator();
        this.isLiteral = TypeUtil.isLiteral(validator);
        this.parent = parent;
    }

    @VisibleForTesting
    public BlockIndex(IndexContext indexContext)
    {
        super(indexContext);
        this.analyzerFactory = indexContext.getAnalyzerFactory();
        this.validator = indexContext.getValidator();
        this.isLiteral = TypeUtil.isLiteral(validator);
        this.parent = null;
    }

    public int maxTermLength()
    {
        return maxTermLength.intValue();
    }

    /**
     * @return Number of value objects
     */
    public int count()
    {
        return count.intValue();
    }

    /**
     * Execute a range query on the block index.
     *
     * Thread safe
     *
     * @param expression Query
     * @param keyRange Key range
     * @return Matches range iterator
     */
    @Override
    public RangeIterator search(Expression expression, AbstractBounds<PartitionPosition> keyRange)
    {
        if (logger.isTraceEnabled())
            logger.trace("Searching memtable index on expression '{}'...", expression);

        switch (expression.getOp())
        {
            case MATCH:
            case EQ:
            case CONTAINS_KEY:
            case CONTAINS_VALUE:
            case RANGE:
                // use range match for all query types
                return rangeMatch(expression, keyRange);
            default:
                throw new IllegalArgumentException("Unsupported expression: " + expression);
        }
    }

    /**
     * Add a key and value to the block index.
     *
     * Single threaded.
     *
     * @param key Partition key
     * @param clustering Clustering key
     * @param value Index value
     * @return Bytes memory used
     */
    @Override
    public void add(DecoratedKey key, Clustering clustering, ByteBuffer value, LongConsumer onHeapAllocationsTracker, LongConsumer offHeapAllocationsTracker)
    {
        if (this.parent != null && !Thread.holdsLock(this.parent.writeLock))
            throw new IllegalStateException("The add method must have a lock.");

        final AbstractAnalyzer analyzer = analyzerFactory.create();
        try
        {
            value = TypeUtil.encode(value, validator);
            analyzer.reset(value.duplicate());
            final PrimaryKey primaryKey = indexContext.keyFactory().create(key, clustering);

            setMinMaxPrimaryKey(primaryKey);

            final BytesRef keyBytesRef = readBytes(primaryKey.asComparableBytes(ByteComparable.Version.OSS41), addBytesBuffer);

            final OffheapBytes offheapBytes = bytesArray.add(keyBytesRef);

            final MutableObject<Values> createdValues = new MutableObject<>(null);

            final Values values = map.computeIfAbsent(offheapBytes, (k) -> {
                Values values2 = new Values();
                createdValues.setValue(values2);
                return values2;
            });

            long valueBytesUsed = createdValues.getValue() == values ? SkipListMemIndex.CSLM_OVERHEAD + 128 : 0;

            while (analyzer.hasNext())
            {
                final ByteBuffer term = analyzer.next();

                setMinMaxTerm(term.duplicate());

                final ByteComparable encodedTerm = encode(term.duplicate());

                final BytesRef encodedTermBytes = readBytes(encodedTerm.asComparableBytes(ByteComparable.Version.OSS41), addBytesBuffer);

                final OffheapBytes offheapEncodedTerm = bytesArray.add(encodedTermBytes);

                // ensure the value set is the maximum
                // although because MultiBlockIndex has a monitor lock on #add
                // there should never be contention here
                final int newMaxLength = maxTermLength.accumulateAndGet(encodedTermBytes.length, Math::max);

                assert newMaxLength >= encodedTermBytes.length;

                valueBytesUsed += encodedTermBytes.length;

                values.values.add(offheapEncodedTerm);

                count.incrementAndGet();
            }

            onHeapAllocationsTracker.accept(valueBytesUsed + key.getKeyLength() + clustering.dataSize() + 64);
        }
        finally
        {
            analyzer.end();
        }
    }

    @Override
    public void setMinMaxTerm(ByteBuffer term)
    {
        if (parent != null)
            parent.setMinMaxTerm(term.duplicate());

        super.setMinMaxTerm(term);
    }

    public static int length(ByteComparable term)
    {
        int count = 0;
        ByteSource byteSource = term.asComparableBytes(ByteComparable.Version.OSS41);
        while ((byteSource.next()) != ByteSource.END_OF_STREAM)
            count++;
        return count;
    }

    @Override
    public Iterator<Pair<ByteComparable, Iterable<ByteComparable>>> iterator()
    {
        throw new UnsupportedOperationException();
    }

    void gatherAllValuesAndKeysBytes(OffheapBytes[] values, OffheapBytes[] keys, MutableInt index)
    {
        for (Map.Entry<OffheapBytes, Values> entry : map.entrySet())
        {
            for (OffheapBytes value : entry.getValue().values)
            {
                values[index.intValue()] = value;
                keys[index.intValue()] = entry.getKey();
                index.getAndIncrement();
            }
        }
    }

    private static class Values
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new Values());

        public final Queue<OffheapBytes> values = new ConcurrentLinkedQueue<>();

        public void add(OffheapBytes value)
        {
            this.values.add(value);
        }

        public long unsharedHeapSize()
        {
            return EMPTY_SIZE;
        }
    }

    private static class ByteCompObject implements Comparable<ByteCompObject>
    {
        protected final ByteComparable bytes;

        public ByteCompObject(ByteComparable bytes)
        {
            this.bytes = bytes;
        }

        @Override
        public int compareTo(ByteCompObject o)
        {
            return ByteComparable.compare(bytes, o.bytes, ByteComparable.Version.OSS41);
        }
    }

    boolean contains(Expression expression)
    {
        ByteComparable lowerBound = null, upperBound = null;
        boolean lowerInclusive = false, upperInclusive = false;
        if (expression.lower != null)
        {
            lowerBound = encode(expression.lower.value.encoded);
            lowerInclusive = expression.lower.inclusive;
            if (!lowerInclusive)
            {
                lowerBound = nudge(lowerBound);
            }
        }

        if (expression.upper != null)
        {
            upperBound = encode(expression.upper.value.encoded);
            upperInclusive = expression.upper.inclusive;
            final int maxTermLen = maxTermLength.get();
            if (!upperInclusive && maxTermLen > 0)
                upperBound = nudgeReverse(upperBound, maxTermLen);
        }

        final ByteComparable encodedMinTerm = encode(getMinTerm());
        final ByteComparable encodedMaxTerm = encode(getMaxTerm());

        final Range<ByteCompObject> range = Range.between(new ByteCompObject(encodedMinTerm), new ByteCompObject(encodedMaxTerm));

        if (lowerBound == null)
            lowerBound = encodedMinTerm;

        if (upperBound == null)
            upperBound = encodedMaxTerm;

        return range.isOverlappedBy(Range.between(new ByteCompObject(lowerBound), new ByteCompObject(upperBound)));
    }

    public static BytesRef readBytes(ByteSource byteSource, BytesRef buffer)
    {
        int readBytes = 0;
        int data;
        while ((data = byteSource.next()) != ByteSource.END_OF_STREAM)
        {
            if (readBytes >= buffer.length - 1)
                buffer.bytes = ArrayUtil.grow(buffer.bytes, readBytes + 1);
            buffer.bytes[readBytes++] = (byte) data;
        }

        buffer.offset = 0;
        buffer.length = readBytes;

        return buffer;
    }

    // NOTE: key range is not implemented because StorageAttachedIndexSearcher.ResultRetriever#computeNext :
    //          if (current.contains(currentKey.partitionKey()))
    //       does not work because only a Token is available from:
    //          AbstractBounds<PartitionPosition> keyRange
    private RangeIterator rangeMatch(final Expression expression, final AbstractBounds<PartitionPosition> keyRange)
    {
        OffheapBytes lowerBound, upperBound;
        boolean lowerInclusive, upperInclusive;
        if (expression.lower != null)
        {
            ByteComparable lowerBoundBC = encode(expression.lower.value.encoded);
            lowerBound = new OffheapBytes(readBytes(lowerBoundBC.asComparableBytes(ByteComparable.Version.OSS41), new BytesRef()));
            lowerInclusive = expression.lower.inclusive;
        }
        else
        {
            lowerBound = new OffheapBytes(new BytesRef(BytesRef.EMPTY_BYTES));
            lowerInclusive = false;
        }

        if (expression.upper != null)
        {
            ByteComparable upperBoundBC = encode(expression.upper.value.encoded);
            upperBound = new OffheapBytes(readBytes(upperBoundBC.asComparableBytes(ByteComparable.Version.OSS41), new BytesRef()));
            upperInclusive = expression.upper.inclusive;
        }
        else
        {
            upperBound = null;
            upperInclusive = false;
        }

        final FilteredIteratorFactory factory = (final PrimaryKey fromKey) -> {
            final byte[] fromKeyBytes = ByteSourceInverse.readBytes(fromKey.asComparableBytes(ByteComparable.Version.OSS41));

            OffheapBytes fromKeyOffheapBytes = new OffheapBytes(new BytesRef(fromKeyBytes));

            final Iterator<Map.Entry<OffheapBytes, Values>> iterator = map.tailMap(fromKeyOffheapBytes).entrySet().iterator();

            return Iterators.filter(iterator, (Map.Entry<OffheapBytes, Values> entry) -> {
                final Values values = entry.getValue();

                // iterate for a match
                for (final OffheapBytes value : values.values)
                {
                    boolean lowerMatch = false;
                    boolean upperMatch = false;

                    if (lowerInclusive && value.compareTo(lowerBound) >= 0)
                        lowerMatch = true;
                    else if (!lowerInclusive && value.compareTo(lowerBound) > 0)
                        lowerMatch = true;

                    if (upperBound == null)
                        upperMatch = true;
                    else if (upperInclusive && value.compareTo(upperBound) <= 0)
                        upperMatch = true;
                    else if (!upperInclusive && value.compareTo(upperBound) < 0)
                        upperMatch = true;

                    if (lowerMatch && upperMatch)
                        return true;
                }
                return false;
            });
        };

        return new ValuesRangeIterator(minKeyRef.get(),
                                       maxKeyRef.get(),
                                       10, // the count is not accurate, does not seem to matter
                                       factory);
    }

    protected interface FilteredIteratorFactory
    {
        Iterator<Map.Entry<OffheapBytes, Values>> create(PrimaryKey fromKey);
    }

    protected class ValuesRangeIterator extends RangeIterator
    {
        private final FilteredIteratorFactory iteratorFactory;
        private PeekingIterator<Map.Entry<OffheapBytes, Values>> filteredIterator;

        public ValuesRangeIterator(PrimaryKey min,
                                   PrimaryKey max,
                                   long count,
                                   FilteredIteratorFactory iteratorFactory)
        {
            super(min, max, count);
            this.iteratorFactory = iteratorFactory;
            this.filteredIterator = Iterators.peekingIterator(iteratorFactory.create(min));
        }

        @Override
        protected void performSkipTo(PrimaryKey nextKey)
        {
            // create a new iterator using nextKey as the min key
            this.filteredIterator = Iterators.peekingIterator(iteratorFactory.create(nextKey));
        }

        @Override
        public void close() throws IOException
        {
        }

        @Override
        protected PrimaryKey computeNext()
        {
            if (filteredIterator.hasNext())
            {
                final OffheapBytes keyBytes = filteredIterator.next().getKey();

                return parseKey(keyBytes.toByteComparable());
            }
            else
                return endOfData();
        }
    }

    private PrimaryKey parseKey(ByteComparable bytes)
    {
        final ByteSource.Peekable peekable = bytes.asPeekableBytes(ByteComparable.Version.OSS41);

        final IPartitioner partitioner = indexContext.partitioner();

        PrimaryKey.Factory primaryKeyFactory = indexContext.keyFactory();

        Token token = partitioner.getTokenFactory().fromComparableBytes(ByteSourceInverse.nextComponentSource(peekable),
                                                                        ByteComparable.Version.OSS41);
        byte[] keyBytes = ByteSourceInverse.getUnescapedBytes(ByteSourceInverse.nextComponentSource(peekable));

        if (keyBytes == null)
        {
            return primaryKeyFactory.createTokenOnly(token);
        }

        DecoratedKey partitionKey = new BufferDecoratedKey(token, ByteBuffer.wrap(keyBytes));

        Clustering clustering = indexContext.comparator().size() == 0
                                ? Clustering.EMPTY
                                : indexContext.comparator().clusteringFromByteComparable(ByteBufferAccessor.instance,
                                                                                         v -> ByteSourceInverse.nextComponentSource(peekable));

        return primaryKeyFactory.create(partitionKey, clustering);
    }

    private void setMinMaxPrimaryKey(PrimaryKey key)
    {
        assert key != null;

        minKeyRef.accumulateAndGet(key, (key1, key2) -> ObjectUtils.min(key1, key2));
        maxKeyRef.accumulateAndGet(key, (key1, key2) -> ObjectUtils.max(key1, key2));
    }

    private ByteComparable encode(ByteBuffer input)
    {
        return isLiteral ? version -> append(ByteSource.of(input, version), ByteSource.TERMINATOR)
                         : version -> TypeUtil.asComparableBytes(input, validator, version);
    }

    private ByteSource append(ByteSource src, int lastByte)
    {
        return new ByteSource()
        {
            boolean done = false;

            @Override
            public int next()
            {
                if (done)
                    return END_OF_STREAM;
                int n = src.next();
                if (n != END_OF_STREAM)
                    return n;

                done = true;
                return lastByte;
            }
        };
    }

    public static ByteComparable nudge(ByteComparable comp)
    {
        byte[] bytes = ByteSourceInverse.readBytes(comp.asComparableBytes(ByteComparable.Version.OSS41));
        byte[] bytes2 = nudge(bytes);
        return ByteComparable.fixedLength(bytes2);
    }

    /**
     * Appends unsigned byte 0 to the array to order correctly.
     * @param orig Original byte array to operate on
     * @return A new byte array that sorts one unit ahead of the given byte array
     */
    public static byte[] nudge(byte[] orig)
    {
        final byte[] bytes = Arrays.copyOf(orig, orig.length + 1);
        bytes[orig.length] = (int) 0; // unsigned requires cast
        return bytes;
    }

    public static ByteComparable nudgeReverse(ByteComparable comp, int maxTermLength)
    {
        final byte[] bytes = ByteSourceInverse.readBytes(comp.asComparableBytes(ByteComparable.Version.OSS41));
        final byte[] bytes2 = nudgeReverse(bytes, maxTermLength);
        return ByteComparable.fixedLength(bytes2);
    }

    /**
     * @param orig Byte array to opreate on
     * @param maxLength The correct ordering the absolute max bytes length of all compared bytes is specified.
     * @return A byte array that sorts 1 byte unit increment lower
     */
    public static byte[] nudgeReverse(byte[] orig, int maxLength)
    {
        assert orig.length > 0 : "Input byte array must be not empty";
        int lastIdx = orig.length - 1;

        if (UnsignedBytes.toInt(orig[lastIdx]) == 0)
            return Arrays.copyOf(orig, lastIdx);
        else
        {
            // bytes must be max length for correct ordering
            byte[] result = Arrays.copyOf(orig, maxLength);
            result[lastIdx] = unsignedDecrement(orig[lastIdx]);
            Arrays.fill(result, orig.length, result.length, (byte) 255);
            return result;
        }
    }

    public static byte unsignedDecrement(byte b)
    {
        int i = UnsignedBytes.toInt(b);
        int i2 = i - 1; // unsigned decrement
        return (byte) i2;
    }
}
