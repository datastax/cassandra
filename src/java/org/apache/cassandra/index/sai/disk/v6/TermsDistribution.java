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

package org.apache.cassandra.index.sai.disk.v6;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.PriorityQueue;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.disk.io.IndexInput;
import org.apache.cassandra.index.sai.disk.io.IndexOutput;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.utils.TypeUtil;

/**
 * Approximates a statistical distribution of term values in a sstable index segment.
 * It is used to quickly estimate how many rows match a given term value or a range of values,
 * without performing the search using the index (which would be more costly).
 * <p>
 * Comprises a histogram and a most frequent term table.
 * <p>
 * To build instances of this class, use the nested {@link Builder} class.
 *
 * @see SegmentMetadata
 */
@ThreadSafe
@Immutable
public class TermsDistribution
{
    // Special virtual bucket placed before all the other buckets of the histogram.
    // Can be considered a bucket at index -1. The existence of this instance allows us to never return null
    // when looking up the bucket by an index and simplifies the code.
    private static final Bucket MIN_BUCKET = new Bucket(null, 0, 0);

    final AbstractType<?> termType;
    final ByteBuffer minTerm;
    final ByteBuffer maxTerm;
    final List<Bucket> histogram;
    final NavigableMap<ByteBuffer, Long> mostFrequentTerms;

    final long numPoints;
    final long numRows;

    private TermsDistribution(AbstractType<?> termType, List<Bucket> histogram, NavigableMap<ByteBuffer, Long> mostFrequentTerms)
    {
        this.termType = termType;
        this.histogram = histogram;
        this.mostFrequentTerms = mostFrequentTerms;

        this.numRows = histogram.isEmpty() ? 0 : histogram.get(histogram.size() - 1).cumulativeRowCount;
        this.numPoints = histogram.isEmpty() ? 0 : histogram.get(histogram.size() - 1).cumulativePointCount;
        this.minTerm = histogram.isEmpty() ? null : histogram.get(0).term;
        this.maxTerm = histogram.isEmpty() ? null : histogram.get(histogram.size() - 1).term;
    }

    public long estimateNumRowsMatchingExact(ByteBuffer term)
    {
        Long count = mostFrequentTerms.get(term);
        if (count != null)
            return count;

        int index = indexOfBucketContaining(term);
        Bucket low = getBucket(index - 1);
        Bucket high = getBucket(index);

        // The histogram buckets include all most frequent terms,
        // but if we're here, we know our term is *not* any of the frequent values.
        // Therefore, we should subtract frequent values to get better precision:
        var mft = mostFrequentTermsInRange(low.term, high.term);

        long points = high.cumulativePointCount - low.cumulativePointCount - mft.size();
        long rows = high.cumulativeRowCount - low.cumulativeRowCount - sumValues(mft);
        return rows == 0 ? 0 : Math.round((double) rows / points);
    }

    /**
     * Estimates the number of rows with a value in given range.
     * Allows to specify inclusiveness/exclusiveness of bounds.
     */
    public long estimateNumRowsInRange(ByteBuffer min, boolean minInclusive, ByteBuffer max, boolean maxInclusive)
    {
        long rowCount = estimateNumRowsInRange(min, max);

        if (minInclusive && min != null)
            rowCount += estimateNumRowsMatchingExact(min);
        if (!maxInclusive && max != null)
            rowCount = Math.max(0, rowCount - estimateNumRowsMatchingExact(max));

        return rowCount;
    }

    /**
     * Estimates the number of rows with a value in given range.
     *
     * @param min exclusive minimum bound
     * @param max inclusive maximum bound
     */
    public long estimateNumRowsInRange(ByteBuffer min, ByteBuffer max)
    {
        Bucket low = (min != null) ? interpolate(min) : getBucket(-1);
        Bucket high = (max != null) ? interpolate(max) : getBucket(histogram.size());
        return Math.max(0, high.cumulativeRowCount - low.cumulativeRowCount);
    }

    /**
     * Returns cumulative point count and cumulative row count for given term
     * by linear interpolation of two adjacent histogram buckets.
     * <p>
     * The information from the most frequent terms map is also included,
     * so if any of the frequent terms are lower or equal to the given term, their
     * row counts will be also added.
     * <p>
     *
     * Example - Let's assume the following histogram:
     * <pre>
     * bucket index  | term      |  cumulativePointCount  |  cumulativeRowCount
     * --------------+-----------+------------------------+----------------------
     * -1            |  null     |                     0  |                   0
     *  0            |  "2.0"    |                   100  |               10000
     *  1            |  "3.0"    |                   140  |               20000
     * </pre>
     * The results of calling this function are as follows:
     * <pre>
     * interpolate("1.0") = Bucket("1.0", 0, 0)
     * interpolate("1.9") = Bucket("1.9", 0, 0)
     * interpolate("2.0") = Bucket("2.0", 100, 10000)
     * interpolate("2.5") = Bucket("2.5", 120, 15000)
     * interpolate("3.0") = Bucket("3.0", 140, 20000)
     * interpolate("4.0") = Bucket("4.0", 140, 20000)
     * </pre>
     */
    private @Nonnull Bucket interpolate(@Nonnull ByteBuffer term)
    {
        int bucketIndex = indexOfBucketContaining(term);
        Bucket bucket = getBucket(bucketIndex);
        Bucket prevBucket = getBucket(bucketIndex - 1);

        if (prevBucket.term == null)
            return new Bucket(term, bucket.cumulativePointCount, bucket.cumulativeRowCount);

        ByteBuffer bucketMinTerm = prevBucket.term;
        ByteBuffer bucketMaxTerm = bucket.term;

        BigDecimal bucketMinValue = toBigDecimal(bucketMinTerm);
        BigDecimal bucketMaxValue = toBigDecimal(bucketMaxTerm);

        // Edge case: this can theoretically happen if our big decimals have insufficient resolution
        // to distinguish terms. If we didn't return early in this case,
        // the later interpolation logic would divide by 0.
        if (bucketMinValue.equals(bucketMaxValue))
            return new Bucket(term, bucket.cumulativePointCount, bucket.cumulativeRowCount);

        // Estimate the fraction of the bucket on the left side of the term.
        // We assume terms are distributed evenly.
        BigDecimal termValue = toBigDecimal(term).min(bucketMaxValue).max(bucketMinValue);
        double termDistance = termValue.subtract(bucketMinValue).doubleValue();
        double bucketSize = bucketMaxValue.subtract(bucketMinValue).doubleValue();
        double fraction = termDistance / bucketSize;
        assert fraction >= 0.0 && fraction <= 1.0: "Invalid fraction value: " + fraction;

        // Total number of points and rows in this bucket:
        long pointCount = bucket.cumulativePointCount - prevBucket.cumulativePointCount;
        long rowCount = bucket.cumulativeRowCount - prevBucket.cumulativeRowCount;

        // We need those to include precise information about most frequent terms in the calculation.
        // For most frequent terms we know the exact number of rows, so if we're matching any most frequent
        // terms, those will be added at the end to the final row count estimate.
        SortedMap<ByteBuffer, Long> bucketMft = mostFrequentTermsInRange(prevBucket.term, bucket.term);
        SortedMap<ByteBuffer, Long> matchedMft = mostFrequentTermsInRange(prevBucket.term, term);
        long matchedMftPointCount = matchedMft.size();
        long matchedMftRowCount = sumValues(matchedMft);

        // We likely don't have the information on all the points in the MFT table.
        // Compute the average number of rows per point for all the non-MFT points, that is
        // as if all the most frequent terms didn't exist.
        // Then we'll multiply this value by the number of matching non-MFT points to get
        // a reasonable row count estimate for the non-MFT points.
        long nonMftPointCount = pointCount - bucketMft.size();
        long nonMftRowCount = rowCount - sumValues(bucketMft);
        assert nonMftPointCount >= 0 : "point count cannot be negative";
        assert nonMftRowCount >= 0 : "row count cannot be negative";
        double rowsPerPoint = nonMftPointCount == 0 ? 0.0 : (double) nonMftRowCount / nonMftPointCount;

        // We assume points are distributed evenly; therefore we use total pointCount here:
        double matchedPointCount = fraction * pointCount;

        double matchedNonMftRowCount = Math.max(0.0, matchedPointCount - matchedMftPointCount) * rowsPerPoint;
        double matchedRowCount = matchedNonMftRowCount + matchedMftRowCount;

        long cumulativePointCount = prevBucket.cumulativePointCount + Math.round(matchedPointCount);
        long cumulativeRowCount = prevBucket.cumulativeRowCount + Math.round(matchedRowCount);
        return new Bucket(term, cumulativePointCount, cumulativeRowCount);
    }

    /**
     * Converts the term value to a big decimal value. Preserves order.
     * If the type represents a number, the correspondence is linear.
     * For non-number types, it reinterprets a bytecomparable serialization as a number,
     * so it is not necessarily linear, but still preserves the order.
     */
    private BigDecimal toBigDecimal(ByteBuffer term)
    {
        return TypeUtil.toBigDecimal(term, termType);
    }

    /**
     * Finds the bucket at given index.
     * Saturates at edges, so never returns null.
     * If index is negative, returns {@link this#MIN_BUCKET}.
     * If index >= histogram.size(), returns the last (highest) bucket.
     */
    private @Nonnull Bucket getBucket(int index)
    {
        if (index < 0 || histogram.isEmpty())
            return MIN_BUCKET;
        if (index >= histogram.size())
            return histogram.get(histogram.size() - 1);

        return histogram.get(index);
    }

    /**
     * Returns the index of the highest bucket whose term value is equal or greater than the given value.
     * If the value is lower than {@link this#minTerm}, returns -1.
     * If the value is higher than {@link this#maxTerm}, returns {@code histogram.size()}.
     */
    private int indexOfBucketContaining(@Nonnull ByteBuffer b)
    {
        Bucket needle = new Bucket(b, 0, 0);
        int index = Collections.binarySearch(histogram, needle, (b1, b2) -> termType.compare(b1.term, b2.term));
        return (index >= -1) ? index : -(index + 1);
    }

    /**
     * Helper function to return the sum of values in a map
     */
    private static long sumValues(Map<?, Long> map)
    {
        return map.values().stream().mapToLong(Long::longValue).sum();
    }

    /**
     * Returns a subtree of {@code mostFrequentTerms} map with values between given range.
     * A null term means a term before the lowest term.
     *
     * @param min exclusive lower bound
     * @param max inclusive upper bound
     */
    private SortedMap<ByteBuffer, Long>  mostFrequentTermsInRange(@Nullable ByteBuffer min, @Nullable ByteBuffer max)
    {
        if (max == null)
            return Collections.emptySortedMap();
        if (min == null)
            return mostFrequentTerms.headMap(max);

        return mostFrequentTerms.subMap(min, false, max, true);
    }

    public void write(IndexOutput out) throws IOException
    {
        out.writeShort((short) histogram.size());
        for (Bucket b : histogram)
        {
            out.writeBytes(b.term);
            out.writeVLong(b.cumulativePointCount);
            out.writeVLong(b.cumulativeRowCount);
        }
        out.writeShort((short) mostFrequentTerms.size());
        for (Map.Entry<ByteBuffer, Long> entry : mostFrequentTerms.entrySet())
        {
            out.writeBytes(entry.getKey());
            out.writeVLong(entry.getValue());
        }
    }

    public static TermsDistribution read(IndexInput input, AbstractType<?> termType) throws IOException
    {
        int bucketCount = input.readShort();
        if (bucketCount < 0)
            throw new IOException("Number of buckets cannot be negative: " + bucketCount);

        List<Bucket> buckets = new ArrayList<>(bucketCount);
        for (int i = 0; i < bucketCount; i++)
        {
            ByteBuffer term = input.readBytes();
            long cumulativePointCount = input.readVLong();
            long cumulativeRowCount = input.readVLong();
            buckets.add(new Bucket(term, cumulativePointCount, cumulativeRowCount));
        }

        int mostFrequentTermsCount = input.readShort();
        if (mostFrequentTermsCount < 0)
            throw new IOException("Number of most frequent terms cannot be negative: " + mostFrequentTermsCount);

        NavigableMap<ByteBuffer, Long> mostFrequentTerms = new TreeMap<>(termType);
        for (int i = 0; i < mostFrequentTermsCount; i++)
        {
            ByteBuffer term = input.readBytes();
            long rowCount = input.readVLong();
            mostFrequentTerms.put(term, rowCount);
        }

        return new TermsDistribution(termType, buckets, mostFrequentTerms);
    }

    @NotThreadSafe
    public static class Builder
    {
        final AbstractType<?> termType;
        final int histogramSize;
        final int mostFrequentTermsTableSize;

        long maxRowsPerBucket;

        List<Bucket> buckets = new ArrayList<>();
        PriorityQueue<Point> mostFrequentTerms = new PriorityQueue<>();

        ByteBuffer lastTerm;
        long cumulativePointCount;
        long cumulativeRowCount;

        public Builder(AbstractType<?> termType, int histogramSize, int mostFrequentTermsTableSize)
        {
            this.termType = termType;
            this.histogramSize = histogramSize;
            this.mostFrequentTermsTableSize = mostFrequentTermsTableSize;

            // Let's start with adding buckets for every point.
            // This will be corrected to a higher value once the histogram gets too large and we'll do shrinking.
            this.maxRowsPerBucket = 1;
        }

        /**
         * Adds a point to the histogram.
         * Points must be added in ascending order of term values
         * (the order of termValues is defined by {@link this#termType}).
         * If the order is not preserved, the behavior is undefined.
         */
        public void add(ByteBuffer term, long rowCount)
        {
            mostFrequentTerms.add(new Point(term, rowCount));
            if (mostFrequentTerms.size() > mostFrequentTermsTableSize)
                mostFrequentTerms.poll();

            cumulativePointCount += 1;
            cumulativeRowCount += rowCount;
            lastTerm = term;

            if (buckets.isEmpty() || cumulativeRowCount > buckets.get(buckets.size() - 1).cumulativeRowCount + maxRowsPerBucket)
            {
                buckets.add(new Bucket(lastTerm, cumulativePointCount, cumulativeRowCount));
                lastTerm = null;

                if (buckets.size() > histogramSize * 2)
                    shrink();
            }
        }

        public TermsDistribution build()
        {
            if (lastTerm != null)
                buckets.add(new Bucket(lastTerm, cumulativePointCount, cumulativeRowCount));

            shrink();

            var mft = new TreeMap<ByteBuffer, Long>(termType);
            for (Point point : mostFrequentTerms) {
                mft.put(point.term, point.rowCount);
            }

            return new TermsDistribution(termType, buckets, mft);
        }

        /**
         * Shrinks the histogram to fit in the histogramSize limit, by removing some points.
         * Tries to keep uniform granulatiry in terms of the number of rows.
         * Runs in O(n) time.
         * Needed because in some cases we don't know the number of points added to the histogram in advance,
         * so we have to build it incrementally.
         */
        private void shrink()
        {
            if (buckets.size() < histogramSize)
                return;

            maxRowsPerBucket = buckets.get(buckets.size() - 1).cumulativeRowCount / histogramSize;
            int targetIndex = 1;
            for (int candidateIndex = 1; candidateIndex < buckets.size(); candidateIndex++)
            {
                Bucket last = buckets.get(targetIndex - 1);
                Bucket candidate = buckets.get(candidateIndex);
                if (candidate.cumulativeRowCount - last.cumulativeRowCount > maxRowsPerBucket || candidateIndex == buckets.size() - 1)
                {
                    buckets.set(targetIndex, candidate);
                    targetIndex++;
                }
            }
            buckets.subList(targetIndex, buckets.size()).clear();
        }
    }

    /**
     * A histogram bucket - keeps the cumulative point and row counts for all the terms smaller or equal given term.
     */
    @ThreadSafe
    @Immutable
    static class Bucket
    {
        final ByteBuffer term;
        final long cumulativePointCount;
        final long cumulativeRowCount;

        Bucket(ByteBuffer term, long cumulativePointCount, long cumulativeRowCount)
        {
            this.term = term;
            this.cumulativePointCount = cumulativePointCount;
            this.cumulativeRowCount = cumulativeRowCount;
        }
    }

    /**
     * A helper class for building the most frequent terms queue.
     * Associates the term with the row count and provides a natural ordering by row count.
     */
    static class Point implements Comparable<Point>
    {
        final ByteBuffer term;
        final long rowCount;

        Point(ByteBuffer term, long rowCount)
        {
            this.term = term;
            this.rowCount = rowCount;
        }

        @Override
        public int compareTo(Point o)
        {
            return Long.compare(rowCount, o.rowCount);
        }
    }

}


