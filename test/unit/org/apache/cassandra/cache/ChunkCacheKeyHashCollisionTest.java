/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.cache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.commons.math3.distribution.ChiSquaredDistribution;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that hash collisions between different ChunkCache keys are rare and nicely distributed.
 * Unfortunately we cannot test all combinations of readerId and position values, because that would be 2^128 pairs.
 * However, we still want to test if all bits present in the inputs are properly accounted for in the hashCode()
 * function. Therefore, we test smaller subsets of readerIds and positions shifted by different number
 * of bits each time and every bit is tested in at least one of the runs.
 * The test fails if the total collision rate is above 0.1% or if any single key collides with more than 2 other keys.
 */
@RunWith(Parameterized.class)
public class ChunkCacheKeyHashCollisionTest
{
    private final int alignment;
    private final int readerBitShift;

    public ChunkCacheKeyHashCollisionTest(int alignment, int readerBitShift) {
        this.alignment = alignment;
        this.readerBitShift = readerBitShift;
    }

    // Controls how many test cases to generate.
    // Smaller step will generate more test-cases and increase the total runtime of the test suite.
    static final int BIT_SHIFT_STEP = 8;
    static final int NUMBER_OF_TEST_CASES = (Long.BYTES * 8 / BIT_SHIFT_STEP) * (Long.BYTES * 8 / BIT_SHIFT_STEP);

    // Number of changing bits in readerId
    static final int READER_BITS = 8;
    // Number of changing bits in position
    static final int POSITION_BITS = 8;

    // Distinct readerIds tested in each run
    static final int READER_COUNT = 1 << READER_BITS;
    // Distinct positions tested in each run
    static final int POSITION_COUNT = 1 << POSITION_BITS;

    @Parameterized.Parameters(name = "alignment={0}, readerBitShift={1}")
    public static Collection<Object[]> data() {
        List<Object[]> params = new ArrayList<>();
        for (int a = 0; a < Long.BYTES * 8 - POSITION_BITS; a += BIT_SHIFT_STEP) {
            for (int r = 0; r < Long.BYTES * 8 - READER_BITS; r += BIT_SHIFT_STEP) {
                params.add(new Object[]{a, r});
            }
        }
        return params;
    }

    @Test
    public void testCollisions()
    {
        final int TOTAL_HASHES_COUNT = READER_COUNT * POSITION_COUNT;
        int[] hashes = new int[TOTAL_HASHES_COUNT];

        int i = 0;
        for (long readerId = 0; readerId < READER_COUNT; readerId++)
        {
            for (long position = 0; position < POSITION_COUNT; position++)
            {
                ChunkCache.Key key = new ChunkCache.Key(readerId << readerBitShift, position << alignment);
                hashes[i++] = key.hashCode();
            }
        }

        // That might look counterintuitive, but sorting is actually significantly faster for this size of data than
        // counting duplicates in a HashSet or HashMap, due to better cache locality and parallelism.
        // And the array uses much less memory, especially compared to
        // a HashMap that otherwise would be needed to track max per-key collisions.
        Arrays.parallelSort(hashes);

        int totalCollisions = 0;
        int maxPerKeyCollisions = 0;
        int currentKeyCollisions = 0;
        for (int j = 1; j < hashes.length; j++) {
            if (hashes[j] == hashes[j - 1]) {
                totalCollisions++;
                currentKeyCollisions++;
            }
            else
            {
                maxPerKeyCollisions = Math.max(maxPerKeyCollisions, currentKeyCollisions);
                currentKeyCollisions = 0;
            }
        }
        maxPerKeyCollisions = Math.max(maxPerKeyCollisions, currentKeyCollisions);

        // The total actual number of collisions should be close to the theoretical expected number of collisions
        // as if the hashes were distributed perfectly at random. For small number of hashes, the theoretical
        // probability of collision gets very low, but the sampling error gets higher, therefore we relax
        // the requirement and clamp it at 0.0001 (which can be still considered a very low collision rate).
        var actualCollisionRate = (double) totalCollisions / TOTAL_HASHES_COUNT;
        var M = (double) (1L << Integer.SIZE);
        var expectedCollisions = TOTAL_HASHES_COUNT - M * (1.0 - Math.pow((M - 1.0) / M, TOTAL_HASHES_COUNT));
        var expectedCollisionRate = expectedCollisions / TOTAL_HASHES_COUNT;
        assertThat(actualCollisionRate).describedAs("Collision rate is too high: total %d collisions out of %d keys (%.4f%%)",
                                              totalCollisions, TOTAL_HASHES_COUNT, actualCollisionRate * 100)
                                       .isLessThan(expectedCollisionRate * 1.2 + 0.0001);

        // Even when the total number of collisions is low, check that collisions are uniformly distributed.
        // We don't want a single key to have a large number of collisions.
        assertThat(maxPerKeyCollisions).describedAs("Max number of collisions for a single key is too large: %d collisions", maxPerKeyCollisions)
                                       .isLessThanOrEqualTo(3);

        // Test slices of the hash. A good hashing function should distribute values uniformly, regardless of
        // which subset of bits of the hash we take. Slices of various sizes are taken from various positions.
        // Many hash-map implementations often use only some number of lower bits of the hash value, so this test
        // has direct relationship to how well given hashing function can work in hash-map-like structures.
        for (int bitCount = 8; bitCount <= READER_BITS + POSITION_BITS - 4; bitCount += 2)
            for (int bitShift = 0; bitShift <= Integer.BYTES * 8 - bitCount; bitShift += BIT_SHIFT_STEP)
                assertHashSlicesAreUniformlyDistributed(hashes, bitCount, bitShift);

    }

    private static void assertHashSlicesAreUniformlyDistributed(int[] hashes, int bitCount, int bitShift)
    {
        // The test doesn't work well if we have too few keys per bin
        Preconditions.checkArgument(hashes.length >= 16 * (1 << bitCount));

        int numBins = 1 << bitCount;
        int mask = (numBins - 1) << bitShift;
        long[] counts = new long[numBins];
        for (int hash : hashes)
            counts[(hash & mask) >>> bitShift]++;

        double expectedCount = (double) hashes.length / numBins;
        double pValue = probabilityOfDataComingFromUniformDistribution(counts, expectedCount);

        // The problem with a statistical test is that it can fail by chance if we're unlucky even if the code under
        // test is good. This parameter controls how much risk of failing the whole test suite by accident we are ok with.
        // The lower the value, the weaker the test gets, and more likely a bad hashing function gets undetected.
        final double PROBABILITY_OF_TEST_SUITE_FAILURE = 0.01;
        assertThat(pValue).describedAs("Bits %d..%d failed uniform distribution test", bitShift, bitShift + bitCount)
                          .isGreaterThan(PROBABILITY_OF_TEST_SUITE_FAILURE / NUMBER_OF_TEST_CASES);
    }

    /**
     * Calculates the probability that the data are uniformly distributed between a fixed number of bins.
     * @param counts the count in each bin
     * @param expectedCount the expected count in every bin if data were distributed ideally uniformly
     */
    private static double probabilityOfDataComingFromUniformDistribution(long[] counts, double expectedCount)
    {
        double chiSquare = 0;
        for (long count : counts)
        {
            double deviation = count - expectedCount;
            chiSquare += deviation * deviation;
        }
        chiSquare /= expectedCount;

        // We are testing if the observed counts of hash values (for lower bits) conform to a uniform distribution.
        // A chi-squared test is appropriate for this "goodness of fit" test.
        // The null hypothesis is that the observed data follows the uniform distribution.
        // A low p-value would lead us to reject the null hypothesis,
        // indicating that the distribution is not uniform.
        // Degrees of freedom = number of bins - 1
        ChiSquaredDistribution chiSquaredDistribution = new ChiSquaredDistribution(counts.length - 1);
        return 1 - chiSquaredDistribution.cumulativeProbability(chiSquare);
    }
}
