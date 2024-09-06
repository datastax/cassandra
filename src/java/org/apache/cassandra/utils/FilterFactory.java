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
package org.apache.cassandra.utils;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.utils.concurrent.Ref;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.MetricNameFactory;
import org.apache.cassandra.metrics.MicrometerMetrics;
import org.apache.cassandra.utils.obs.IBitSet;
import org.apache.cassandra.utils.obs.MemoryLimiter;
import org.apache.cassandra.utils.obs.OffHeapBitSet;

import static org.apache.cassandra.config.CassandraRelevantProperties.USE_MICROMETER;
import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class FilterFactory
{
    public static final IFilter AlwaysPresent = AlwaysPresentFilter.instance;
    // marker for lazy bloom filter
    public static final IFilter AlwaysPresentForLazyLoading = new AlwaysPresentFilter();

    public static final FilterFactoryMetrics metrics = FilterFactoryMetrics.create();

    private static final Logger logger = LoggerFactory.getLogger(FilterFactory.class);
    private static final long BITSET_EXCESS = 20;

    /**
     * @return A BloomFilter with the lowest practical false positive
     *         probability for the given number of elements.
     */
    public static IFilter getFilter(long numElements, int targetBucketsPerElem)
    {
        return getFilter(numElements, targetBucketsPerElem, BloomFilter.memoryLimiter);
    }

    public static IFilter getFilter(long numElements, int targetBucketsPerElem, MemoryLimiter memoryLimiter)
    {
        int maxBucketsPerElement = Math.max(1, BloomCalculations.maxBucketsPerElement(numElements));
        int bucketsPerElement = Math.min(targetBucketsPerElem, maxBucketsPerElement);
        if (bucketsPerElement < targetBucketsPerElem)
        {
            logger.warn("Cannot provide an optimal BloomFilter for {} elements ({}/{} buckets per element).", numElements, bucketsPerElement, targetBucketsPerElem);
        }
        BloomCalculations.BloomSpecification spec = BloomCalculations.computeBloomSpec(bucketsPerElement);
        return createFilter(spec.K, numElements, spec.bucketsPerElement, memoryLimiter);
    }

    /**
     * @return The smallest BloomFilter that can provide the given false
     * positive probability rate for the given number of elements.
     * <p>
     * Asserts that the given probability can be satisfied using this
     * filter.
     */
    public static IFilter getFilter(long numElements, double maxFalsePosProbability)
    {
        return getFilter(numElements, maxFalsePosProbability, BloomFilter.memoryLimiter);
    }

    public static IFilter getFilter(long numElements, double maxFalsePosProbability, MemoryLimiter memoryLimiter)
    {
        assert maxFalsePosProbability <= 1.0 : "Invalid probability";
        if (maxFalsePosProbability == 1.0)
            return FilterFactory.AlwaysPresent;
        int bucketsPerElement = BloomCalculations.maxBucketsPerElement(numElements);
        BloomCalculations.BloomSpecification spec = BloomCalculations.computeBloomSpec(bucketsPerElement, maxFalsePosProbability);
        return createFilter(spec.K, numElements, spec.bucketsPerElement, memoryLimiter);
    }

    @SuppressWarnings("resource")
    private static IFilter createFilter(int hash, long numElements, int bucketsPer, MemoryLimiter memoryLimiter)
    {
        try
        {
            long numBits = (numElements * bucketsPer) + BITSET_EXCESS;
            IBitSet bitset = new OffHeapBitSet(numBits, memoryLimiter);
            return new BloomFilter(hash, bitset);
        }
        catch (MemoryLimiter.ReachedMemoryLimitException | OutOfMemoryError e)
        {
            logger.error("Failed to create new Bloom filter with {} elements: ({}) - " +
                         "continuing but this will have severe performance implications. Consider increasing FP chance " +
                         "(bloom_filter_fp_chance) or increasing system ram space or" +
                         "lowering number of sstables through compaction", numElements, e.getMessage());
            metrics.incrementOOMError();
            return AlwaysPresent;
        }
    }

    private static class AlwaysPresentFilter implements IFilter
    {
        public static final AlwaysPresentFilter instance = new AlwaysPresentFilter();

        private AlwaysPresentFilter() { }

        public boolean isPresent(FilterKey key)
        {
            return true;
        }

        public void add(FilterKey key) { }

        public void clear() { }

        public void close() { }

        public IFilter sharedCopy()
        {
            return this;
        }

        public Throwable close(Throwable accumulate)
        {
            return accumulate;
        }

        public void addTo(Ref.IdentityCollection identities)
        {
        }

        public long serializedSize(boolean oldSerializationFormat) { return 0; }

        @Override
        public void serialize(DataOutputStreamPlus out, boolean oldSerializationFormat) throws IOException
        {
            // no-op
        }

        @Override
        public long offHeapSize()
        {
            return 0;
        }

        @Override
        public boolean isInformative()
        {
            return false;
        }
    }

    public interface FilterFactoryMetrics
    {
        static FilterFactoryMetrics create()
        {
            return USE_MICROMETER.getBoolean() ? new FilterFactoryMicormeterMetrics()
                                                : new FilterFactoryCodahaleMetrics();
        }

        void incrementOOMError();

        long oomErrors();
    }

    /**
     * Metrics exposed in Prometheus friendly format
     */
    public static final class FilterFactoryMicormeterMetrics extends MicrometerMetrics implements FilterFactoryMetrics
    {
        public static final String METRICS_PREFIX = "bloom_filter";
        public static final String OOM_ERRORS = METRICS_PREFIX + "_oom_errors";
        private volatile Counter oomCounter;
        public FilterFactoryMicormeterMetrics()
        {
            this.oomCounter = counter(OOM_ERRORS);
        }

        @Override
        public synchronized void register(MeterRegistry newRegistry, Tags newTags)
        {
            super.register(newRegistry, newTags);
            this.oomCounter = counter(OOM_ERRORS);
        }

        @Override
        public void incrementOOMError()
        {
            oomCounter.increment();
        }

        @Override
        public long oomErrors()
        {
            return (long) oomCounter.count();
        }
    }

    /**
     * Metrics exposed in Codahale format
     */
    public static final class FilterFactoryCodahaleMetrics implements FilterFactoryMetrics
    {
        private static final MetricNameFactory metricNameFactory = new DefaultNameFactory("BloomFilter");
        private static final com.codahale.metrics.Counter oomCounter = Metrics.counter(metricNameFactory.createMetricName("OutOfMemory"));

        @Override
        public void incrementOOMError()
        {
            oomCounter.inc();
        }

        @Override
        public long oomErrors()
        {
            return oomCounter.getCount();
        }
    }
}
