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

package org.apache.cassandra.io.sstable.metadata;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.io.sstable.metadata.MetadataCollector.defaultPartitionSizeHistogram;
import static org.junit.Assert.*;

public class MetadataCollectorTest
{
    private static final Logger logger = LoggerFactory.getLogger(MetadataCollectorTest.class);

    @Test
    public void testNoOverflow()
    {
        EstimatedHistogram histogram = defaultPartitionSizeHistogram();
        histogram.add(1414838745986L);
        assertFalse(histogram.isOverflowed());
    }

    @Test
    public void testFindMaxSampleWithoutOverflow()
    {
        logger.info("dse compatible boundaries: {}", CassandraRelevantProperties.USE_DSE_COMPATIBLE_HISTOGRAM_BOUNDARIES.getBoolean());

        long low = 0;
        long high = Long.MAX_VALUE;
        long result = -1;

        while (low <= high) {
            long mid = low + (high - low) / 2;  // Avoid potential overflow in (low + high) / 2

            // Create a fresh histogram for each test to avoid accumulated state
            EstimatedHistogram testHistogram = defaultPartitionSizeHistogram();
            testHistogram.add(mid);

            if (testHistogram.isOverflowed()) {
                high = mid - 1;
            } else {
                result = mid;  // Keep track of the last working value
                low = mid + 1;
            }
        }

        logger.info("Max value without overflow: {}", result);

        // Verify the result
        EstimatedHistogram finalHistogram = defaultPartitionSizeHistogram();
        finalHistogram.add(result);
        assertFalse(finalHistogram.isOverflowed());

        // Verify that result + 1 causes overflow
        EstimatedHistogram overflowHistogram = defaultPartitionSizeHistogram();
        overflowHistogram.add(result + 1);
        assertTrue(overflowHistogram.isOverflowed());
    }
}