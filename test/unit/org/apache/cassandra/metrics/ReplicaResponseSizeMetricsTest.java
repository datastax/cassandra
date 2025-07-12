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
package org.apache.cassandra.metrics;

import org.junit.BeforeClass;
import org.junit.Test;

import com.codahale.metrics.Snapshot;

import static org.junit.Assert.*;

public class ReplicaResponseSizeMetricsTest
{
    @BeforeClass
    public static void setup()
    {
        // Note: Counter metrics cannot be reset, so tests track deltas
    }
    
    @Test
    public void testReadResponseMetrics()
    {
        long initialTotal = ReplicaResponseSizeMetrics.totalBytesReceived.getCount();
        long initialRead = ReplicaResponseSizeMetrics.readResponseBytesReceived.getCount();
        
        // Record a read response
        int responseSize = 1024;
        ReplicaResponseSizeMetrics.recordReplicaResponseSize(responseSize, true);
        
        // Verify counters
        assertEquals(initialTotal + responseSize, ReplicaResponseSizeMetrics.totalBytesReceived.getCount());
        assertEquals(initialRead + responseSize, ReplicaResponseSizeMetrics.readResponseBytesReceived.getCount());
        
        // Verify histogram recorded the value
        Snapshot readSnapshot = ReplicaResponseSizeMetrics.readResponseBytesPerResponse.getSnapshot();
        assertTrue(readSnapshot.size() > 0);
        // Check that the histogram contains values in the expected range
        assertTrue(readSnapshot.getMax() >= responseSize);
    }
    
    @Test
    public void testWriteResponseMetrics()
    {
        long initialTotal = ReplicaResponseSizeMetrics.totalBytesReceived.getCount();
        long initialWrite = ReplicaResponseSizeMetrics.writeResponseBytesReceived.getCount();
        
        // Record a write response
        int responseSize = 256;
        ReplicaResponseSizeMetrics.recordReplicaResponseSize(responseSize, false);
        
        // Verify counters
        assertEquals(initialTotal + responseSize, ReplicaResponseSizeMetrics.totalBytesReceived.getCount());
        assertEquals(initialWrite + responseSize, ReplicaResponseSizeMetrics.writeResponseBytesReceived.getCount());
        
        // Verify histogram
        Snapshot writeSnapshot = ReplicaResponseSizeMetrics.writeResponseBytesPerResponse.getSnapshot();
        assertTrue(writeSnapshot.size() > 0);
    }
    
    @Test
    public void testMultipleResponses()
    {
        long initialTotal = ReplicaResponseSizeMetrics.totalBytesReceived.getCount();
        
        // Record multiple responses
        int[] sizes = {100, 200, 300, 400, 500};
        int expectedTotal = 0;
        
        for (int size : sizes)
        {
            ReplicaResponseSizeMetrics.recordReplicaResponseSize(size, size % 2 == 0);
            expectedTotal += size;
        }
        
        // Verify total
        assertEquals(initialTotal + expectedTotal, ReplicaResponseSizeMetrics.totalBytesReceived.getCount());
        
        // Verify histogram captures all sizes
        Snapshot totalSnapshot = ReplicaResponseSizeMetrics.bytesReceivedPerResponse.getSnapshot();
        assertTrue(totalSnapshot.size() >= sizes.length);
        assertTrue(totalSnapshot.getMean() > 0);
    }
}