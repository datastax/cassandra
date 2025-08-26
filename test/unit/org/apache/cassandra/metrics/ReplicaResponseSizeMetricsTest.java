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
        
        int responseSize = 1024;
        ReplicaResponseSizeMetrics.recordReadResponseSize(responseSize);
        
        assertEquals(initialTotal + responseSize, ReplicaResponseSizeMetrics.totalBytesReceived.getCount());
        assertEquals(initialRead + responseSize, ReplicaResponseSizeMetrics.readResponseBytesReceived.getCount());
        
        Snapshot readSnapshot = ReplicaResponseSizeMetrics.readResponseBytesPerResponse.getSnapshot();
        assertTrue(readSnapshot.size() > 0);
        assertTrue(readSnapshot.getMax() >= responseSize);
    }
    
    @Test
    public void testWriteResponseMetrics()
    {
        long initialTotal = ReplicaResponseSizeMetrics.totalBytesReceived.getCount();
        long initialWrite = ReplicaResponseSizeMetrics.writeResponseBytesReceived.getCount();
        
        int responseSize = 256;
        ReplicaResponseSizeMetrics.recordWriteResponseSize(responseSize);
        
        assertEquals(initialTotal + responseSize, ReplicaResponseSizeMetrics.totalBytesReceived.getCount());
        assertEquals(initialWrite + responseSize, ReplicaResponseSizeMetrics.writeResponseBytesReceived.getCount());
        
        Snapshot writeSnapshot = ReplicaResponseSizeMetrics.writeResponseBytesPerResponse.getSnapshot();
        assertTrue(writeSnapshot.size() > 0);
    }
    
    @Test
    public void testMultipleResponses()
    {
        long initialTotal = ReplicaResponseSizeMetrics.totalBytesReceived.getCount();
        
        int[] sizes = {100, 200, 300, 400, 500};
        int expectedTotal = 0;
        
        for (int size : sizes)
        {
            // Alternate between read and write responses
            if (size % 2 == 0)
                ReplicaResponseSizeMetrics.recordReadResponseSize(size);
            else
                ReplicaResponseSizeMetrics.recordWriteResponseSize(size);
            expectedTotal += size;
        }
        
        assertEquals(initialTotal + expectedTotal, ReplicaResponseSizeMetrics.totalBytesReceived.getCount());
        
        Snapshot totalSnapshot = ReplicaResponseSizeMetrics.bytesReceivedPerResponse.getSnapshot();
        assertTrue(totalSnapshot.size() >= sizes.length);
        assertTrue(totalSnapshot.getMean() > 0);
    }
}
