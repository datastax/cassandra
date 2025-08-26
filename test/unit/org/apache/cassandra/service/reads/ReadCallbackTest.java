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
package org.apache.cassandra.service.reads;

import java.nio.ByteBuffer;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.MultiRangeReadCommand;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SimpleBuilders;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.metrics.ReplicaResponseSizeMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.locator.ReplicaUtils.full;
import static org.junit.Assert.*;

/**
 * Integration tests for ReadCallback replica response size metrics tracking.
 * This test class verifies that read responses from replicas are properly tracked in metrics.
 */
public class ReadCallbackTest extends AbstractReadResponseTest
{
    private SinglePartitionReadCommand command;
    private EndpointsForToken targetReplicas;
    
    @Before
    public void setUp()
    {
        dk = dk("key1");
        nowInSec = FBUtilities.nowInSeconds();
        command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk);
        targetReplicas = EndpointsForToken.of(dk.getToken(), full(EP1), full(EP2), full(EP3));
    }
    
    /**
     * Test that read response metrics are properly collected for remote responses
     */
    @Test
    public void testReadResponseMetricsForRemoteResponses() throws Throwable
    {
        long initialTotalBytes = ReplicaResponseSizeMetrics.totalBytesReceived.getCount();
        long initialReadBytes = ReplicaResponseSizeMetrics.readResponseBytesReceived.getCount();
        long initialHistogramCount = ReplicaResponseSizeMetrics.readResponseBytesPerResponse.getCount();
        
        ReplicaPlan.SharedForTokenRead plan = plan(ConsistencyLevel.QUORUM, targetReplicas);
        DigestResolver<EndpointsForToken, ReplicaPlan.ForTokenRead> resolver = 
            new DigestResolver<>(command, plan, queryStartNanoTime(), noopReadTracker());
        ReadCallback<EndpointsForToken, ReplicaPlan.ForTokenRead> callback = 
            new ReadCallback<>(resolver, command, plan, queryStartNanoTime());
        
        PartitionUpdate.Builder updateBuilder = PartitionUpdate.builder(cfm, dk, cfm.regularAndStaticColumns(), 1);
        updateBuilder.add(createRow(1000, 1, "value1"));
        PartitionUpdate update = updateBuilder.build();
        ReadResponse readResponse = command.createResponse(iter(update), command.executionController().getRepairedDataInfo());
        
        callback.onResponse(createReadResponseMessage(readResponse, EP1));
        callback.onResponse(createReadResponseMessage(readResponse, EP2));
        
        callback.awaitResults();
        
        assertTrue("Total bytes metric should have increased",
                   ReplicaResponseSizeMetrics.totalBytesReceived.getCount() > initialTotalBytes);
        assertTrue("Read bytes metric should have increased", 
                   ReplicaResponseSizeMetrics.readResponseBytesReceived.getCount() > initialReadBytes);
        assertTrue("Histogram count should have increased by at least 2", 
                   ReplicaResponseSizeMetrics.readResponseBytesPerResponse.getCount() >= initialHistogramCount + 2);
    }
    
    
    /**
     * Test that read response metrics track all responses, including those after consistency is met
     */
    @Test
    public void testReadResponseMetricsTracksAllResponses() throws Throwable
    {
        long initialHistogramCount = ReplicaResponseSizeMetrics.readResponseBytesPerResponse.getCount();
        
        ReplicaPlan.SharedForTokenRead plan = plan(ConsistencyLevel.QUORUM, targetReplicas);
        DigestResolver<EndpointsForToken, ReplicaPlan.ForTokenRead> resolver = 
            new DigestResolver<>(command, plan, queryStartNanoTime(), noopReadTracker());
        ReadCallback<EndpointsForToken, ReplicaPlan.ForTokenRead> callback = 
            new ReadCallback<>(resolver, command, plan, queryStartNanoTime());
        
        PartitionUpdate.Builder updateBuilder = PartitionUpdate.builder(cfm, dk, cfm.regularAndStaticColumns(), 1);
        updateBuilder.add(createRow(1000, 1, "value1"));
        PartitionUpdate update = updateBuilder.build();
        ReadResponse readResponse = command.createResponse(iter(update), command.executionController().getRepairedDataInfo());
        
        callback.onResponse(createReadResponseMessage(readResponse, EP1));
        callback.onResponse(createReadResponseMessage(readResponse, EP2));
        callback.onResponse(createReadResponseMessage(readResponse, EP3)); // Extra response after consistency met
        
        assertTrue("All responses should be tracked, not just those meeting consistency",
                   ReplicaResponseSizeMetrics.readResponseBytesPerResponse.getCount() >= initialHistogramCount + 3);
    }
    
    /**
     * Test that metrics collection handles unsupported responses
     */
    @Test
    public void testReadResponseMetricsHandlesUnsupportedResponse() throws Throwable
    {
        ReplicaPlan.SharedForTokenRead plan = plan(ConsistencyLevel.ONE, targetReplicas);
        DigestResolver<EndpointsForToken, ReplicaPlan.ForTokenRead> resolver = 
            new DigestResolver<>(command, plan, queryStartNanoTime(), noopReadTracker());
        ReadCallback<EndpointsForToken, ReplicaPlan.ForTokenRead> callback = 
            new ReadCallback<>(resolver, command, plan, queryStartNanoTime());
        
        // Test with a response that doesn't support size tracking
        ReadResponse unsupportedResponse = new TestUnsupportedReadResponse();
        Message<ReadResponse> message = Message.builder(Verb.READ_RSP, unsupportedResponse)
                                               .from(EP1)
                                               .build();
        
        // This should not throw an exception even though the response doesn't support tracking
        callback.onResponse(message);
    }
    
    /**
     * Test that MultiRangeReadResponse doesn't cause exceptions in metrics tracking
     */
    @Test
    public void testReadResponseMetricsWithMultiRangeReadResponse() throws Throwable
    {
        long initialHistogramCount = ReplicaResponseSizeMetrics.readResponseBytesPerResponse.getCount();
        
        PartitionRangeReadCommand partitionRangeCommand = PartitionRangeReadCommand.allDataRead(cfm, nowInSec);
        
        AbstractBounds<PartitionPosition> keyRange = new Bounds<>(dk.getToken().minKeyBound(), dk.getToken().maxKeyBound());
        MultiRangeReadCommand multiRangeCommand = MultiRangeReadCommand.create(partitionRangeCommand, 
                                                                                Collections.singletonList(keyRange), 
                                                                                false);
        
        ReplicaPlan.SharedForTokenRead plan = plan(ConsistencyLevel.QUORUM, targetReplicas);
        DigestResolver<EndpointsForToken, ReplicaPlan.ForTokenRead> resolver = 
            new DigestResolver<>(command, plan, queryStartNanoTime(), noopReadTracker());
        ReadCallback<EndpointsForToken, ReplicaPlan.ForTokenRead> callback = 
            new ReadCallback<>(resolver, command, plan, queryStartNanoTime());
        
        PartitionUpdate.Builder updateBuilder = PartitionUpdate.builder(cfm, dk, cfm.regularAndStaticColumns(), 1);
        updateBuilder.add(createRow(1000, 1, "value1"));
        PartitionUpdate update = updateBuilder.build();
        UnfilteredPartitionIterator data = iter(update);
        
        ReadResponse multiRangeResponse = multiRangeCommand.createResponse(data, null);
        
        Message<ReadResponse> message = Message.builder(Verb.READ_RSP, multiRangeResponse)
                                               .from(EP1)
                                               .build();
        
        // Should handle gracefully without exceptions
        callback.onResponse(message);
        
        // Another response to meet consistency
        callback.onResponse(createReadResponseMessage(
            command.createResponse(iter(update), command.executionController().getRepairedDataInfo()), EP2));
        
        callback.awaitResults();
        
        // Verify we didn't increment metrics for the MultiRangeReadResponse since it doesn't support tracking
        // But we should have incremented for the regular response
        assertTrue("Regular response should still be tracked",
                   ReplicaResponseSizeMetrics.readResponseBytesPerResponse.getCount() > initialHistogramCount);
    }
    
    /**
     * Test that digest responses are handled correctly for metrics
     */
    @Test
    public void testReadResponseMetricsWithDigestResponses() throws Throwable
    {
        long initialReadBytes = ReplicaResponseSizeMetrics.readResponseBytesReceived.getCount();
        
        ReplicaPlan.SharedForTokenRead plan = plan(ConsistencyLevel.QUORUM, targetReplicas);
        DigestResolver<EndpointsForToken, ReplicaPlan.ForTokenRead> resolver = 
            new DigestResolver<>(command, plan, queryStartNanoTime(), noopReadTracker());
        ReadCallback<EndpointsForToken, ReplicaPlan.ForTokenRead> callback = 
            new ReadCallback<>(resolver, command, plan, queryStartNanoTime());
        
        ReadResponse digestResponse = ReadResponse.createDigestResponse(iter(PartitionUpdate.emptyUpdate(cfm, dk)), command);
        
        callback.onResponse(createReadResponseMessage(digestResponse, EP1));
        callback.onResponse(createReadResponseMessage(digestResponse, EP2));
        
        assertTrue("Digest responses should also be tracked in metrics",
                   ReplicaResponseSizeMetrics.readResponseBytesReceived.getCount() > initialReadBytes);
    }
    
    // Helper methods
    
    private Message<ReadResponse> createReadResponseMessage(ReadResponse response, InetAddressAndPort from)
    {
        return Message.builder(Verb.READ_RSP, response)
                      .from(from)
                      .build();
    }
    
    private Row createRow(long timestamp, int clustering, String value)
    {
        SimpleBuilders.RowBuilder builder = new SimpleBuilders.RowBuilder(cfm, Integer.toString(clustering));
        builder.timestamp(timestamp).add("c1", value);
        return builder.build();
    }
    
    private Dispatcher.RequestTime queryStartNanoTime()
    {
        return Dispatcher.RequestTime.forImmediateExecution();
    }
    
    private ReplicaPlan.SharedForTokenRead plan(ConsistencyLevel consistencyLevel, EndpointsForToken replicas)
    {
        return ReplicaPlan.shared(new ReplicaPlan.ForTokenRead(ks, ks.getReplicationStrategy(), consistencyLevel, replicas, replicas));
    }
    
    // Test implementation that doesn't support response size tracking
    private static class TestUnsupportedReadResponse extends ReadResponse
    {
        @Override
        public UnfilteredPartitionIterator makeIterator(ReadCommand command)
        {
            return EmptyIterators.unfilteredPartition(command.metadata());
        }
        
        @Override
        public ByteBuffer digest(ReadCommand command)
        {
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;
        }
        
        @Override
        public ByteBuffer repairedDataDigest()
        {
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;
        }
        
        @Override
        public boolean isRepairedDigestConclusive()
        {
            return false;
        }
        
        @Override
        public boolean mayIncludeRepairedDigest()
        {
            return false;
        }
        
        @Override
        public boolean isDigestResponse()
        {
            return false;
        }
        
        @Override
        public boolean supportsResponseSizeTracking()
        {
            return false; // This response type doesn't support size tracking
        }
    }
}
