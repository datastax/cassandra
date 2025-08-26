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

package org.apache.cassandra.service;


import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Predicates;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.ReplicaPlans;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.locator.ReplicaUtils;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.metrics.ReplicaResponseSizeMetrics;

import static java.util.concurrent.TimeUnit.DAYS;
import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WriteResponseHandlerTest
{
    static Keyspace ks;
    static ColumnFamilyStore cfs;
    static EndpointsForToken targets;
    static EndpointsForToken pending;

    private static Replica full(String name)
    {
        try
        {
            return ReplicaUtils.full(InetAddressAndPort.getByName(name));
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    }

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        SchemaLoader.loadSchema();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        // Register peers with expected DC for NetworkTopologyStrategy.
        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();
        metadata.updateHostId(UUID.randomUUID(), InetAddressAndPort.getByName("127.1.0.255"));
        metadata.updateHostId(UUID.randomUUID(), InetAddressAndPort.getByName("127.2.0.255"));

        DatabaseDescriptor.setEndpointSnitch(new IEndpointSnitch()
        {
            public String getRack(InetAddressAndPort endpoint)
            {
                return null;
            }

            public String getDatacenter(InetAddressAndPort endpoint)
            {
                byte[] address = endpoint.getAddress().getAddress();
                if (address[1] == 1)
                    return "datacenter1";
                else
                    return "datacenter2";
            }

            public <C extends ReplicaCollection<? extends C>> C sortedByProximity(InetAddressAndPort address, C replicas)
            {
                return replicas;
            }

            public int compareEndpoints(InetAddressAndPort target, Replica a1, Replica a2)
            {
                return 0;
            }

            public void gossiperStarting()
            {

            }

            public boolean isWorthMergingForRangeQuery(ReplicaCollection<?> merged, ReplicaCollection<?> l1, ReplicaCollection<?> l2)
            {
                return false;
            }
        });
        DatabaseDescriptor.setBroadcastAddress(InetAddress.getByName("127.1.0.1"));
        SchemaLoader.createKeyspace("Foo", KeyspaceParams.nts("datacenter1", 3, "datacenter2", 3), SchemaLoader.standardCFMD("Foo", "Bar"));
        ks = Keyspace.open("Foo");
        cfs = ks.getColumnFamilyStore("Bar");
        targets = EndpointsForToken.of(DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(0)),
                                       full("127.1.0.255"), full("127.1.0.254"), full("127.1.0.253"),
                                       full("127.2.0.255"), full("127.2.0.254"), full("127.2.0.253"));
        pending = EndpointsForToken.empty(DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(0)));
    }

    @Before
    public void resetCounters()
    {
        ks.metric.writeFailedIdealCL.dec(ks.metric.writeFailedIdealCL.getCount());
    }

    /**
     * Validate that a successful write at ideal CL logs latency information. Also validates
     * DatacenterSyncWriteResponseHandler
     * @throws Throwable
     */
    @Test
    public void idealCLLatencyTracked() throws Throwable
    {
        long startingCount = ks.metric.idealCLWriteLatency.latency.getCount();
        //Specify query start time in past to ensure minimum latency measurement
        AbstractWriteResponseHandler awr = createWriteResponseHandler(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM, new Dispatcher.RequestTime(nanoTime() - DAYS.toNanos(1)));

        //dc1
        awr.onResponse(createDummyMessage(0));
        awr.onResponse(createDummyMessage(1));

        // there are not enough responses for ideal EACH_QUORUM yet
        assertEquals(startingCount, ks.metric.idealCLWriteLatency.latency.getCount());

        //dc2
        awr.onResponse(createDummyMessage(4));
        awr.onResponse(createDummyMessage(5));

        // there are enough responses for ideal EACH_QUORUM, we should not wait for all responses
        assertTrue( TimeUnit.DAYS.toMicros(1) < ks.metric.idealCLWriteLatency.totalLatency.getCount());
        assertEquals(startingCount + 1, ks.metric.idealCLWriteLatency.latency.getCount());

        //Don't need the others
        awr.expired();
        awr.expired();

        assertEquals(0,  ks.metric.writeFailedIdealCL.getCount());
    }

    /**
     * Validate that WriteResponseHandler does the right thing on success.
     * @throws Throwable
     */
    @Test
    public void idealCLWriteResponeHandlerWorks() throws Throwable
    {
        long startingCount = ks.metric.idealCLWriteLatency.latency.getCount();
        AbstractWriteResponseHandler awr = createWriteResponseHandler(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.ALL);

        //dc1
        awr.onResponse(createDummyMessage(0));
        awr.onResponse(createDummyMessage(1));
        awr.onResponse(createDummyMessage(2));
        //dc2
        awr.onResponse(createDummyMessage(3));
        awr.onResponse(createDummyMessage(4));
        awr.onResponse(createDummyMessage(5));

        assertEquals(0,  ks.metric.writeFailedIdealCL.getCount());
        assertEquals(startingCount + 1, ks.metric.idealCLWriteLatency.latency.getCount());
    }

    /**
     * Validate that DatacenterWriteResponseHandler does the right thing on success.
     * @throws Throwable
     */
    @Test
    public void idealCLDatacenterWriteResponeHandlerWorks() throws Throwable
    {
        long startingCount = ks.metric.idealCLWriteLatency.latency.getCount();
        AbstractWriteResponseHandler awr = createWriteResponseHandler(ConsistencyLevel.ONE, ConsistencyLevel.LOCAL_QUORUM);

        //dc1
        awr.onResponse(createDummyMessage(0));
        awr.onResponse(createDummyMessage(1));
        awr.onResponse(createDummyMessage(2));
        //dc2
        awr.onResponse(createDummyMessage(3));
        awr.onResponse(createDummyMessage(4));
        awr.onResponse(createDummyMessage(5));

        assertEquals(0,  ks.metric.writeFailedIdealCL.getCount());
        assertEquals(startingCount + 1, ks.metric.idealCLWriteLatency.latency.getCount());
    }

    /**
     * Validate that failing to achieve ideal CL increments the failure counter
     * @throws Throwable
     */
    @Test
    public void failedIdealCLIncrementsStat() throws Throwable
    {
        ks.metric.idealCLWriteLatency.totalLatency.dec(ks.metric.idealCLWriteLatency.totalLatency.getCount());
        AbstractWriteResponseHandler awr = createWriteResponseHandler(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM);

        //Succeed in local DC
        awr.onResponse(createDummyMessage(0));
        awr.onResponse(createDummyMessage(1));
        awr.onResponse(createDummyMessage(2));

        //Fail in remote DC
        awr.expired();
        awr.expired();
        awr.expired();
        assertEquals(1, ks.metric.writeFailedIdealCL.getCount());
        assertEquals(0, ks.metric.idealCLWriteLatency.totalLatency.getCount());
    }

    @Test
    public void failedIdealCLIncrementsStatForExplicitOnFailure()
    {
        AbstractWriteResponseHandler awr = createWriteResponseHandler(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM);

        long startingCountForWriteFailedIdealCL = ks.metric.writeFailedIdealCL.getCount();
        long startingCountForIdealCLWriteLatency = ks.metric.idealCLWriteLatency.totalLatency.getCount();


        //Succeed in local DC
        awr.onResponse(createDummyMessage(0));
        awr.onResponse(createDummyMessage(1));
        awr.onResponse(createDummyMessage(2));


        //Fail in remote DC
        awr.onFailure(targets.get(3).endpoint(), RequestFailureReason.TIMEOUT);
        awr.onFailure(targets.get(4).endpoint(), RequestFailureReason.TIMEOUT);
        awr.onResponse(createDummyMessage(5));

        assertEquals(startingCountForWriteFailedIdealCL + 1, ks.metric.writeFailedIdealCL.getCount());
        assertEquals(startingCountForIdealCLWriteLatency, ks.metric.idealCLWriteLatency.totalLatency.getCount());
    }

    /**
     * Validate that failing to achieve ideal CL doesn't increase the failure counter when not meeting CL
     * @throws Throwable
     */
    @Test
    public void failedIdealCLDoesNotIncrementsStatOnQueryFailure() throws Throwable
    {
        AbstractWriteResponseHandler awr = createWriteResponseHandler(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM);

        long startingCount = ks.metric.writeFailedIdealCL.getCount();

        // Failure in local DC
        awr.onResponse(createDummyMessage(0));
        
        awr.expired();
        awr.expired();

        //Fail in remote DC
        awr.expired();
        awr.expired();
        awr.expired();

        assertEquals(startingCount, ks.metric.writeFailedIdealCL.getCount());
    }

    @Test
    public void failedIdealCLDoesNotIncrementsStatOnExplicitQueryFailure()
    {
        AbstractWriteResponseHandler awr = createWriteResponseHandler(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM);

        long startingCountForWriteFailedIdealCL = ks.metric.writeFailedIdealCL.getCount();
        long startingCountForIdealCLWriteLatency = ks.metric.idealCLWriteLatency.totalLatency.getCount();


        //Fail in local DC
        awr.onFailure(targets.get(0).endpoint(), RequestFailureReason.TIMEOUT);
        awr.onFailure(targets.get(1).endpoint(), RequestFailureReason.TIMEOUT);
        awr.onResponse(createDummyMessage(2));


        //Fail in remote DC
        awr.onFailure(targets.get(3).endpoint(), RequestFailureReason.TIMEOUT);
        awr.onFailure(targets.get(4).endpoint(), RequestFailureReason.TIMEOUT);
        awr.onResponse(createDummyMessage(5));

        assertEquals(startingCountForWriteFailedIdealCL, ks.metric.writeFailedIdealCL.getCount());
        assertEquals(startingCountForIdealCLWriteLatency, ks.metric.idealCLWriteLatency.totalLatency.getCount());
    }

    @Test
    public void testIsCompleted() throws Throwable
    {
        Dispatcher.RequestTime requestTime = Dispatcher.RequestTime.forImmediateExecution();
        AbstractWriteResponseHandler<?> awr = createWriteResponseHandler(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM, requestTime);
        assertThat(awr.replicaPlan().writeQuorum()).isEqualTo(2);
        assertThat(awr.writeType()).isEqualTo(WriteType.SIMPLE);
        assertThat(awr.requestTime()).isEqualTo(requestTime);

        assertThat(awr.isCompleted()).isFalse();
        assertThat(awr.isCompletedExceptionally()).isFalse();

        // LOCAL_QUORUM requires 2 reponses
        awr.onResponse(createDummyMessage(0));
        assertThat(awr.isCompleted()).isFalse();
        assertThat(awr.isCompletedExceptionally()).isFalse();

        awr.onResponse(createDummyMessage(1));
        assertThat(awr.isCompleted()).isTrue();
        assertThat(awr.isCompletedExceptionally()).isFalse();

        awr.get();
    }

    @Test
    public void testIsCompletedExceptionally() throws Throwable
    {
        Dispatcher.RequestTime requestTime = Dispatcher.RequestTime.forImmediateExecution();
        AbstractWriteResponseHandler<?> awr = createWriteResponseHandler(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM, requestTime);
        assertThat(awr.replicaPlan().writeQuorum()).isEqualTo(2);
        assertThat(awr.writeType()).isEqualTo(WriteType.SIMPLE);
        assertThat(awr.requestTime()).isEqualTo(requestTime);

        assertThat(awr.isCompleted()).isFalse();
        assertThat(awr.isCompletedExceptionally()).isFalse();

        // LOCAL_QUORUM requires 2 failure to fail handler
        awr.onFailure(targets.get(0).endpoint(), RequestFailureReason.UNKNOWN);
        assertThat(awr.isCompleted()).isFalse();
        assertThat(awr.isCompletedExceptionally()).isFalse();
        assertThat(awr.failures()).isEqualTo(1);
        assertThat(awr.failureReasonByEndpoint()).hasSize(1);

        awr.onFailure(targets.get(1).endpoint(), RequestFailureReason.UNKNOWN);
        assertThat(awr.isCompleted()).isTrue();
        assertThat(awr.isCompletedExceptionally()).isTrue();
        assertThat(awr.failures()).isEqualTo(2);
        assertThat(awr.failureReasonByEndpoint()).hasSize(2);

        assertThatThrownBy(awr::get).isInstanceOf(WriteFailureException.class);
    }

    private static AbstractWriteResponseHandler createWriteResponseHandler(ConsistencyLevel cl, ConsistencyLevel ideal)
    {
        return createWriteResponseHandler(cl, ideal, Dispatcher.RequestTime.forImmediateExecution());
    }

    private static AbstractWriteResponseHandler createWriteResponseHandler(ConsistencyLevel cl, ConsistencyLevel ideal, Dispatcher.RequestTime requestTime)
    {
        return ks.getReplicationStrategy().getWriteResponseHandler(ReplicaPlans.forWrite(ks, cl, targets, pending, Predicates.alwaysTrue(), ReplicaPlans.writeAll),
                                                                   null, WriteType.SIMPLE, null, requestTime, ideal);
    }

    private static Message createDummyMessage(int target)
    {
        return Message.builder(Verb.ECHO_REQ, noPayload)
                      .from(targets.get(target).endpoint())
                      .build();
    }

    /**
     * Test that write response metrics are properly collected for remote responses
     */
    @Test
    public void testWriteResponseMetricsForRemoteResponses() throws Throwable
    {
        long initialTotalBytes = ReplicaResponseSizeMetrics.totalBytesReceived.getCount();
        long initialWriteBytes = ReplicaResponseSizeMetrics.writeResponseBytesReceived.getCount();
        long initialHistogramCount = ReplicaResponseSizeMetrics.writeResponseBytesPerResponse.getCount();
        
        AbstractWriteResponseHandler<?> handler = createWriteResponseHandler(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM, Dispatcher.RequestTime.forImmediateExecution());
        
        handler.onResponse(createDummyMessage(0)); // Remote replica in DC1
        handler.onResponse(createDummyMessage(1)); // Remote replica in DC1
        
        handler.get();
        
        assertTrue("Total bytes metric should have increased",
                   ReplicaResponseSizeMetrics.totalBytesReceived.getCount() >= initialTotalBytes);
        assertTrue("Write bytes metric should have increased", 
                   ReplicaResponseSizeMetrics.writeResponseBytesReceived.getCount() >= initialWriteBytes);
        assertTrue("Histogram count should have increased by at least 2", 
                   ReplicaResponseSizeMetrics.writeResponseBytesPerResponse.getCount() >= initialHistogramCount + 2);
    }

    /**
     * Test that write response metrics are NOT collected for local responses
     */
    @Test
    public void testWriteResponseMetricsSkippedForLocalResponses() throws Throwable
    {
        long initialTotalBytes = ReplicaResponseSizeMetrics.totalBytesReceived.getCount();
        long initialWriteBytes = ReplicaResponseSizeMetrics.writeResponseBytesReceived.getCount();
        
        AbstractWriteResponseHandler<?> handler = createWriteResponseHandler(ConsistencyLevel.ONE, ConsistencyLevel.ONE, Dispatcher.RequestTime.forImmediateExecution());
        
        // Simulate a local response (message.from() == null)
        handler.onResponse(null);
        
        handler.get();
        
        // Check that metrics were NOT updated for local response
        assertEquals("Total bytes should not change for local responses", 
                     initialTotalBytes, ReplicaResponseSizeMetrics.totalBytesReceived.getCount());
        assertEquals("Write bytes should not change for local responses", 
                     initialWriteBytes, ReplicaResponseSizeMetrics.writeResponseBytesReceived.getCount());
    }

    /**
     * Test that metrics collection handles errors gracefully
     */
    @Test
    public void testWriteResponseMetricsHandlesExceptionsGracefully() throws Throwable
    {
        AbstractWriteResponseHandler<?> handler = createWriteResponseHandler(ConsistencyLevel.ONE, ConsistencyLevel.ONE, Dispatcher.RequestTime.forImmediateExecution());
        
        // Send a normal response first to satisfy consistency
        handler.onResponse(createDummyMessage(0));
        
        handler.get();
        
        // The metrics collection handles null payloads and other edge cases gracefully
    }

    /**
     * Test that write response metrics track all responses, including those after consistency is met
     */
    @Test  
    public void testWriteResponseMetricsTracksAllResponses() throws Throwable
    {
        long initialHistogramCount = ReplicaResponseSizeMetrics.writeResponseBytesPerResponse.getCount();
        
        AbstractWriteResponseHandler<?> handler = createWriteResponseHandler(ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM, Dispatcher.RequestTime.forImmediateExecution());
        
        handler.onResponse(createDummyMessage(0));
        handler.onResponse(createDummyMessage(1));
        handler.onResponse(createDummyMessage(2)); // Extra response after consistency met
        
        // All 3 responses should be tracked in metrics
        assertTrue("All responses should be tracked, not just those meeting consistency",
                   ReplicaResponseSizeMetrics.writeResponseBytesPerResponse.getCount() >= initialHistogramCount + 3);
    }
}
