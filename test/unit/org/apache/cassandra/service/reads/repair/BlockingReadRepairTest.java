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

package org.apache.cassandra.service.reads.repair;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.net.EndpointMessagingVersions;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.reads.ReadCallback;
import org.apache.cassandra.transport.Dispatcher;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class BlockingReadRepairTest extends AbstractReadRepairTest
{
    private static class InstrumentedReadRepairHandler
            extends BlockingPartitionRepair
    {
        public InstrumentedReadRepairHandler(Map<Replica, Mutation> repairs, ReplicaPlan.ForWrite writePlan)
        {
            super(Util.dk("not a real usable value"), repairs, writePlan);
        }

        Map<InetAddressAndPort, Mutation> mutationsSent = new HashMap<>();

        protected void sendRR(Message<Mutation> message, InetAddressAndPort endpoint)
        {
            mutationsSent.put(endpoint, message.payload);
        }
    }

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        configureClass(ReadRepairStrategy.BLOCKING);
    }

    private static InstrumentedReadRepairHandler createRepairHandler(Map<Replica, Mutation> repairs, ReplicaPlan.ForWrite writePlan)
    {
        return new InstrumentedReadRepairHandler(repairs, writePlan);
    }

    private static InstrumentedReadRepairHandler createRepairHandler(Map<Replica, Mutation> repairs)
    {
        EndpointsForRange replicas = EndpointsForRange.copyOf(Lists.newArrayList(repairs.keySet()));
        return createRepairHandler(repairs, repairPlan(replicas, replicas));
    }

    private static class InstrumentedBlockingReadRepair<E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E, P>>
            extends BlockingReadRepair<E, P> implements InstrumentedReadRepair<E, P>
    {
        public InstrumentedBlockingReadRepair(ReadCommand command, ReplicaPlan.Shared<E, P> replicaPlan, Dispatcher.RequestTime requestTime)
        {
            super(command, replicaPlan, requestTime);
        }

        Set<InetAddressAndPort> readCommandRecipients = new HashSet<>();
        ReadCallback readCallback = null;

        @Override
        void sendReadCommand(Replica to, ReadCallback callback, boolean speculative, boolean trackRepairedStatus)
        {
            assert readCallback == null || readCallback == callback;
            readCommandRecipients.add(to.endpoint());
            readCallback = callback;
        }

        @Override
        public Set<InetAddressAndPort> getReadRecipients()
        {
            return readCommandRecipients;
        }

        @Override
        public ReadCallback getReadCallback()
        {
            return readCallback;
        }
    }

    @Override
    public InstrumentedReadRepair createInstrumentedReadRepair(ReadCommand command, ReplicaPlan.Shared<?, ?> replicaPlan, Dispatcher.RequestTime requestTime)
    {
        return new InstrumentedBlockingReadRepair(command, replicaPlan, requestTime);
    }

    @Test
    public void consistencyLevelTest() throws Exception
    {
        AbstractReplicationStrategy rs = ks.getReplicationStrategy();
        Assert.assertTrue(ConsistencyLevel.QUORUM.satisfies(ConsistencyLevel.QUORUM, rs));
        Assert.assertTrue(ConsistencyLevel.THREE.satisfies(ConsistencyLevel.LOCAL_QUORUM, rs));
        Assert.assertTrue(ConsistencyLevel.TWO.satisfies(ConsistencyLevel.LOCAL_QUORUM, rs));
        Assert.assertFalse(ConsistencyLevel.ONE.satisfies(ConsistencyLevel.LOCAL_QUORUM, rs));
        Assert.assertFalse(ConsistencyLevel.ANY.satisfies(ConsistencyLevel.LOCAL_QUORUM, rs));
    }


    @Test
    public void additionalMutationRequired() throws Exception
    {

        Mutation repair1 = mutation(cell2);
        Mutation repair2 = mutation(cell1);

        // check that the correct repairs are calculated
        Map<Replica, Mutation> repairs = new HashMap<>();
        repairs.put(replica1, repair1);
        repairs.put(replica2, repair2);

        ReplicaPlan.ForWrite writePlan = repairPlan(replicas, EndpointsForRange.copyOf(Lists.newArrayList(repairs.keySet())));
        InstrumentedReadRepairHandler handler = createRepairHandler(repairs, writePlan);

        Assert.assertTrue(handler.mutationsSent.isEmpty());

        // check that the correct mutations are sent
        handler.sendInitialRepairs();
        Assert.assertEquals(2, handler.mutationsSent.size());
        assertMutationEqual(repair1, handler.mutationsSent.get(target1));
        assertMutationEqual(repair2, handler.mutationsSent.get(target2));

        // check that a combined mutation is speculatively sent to the 3rd target
        handler.mutationsSent.clear();
        handler.maybeSendAdditionalWrites(0, TimeUnit.NANOSECONDS);
        Assert.assertEquals(1, handler.mutationsSent.size());
        assertMutationEqual(resolved, handler.mutationsSent.get(target3));

        // check repairs stop blocking after receiving 2 acks
        Assert.assertFalse(getCurrentRepairStatus(handler));
        handler.ack(target1);
        Assert.assertFalse(getCurrentRepairStatus(handler));
        handler.ack(target3);
        Assert.assertTrue(getCurrentRepairStatus(handler));

    }

    /**
     * DSE 6 compatibility mode: the speculative repair write to a replica at the pre-4.0 version 11 of a DSE 6 node,
     * below {@code MessagingService.minimum_version}, is sent like the one to a replica at the current version, and
     * the mutation sent can be sized, serialized and read back at the replica's version.
     */
    @Test
    public void additionalMutationToDse6Replica() throws Exception
    {
        additionalMutationToReplicaAtVersion(MessagingService.VERSION_3014);
    }

    /**
     * As {@link #additionalMutationToDse6Replica()}, at the oldest pre-4.0 version a peer may request: the first slot
     * of the per-version mutations (no DSE 6 node is at 10).
     */
    @Test
    public void additionalMutationToOldestPre40Replica() throws Exception
    {
        additionalMutationToReplicaAtVersion(MessagingService.VERSION_30);
    }

    /** As {@link #additionalMutationToDse6Replica()}, to a replica of this software in a cluster in DSE 6 compatibility mode. */
    @Test
    public void additionalMutationToCurrentReplicaInDse6Mode() throws Exception
    {
        additionalMutationToReplicaAtVersion(MessagingService.current_version);
    }

    /**
     * Without DSE 6 compatibility mode the per-version mutations still start at {@code MessagingService.minimum_version}
     * as before the mode existed: a replica stored below it (only possible through a gossiped version, the handshake
     * refuses such a peer) still fails the speculation as it did.
     */
    @Test
    public void pre40ReplicaWithoutDse6ModeKeepsTheIndexing()
    {
        Assert.assertFalse(DatabaseDescriptor.isDse6CompatibilityMode());
        EndpointMessagingVersions versions = MessagingService.instance().versions;
        int minClusterVersionBefore = versions.minClusterVersion;
        Integer target3VersionBefore = versions.knows(target3) ? versions.getRaw(target3) : null;
        try
        {
            versions.set(target3, MessagingService.VERSION_3014);

            Map<Replica, Mutation> repairs = new HashMap<>();
            repairs.put(replica1, mutation(cell2));
            repairs.put(replica2, mutation(cell1));
            InstrumentedReadRepairHandler handler = createRepairHandler(repairs, repairPlan(replicas, EndpointsForRange.copyOf(Lists.newArrayList(repairs.keySet()))));
            handler.sendInitialRepairs();
            handler.mutationsSent.clear();

            // deliberate NONE-invariance pin: without the mode the indexing stays as it was before the mode existed
            Assert.assertThrows("NONE mode keeps the minimum_version indexing", ArrayIndexOutOfBoundsException.class,
                                () -> handler.maybeSendAdditionalWrites(0, TimeUnit.NANOSECONDS));
            Assert.assertTrue(handler.mutationsSent.isEmpty());
        }
        finally
        {
            restoreVersion(versions, target3VersionBefore, minClusterVersionBefore);
        }
    }

    private static void restoreVersion(EndpointMessagingVersions versions, Integer target3VersionBefore, int minClusterVersionBefore)
    {
        if (target3VersionBefore == null)
            versions.reset(target3);
        else
            versions.set(target3, target3VersionBefore);
        versions.minClusterVersion = minClusterVersionBefore;
    }

    private void additionalMutationToReplicaAtVersion(int target3Version) throws Exception
    {
        InternodeCompatibilityMode modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        EndpointMessagingVersions versions = MessagingService.instance().versions;
        int minClusterVersionBefore = versions.minClusterVersion;
        Integer target3VersionBefore = versions.knows(target3) ? versions.getRaw(target3) : null;
        try
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
            versions.set(target3, target3Version);

            Map<Replica, Mutation> repairs = new HashMap<>();
            repairs.put(replica1, mutation(cell2));
            repairs.put(replica2, mutation(cell1));

            ReplicaPlan.ForWrite writePlan = repairPlan(replicas, EndpointsForRange.copyOf(Lists.newArrayList(repairs.keySet())));
            InstrumentedReadRepairHandler handler = createRepairHandler(repairs, writePlan);
            handler.sendInitialRepairs();

            handler.mutationsSent.clear();
            handler.maybeSendAdditionalWrites(0, TimeUnit.NANOSECONDS);
            Assert.assertEquals(1, handler.mutationsSent.size());
            Mutation speculative = handler.mutationsSent.get(target3);
            Assert.assertNotNull("no speculative repair sent to " + target3, speculative);
            assertMutationEqual(resolved, speculative);

            Message<Mutation> message = Message.out(Verb.READ_REPAIR_REQ, speculative);
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                Message.serializer.serialize(message, out, target3Version);
                Assert.assertEquals(message.serializedSize(target3Version), out.getLength());
            }
            // a round trip through this software's own serializer: the pre-4.0 layout itself is checked byte by byte
            // by V11SerializationAuditTest, and against HCD 1.2 by the mixed-mode in-JVM tests
            // (MixedModeSpeculativeReadRepairTest for this write)
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                Mutation.serializer.serialize(speculative, out, target3Version);
                Assert.assertEquals(speculative.serializedSize(target3Version), out.getLength());
                try (DataInputBuffer in = new DataInputBuffer(out.getData(), 0, out.getLength()))
                {
                    assertMutationEqual(resolved, Mutation.serializer.deserialize(in, target3Version));
                }
            }
        }
        finally
        {
            restoreVersion(versions, target3VersionBefore, minClusterVersionBefore);
            DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
        }
    }

    /**
     * If we've received enough acks, we shouldn't send any additional mutations
     */
    @Test
    public void noAdditionalMutationRequired() throws Exception
    {
        Map<Replica, Mutation> repairs = new HashMap<>();
        repairs.put(replica1, mutation(cell2));
        repairs.put(replica2, mutation(cell1));

        InstrumentedReadRepairHandler handler = createRepairHandler(repairs);
        handler.sendInitialRepairs();
        handler.ack(target1);
        handler.ack(target2);

        // both replicas have acked, we shouldn't send anything else out
        handler.mutationsSent.clear();
        handler.maybeSendAdditionalWrites(0, TimeUnit.NANOSECONDS);
        Assert.assertTrue(handler.mutationsSent.isEmpty());
    }

    /**
     * If there are no additional nodes we can send mutations to, we... shouldn't
     */
    @Test
    public void noAdditionalMutationPossible() throws Exception
    {
        Map<Replica, Mutation> repairs = new HashMap<>();
        repairs.put(replica1, mutation(cell2));
        repairs.put(replica2, mutation(cell1));

        InstrumentedReadRepairHandler handler = createRepairHandler(repairs);
        handler.sendInitialRepairs();

        // we've already sent mutations to all candidates, so we shouldn't send any more
        handler.mutationsSent.clear();
        handler.maybeSendAdditionalWrites(0, TimeUnit.NANOSECONDS);
        Assert.assertTrue(handler.mutationsSent.isEmpty());
    }

    /**
     * If we didn't send a repair to a replica because there wasn't a diff with the
     * resolved column family, we shouldn't send it a speculative mutation
     */
    @Test
    public void mutationsArentSentToInSyncNodes() throws Exception
    {
        Mutation repair1 = mutation(cell2);

        Map<Replica, Mutation> repairs = new HashMap<>();
        repairs.put(replica1, repair1);

        // check that the correct initial mutations are sent out
        InstrumentedReadRepairHandler handler = createRepairHandler(repairs, repairPlan(replicas, EndpointsForRange.of(replica1, replica2)));
        handler.sendInitialRepairs();
        Assert.assertEquals(1, handler.mutationsSent.size());
        Assert.assertTrue(handler.mutationsSent.containsKey(target1));

        // check that speculative mutations aren't sent to target2
        handler.mutationsSent.clear();
        handler.maybeSendAdditionalWrites(0, TimeUnit.NANOSECONDS);
        Assert.assertEquals(1, handler.mutationsSent.size());
        Assert.assertTrue(handler.mutationsSent.containsKey(target3));
    }

    @Test
    public void onlyBlockOnQuorum()
    {
        Map<Replica, Mutation> repairs = new HashMap<>();
        repairs.put(replica1, mutation(cell1));
        repairs.put(replica2, mutation(cell2));
        repairs.put(replica3, mutation(cell3));
        Assert.assertEquals(3, repairs.size());

        InstrumentedReadRepairHandler handler = createRepairHandler(repairs);
        handler.sendInitialRepairs();

        Assert.assertFalse(getCurrentRepairStatus(handler));
        handler.ack(target1);
        Assert.assertFalse(getCurrentRepairStatus(handler));

        // here we should stop blocking, even though we've sent 3 repairs
        handler.ack(target2);
        Assert.assertTrue(getCurrentRepairStatus(handler));
    }

    /**
     * For dc local consistency levels, we will run into assertion error because no remote DC replicas should be contacted
     */
    @Test(expected = IllegalStateException.class)
    public void remoteDCSpeculativeRetryTest() throws Exception
    {
        Map<Replica, Mutation> repairs = new HashMap<>();
        repairs.put(replica1, mutation(cell1));
        repairs.put(remoteReplica1, mutation(cell1));

        EndpointsForRange participants = EndpointsForRange.of(replica1, replica2, remoteReplica1, remoteReplica2);
        ReplicaPlan.ForWrite writePlan = repairPlan(replicaPlan(ks, ConsistencyLevel.LOCAL_QUORUM, participants));
        createRepairHandler(repairs, writePlan);
    }

    private boolean getCurrentRepairStatus(BlockingPartitionRepair handler)
    {
        return handler.awaitRepairsUntil(nanoTime(), NANOSECONDS);
    }
}
