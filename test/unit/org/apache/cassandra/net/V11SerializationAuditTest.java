/*
 * Copyright IBM Corp.
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

package org.apache.cassandra.net;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.batchlog.Batch;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.BufferClusteringBound;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.SnapshotCommand;
import org.apache.cassandra.db.TruncateRequest;
import org.apache.cassandra.db.TruncateResponse;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.IndexHints;
import org.apache.cassandra.db.filter.OptimizerOptions;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.CellData;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.GossipDigest;
import org.apache.cassandra.gms.GossipDigestSyn;
import org.apache.cassandra.gms.GossipMessages;
import org.apache.cassandra.gms.GossipShutdown;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.hints.Hint;
import org.apache.cassandra.hints.HintMessages;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.BallotGenerator;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PrepareResponse;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.StorageCompatibilityMode;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL;
import static org.apache.cassandra.locator.InetAddressAndPort.Serializer.inetAddressAndPortSerializer;
import static org.apache.cassandra.net.MessagingService.VERSION_3014;
import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * The serializer audit of the DSE 6 compatibility mode: every verb a DSE 6 peer can receive or send at messaging
 * version 11 is serialized through {@link Message.Serializer} in the pre-4.0 layout with a representative payload,
 * sized, size-inferred, deserialized and serialized again byte for byte; every other verb is refused before anything
 * is written. The header, the params and the payloads whose 3.0 layout differs from 4.0 are checked byte by byte
 * against the layout DSE 6 uses with non-DSE peers, the layout of Cassandra 3.0.14. The pre-4.0 layouts are only
 * spoken in DSE 6 compatibility mode, which every test here runs in but
 * {@link #pre40LayoutsAreNotUsedWithTheModeOff()}.
 */
public class V11SerializationAuditTest extends CQLTester
{
    /** The 3.0 verb ids DSE maps: the requests of this table must all serialize at version 11. */
    private static final int[] DSE_REQUEST_IDS = { 0, 1, 2, 3, 5, 6, 9, 14, 15, 16, 18, 19, 20, 22, 24, 27, 28, 29, 31, 33, 34, 35 };

    private static final InetAddressAndPort PEER = peer("127.0.0.42");
    /** {@code ReadCommand.Serializer.NEEDS_RECONCILIATION}. */
    private static final int RECONCILIATION_BIT = 0x10;
    /** {@code ReadCommand.Serializer.ACCEPTS_TRANSIENT}. */
    private static final int ACCEPTS_TRANSIENT_BIT = 0x08;
    /** Where the address starts in a pre-4.0 message: after the magic, the id and the creation time. */
    private static final int ADDRESS_OFFSET = 12;

    private InternodeCompatibilityMode modeBefore;
    private WithProperties allowDdl;

    private static InetAddressAndPort peer(String address)
    {
        try
        {
            return InetAddressAndPort.getByName(address);
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }

    @Before
    public void dse6Mode()
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        allowDdl = new WithProperties().set(INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL, true);
    }

    @After
    public void restoreMode()
    {
        allowDdl.close();
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
    }

    @Test
    public void everyVerbEitherMapsToThe30DialectOrIsRefused()
    {
        List<Verb> mapped = new ArrayList<>();
        List<Verb> refused = new ArrayList<>();
        for (Verb verb : Verb.getValues())
        {
            if (verb.isResponse() || verb == Verb.FAILURE_RSP)
            {
                assertTrue(verb.existsInPre40Dialect());
                Verb pre40 = verb.toPre40Verb();
                assertTrue(verb + " -> " + pre40, pre40 == Verb.REQUEST_RSP || pre40 == Verb.INTERNAL_RSP);
                continue;
            }
            if (Arrays.binarySearch(DSE_REQUEST_IDS, verb.id) >= 0)
            {
                assertTrue(verb.existsInPre40Dialect());
                assertSame(verb, verb.toPre40Verb());
                mapped.add(verb);
            }
            else
            {
                assertFalse(verb.existsInPre40Dialect());
                assertThrows(verb.toString(), UnsupportedOperationException.class, verb::toPre40Verb);
                refused.add(verb);
            }
        }
        assertEquals("every DSE request id has a verb", DSE_REQUEST_IDS.length, mapped.size());
        assertTrue(refused.contains(Verb.PING_REQ));
        assertTrue(refused.contains(Verb.MULTI_RANGE_REQ));
        assertTrue(refused.contains(Verb.PAXOS2_PREPARE_REQ));
        assertTrue(refused.contains(Verb.VALIDATION_REQ));
        assertTrue(refused.contains(Verb.PREPARE_MSG));
        // the test verbs have the ids of Cassandra 3.0's BOOTSTRAP_TOKEN and TREE_REQUEST, which DSE dropped
        assertTrue(refused.contains(Verb._TEST_1));
        assertTrue(refused.contains(Verb._TEST_2));
        assertSame(Verb.INTERNAL_RSP, Verb.FAILURE_RSP.toPre40Verb());
        assertSame(Verb.INTERNAL_RSP, Verb.ECHO_RSP.toPre40Verb());
        assertSame(Verb.REQUEST_RSP, Verb.READ_RSP.toPre40Verb());
    }

    @Test
    public void refusedVerbsAreNotSerialized()
    {
        Message<PingRequest> ping = Message.out(Verb.PING_REQ, PingRequest.forSmall);
        assertThrows(UnsupportedOperationException.class, () -> ping.serializedSize(VERSION_3014));
        assertThrows(UnsupportedOperationException.class, () -> serialize(ping));
    }

    @Test
    public void requestsRoundTripAtVersion11() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, s int static, PRIMARY KEY (k, c))");
        TableMetadata metadata = currentTableMetadata();
        Mutation mutation = mutation(metadata, 1);
        DecoratedKey key = metadata.partitioner.decorateKey(Int32Type.instance.decompose(1));
        long nowInSec = FBUtilities.nowInSeconds();
        Ballot ballot = BallotGenerator.Global.nextBallot(Ballot.Flag.NONE);
        Commit commit = Commit.Proposal.of(ballot, mutation.getPartitionUpdate(metadata));
        EndpointState state = endpointState();
        Map<InetAddressAndPort, EndpointState> states = new HashMap<>();
        states.put(PEER, state);
        GossipDigest digest = GossipMessages.digest(PEER, 7, 9);

        Map<Verb, Object> payloads = new HashMap<>();
        payloads.put(Verb.MUTATION_REQ, mutation);
        payloads.put(Verb.HINT_REQ, HintMessages.create(UUID.randomUUID(), Hint.create(mutation, FBUtilities.timestampMicros() / 1000)));
        payloads.put(Verb.READ_REPAIR_REQ, mutation);
        payloads.put(Verb.READ_REQ, SinglePartitionReadCommand.fullPartitionRead(metadata, nowInSec, key));
        payloads.put(Verb.BATCH_STORE_REQ, Batch.createLocal(TimeUUID.Generator.nextTimeUUID(), FBUtilities.timestampMicros(), Collections.singleton(mutation)));
        payloads.put(Verb.BATCH_REMOVE_REQ, TimeUUID.Generator.nextTimeUUID());
        payloads.put(Verb.RANGE_REQ, PartitionRangeReadCommand.allDataRead(metadata, nowInSec));
        payloads.put(Verb.GOSSIP_DIGEST_SYN, new GossipDigestSyn("cluster", DatabaseDescriptor.getPartitionerName(), Collections.singletonList(digest)));
        payloads.put(Verb.GOSSIP_DIGEST_ACK, GossipMessages.ack(Collections.singletonList(digest), states));
        payloads.put(Verb.GOSSIP_DIGEST_ACK2, GossipMessages.ack2(states));
        payloads.put(Verb.SCHEMA_PUSH_REQ, Collections.singletonList(mutation));
        payloads.put(Verb.TRUNCATE_REQ, new TruncateRequest(metadata.keyspace, metadata.name));
        payloads.put(Verb.SCHEMA_VERSION_REQ, noPayload);
        payloads.put(Verb.REPLICATION_DONE_REQ, noPayload);
        payloads.put(Verb.COUNTER_MUTATION_REQ, new CounterMutation(mutation, ConsistencyLevel.ONE));
        payloads.put(Verb.SNAPSHOT_REQ, new SnapshotCommand(metadata.keyspace, metadata.name, "tag", false));
        payloads.put(Verb.SCHEMA_PULL_REQ, noPayload);
        payloads.put(Verb.GOSSIP_SHUTDOWN, new GossipShutdown(state));
        payloads.put(Verb.ECHO_REQ, noPayload);
        payloads.put(Verb.PAXOS_PREPARE_REQ, commit);
        payloads.put(Verb.PAXOS_PROPOSE_REQ, commit);
        payloads.put(Verb.PAXOS_COMMIT_REQ, new Commit.Agreed(ballot, mutation.getPartitionUpdate(metadata)));

        for (int id : DSE_REQUEST_IDS)
        {
            Verb verb = Verb.fromId(id);
            assertTrue("payload for " + verb, payloads.containsKey(verb));
            Message<?> request = Message.out(verb, payloads.get(verb));
            Message<?> received = roundTrip(request, verb.id);
            assertSame(verb, received.verb());
            assertEquals(request.id(), received.id());
            assertEquals(PEER, received.from());
        }
    }

    @Test
    public void responsesRoundTripAtVersion11() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))");
        TableMetadata metadata = currentTableMetadata();
        Mutation mutation = mutation(metadata, 2);
        long nowInSec = FBUtilities.nowInSeconds();
        ReadCommand read = PartitionRangeReadCommand.allDataRead(metadata, nowInSec);
        Ballot ballot = BallotGenerator.Global.nextBallot(Ballot.Flag.NONE);
        Commit commit = Commit.Proposal.of(ballot, mutation.getPartitionUpdate(metadata));

        Map<Verb, Object> responses = new HashMap<>();
        responses.put(Verb.MUTATION_RSP, noPayload);
        responses.put(Verb.HINT_RSP, noPayload);
        responses.put(Verb.READ_REPAIR_RSP, noPayload);
        responses.put(Verb.READ_RSP, ReadResponse.createSimpleDataResponse(EmptyIterators.unfilteredPartition(metadata), ColumnFilter.all(metadata)));
        responses.put(Verb.BATCH_STORE_RSP, noPayload);
        responses.put(Verb.BATCH_REMOVE_RSP, noPayload);
        responses.put(Verb.RANGE_RSP, ReadResponse.createDigestResponse(EmptyIterators.unfilteredPartition(metadata), read));
        responses.put(Verb.SCHEMA_PUSH_RSP, noPayload);
        responses.put(Verb.TRUNCATE_RSP, new TruncateResponse(metadata.keyspace, metadata.name, true));
        responses.put(Verb.SCHEMA_VERSION_RSP, UUID.randomUUID());
        responses.put(Verb.REPLICATION_DONE_RSP, noPayload);
        responses.put(Verb.COUNTER_MUTATION_RSP, noPayload);
        responses.put(Verb.SNAPSHOT_RSP, noPayload);
        responses.put(Verb.SCHEMA_PULL_RSP, Collections.singletonList(mutation));
        responses.put(Verb.ECHO_RSP, noPayload);
        responses.put(Verb.PAXOS_PREPARE_RSP, new PrepareResponse(true, commit, commit));
        responses.put(Verb.PAXOS_PROPOSE_RSP, Boolean.TRUE);
        responses.put(Verb.PAXOS_COMMIT_RSP, noPayload);

        for (int id : DSE_REQUEST_IDS)
        {
            Verb requestVerb = Verb.fromId(id);
            Verb responseVerb = requestVerb.responseVerb;
            if (responseVerb == null)
                continue;
            assertTrue("payload for " + responseVerb, responses.containsKey(responseVerb));
            // a response is only readable when the request it answers is known: a DSE peer names no verb in it
            Message<?> request = Message.out(requestVerb, noPayload);
            // (mutation callbacks need a write handler; their responses carry no payload, so none is needed to read them)
            boolean registered = requestVerb != Verb.MUTATION_REQ && requestVerb != Verb.COUNTER_MUTATION_REQ;
            if (registered)
                MessagingService.instance().callbacks.addWithExpiration(message -> {}, request, PEER);
            try
            {
                Message<?> response = request.responseWith(responses.get(responseVerb));
                int expectedId = responseVerb.toPre40Verb().id;
                Message<?> received = roundTrip(response, expectedId);
                assertSame(responseVerb.toPre40Verb(), received.verb());
                assertEquals(request.id(), received.id());
                if (responses.get(responseVerb) == noPayload)
                    assertTrue(received.payload == null || received.payload == noPayload);
                else
                    assertNotNull(responseVerb.toString(), received.payload);
            }
            finally
            {
                if (registered)
                    MessagingService.instance().callbacks.remove(request.id(), PEER);
            }
        }

        // a failure response travels as INTERNAL_RSP with the FAIL and FAIL_REASON params and comes back as FAILURE_RSP
        Message<?> request = Message.out(Verb.READ_REQ, noPayload);
        MessagingService.instance().callbacks.addWithExpiration(message -> {}, request, PEER);
        try
        {
            Message<RequestFailureReason> failure = request.failureResponse(RequestFailureReason.READ_TOO_MANY_TOMBSTONES);
            Message<?> received = roundTrip(failure, Verb.INTERNAL_RSP.id);
            assertSame(Verb.FAILURE_RSP, received.verb());
            assertEquals(RequestFailureReason.READ_TOO_MANY_TOMBSTONES, received.payload);
            // a reason Cassandra 3.0 does not know is sent as UNKNOWN
            Message<?> unknown = roundTrip(request.failureResponse(RequestFailureReason.INDEX_NOT_AVAILABLE), Verb.INTERNAL_RSP.id);
            assertEquals(RequestFailureReason.UNKNOWN, unknown.payload);
        }
        finally
        {
            MessagingService.instance().callbacks.remove(request.id(), PEER);
        }
    }

    /**
     * The header: magic, int id, int creation time, the sender's address with a 1-byte length and
     * no port, int verb id, int param count, then each param as its UTF-8 alias, an int length and the value, and the
     * payload as an int length and the bytes. A flag travels as a param with the single byte 0 as value.
     */
    @Test
    public void headerAndFlagParamBytes() throws Throwable
    {
        Message<NoPayload> message = Message.builder(Verb.ECHO_REQ, noPayload)
                                            .withId(17)
                                            .from(peer("127.0.0.1"))
                                            .withFlag(MessageFlag.CALL_BACK_ON_FAILURE)
                                            .build();
        byte[] bytes = serialize(message);
        byte[] expected = expectedMessage(bytes, message.from(), 17, Verb.ECHO_REQ.id, 1, out -> {
            out.writeUTF("CAL_BAC");
            out.writeInt(1);
            out.writeByte(0);
        }, new byte[0]);
        assertArrayEquals(expected, bytes);
        assertEquals("00074341 4c5f4241 43", hex(bytes, 25, 9)); // the alias as writeUTF writes it: a 2-byte length and ASCII
    }

    /** A failure response: INTERNAL_RSP, the FAIL flag and the FAIL_REASON param, a 2-byte code, no payload. */
    @Test
    public void failureResponseBytes() throws Throwable
    {
        Message<?> request = Message.builder(Verb.READ_REQ, noPayload).withId(23).from(peer("127.0.0.1")).build();
        Message<RequestFailureReason> failure = request.failureResponse(RequestFailureReason.READ_TOO_MANY_TOMBSTONES);
        byte[] bytes = serialize(failure);
        // REQUEST_VERB_NAME has no pre-4.0 alias: it is not sent
        byte[] expected = expectedMessage(bytes, failure.from(), 23, Verb.INTERNAL_RSP.id, 2, out -> {
            out.writeUTF("FAIL");
            out.writeInt(1);
            out.writeByte(0);
            out.writeUTF("FAIL_REASON");
            out.writeInt(2);
            out.writeShort(RequestFailureReason.READ_TOO_MANY_TOMBSTONES.code);
        }, new byte[0]);
        assertArrayEquals(expected, bytes);
    }

    /**
     * Forwarding: FWD_TO is an int count and, for each target, its address with a 1-byte length and no port
     * and an int message id; FWD_FRM is the raw address, 4 or 16 bytes, with neither a length nor a port.
     */
    @Test
    public void forwardingParamBytes() throws Throwable
    {
        InetAddressAndPort forwardTo = peer("127.0.0.43");
        InetAddressAndPort respondTo = peer("127.0.0.44");
        Message<NoPayload> message = Message.builder(Verb.ECHO_REQ, noPayload)
                                            .withId(19)
                                            .from(peer("127.0.0.1"))
                                            .withParam(ParamType.FORWARD_TO, new ForwardingInfo(Collections.singletonList(forwardTo), new long[]{ 18 }))
                                            .withParam(ParamType.RESPOND_TO, respondTo)
                                            .build();
        byte[] bytes = serialize(message);
        byte[] expected = expectedMessage(bytes, message.from(), 19, Verb.ECHO_REQ.id, 2, out -> {
            out.writeUTF("FWD_TO");
            out.writeInt(4 + 1 + 4 + 4);
            out.writeInt(1);
            out.writeByte(4);
            out.write(forwardTo.getAddress().getAddress());
            out.writeInt(18);
            out.writeUTF("FWD_FRM");
            out.writeInt(4);
            out.write(respondTo.getAddress().getAddress());
        }, new byte[0]);
        assertArrayEquals(expected, bytes);

        Message<?> received = roundTrip(message, Verb.ECHO_REQ.id);
        assertEquals(respondTo, received.respondTo());
        assertEquals(forwardTo, received.forwardTo().targets.get(0));
        assertEquals(18, received.forwardTo().messageIds[0]);
    }

    /**
     * The column filter at 11 has no FETCH_ALL_STATICS bit (0x08), and a filter read at 11 that fetches all regular columns
     * fetches all static columns too, as in Cassandra 3.0 (ColumnFilterTest has the other layouts).
     */
    @Test
    public void columnFilterAtVersion11() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, s int static, PRIMARY KEY (k, c))");
        TableMetadata metadata = currentTableMetadata();
        ColumnMetadata v = metadata.getColumn(ColumnIdentifier.getInterned("v", false));
        ColumnFilter filter = ColumnFilter.allRegularColumnsBuilder(metadata, true).add(v).build();
        assertTrue(filter.fetchesAllColumns(true));
        byte[] at11;
        byte[] at12;
        try (DataOutputBuffer out11 = new DataOutputBuffer(); DataOutputBuffer out12 = new DataOutputBuffer())
        {
            ColumnFilter.serializer.serialize(filter, out11, VERSION_3014);
            ColumnFilter.serializer.serialize(filter, out12, MessagingService.VERSION_40);
            at11 = out11.toByteArray();
            at12 = out12.toByteArray();
        }
        assertEquals(0x03, at11[0]); // FETCH_ALL_REGULARS | HAS_QUERIED
        assertEquals(0x0B, at12[0]); // and FETCH_ALL_STATICS
        assertArrayEquals(Arrays.copyOfRange(at12, 1, at12.length), Arrays.copyOfRange(at11, 1, at11.length));

        ColumnFilter partial = ColumnFilter.allRegularColumnsBuilder(metadata, false).add(v).build();
        assertFalse(partial.fetchesAllColumns(true));
        ReadCommand read = PartitionRangeReadCommand.create(metadata, FBUtilities.nowInSeconds(), partial, RowFilter.none(),
                                                            DataLimits.NONE, DataRange.allData(metadata.partitioner));
        Message<?> received = roundTrip(Message.out(Verb.RANGE_REQ, read), Verb.RANGE_REQ.id);
        assertTrue(((ReadCommand) received.payload).columnFilter().fetchesAllColumns(true));
    }

    /** A data response at 11 has no repaired-data digest and no conclusive flag: only the digest and the data. */
    @Test
    public void readResponseAtVersion11() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))");
        TableMetadata metadata = currentTableMetadata();
        ReadCommand read = PartitionRangeReadCommand.allDataRead(metadata, FBUtilities.nowInSeconds());
        ByteBuffer repairedDigest = ByteBuffer.wrap(new byte[]{ 1, 2, 3, 4 });
        ReadResponse response = ReadResponse.createRemoteDataResponse(EmptyIterators.unfilteredPartition(metadata), repairedDigest, false,
                                                                      read, MessagingService.current_version);
        byte[] at11 = serialize(response, VERSION_3014);
        byte[] at12 = serialize(response, MessagingService.VERSION_40);
        // 4.0: empty digest, the repaired-data digest (vint length and bytes), the conclusive flag, then the data;
        // 3.0: empty digest, then the data
        assertArrayEquals(new byte[]{ 0, 4, 1, 2, 3, 4, 0 }, Arrays.copyOfRange(at12, 0, 7));
        byte[] expected = new byte[at12.length - 6];
        expected[0] = 0;
        System.arraycopy(at12, 7, expected, 1, at12.length - 7);
        assertArrayEquals(expected, at11);
        try (DataInputBuffer in = new DataInputBuffer(at11))
        {
            ReadResponse received = ReadResponse.serializer.deserialize(in, VERSION_3014);
            assertEquals(0, in.available());
            assertFalse(received.isDigestResponse());
            assertEquals(ByteBufferUtil.EMPTY_BYTE_BUFFER, received.repairedDataDigest());
            assertTrue(received.isRepairedDigestConclusive());
        }
    }

    /**
     * A filtered read above CL ONE carries the NEEDS_RECONCILIATION flag (CASSANDRA-19018), bit 4 of the flags byte. The
     * 3.0 layout has only the digest, thrift and index bits: at version 11 the bit is left out, and the
     * DSE replica filters strictly as before; from 4.0 on it is sent as usual.
     */
    @Test
    public void reconciliationFlagIsNotSentAtVersion11() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))");
        TableMetadata metadata = currentTableMetadata();
        RowFilter.Builder builder = RowFilter.builder(true);
        builder.add(metadata.getColumn(ColumnIdentifier.getInterned("v", false)), Operator.EQ, UTF8Type.instance.decompose("x"));
        RowFilter filter = builder.build();
        assertTrue(filter.needsReconciliation());
        ReadCommand read = PartitionRangeReadCommand.create(metadata, FBUtilities.nowInSeconds(), ColumnFilter.all(metadata), filter,
                                                            DataLimits.NONE, DataRange.allData(metadata.partitioner));

        for (int version : new int[]{ MessagingService.VERSION_30, VERSION_3014, MessagingService.VERSION_40, MessagingService.current_version })
        {
            byte[] bytes;
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                ReadCommand.serializer.serialize(read, out, version);
                bytes = out.toByteArray();
            }
            assertEquals(ReadCommand.serializer.serializedSize(read, version), bytes.length);
            boolean pre40 = version < MessagingService.VERSION_40;
            // byte 0 is the kind, byte 1 the flags: nothing but the reconciliation bit is set for this command
            assertEquals("flags at version " + version, pre40 ? 0 : RECONCILIATION_BIT, bytes[1]);
            try (DataInputBuffer in = new DataInputBuffer(bytes))
            {
                ReadCommand received = ReadCommand.serializer.deserialize(in, version);
                assertEquals(0, in.available());
                assertEquals(!pre40, received.rowFilter().needsReconciliation());
                assertEquals(filter.expressions().toString(), received.rowFilter().expressions().toString());
            }
        }

        // and through the message codec, as a RANGE_REQ
        Message<?> received = roundTrip(Message.out(Verb.RANGE_REQ, read), Verb.RANGE_REQ.id);
        assertFalse(((ReadCommand) received.payload).rowFilter().needsReconciliation());

        // a DSE 6 node is never a transient replica: the ACCEPTS_TRANSIENT bit (0x08) is not sent at 11 either
        ReadCommand transientRead = PartitionRangeReadCommand.allDataRead(metadata, FBUtilities.nowInSeconds())
                                                             .copyAsTransientQuery(Replica.transientReplica(PEER, new Range<>(metadata.partitioner.getMinimumToken(),
                                                                                                                              metadata.partitioner.getMinimumToken())));
        assertTrue(transientRead.acceptsTransient());
        assertEquals(0, readCommandFlags(transientRead, VERSION_3014));
        assertEquals(ACCEPTS_TRANSIENT_BIT, readCommandFlags(transientRead, MessagingService.VERSION_40));

        // with the mode off version 10 keeps the flags it had before the mode existed (the DSE <-> CC migration path)
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        assertEquals(RECONCILIATION_BIT, readCommandFlags(read, MessagingService.VERSION_30));
        assertEquals(ACCEPTS_TRANSIENT_BIT, readCommandFlags(transientRead, MessagingService.VERSION_30));
    }

    /**
     * The SAI optimizer options of a row filter (CNDB-19234) are on the wire from {@code VERSION_DS_20} only. A read
     * without them ({@link OptimizerOptions#NONE}, every read without a {@code WITH optimizer_options} clause) is
     * written at version 11 as before: nothing is written for the options, so the filter's bytes are those it has at
     * {@code VERSION_40}, where neither options nor index hints exist, and the read round-trips with no byte left over.
     * A read with options cannot be written at 11 (in {@code DSE_6} the coordinator refuses it first).
     */
    @Test
    public void optimizerOptionsAreNotSentAtVersion11() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))");
        TableMetadata metadata = currentTableMetadata();
        ColumnMetadata v = metadata.getColumn(ColumnIdentifier.getInterned("v", false));
        OptimizerOptions options = OptimizerOptions.create(0, 3, true, null);

        RowFilter.Builder plainBuilder = RowFilter.builder(null, IndexHints.NONE);
        plainBuilder.add(v, Operator.EQ, UTF8Type.instance.decompose("x"));
        RowFilter plainFilter = plainBuilder.build();
        assertSame(OptimizerOptions.NONE, plainFilter.optimizerOptions);
        RowFilter.Builder optimizedBuilder = RowFilter.builder(null, IndexHints.NONE, options);
        optimizedBuilder.add(v, Operator.EQ, UTF8Type.instance.decompose("x"));
        RowFilter optimizedFilter = optimizedBuilder.build();

        ReadCommand plain = PartitionRangeReadCommand.create(metadata, FBUtilities.nowInSeconds(), ColumnFilter.all(metadata), plainFilter,
                                                             DataLimits.NONE, DataRange.allData(metadata.partitioner));
        ReadCommand optimized = PartitionRangeReadCommand.create(metadata, FBUtilities.nowInSeconds(), ColumnFilter.all(metadata), optimizedFilter,
                                                                 DataLimits.NONE, DataRange.allData(metadata.partitioner));

        // NONE: nothing written for the options at 11; the filter's bytes at 11 are its bytes at VERSION_40
        assertEquals(0, OptimizerOptions.serializer.serializedSize(OptimizerOptions.NONE, VERSION_3014));
        byte[] filterAt11 = filterBytes(plainFilter, VERSION_3014);
        // the 3.0.14 layout: the unused thrift boolean, the number of expressions (an unsigned vint), then the
        // expression: its kind (SIMPLE), the column name with a short length, the operator (EQ) in an int, the value
        // with a short length
        assertEquals("00" + "01" + "00" + "0001" + "76" + "00000000" + "0001" + "78",
                     ByteBufferUtil.bytesToHex(ByteBuffer.wrap(filterAt11)));
        assertArrayEquals(filterBytes(plainFilter, MessagingService.VERSION_40), filterAt11);
        assertEquals(RowFilter.serializer.serializedSize(plainFilter, VERSION_3014), filterAt11.length);
        // from VERSION_DS_20 even NONE is written, as flags 0 in an int, after the bytes VERSION_DS_12 has
        byte[] filterAtDs12 = filterBytes(plainFilter, MessagingService.VERSION_DS_12);
        byte[] filterAtDs20 = filterBytes(plainFilter, MessagingService.VERSION_DS_20);
        assertArrayEquals(Arrays.copyOf(filterAtDs12, filterAtDs12.length + Integer.BYTES), filterAtDs20);

        // and the read round-trips at 11 with no byte left over
        byte[] bytes;
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            ReadCommand.serializer.serialize(plain, out, VERSION_3014);
            bytes = out.toByteArray();
        }
        assertEquals(ReadCommand.serializer.serializedSize(plain, VERSION_3014), bytes.length);
        try (DataInputBuffer in = new DataInputBuffer(bytes))
        {
            ReadCommand received = ReadCommand.serializer.deserialize(in, VERSION_3014);
            assertEquals(0, in.available());
            assertSame(OptimizerOptions.NONE, received.rowFilter().optimizerOptions);
            assertEquals(plainFilter.expressions().toString(), received.rowFilter().expressions().toString());
        }

        // options other than NONE: never written at 11
        IllegalStateException refused = assertThrows(IllegalStateException.class, () -> {
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                ReadCommand.serializer.serialize(optimized, out, VERSION_3014);
            }
        });
        assertEquals("Unable to serialize SAI optimizer options with messaging version: " + VERSION_3014, refused.getMessage());

        // while at the current version they round-trip
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            ReadCommand.serializer.serialize(optimized, out, MessagingService.current_version);
            try (DataInputBuffer in = new DataInputBuffer(out.toByteArray()))
            {
                assertEquals(options, ReadCommand.serializer.deserialize(in, MessagingService.current_version).rowFilter().optimizerOptions);
                assertEquals(0, in.available());
            }
        }
    }

    private static byte[] filterBytes(RowFilter filter, int version) throws Exception
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            RowFilter.serializer.serialize(filter, out, version);
            return out.toByteArray();
        }
    }

    private static int readCommandFlags(ReadCommand read, int version) throws Exception
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            ReadCommand.serializer.serialize(read, out, version);
            return out.toByteArray()[1]; // byte 0 is the kind, byte 1 the flags
        }
    }

    /**
     * A response read at 11 carries the generic REQUEST_RSP verb. While its callback is registered it can be sized and
     * serialized again at any version, both through the serializer of the request's response verb (as in HCD 1.2).
     */
    @Test
    public void responseReadAtVersion11SerializesAtEveryVersion() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))");
        TableMetadata metadata = currentTableMetadata();
        ReadCommand read = PartitionRangeReadCommand.allDataRead(metadata, FBUtilities.nowInSeconds());
        Message<ReadCommand> request = Message.out(Verb.RANGE_REQ, read);
        MessagingService.instance().callbacks.addWithExpiration(message -> {}, request, PEER);
        try
        {
            Message<?> response = request.responseWith(ReadResponse.createDigestResponse(EmptyIterators.unfilteredPartition(metadata), read));
            Message<?> received = roundTrip(response, Verb.REQUEST_RSP.id);
            assertSame(Verb.REQUEST_RSP, received.verb());
            for (int version : new int[]{ MessagingService.VERSION_40, MessagingService.current_version })
            {
                try (DataOutputBuffer out = new DataOutputBuffer())
                {
                    Message.serializer.serialize(received, out, version);
                    assertEquals("version " + version, received.serializedSize(version), out.getLength());
                }
            }
        }
        finally
        {
            MessagingService.instance().callbacks.remove(request.id(), PEER);
        }
    }

    /**
     * A DSE 6 peer stamps a response with the start of the operation and names only the generic response verbs: such a
     * response expires on receipt with the callback of its request, not with the RPC timeout of REQUEST_RSP counted from
     * that stamp, or a TRUNCATE response slower than the RPC timeout would be dropped with cross_node_timeout on.
     */
    @Test
    public void pre40ResponseExpiresWithItsCallback() throws Throwable
    {
        boolean crossNodeTimeout = DatabaseDescriptor.hasCrossNodeTimeout();
        DatabaseDescriptor.setCrossNodeTimeout(true);
        Message<TruncateRequest> request = Message.out(Verb.TRUNCATE_REQ, new TruncateRequest("ks", "t"));
        MessagingService.instance().callbacks.addWithExpiration(message -> {}, request, PEER);
        try
        {
            long rpcTimeout = DatabaseDescriptor.getRpcTimeout(TimeUnit.NANOSECONDS);
            assertTrue(Verb.TRUNCATE_REQ.expiresAfterNanos() > rpcTimeout + TimeUnit.SECONDS.toNanos(2));
            // started a little over the RPC timeout ago, well within the truncate timeout
            long createdAt = approxTime.now() - rpcTimeout - TimeUnit.SECONDS.toNanos(1);
            Message<TruncateResponse> response = Message.builder(Verb.TRUNCATE_RSP, new TruncateResponse("ks", "t", true))
                                                        .withId(request.id())
                                                        .from(PEER)
                                                        .withCreatedAt(createdAt)
                                                        .build();
            byte[] bytes = serialize(response);
            long now = approxTime.now();
            Message.Header header = Message.serializer.extractHeader(ByteBuffer.wrap(bytes), PEER, now, VERSION_3014);
            assertSame(Verb.INTERNAL_RSP, header.verb);
            long callbackDeadline = MessagingService.instance().callbacks.get(request.id(), PEER).expiresAtNanos;
            assertEquals(callbackDeadline, header.expiresAtNanos);
            assertFalse(approxTime.isAfter(now, header.expiresAtNanos));
            // and so does the whole message, read in one go
            try (DataInputBuffer in = new DataInputBuffer(bytes))
            {
                Message<?> received = Message.serializer.deserialize(in, PEER, VERSION_3014);
                assertEquals(callbackDeadline, received.expiresAtNanos());
            }

            // without a callback the response verb's own timeout applies, from the peer's stamp: long expired here
            MessagingService.instance().callbacks.remove(request.id(), PEER);
            Message.Header orphan = Message.serializer.extractHeader(ByteBuffer.wrap(bytes), PEER, now, VERSION_3014);
            assertTrue(approxTime.isAfter(now, orphan.expiresAtNanos));
        }
        finally
        {
            MessagingService.instance().callbacks.remove(request.id(), PEER);
            DatabaseDescriptor.setCrossNodeTimeout(crossNodeTimeout);
        }
    }

    /** The payload of a pre-4.0 response nobody waits for is skipped, and counted. */
    @Test
    public void pre40PayloadWithoutCallbackIsSkipped() throws Throwable
    {
        Message<TruncateResponse> response = Message.builder(Verb.TRUNCATE_RSP, new TruncateResponse("ks", "t", true))
                                                    .withId(4242)
                                                    .from(PEER)
                                                    .build();
        byte[] bytes = serialize(response);
        long skippedBefore = Message.Serializer.skippedPayloadsPre40();
        try (DataInputBuffer in = new DataInputBuffer(bytes))
        {
            Message<?> received = Message.serializer.deserialize(in, PEER, VERSION_3014);
            assertEquals(0, in.available());
            assertNull(received.payload);
        }
        assertEquals(skippedBefore + 1, Message.Serializer.skippedPayloadsPre40());
    }

    /**
     * The acknowledgement of a hint: REQUEST_RSP, no params, an empty payload, the id of the hint. Ours has
     * the layout DSE sends, and read header first, as the inbound handler does, it expires with the hint's callback.
     * Read once no callback waits for it any more, it has a null payload, and the response verb handler drops it
     * without running the callbacks that are registered: the same id from another peer, another id from the peer.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void hintAcknowledgementAtVersion11() throws Throwable
    {
        // expiring in five minutes, not with the hint timeout, so that the callback is still there to be compared
        Message<?> hint = Message.out(Verb.HINT_REQ, noPayload, approxTime.now() + TimeUnit.MINUTES.toNanos(5));
        MessagingService.instance().callbacks.addWithExpiration(message -> {}, hint, PEER);
        RequestCallbacks.CallbackInfo callback = MessagingService.instance().callbacks.get(hint.id(), PEER);
        assertNotNull(callback);
        byte[] bytes;
        try
        {
            Message<?> acknowledgement = hint.emptyResponse();
            bytes = serialize(acknowledgement);
            // DSE's REQUEST_RESPONSE is verb 4
            assertArrayEquals(expectedMessage(bytes, acknowledgement.from(), (int) hint.id(), 4, 0, out -> {}, new byte[0]), bytes);

            Message<?> received = roundTrip(acknowledgement, 4);
            assertSame(Verb.REQUEST_RSP, received.verb());
            assertEquals(hint.id(), received.id());
            assertNull(received.payload);
            long callbackDeadline = callback.expiresAtNanos;
            Message.Header header = Message.serializer.extractHeader(ByteBuffer.wrap(bytes), PEER, approxTime.now(), VERSION_3014);
            assertEquals(callbackDeadline, header.expiresAtNanos);
        }
        finally
        {
            MessagingService.instance().callbacks.remove(hint.id(), PEER);
        }

        Message<?> late;
        try (DataInputBuffer in = new DataInputBuffer(bytes))
        {
            late = Message.serializer.deserialize(in, PEER, VERSION_3014);
            assertEquals(0, in.available());
        }
        assertSame(Verb.REQUEST_RSP, late.verb());
        assertNull(late.payload);

        InetAddressAndPort otherPeer = peer("127.0.0.43");
        Message<?> sameIdOtherPeer = Message.builder(Verb.HINT_REQ, noPayload).withId(hint.id()).build();
        Message<?> otherId = Message.out(Verb.HINT_REQ, noPayload);
        AtomicInteger callbackRuns = new AtomicInteger();
        MessagingService.instance().callbacks.addWithExpiration(message -> callbackRuns.incrementAndGet(), sameIdOtherPeer, otherPeer);
        MessagingService.instance().callbacks.addWithExpiration(message -> callbackRuns.incrementAndGet(), otherId, PEER);
        try
        {
            // the handler itself, so that an exception reaches the test
            Verb.REQUEST_RSP.<Object>handler().doVerb((Message<Object>) late);
            assertEquals("no callback ran", 0, callbackRuns.get());
            assertNotNull(MessagingService.instance().callbacks.get(hint.id(), otherPeer));
            assertNotNull(MessagingService.instance().callbacks.get(otherId.id(), PEER));
        }
        finally
        {
            MessagingService.instance().callbacks.remove(hint.id(), otherPeer);
            MessagingService.instance().callbacks.remove(otherId.id(), PEER);
        }
    }

    @Test
    public void headerParamsRoundTripAtVersion11() throws Throwable
    {
        InetAddressAndPort forwardTo = peer("127.0.0.43");
        InetAddressAndPort respondTo = peer("127.0.0.44");
        Message<NoPayload> message = Message.builder(Verb.ECHO_REQ, noPayload)
                                            .withId(17)
                                            .from(FBUtilities.getBroadcastAddressAndPort())
                                            .withFlag(MessageFlag.CALL_BACK_ON_FAILURE)
                                            .withFlag(MessageFlag.TRACK_REPAIRED_DATA)
                                            .withParam(ParamType.FORWARD_TO, new ForwardingInfo(Collections.singletonList(forwardTo), new long[] { 18 }))
                                            .withParam(ParamType.RESPOND_TO, respondTo)
                                            .withParam(ParamType.TRACE_SESSION, TimeUUID.Generator.nextTimeUUID())
                                            .withParam(ParamType.REQUEST_VERB_NAME, "not in the dialect")
                                            .build();
        Message<?> received = roundTrip(message, Verb.ECHO_REQ.id);
        assertTrue(received.callBackOnFailure());
        assertTrue(received.trackRepairedData());
        assertEquals(message.traceSession(), received.traceSession());
        assertEquals(respondTo, received.respondTo());
        assertNotNull(received.forwardTo());
        assertEquals(forwardTo, received.forwardTo().targets.get(0));
        assertEquals(18, received.forwardTo().messageIds[0]);
        // params unknown to the dialect are not sent, and the flags come back as flags, not params
        assertNull(received.header.params().get(ParamType.REQUEST_VERB_NAME));
        assertNull(received.header.params().get(ParamType.FAILURE_CALLBACK));
    }

    /**
     * The dialect has 32-bit deletion times (the last one early in 2038). DSE_6 requires storage_compatibility_mode HCD_1,
     * under which no cell or tombstone carries a later one; if one ever did, sending it at 11 fails before anything is written.
     */
    @Test
    public void extendedDeletionTimesAreNeverSentAtVersion11() throws Throwable
    {
        assertFalse(MessagingService.Version.supportsExtendedDeletionTime(VERSION_3014));
        StorageCompatibilityMode previous = DatabaseDescriptor.getStorageCompatibilityMode();
        try
        {
            DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.HCD_1);
            assertEquals(CellData.MAX_DELETION_TIME_2038_LEGACY_CAP, CellData.getVersionedMaxDeletiontionTime());
        }
        finally
        {
            DatabaseDescriptor.setStorageCompatibilityMode(previous);
        }

        createTable("CREATE TABLE %s (k int, c int, v text, s set<text>, PRIMARY KEY (k, c))");
        TableMetadata metadata = currentTableMetadata();
        ColumnMetadata v = metadata.getColumn(ColumnIdentifier.getInterned("v", false));
        DecoratedKey key = metadata.partitioner.decorateKey(Int32Type.instance.decompose(1));
        long past2038 = CellData.MAX_DELETION_TIME_2038_LEGACY_CAP + 1000L;
        Clustering<?> clustering = Clustering.make(Int32Type.instance.decompose(1));
        ByteBuffer one = Int32Type.instance.decompose(1);
        ByteBuffer three = Int32Type.instance.decompose(3);

        Map<String, Mutation> pastCap = new HashMap<>();
        pastCap.put("partition deletion", new Mutation(PartitionUpdate.fullPartitionDelete(metadata, key, 1000, past2038)));
        PartitionUpdate.SimpleBuilder rowDeletion = PartitionUpdate.simpleBuilder(metadata, 1).timestamp(1000).nowInSec(past2038);
        rowDeletion.row(1).delete();
        pastCap.put("row deletion", rowDeletion.buildAsMutation());
        pastCap.put("cell tombstone", new Mutation(PartitionUpdate.singleRowUpdate(metadata, key, BTreeRow.singleCellRow(clustering, BufferCell.tombstone(v, 1000, past2038)))));
        // (built with the expiration time itself: the expiration overflow policy may cap one computed from a TTL)
        pastCap.put("expiring cell", new Mutation(PartitionUpdate.singleRowUpdate(metadata, key, BTreeRow.singleCellRow(clustering, new BufferCell(v, 1000, 100, past2038, UTF8Type.instance.decompose("x"), null)))));
        pastCap.put("expiring row liveness", new Mutation(PartitionUpdate.singleRowUpdate(metadata, key, BTreeRow.noCellLiveRow(clustering, LivenessInfo.withExpirationTime(1000, 100, past2038)))));
        // a whole-collection assignment deletes the previous collection: a complex deletion
        PartitionUpdate.SimpleBuilder complexDeletion = PartitionUpdate.simpleBuilder(metadata, 1).timestamp(1000).nowInSec(past2038);
        complexDeletion.row(1).add("s", Collections.singleton("a"));
        pastCap.put("complex deletion", complexDeletion.buildAsMutation());
        pastCap.put("range tombstone bounds", PartitionUpdate.simpleBuilder(metadata, 1)
                                                             .addRangeTombstone(new RangeTombstone(Slice.make(BufferClusteringBound.inclusiveStartOf(one), BufferClusteringBound.inclusiveEndOf(three)),
                                                                                                   DeletionTime.build(1000, past2038)))
                                                             .buildAsMutation());
        for (Map.Entry<String, Mutation> e : pastCap.entrySet())
        {
            Mutation mutation = e.getValue();
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                IllegalStateException refused = assertThrows(e.getKey(), IllegalStateException.class, () -> Mutation.serializer.serialize(mutation, out, VERSION_3014));
                assertTrue(e.getKey() + ": " + refused.getMessage(),
                           refused.getMessage().matches("DSE 6 compatibility mode: local deletion time \\d+ is past 2038 and cannot be sent at messaging version " +
                                                        VERSION_3014 + ", whose deletion times are 32-bit"));
            }
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                Mutation.serializer.serialize(mutation, out, MessagingService.current_version);
            }
        }
        // a boundary marker (two adjacent range tombstones) carries two deletion times: either one past the cap is refused
        SerializationHelper helper = new SerializationHelper(new SerializationHeader(false, metadata, metadata.regularAndStaticColumns(), EncodingStats.NO_STATS));
        DeletionTime withinCap = DeletionTime.build(1000, CellData.MAX_DELETION_TIME_2038_LEGACY_CAP);
        DeletionTime beyondCap = DeletionTime.build(2000, past2038);
        ByteBuffer[] boundaryValues = { three };
        RangeTombstoneBoundaryMarker closesPastCap = RangeTombstoneBoundaryMarker.exclusiveCloseInclusiveOpen(false, boundaryValues, ByteBufferAccessor.instance, beyondCap, withinCap);
        RangeTombstoneBoundaryMarker opensPastCap = RangeTombstoneBoundaryMarker.exclusiveCloseInclusiveOpen(false, boundaryValues, ByteBufferAccessor.instance, withinCap, beyondCap);
        for (RangeTombstoneBoundaryMarker marker : new RangeTombstoneBoundaryMarker[]{ closesPastCap, opensPastCap })
        {
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                IllegalStateException refused = assertThrows(marker.toString(), IllegalStateException.class,
                                                              () -> UnfilteredSerializer.serializer.serialize(marker, helper, out, VERSION_3014));
                assertEquals("DSE 6 compatibility mode: local deletion time " + past2038 + " is past 2038 and cannot be sent at messaging version " +
                             VERSION_3014 + ", whose deletion times are 32-bit", refused.getMessage());
                assertEquals("nothing is written before the refusal", 0, out.getLength());
            }
        }
        // within the cap on both sides, the boundary is sent and read back with both deletion times
        RangeTombstoneBoundaryMarker boundary = RangeTombstoneBoundaryMarker.exclusiveCloseInclusiveOpen(false, boundaryValues, ByteBufferAccessor.instance,
                                                                                                         withinCap, DeletionTime.build(900, 1000));
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            UnfilteredSerializer.serializer.serialize(boundary, helper, out, VERSION_3014);
            assertEquals(UnfilteredSerializer.serializer.serializedSize(boundary, helper, VERSION_3014), out.getLength());
            try (DataInputBuffer in = new DataInputBuffer(out.toByteArray()))
            {
                RangeTombstoneBoundaryMarker received = (RangeTombstoneBoundaryMarker) UnfilteredSerializer.serializer.deserialize(in, helper.header,
                                                                                                                                  new DeserializationHelper(metadata, VERSION_3014, DeserializationHelper.Flag.FROM_REMOTE),
                                                                                                                                  BTreeRow.sortedBuilder());
                assertEquals(withinCap, received.endDeletionTime());
                assertEquals(DeletionTime.build(900, 1000), received.startDeletionTime());
            }
        }

        // the exact value is named
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            IllegalStateException refused = assertThrows(IllegalStateException.class, () -> Mutation.serializer.serialize(pastCap.get("cell tombstone"), out, VERSION_3014));
            assertEquals("DSE 6 compatibility mode: local deletion time " + past2038 + " is past 2038 and cannot be sent at messaging version " +
                         VERSION_3014 + ", whose deletion times are 32-bit", refused.getMessage());
        }
        // the last 32-bit deletion time is still sent
        Mutation within = new Mutation(PartitionUpdate.singleRowUpdate(metadata, key, BTreeRow.singleCellRow(clustering, BufferCell.tombstone(v, 1000, CellData.MAX_DELETION_TIME_2038_LEGACY_CAP))));
        roundTrip(Message.out(Verb.MUTATION_REQ, within), Verb.MUTATION_REQ.id);

        // with the mode off the guard is not there, as before the mode existed
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            Mutation.serializer.serialize(pastCap.get("cell tombstone"), out, VERSION_3014);
        }
    }

    /**
     * With the mode off versions 10 and 11 keep what they did before the mode existed: the payload serializers that
     * asserted 4.0 assert it, the size caches have no entry for them, and a 4.0 peer's value under a pre-4.0-only param
     * id is skipped.
     */
    @Test
    public void pre40LayoutsAreNotUsedWithTheModeOff() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))");
        TableMetadata metadata = currentTableMetadata();
        Mutation mutation = mutation(metadata, 3);
        CounterMutation counterMutation = new CounterMutation(mutation, ConsistencyLevel.ONE);
        ReadResponse response = ReadResponse.createDigestResponse(EmptyIterators.unfilteredPartition(metadata),
                                                                  PartitionRangeReadCommand.allDataRead(metadata, FBUtilities.nowInSeconds()));
        ForwardingInfo forwarding = new ForwardingInfo(Collections.singletonList(PEER), new long[]{ 1 });

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        for (int version : new int[]{ MessagingService.VERSION_30, VERSION_3014 })
        {
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                assertThrows(AssertionError.class, () -> ReadResponse.serializer.serialize(response, out, version));
                assertThrows(AssertionError.class, () -> ReadResponse.serializer.serializedSize(response, version));
                assertThrows(AssertionError.class, () -> ForwardingInfo.serializer.serialize(forwarding, out, version));
                assertThrows(AssertionError.class, () -> ForwardingInfo.serializer.serializedSize(forwarding, version));
                assertThrows(AssertionError.class, () -> RequestFailureReason.serializer.serialize(RequestFailureReason.UNKNOWN, out, version));
                assertThrows(AssertionError.class, () -> RequestFailureReason.serializer.serializedSize(RequestFailureReason.UNKNOWN, version));
                assertThrows(AssertionError.class, () -> InetAddressAndPort.FwdFrmSerializer.fwdFrmSerializer.serialize(PEER, out, version));
                assertThrows(AssertionError.class, () -> InetAddressAndPort.FwdFrmSerializer.fwdFrmSerializer.serializedSize(PEER, version));
                assertEquals("pre-4.0 serialization version " + version,
                             assertThrows(AssertionError.class, () -> inetAddressAndPortSerializer.serialize(PEER, out, version)).getMessage());
                assertEquals("pre-4.0 serialization version " + version,
                             assertThrows(AssertionError.class, () -> inetAddressAndPortSerializer.serializedSize(PEER, version)).getMessage());
            }
            // and on the way in, where each of them asserted 4.0 before reading anything
            byte[] anyBytes = new byte[16];
            assertThrows(AssertionError.class, () -> ReadResponse.serializer.deserialize(new DataInputBuffer(anyBytes), version));
            assertThrows(AssertionError.class, () -> ForwardingInfo.serializer.deserialize(new DataInputBuffer(anyBytes), version));
            assertThrows(AssertionError.class, () -> RequestFailureReason.serializer.deserialize(new DataInputBuffer(anyBytes), version));
            assertThrows(AssertionError.class, () -> InetAddressAndPort.FwdFrmSerializer.fwdFrmSerializer.pre40DeserializeWithLength(new DataInputBuffer(anyBytes), version, 4));
            // the address-only layout (a 4- or 16-byte address, no port) is refused whatever the version, as before
            for (byte[] addressOnly : new byte[][]{ { 4, 127, 0, 0, 42 }, ByteBuffer.allocate(17).put(0, (byte) 16).array() })
            {
                assertEquals("pre-4.0 serialization size " + addressOnly[0],
                             assertThrows(AssertionError.class, () -> inetAddressAndPortSerializer.deserialize(new DataInputBuffer(addressOnly), version)).getMessage());
                assertEquals("Unexpected pre-4.0 InetAddressAndPort size " + addressOnly[0],
                             assertThrows(AssertionError.class, () -> inetAddressAndPortSerializer.extract(ByteBuffer.wrap(addressOnly), 0)).getMessage());
            }
            assertEquals("Unknown serialization version: " + version, assertThrows(IllegalStateException.class, () -> mutation.serializedSize(version)).getMessage());
            assertEquals("Unknown serialization version: " + version, assertThrows(IllegalStateException.class, () -> counterMutation.serializedSize(version)).getMessage());
        }

        // the ids of the pre-4.0-only params (2, 3, 4, 7): from a 4.0 peer their value is skipped, as before they existed,
        // and the params after them are read as usual
        Map<ParamType, Object> pre40Only = new HashMap<>();
        pre40Only.put(ParamType.FAILURE_RESPONSE, LegacyFlag.instance);
        pre40Only.put(ParamType.FAILURE_REASON, RequestFailureReason.READ_TOO_MANY_TOMBSTONES);
        pre40Only.put(ParamType.FAILURE_CALLBACK, LegacyFlag.instance);
        pre40Only.put(ParamType.TRACK_REPAIRED_DATA, LegacyFlag.instance);
        TimeUUID session = TimeUUID.Generator.nextTimeUUID();
        int withoutSize = Message.builder(Verb.ECHO_REQ, noPayload).withParam(ParamType.TRACE_SESSION, session).build().serializedSize(MessagingService.VERSION_40);
        for (Map.Entry<ParamType, Object> param : pre40Only.entrySet())
        {
            Message<NoPayload> message = Message.builder(Verb.ECHO_REQ, noPayload)
                                                .withParam(param.getKey(), param.getValue())
                                                .withParam(ParamType.TRACE_SESSION, session)
                                                .build();
            byte[] bytes;
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                Message.serializer.serialize(message, out, MessagingService.VERSION_40);
                bytes = out.toByteArray();
            }
            assertTrue(param.getKey() + " is on the wire", bytes.length > withoutSize);
            try (DataInputBuffer in = new DataInputBuffer(bytes))
            {
                Message<?> received = Message.serializer.deserialize(in, PEER, MessagingService.VERSION_40);
                assertEquals(0, in.available());
                assertFalse(received.header.params().containsKey(param.getKey()));
                assertEquals(session, received.traceSession());
            }
        }
    }

    private interface Writer
    {
        void write(DataOutputBuffer out) throws Exception;
    }

    /**
     * The pre-4.0 bytes of a message sent from an IPv4 address, in the Cassandra 3.0 layout; the creation time, derived
     * from the clock, is taken from the actual bytes.
     */
    private static byte[] expectedMessage(byte[] actual, InetAddressAndPort from, int id, int verbId, int paramCount, Writer params, byte[] payload) throws Exception
    {
        byte[] address = from.getAddress().getAddress();
        assertEquals(4, address.length);
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.writeInt(Message.PROTOCOL_MAGIC);
            out.writeInt(id);
            out.write(actual, 8, 4);
            out.writeByte(4);
            out.write(address);
            out.writeInt(verbId);
            out.writeInt(paramCount);
            params.write(out);
            out.writeInt(payload.length);
            out.write(payload);
            return out.toByteArray();
        }
    }

    private static String hex(byte[] bytes, int offset, int length)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++)
        {
            if (i > 0 && i % 4 == 0)
                sb.append(' ');
            sb.append(String.format("%02x", bytes[offset + i]));
        }
        return sb.toString();
    }

    /** Serializes at version 11, checks the size, the inferred size, the verb id on the wire, deserializes and re-serializes. */
    private static Message<?> roundTrip(Message<?> message, int expectedVerbId) throws Throwable
    {
        byte[] bytes = serialize(message);
        assertEquals(message.serializedSize(VERSION_3014), bytes.length);
        assertEquals(bytes.length, Message.serializer.inferMessageSize(ByteBuffer.wrap(bytes), 0, bytes.length, VERSION_3014));
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        assertEquals(Message.PROTOCOL_MAGIC, buffer.getInt(0));
        int addressLength = buffer.get(ADDRESS_OFFSET);
        int afterAddress = ADDRESS_OFFSET + 1 + addressLength;
        assertEquals("verb id on the wire", expectedVerbId, buffer.getInt(afterAddress));

        Message<?> received;
        try (DataInputBuffer in = new DataInputBuffer(bytes))
        {
            received = Message.serializer.deserialize(in, PEER, VERSION_3014);
            assertEquals("unread bytes after " + message.verb(), 0, in.available());
        }
        // the header can also be read alone first (the inbound handler does that)
        Message.Header header = Message.serializer.extractHeader(buffer.duplicate(), PEER, approxTime.now(), VERSION_3014);
        // a failure response is recognised from its params only once the whole message is read
        assertEquals(received.verb(), header.verb == Verb.INTERNAL_RSP && header.params().containsKey(ParamType.FAILURE_RESPONSE) ? Verb.FAILURE_RSP : header.verb);
        assertEquals(received.id(), header.id);

        // what was read serializes again to the same bytes, except the creation time (re-derived on receipt) and the
        // sender address (the receiver records the connection's peer, as a DSE node does)
        byte[] again = serialize(received);
        assertEquals(message.verb() + " length", bytes.length, again.length);
        assertArrayEquals(message.verb() + " prefix", Arrays.copyOfRange(bytes, 0, 8), Arrays.copyOfRange(again, 0, 8));
        assertArrayEquals(message.verb() + " body", Arrays.copyOfRange(bytes, afterAddress, bytes.length), Arrays.copyOfRange(again, afterAddress, again.length));
        return received;
    }

    private static byte[] serialize(Message<?> message) throws Throwable
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            Message.serializer.serialize(message, out, VERSION_3014);
            return out.toByteArray();
        }
    }

    private static byte[] serialize(ReadResponse response, int version) throws Exception
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            ReadResponse.serializer.serialize(response, out, version);
            byte[] bytes = out.toByteArray();
            assertEquals(ReadResponse.serializer.serializedSize(response, version), bytes.length);
            return bytes;
        }
    }

    private static Mutation mutation(TableMetadata metadata, int k)
    {
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, k).timestamp(1000);
        builder.row(1).add("v", "value");
        return builder.buildAsMutation();
    }

    private static EndpointState endpointState()
    {
        VersionedValue.VersionedValueFactory values = new VersionedValue.VersionedValueFactory(DatabaseDescriptor.getPartitioner());
        EndpointState state = new EndpointState(new HeartBeatState(3, 4));
        state.addApplicationState(ApplicationState.NET_VERSION, values.networkVersion(MessagingService.current_version));
        state.addApplicationState(ApplicationState.HOST_ID, values.hostId(UUID.randomUUID()));
        return state;
    }
}
