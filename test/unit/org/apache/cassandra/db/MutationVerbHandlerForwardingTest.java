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

package org.apache.cassandra.db;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.DseForwardFromCaptureTest;
import org.apache.cassandra.net.DseForwardToCaptureTest;
import org.apache.cassandra.net.ForwardingInfo;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageFlag;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Hex;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The forwarder's side of a write forwarded to a remote data center ({@code FWD_TO}): the ids of the mutations it sends to
 * the targets of its data center. A coordinator of the 4.0 line or later gives every target the id of the message it
 * sends to the forwarder, and keys its callbacks by id and peer. A DSE 6 coordinator, at the pre-4.0 dialect, gives every
 * target an id of its own, under which the target answers, as DSE 6.9.26 does in the captures of
 * {@link DseForwardToCaptureTest}; Cassandra 3.0, whose dialect it speaks, keys its callbacks by id only. In DSE 6
 * compatibility mode each target must therefore get, and answer with, the id listed for it.
 */
public class MutationVerbHandlerForwardingTest
{
    private static final String KEYSPACE = "mutation_forwarding_test";
    private static final String TABLE = "t";
    private static final long FORWARDER_ID = 100;
    /**
     * Seen on node 5 in the capture of {@link DseForwardFromCaptureTest}, the message after
     * {@code 192.168.57.15 -> 192.168.57.14:7000 #4}: the answer (id 168553) of the upgraded node 192.168.57.15 to a
     * mutation the DSE node 192.168.57.14 coordinated (the message after {@code 192.168.57.14 -> 192.168.57.15:7000 #3}),
     * at version 11: the verb of a successful write's answer, {@code REQUEST_RESPONSE} (4), no params, no payload. It
     * answers a write the DSE node 192.168.57.14 coordinated in its own data center (no FWD_FRM), not a forwarded one:
     * the capture keeps only the connections that carry FWD_FRM, so no answer of an upgraded node to a coordinator of
     * {@code dc1} was captured.
     */
    private static final String UPGRADED_NODE_ANSWER = "ca552dfa000292693bd7295504c0a8390f000000040000000000000000";

    private static TableMetadata metadata;
    private static InternodeCompatibilityMode modeBefore;
    private static InetAddressAndPort coordinator;
    private static InetAddressAndPort target1;
    private static InetAddressAndPort target2;

    /** The mutations this node forwarded, by target, and how many it sent. */
    private final Map<InetAddressAndPort, Message<?>> forwarded = new ConcurrentHashMap<>();
    private final AtomicInteger sent = new AtomicInteger();
    /**
     * The answers this node sent, failures included, with their destination. The local write of an earlier test may
     * answer while a later test runs, so a test looks only for the ids of its own messages.
     */
    private final List<Map.Entry<InetAddressAndPort, Message<?>>> answers = new CopyOnWriteArrayList<>();

    @BeforeClass
    public static void defineSchema() throws UnknownHostException
    {
        SchemaLoader.prepareServer();
        metadata = TableMetadata.builder(KEYSPACE, TABLE)
                                .addPartitionKeyColumn("k", Int32Type.instance)
                                .addRegularColumn("v", Int32Type.instance)
                                .build();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), metadata);
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        coordinator = InetAddressAndPort.getByName("127.0.0.10");
        target1 = InetAddressAndPort.getByName("127.0.0.11");
        target2 = InetAddressAndPort.getByName("127.0.0.12");
    }

    @AfterClass
    public static void restoreModeAndSinks()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
        MessagingService.instance().outboundSink.clear();
    }

    /**
     * Records the forwarded mutations and the answers, and drops every outbound message, the forwarder's own answer to
     * the coordinator included, which the asynchronous local write may send after the test: the sink stays until the
     * next test or the end of the class.
     */
    @Before
    public void captureForwardedMutations()
    {
        MessagingService.instance().outboundSink.clear();
        MessagingService.instance().outboundSink.add((message, to) -> {
            if (message.verb() == Verb.MUTATION_REQ)
            {
                forwarded.put(to, message);
                sent.incrementAndGet();
            }
            else if (message.verb() == Verb.MUTATION_RSP || message.verb() == Verb.FAILURE_RSP)
            {
                answers.add(new AbstractMap.SimpleImmutableEntry<>(to, message));
            }
            return false;
        });
    }

    /** DSE 6 coordinator: one id per target, each target gets its own. */
    @Test
    public void idPerTargetFromDseCoordinator()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        forward(new long[]{ 101, 102 });
        assertForwarded(target1, 101);
        assertForwarded(target2, 102);
    }

    /** DSE's ids are a wrapping int, read back sign-extended: negative ids are kept too. */
    @Test
    public void negativeIdsPerTarget()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        forward(new long[]{ -2, Integer.MIN_VALUE });
        assertForwarded(target1, -2);
        assertForwarded(target2, Integer.MIN_VALUE);
    }

    /**
     * A negative id of DSE's survives the hop from the forwarder to an upgraded target, at this software's messaging
     * version (an unsigned vint of the sign-extended id), and the target's answer to DSE, at the pre-4.0 dialect (a 4-byte
     * int).
     */
    @Test
    public void negativeIdSurvivesTheHopToAnUpgradedTargetAndTheAnswerToDse() throws IOException
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        forward(new long[]{ Integer.MIN_VALUE, -2 });
        Message<?> received = roundTrip(forwarded.get(target1), MessagingService.current_version);
        assertThat(received.id()).as("id of the mutation the upgraded target receives").isEqualTo(Integer.MIN_VALUE);
        assertThat(received.respondTo()).as("the target answers the coordinator").isEqualTo(coordinator);
        Message<?> answer = roundTrip(received.emptyResponse(), MessagingService.VERSION_3014);
        assertThat(answer.id()).as("id of the answer DSE receives").isEqualTo(Integer.MIN_VALUE);
    }

    /** One target listed with the forwarder's id, one with another: each gets the id listed for it. */
    @Test
    public void idPerTargetWhenOnlySomeIdsDiffer()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        forward(new long[]{ FORWARDER_ID, 102 });
        assertForwarded(target1, FORWARDER_ID);
        assertForwarded(target2, 102);
        assertThat(forwarded.get(target1)).as("a message for each target").isNotSameAs(forwarded.get(target2));
    }

    /** A coordinator of the 4.0 line or later, in DSE 6 mode as well: every target gets the forwarder's id. */
    @Test
    public void forwarderIdFromCoordinatorOfThisSoftwareInDse6Mode()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        forward(new long[]{ FORWARDER_ID, FORWARDER_ID });
        assertForwarded(target1, FORWARDER_ID);
        assertForwarded(target2, FORWARDER_ID);
        assertThat(forwarded.get(target1)).as("one message for every target").isSameAs(forwarded.get(target2));
    }

    /** Mode NONE keeps the behaviour of this software before the DSE 6 mode: the forwarder's id, whatever FWD_TO lists. */
    @Test
    public void forwarderIdInModeNone()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        forward(new long[]{ 101, 102 });
        assertForwarded(target1, FORWARDER_ID);
        assertForwarded(target2, FORWARDER_ID);
        assertThat(forwarded.get(target1)).as("one message for every target").isSameAs(forwarded.get(target2));
    }

    /**
     * The mutation of a DSE coordinator captured on the test cluster ({@link DseForwardToCaptureTest#DSE_COORDINATOR_MUTATION}):
     * its header and params as DSE wrote them, FWD_TO padded to 128 bytes, then a mutation of this test's table as the
     * payload. Each target gets the id DSE listed for it.
     */
    @Test
    public void idPerTargetFromCapturedDseMessage() throws IOException
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        InetAddressAndPort dse = InetAddressAndPort.getByName("192.168.57.11");
        InetAddressAndPort first = InetAddressAndPort.getByName("192.168.57.14");
        InetAddressAndPort second = InetAddressAndPort.getByName("192.168.57.16");
        Message<Mutation> message = capturedWithLocalPayload(DseForwardToCaptureTest.DSE_COORDINATOR_MUTATION,
                                                             DseForwardToCaptureTest.DSE_COORDINATOR_PARAMS_END, dse);
        assertThat(message.id()).as("id of the message to the forwarder").isEqualTo(228716);
        // the CAL_BAC after the padded FWD_TO: the flag the forwarder and its targets need to answer DSE with a failure
        int callBackOnFailure = callBackOnFailureFlag();
        assertThat(message.header.flags() & callBackOnFailure).as("CAL_BAC of the message to the forwarder").isEqualTo(callBackOnFailure);

        MutationVerbHandler.instance.doVerb(message);
        assertThat(forwarded.keySet()).as("targets of the forwarded mutations").containsExactlyInAnyOrder(first, second);
        assertForwarded(first, 228717, dse);
        assertForwarded(second, 228718, dse);
        assertThat(forwarded.get(first).header.flags() & callBackOnFailure).as("CAL_BAC of the mutation forwarded to %s", first).isEqualTo(callBackOnFailure);
        assertThat(forwarded.get(second).header.flags() & callBackOnFailure).as("CAL_BAC of the mutation forwarded to %s", second).isEqualTo(callBackOnFailure);
    }

    /**
     * The copy of that captured message this node forwards to 192.168.57.14, written at version 11 and compared with the
     * copy the upgraded forwarder 192.168.57.15 sent ({@link DseForwardFromCaptureTest#UPGRADED_FORWARDER_MUTATION})
     * and with the copy the DSE forwarder 192.168.57.14 sent for another write of the same coordinator
     * ({@link DseForwardFromCaptureTest#DSE_FORWARDER_MUTATION}). The magic, the id, the address (the coordinator's, where
     * DSE writes its own) and the verb are those of the captured copy. The params are FWD_FRM, as in the captured copy, then
     * the CAL_BAC the captured copy lacks (that forwarder read the coordinator's padded FWD_TO before the params were read within
     * their declared length, and lost the CAL_BAC after it): the params of DSE's copy, byte for byte. The payload is a
     * write of this test's table. The creation time is now, set by this test; the copy keeps the creation time of the
     * message it forwards, as the captured copy did, which is checked on the messages rather than on the bytes, as each write
     * of the time goes through the millisecond translation of the clock.
     */
    @Test
    public void forwardedCopyOfCapturedDseMessageAtVersion11() throws IOException
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        InetAddressAndPort dse = InetAddressAndPort.getByName("192.168.57.11");
        InetAddressAndPort target = InetAddressAndPort.getByName("192.168.57.14");
        Message<Mutation> message = capturedWithLocalPayload(DseForwardToCaptureTest.DSE_COORDINATOR_MUTATION,
                                                             DseForwardToCaptureTest.DSE_COORDINATOR_PARAMS_END, dse);
        MutationVerbHandler.instance.doVerb(message);
        Message<?> copy = forwarded.get(target);
        assertThat(copy).as("mutation forwarded to %s", target).isNotNull();
        assertThat(copy.createdAtNanos()).as("creation time of the copy").isEqualTo(message.createdAtNanos());
        byte[] written;
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            Message.serializer.serialize(copy, out, MessagingService.VERSION_3014);
            written = out.toByteArray();
        }

        byte[] capturedCopy = Hex.hexToBytes(DseForwardFromCaptureTest.UPGRADED_FORWARDER_MUTATION);
        byte[] dseCopy = Hex.hexToBytes(DseForwardFromCaptureTest.DSE_FORWARDER_MUTATION);
        int paramsEnd = DseForwardFromCaptureTest.DSE_FORWARDER_PARAMS_END;
        assertThat(Arrays.copyOfRange(written, 0, DseForwardToCaptureTest.CREATED_AT_OFFSET))
        .as("magic and id")
        .isEqualTo(Arrays.copyOfRange(capturedCopy, 0, DseForwardToCaptureTest.CREATED_AT_OFFSET));
        assertThat(Arrays.copyOfRange(written, DseForwardFromCaptureTest.ADDRESS_OFFSET, DseForwardFromCaptureTest.PARAM_COUNT_OFFSET))
        .as("address and verb")
        .isEqualTo(Arrays.copyOfRange(capturedCopy, DseForwardFromCaptureTest.ADDRESS_OFFSET, DseForwardFromCaptureTest.PARAM_COUNT_OFFSET));
        assertThat(Arrays.copyOfRange(written, DseForwardFromCaptureTest.ADDRESS_OFFSET, DseForwardFromCaptureTest.VERB_OFFSET))
        .as("address: the coordinator's")
        .containsExactly(4, 192, 168, 57, 11);
        assertThat(Arrays.copyOfRange(dseCopy, DseForwardFromCaptureTest.ADDRESS_OFFSET, DseForwardFromCaptureTest.VERB_OFFSET))
        .as("address in DSE's copy: the forwarder's")
        .containsExactly(4, 192, 168, 57, 14);
        assertThat(Arrays.copyOfRange(written, DseForwardFromCaptureTest.PARAM_COUNT_OFFSET + 4, DseForwardFromCaptureTest.FWD_FRM_END))
        .as("FWD_FRM")
        .isEqualTo(Arrays.copyOfRange(capturedCopy, DseForwardFromCaptureTest.PARAM_COUNT_OFFSET + 4, DseForwardFromCaptureTest.FWD_FRM_END));
        assertThat(ByteBuffer.wrap(capturedCopy).getInt(DseForwardFromCaptureTest.PARAM_COUNT_OFFSET)).as("param count of the captured copy").isEqualTo(1);
        assertThat(Arrays.copyOfRange(written, DseForwardFromCaptureTest.PARAM_COUNT_OFFSET, paramsEnd))
        .as("params: those of DSE's copy")
        .isEqualTo(Arrays.copyOfRange(dseCopy, DseForwardFromCaptureTest.PARAM_COUNT_OFFSET, paramsEnd));
        assertThat(ByteBuffer.wrap(written).getInt(paramsEnd)).as("payload size").isEqualTo(written.length - paramsEnd - 4);
    }

    /**
     * The mutation the DSE forwarder 192.168.57.14 sent an upgraded target on the test cluster
     * ({@link DseForwardFromCaptureTest#DSE_FORWARDER_MUTATION}): its header and params as DSE wrote them, FWD_FRM with the
     * coordinator 192.168.57.11 then CAL_BAC, and a mutation of this test's table, on a partition of its own, as the
     * payload. The target applies it and answers the coordinator under DSE's id, not the forwarder; it forwards nothing.
     * Its answer, written at version 11, has the layout of the answer the upgraded node 192.168.57.15 sent DSE
     * ({@link #UPGRADED_NODE_ANSWER}).
     */
    @Test
    public void answersTheCoordinatorOfCapturedDseForwardedMessage() throws IOException
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        InetAddressAndPort forwarder = InetAddressAndPort.getByName("192.168.57.14");
        InetAddressAndPort dse = InetAddressAndPort.getByName("192.168.57.11");
        Mutation mutation = new RowUpdateBuilder(metadata, 0, 228708).add("v", 8362).build();
        Message<Mutation> message = capturedWithLocalPayload(DseForwardFromCaptureTest.DSE_FORWARDER_MUTATION,
                                                             DseForwardFromCaptureTest.DSE_FORWARDER_PARAMS_END, forwarder, mutation);
        assertThat(message.id()).as("id of the message").isEqualTo(228708);
        assertThat(message.from()).as("sender of the message").isEqualTo(forwarder);
        assertThat(message.respondTo()).as("FWD_FRM of the message").isEqualTo(dse);
        int callBackOnFailure = callBackOnFailureFlag();
        assertThat(message.header.flags() & callBackOnFailure).as("CAL_BAC of the message").isEqualTo(callBackOnFailure);

        MutationVerbHandler.instance.doVerb(message);
        Awaitility.await("the answer to the write").atMost(1, TimeUnit.MINUTES).until(() -> !answersTo(228708).isEmpty());
        List<Map.Entry<InetAddressAndPort, Message<?>>> answered = answersTo(228708);
        assertThat(answered).as("answers to the write").hasSize(1);
        assertThat(answered.get(0).getValue().verb()).as("verb of the answer").isEqualTo(Verb.MUTATION_RSP);
        assertThat(answered.get(0).getKey()).as("destination of the answer").isEqualTo(dse);
        assertThat(forwarded).as("mutations forwarded").isEmpty();
        Row row = Util.getOnlyRow(Util.cmd(Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE), 228708).build());
        assertThat(Int32Type.instance.compose(row.getCell(metadata.getColumn(ByteBufferUtil.bytes("v"))).buffer()))
        .as("value written by the mutation")
        .isEqualTo(8362);

        byte[] written;
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            Message.serializer.serialize(answered.get(0).getValue(), out, MessagingService.VERSION_3014);
            written = out.toByteArray();
        }
        byte[] labAnswer = Hex.hexToBytes(UPGRADED_NODE_ANSWER);
        assertThat(written).as("the answer").hasSameSizeAs(labAnswer);
        assertThat(Arrays.copyOfRange(written, 0, 4)).as("magic of the answer").isEqualTo(Arrays.copyOfRange(labAnswer, 0, 4));
        assertThat(ByteBuffer.wrap(written).getInt(4)).as("id of the answer").isEqualTo(228708);
        byte[] self = FBUtilities.getBroadcastAddressAndPort().addressBytes;
        assertThat(written[DseForwardFromCaptureTest.ADDRESS_OFFSET]).as("address length in the answer").isEqualTo((byte) self.length);
        assertThat(Arrays.copyOfRange(written, DseForwardFromCaptureTest.ADDRESS_OFFSET + 1, DseForwardFromCaptureTest.VERB_OFFSET))
        .as("address in the answer: this node's, as the captured answer carries its sender's")
        .isEqualTo(self);
        assertThat(Arrays.copyOfRange(written, DseForwardFromCaptureTest.VERB_OFFSET, written.length))
        .as("verb, params and payload of the answer")
        .isEqualTo(Arrays.copyOfRange(labAnswer, DseForwardFromCaptureTest.VERB_OFFSET, labAnswer.length));
    }

    private List<Map.Entry<InetAddressAndPort, Message<?>>> answersTo(long id)
    {
        return answers.stream().filter(answer -> answer.getValue().id() == id).collect(Collectors.toList());
    }

    /**
     * A captured pre-4.0 mutation from {@code sender}: the header and params of the capture, up to {@code paramsEnd},
     * with a creation time of now (the captured one would make the write expire whenever it falls in the timeout window)
     * and a write of this test's table as the payload.
     */
    private static Message<Mutation> capturedWithLocalPayload(String hex, int paramsEnd, InetAddressAndPort sender) throws IOException
    {
        return capturedWithLocalPayload(hex, paramsEnd, sender, new RowUpdateBuilder(metadata, 0, 1).add("v", 1).build());
    }

    /** As {@link #capturedWithLocalPayload(String, int, InetAddressAndPort)}, with the given mutation as the payload. */
    private static Message<Mutation> capturedWithLocalPayload(String hex, int paramsEnd, InetAddressAndPort sender, Mutation mutation) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            ByteBuffer captured = ByteBuffer.wrap(Hex.hexToBytes(hex));
            captured.putInt(DseForwardToCaptureTest.CREATED_AT_OFFSET, (int) FBUtilities.now().toEpochMilli());
            out.write(captured.array(), 0, paramsEnd);
            out.writeInt(Ints.checkedCast(Mutation.serializer.serializedSize(mutation, MessagingService.VERSION_3014)));
            Mutation.serializer.serialize(mutation, out, MessagingService.VERSION_3014);
            try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
            {
                Message<Mutation> message = Message.serializer.deserialize(in, sender, MessagingService.VERSION_3014);
                assertThat(in.available()).as("bytes left after the message").isZero();
                return message;
            }
        }
    }

    /** The bit of {@link MessageFlag#CALL_BACK_ON_FAILURE} in the flags of a message. */
    private static int callBackOnFailureFlag()
    {
        Mutation mutation = new RowUpdateBuilder(metadata, 0, 1).add("v", 1).build();
        return Message.builder(Verb.MUTATION_REQ, mutation).withFlag(MessageFlag.CALL_BACK_ON_FAILURE).build().header.flags();
    }

    private void forward(long[] targetIds)
    {
        Mutation mutation = new RowUpdateBuilder(metadata, 0, 1).add("v", 1).build();
        List<InetAddressAndPort> targets = ImmutableList.of(target1, target2);
        Message<Mutation> message = Message.builder(Verb.MUTATION_REQ, mutation)
                                           .from(coordinator)
                                           .withId(FORWARDER_ID)
                                           .withParam(ParamType.FORWARD_TO, new ForwardingInfo(targets, targetIds))
                                           .build();
        MutationVerbHandler.instance.doVerb(message);
        // the forwarding is synchronous: one mutation to each target, nothing else
        assertThat(forwarded.keySet()).as("targets of the forwarded mutations").containsExactlyInAnyOrder(target1, target2);
        assertThat(sent.get()).as("forwarded mutations").isEqualTo(2);
    }

    private static Message<?> roundTrip(Message<?> message, int version) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            Message.serializer.serialize(message, out, version);
            try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
            {
                return Message.serializer.deserialize(in, message.from(), version);
            }
        }
    }

    private void assertForwarded(InetAddressAndPort target, long id)
    {
        assertForwarded(target, id, coordinator);
    }

    private void assertForwarded(InetAddressAndPort target, long id, InetAddressAndPort from)
    {
        Message<?> message = forwarded.get(target);
        assertThat(message).as("mutation forwarded to %s", target).isNotNull();
        assertThat(message.id()).as("id of the mutation forwarded to %s", target).isEqualTo(id);
        assertThat(message.respondTo()).as("the target answers the coordinator").isEqualTo(from);
        assertThat(message.forwardTo()).as("the target forwards nothing").isNull();
    }
}
