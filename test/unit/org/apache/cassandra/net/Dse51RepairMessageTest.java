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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.dsecompat.Dse51Fixtures;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.net.MessagingService.VERSION_3014;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * In DSE 5.1 compatibility mode the repair messages a DSE 5.1.51 coordinator sends ({@code REPAIR_MESSAGE}, the verb id
 * 32 of Apache Cassandra 3.x, which this software does not have) are skipped by the inbound handler, which keeps the
 * connection open and reads the next message: those that ask for a failure response ({@code CAL_BAC}: the
 * {@code ANTICOMPACTION_REQUEST}, {@code PREPARE_MESSAGE} and {@code SNAPSHOT} of the fixtures) get one, an
 * {@code INTERNAL_RESPONSE} with the request's id and the {@code FAIL} param, with which the DSE 5.1 coordinator stops
 * the repair at once; the others get none. The messages are those of the incremental repair and of the full sequential
 * one captured between DSE 5.1.51 nodes ({@link Dse51Fixtures#messages}); the inputs no capture holds are made from
 * captured messages: a {@code CLEANUP} (type 7), which DSE 5.1.51 never sent, by changing the type of a one-way
 * message; a message of type 9, one with an empty payload and one split over the frames of a large message, from a
 * {@code PREPARE_MESSAGE}. The same handler answers the read of a Thrift client a DSE 5.1 coordinator
 * forwards with a failure response; that read is the captured read of the fixtures with Apache Cassandra 3.x's Thrift
 * flag set, which no capture holds.
 */
public class Dse51RepairMessageTest
{
    private static final Dse51Fixtures DSE51 = DseFixtures.dse51();

    private InternodeCompatibilityMode modeBefore;
    private final List<Pair<Message<?>, InetAddressAndPort>> sent = new CopyOnWriteArrayList<>();
    private final List<Message<?>> received = new CopyOnWriteArrayList<>();
    private final BiPredicate<Message<?>, InetAddressAndPort> outbound = (message, to) -> {
        sent.add(Pair.create(message, to));
        return false;
    };
    private final Predicate<Message<?>> inbound = message -> {
        received.add(message);
        return false;
    };

    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void dse51Mode()
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        DseCompatRefusals.reset();
        MessagingService.instance().outboundSink.add(outbound);
        MessagingService.instance().inboundSink.add(inbound);
    }

    @After
    public void restore()
    {
        MessagingService.instance().outboundSink.remove(outbound);
        MessagingService.instance().inboundSink.remove(inbound);
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
        DseCompatRefusals.reset();
    }

    @Test
    public void theIncrementalRepairIsAnsweredWithFailures() throws Throwable
    {
        theRepairIsAnsweredWithFailures("repair-incremental");
    }

    @Test
    public void theFullSequentialRepairIsAnsweredWithFailures() throws Throwable
    {
        theRepairIsAnsweredWithFailures("repair-full-sequential");
    }

    /**
     * Every captured repair message, sent to this node in one frame with the captured gossip ACK after it: the messages
     * with {@code CAL_BAC} are answered with a failure response each, the others with nothing, all are counted, and the
     * ACK after them is read and handed on.
     */
    private void theRepairIsAnsweredWithFailures(String file) throws Throwable
    {
        List<Dse51Fixtures.Message> repair = DSE51.messages(file);
        List<byte[]> frame = new ArrayList<>();
        List<Long> expectingFailure = new ArrayList<>();
        for (Dse51Fixtures.Message message : repair)
        {
            assertThat(message.verb).isEqualTo("REPAIR_MESSAGE");
            frame.add(message.bytes);
            boolean callBack = message.name.equals("ANTICOMPACTION_REQUEST") || message.name.equals("PREPARE_MESSAGE")
                               || message.name.equals("SNAPSHOT");
            Message.Pre40RepairMessage parsed = parse(message.bytes, VERSION_3014);
            assertThat(parsed).as(message.name).isNotNull();
            assertThat(parsed.typeName()).isEqualTo(message.name);
            assertThat(parsed.callBackOnFailure).as(message.name).isEqualTo(callBack);
            assertThat(parsed.size).isEqualTo(message.bytes.length);
            // the id is the 4 bytes after the magic
            long id = ByteBuffer.wrap(message.bytes).getInt(4);
            assertThat(parsed.id).as(message.name).isEqualTo(id);
            if (callBack)
                expectingFailure.add(id);
        }
        assertThat(expectingFailure).isNotEmpty();
        Dse51Fixtures.Message ack = DSE51.messages("gossip-digest-ack").get(0);
        frame.add(ack.bytes);

        InetAddressAndPort coordinator = InetAddressAndPort.getByName(repair.get(0).from);
        process(coordinator, frame);

        List<Long> answered = new ArrayList<>();
        for (Pair<Message<?>, InetAddressAndPort> response : sent)
        {
            assertThat(response.right).isEqualTo(coordinator);
            assertFailureResponse(response.left);
            answered.add(response.left.id());
        }
        assertThat(answered).containsExactlyElementsOf(expectingFailure);
        assertThat(DseCompatRefusals.count(DseCompat.REFUSED_DSE51_REPAIR_MESSAGE))
        .isEqualTo(repair.size());
        awaitReceived(1);
        assertThat(received.get(0).verb()).isEqualTo(Verb.GOSSIP_DIGEST_ACK);
    }

    /** A {@code CLEANUP}, type 7, which DSE 5.1.51 never sent: one-way, so skipped with no answer, and counted. */
    @Test
    public void aCleanupIsSkippedWithoutAnAnswer() throws Throwable
    {
        Dse51Fixtures.Message validation = DSE51.messages("repair-full-sequential").stream()
                                                .filter(m -> m.name.equals("VALIDATION_REQUEST"))
                                                .findFirst().orElseThrow();
        byte[] cleanup = Arrays.copyOf(validation.bytes, validation.bytes.length);
        Message.Pre40RepairMessage parsed = parse(cleanup, VERSION_3014);
        assertThat(parsed.typeName()).isEqualTo("VALIDATION_REQUEST");
        assertThat(parsed.callBackOnFailure).isFalse();
        // the type is the first byte of the payload, which the message ends with
        int typeIndex = cleanup.length - payloadSize(cleanup);
        assertThat(cleanup[typeIndex]).isEqualTo((byte) 0);
        cleanup[typeIndex] = 7;
        assertThat(parse(cleanup, VERSION_3014).typeName()).isEqualTo("CLEANUP");

        byte[] ack = DSE51.messages("gossip-digest-ack").get(0).bytes;
        process(InetAddressAndPort.getByName(validation.from), List.of(cleanup, ack));
        awaitReceived(1);
        assertThat(sent).isEmpty();
        assertThat(DseCompatRefusals.count(DseCompat.REFUSED_DSE51_REPAIR_MESSAGE)).isEqualTo(1);
    }

    /**
     * A captured message with {@code CAL_BAC} whose type is changed to 9, which neither Apache Cassandra 3.x nor DSE
     * 5.1 defines: skipped all the same, answered with a failure, as it asks for one, and counted.
     */
    @Test
    public void aRepairMessageOfAnUnknownTypeIsAnsweredWithAFailure() throws Throwable
    {
        Dse51Fixtures.Message prepare = prepareMessage();
        byte[] unknown = Arrays.copyOf(prepare.bytes, prepare.bytes.length);
        int typeIndex = unknown.length - payloadSize(unknown);
        assertThat(unknown[typeIndex]).isEqualTo((byte) 5);
        unknown[typeIndex] = 9;
        Message.Pre40RepairMessage parsed = parse(unknown, VERSION_3014);
        assertThat(parsed.typeName()).isEqualTo("type 9");
        assertThat(parsed.callBackOnFailure).isTrue();

        InetAddressAndPort coordinator = InetAddressAndPort.getByName(prepare.from);
        process(coordinator, List.of(unknown, DSE51.messages("gossip-digest-ack").get(0).bytes));
        awaitReceived(1);
        assertThat(sent).hasSize(1);
        assertThat(sent.get(0).left.id()).isEqualTo(parsed.id);
        assertFailureResponse(sent.get(0).left);
        assertThat(DseCompatRefusals.count(DseCompat.REFUSED_DSE51_REPAIR_MESSAGE)).isEqualTo(1);
    }

    /**
     * A captured message with {@code CAL_BAC} cut to an empty payload, which no repair message has: its type is not
     * read, and it is skipped, answered with a failure and counted all the same.
     */
    @Test
    public void aRepairMessageWithAnEmptyPayloadIsAnsweredWithAFailure() throws Throwable
    {
        Dse51Fixtures.Message prepare = prepareMessage();
        int payloadStart = prepare.bytes.length - payloadSize(prepare.bytes);
        byte[] empty = Arrays.copyOf(prepare.bytes, payloadStart);
        ByteBuffer.wrap(empty).putInt(payloadStart - 4, 0);
        assertThat(payloadSize(empty)).isZero();
        Message.Pre40RepairMessage parsed = parse(empty, VERSION_3014);
        assertThat(parsed.type).isEqualTo(-1);
        assertThat(parsed.typeName()).isEqualTo("of unknown type");
        assertThat(parsed.size).isEqualTo(empty.length);

        InetAddressAndPort coordinator = InetAddressAndPort.getByName(prepare.from);
        process(coordinator, List.of(empty, DSE51.messages("gossip-digest-ack").get(0).bytes));
        awaitReceived(1);
        assertThat(sent).hasSize(1);
        assertThat(sent.get(0).left.id()).isEqualTo(parsed.id);
        assertFailureResponse(sent.get(0).left);
        assertThat(DseCompatRefusals.count(DseCompat.REFUSED_DSE51_REPAIR_MESSAGE)).isEqualTo(1);
    }

    /**
     * A repair message too large for one frame (the frames of a large message, which do not hold it whole) is refused
     * on its first frame, answered with a failure, as it asks for one, and counted; the bytes of its other frames are
     * skipped, and the connection reads the next message. The type is told when the first frame holds the first byte of
     * the payload, and not otherwise; the failure response, which the header asks for, is sent either way.
     */
    @Test
    public void aRepairMessageInSeveralFramesIsAnsweredWithAFailure() throws Throwable
    {
        Dse51Fixtures.Message prepare = prepareMessage();
        InetAddressAndPort coordinator = InetAddressAndPort.getByName(prepare.from);
        long id = ByteBuffer.wrap(prepare.bytes).getInt(4);
        int payloadStart = prepare.bytes.length - payloadSize(prepare.bytes);
        byte[] ack = DSE51.messages("gossip-digest-ack").get(0).bytes;
        // the first frame ends after the type, or right before it
        for (int firstFrame : new int[]{ payloadStart + 1, payloadStart })
        {
            sent.clear();
            received.clear();
            DseCompatRefusals.reset();
            byte[] first = Arrays.copyOfRange(prepare.bytes, 0, firstFrame);
            byte[] rest = Arrays.copyOfRange(prepare.bytes, firstFrame, prepare.bytes.length);
            assertThat(parse(first, VERSION_3014).typeName()).isEqualTo(firstFrame > payloadStart ? "PREPARE_MESSAGE"
                                                                                                  : "of unknown type");
            processFrames(coordinator, new FrameDecoder.IntactFrame(false, ShareableBytes.wrap(ByteBuffer.wrap(first))),
                          new FrameDecoder.IntactFrame(false, ShareableBytes.wrap(ByteBuffer.wrap(rest))),
                          new FrameDecoder.IntactFrame(true, ShareableBytes.wrap(ByteBuffer.wrap(ack))));
            awaitReceived(1);
            assertThat(received.get(0).verb()).isEqualTo(Verb.GOSSIP_DIGEST_ACK);
            assertThat(sent).hasSize(1);
            assertThat(sent.get(0).right).isEqualTo(coordinator);
            assertThat(sent.get(0).left.id()).isEqualTo(id);
            assertFailureResponse(sent.get(0).left);
            assertThat(DseCompatRefusals.count(DseCompat.REFUSED_DSE51_REPAIR_MESSAGE)).isEqualTo(1);
        }
    }

    /**
     * The captured read of a DSE 5.1.51 coordinator, with Apache Cassandra 3.x's Thrift flag set: refused when it is
     * read, answered with a failure response, as it asks for one, and counted; the connection reads the next message.
     */
    @Test
    public void aThriftReadIsAnsweredWithAFailure() throws Throwable
    {
        Dse51Fixtures.Message read = DSE51.messages("rlac-read").get(0);
        byte[] thrift = Arrays.copyOf(read.bytes, read.bytes.length);
        InetAddressAndPort coordinator = InetAddressAndPort.getByName(read.from);
        Message.Header header = Message.serializer.extractHeader(ByteBuffer.wrap(thrift), coordinator, approxTime.now(),
                                                                 VERSION_3014);
        assertThat(header.verb).isIn(Verb.READ_REQ, Verb.RANGE_REQ);
        assertThat(header.callBackOnFailure()).isTrue();
        // the payload: the kind of the command, then its flags
        int payloadStart = thrift.length - payloadSize(thrift);
        thrift[payloadStart + 1] |= 0x02;

        process(coordinator, List.of(thrift, DSE51.messages("gossip-digest-ack").get(0).bytes));
        awaitReceived(1);
        assertThat(sent).hasSize(1);
        assertThat(sent.get(0).right).isEqualTo(coordinator);
        assertThat(sent.get(0).left.id()).isEqualTo(header.id);
        assertFailureResponse(sent.get(0).left);
        assertThat(DseCompatRefusals.count(DseCompat.REFUSED_DSE51_THRIFT_READ)).isEqualTo(1);
    }

    /** Outside DSE_5_1 the parsing gives nothing: the verb id refuses the message, as before. */
    @Test
    public void outsideTheModeTheVerbIdIsRefused() throws Throwable
    {
        byte[] prepare = DSE51.messages("repair-incremental").get(0).bytes;
        // in DSE_6, where the dialect is open too, the verb id fails the header, which closes the connection, as before
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        assertThat(parse(prepare, VERSION_3014)).isNull();
        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.1");
        assertThatThrownBy(() -> Message.serializer.extractHeader(ByteBuffer.wrap(prepare), peer, 0, VERSION_3014))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown verb id 32");
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        assertThat(parse(prepare, VERSION_3014)).isNull();
        // at 4.0 or later there is no pre-4.0 message to parse
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        assertThat(parse(prepare, MessagingService.VERSION_40)).isNull();
    }

    /**
     * The failure response as it goes to a DSE 5.1 node at version 11: {@code INTERNAL_RESPONSE} (verb id 23 in Apache
     * Cassandra 3.x) with the {@code FAIL} param and the optional {@code FAIL_REASON}, no other param, no payload.
     */
    private static void assertFailureResponse(Message<?> response) throws Exception
    {
        assertThat(response.verb()).isEqualTo(Verb.FAILURE_RSP);
        byte[] bytes;
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            Message.serializer.serialize(response, out, VERSION_3014);
            bytes = out.toByteArray();
        }
        try (DataInputBuffer in = new DataInputBuffer(bytes))
        {
            assertThat(in.readInt()).isEqualTo(0xCA552DFA);
            assertThat((long) in.readInt()).isEqualTo(response.id());
            in.readInt(); // the creation time
            in.skipBytesFully(in.readUnsignedByte()); // the sender
            assertThat(in.readInt()).isEqualTo(Verb.INTERNAL_RSP.id).isEqualTo(23);
            int params = in.readInt();
            Map<String, byte[]> values = new HashMap<>();
            for (int i = 0; i < params; i++)
            {
                String name = in.readUTF();
                byte[] value = new byte[in.readInt()];
                in.readFully(value);
                values.put(name, value);
            }
            assertThat(values.keySet()).containsExactlyInAnyOrder("FAIL", "FAIL_REASON");
            // FAIL is a flag, one byte 0; FAIL_REASON the 2-byte code of the reason, 0 for UNKNOWN
            assertThat(values.get("FAIL")).containsExactly(0);
            assertThat(values.get("FAIL_REASON")).containsExactly(0, 0);
            assertThat(in.readInt()).as("payload size").isZero();
            assertThat(in.available()).isZero();
        }
    }

    /**
     * The size of the payload a pre-4.0 message ends with, read from the header: the magic, the id and the creation
     * time, the sender with its 1-byte length, the verb, the params (each a name with its 2-byte length and a value
     * with its 4-byte length), then the 4-byte payload size.
     */
    private static int payloadSize(byte[] message) throws Exception
    {
        ByteBuffer buf = ByteBuffer.wrap(message);
        assertThat(Message.serializer.inferMessageSize(buf, 0, message.length, VERSION_3014)).isEqualTo(message.length);
        int index = 12;
        index += 1 + (buf.get(index) & 0xFF);
        index += 4;
        int params = buf.getInt(index);
        index += 4;
        for (int i = 0; i < params; i++)
        {
            index += 2 + (buf.getShort(index) & 0xFFFF);
            index += 4 + buf.getInt(index);
        }
        int size = buf.getInt(index);
        assertThat(index + 4 + size).isEqualTo(message.length);
        return size;
    }

    /** Hands {@code frames} to an inbound handler of a large message connection from {@code peer} at version 11. */
    private static void processFrames(InetAddressAndPort peer, FrameDecoder.IntactFrame... frames) throws Exception
    {
        InboundMessageHandlers handlers = MessagingService.instance().getInbound(peer);
        EmbeddedChannel channel = new EmbeddedChannel();
        try
        {
            FrameDecoder decoder = new FrameDecoderLegacy(GlobalBufferPoolAllocator.instance, VERSION_3014);
            InboundMessageHandler handler = handlers.createHandler(decoder, ConnectionType.LARGE_MESSAGES, channel,
                                                                   VERSION_3014);
            for (FrameDecoder.IntactFrame frame : frames)
                assertThat(handler.process(frame)).isTrue();
            assertThat(channel.isOpen()).isTrue();
        }
        finally
        {
            channel.finishAndReleaseAll();
        }
    }

    /** Hands {@code messages} to an inbound handler of a connection from {@code peer} at version 11, in one frame. */
    private static void process(InetAddressAndPort peer, List<byte[]> messages) throws Exception
    {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        for (byte[] message : messages)
            bytes.write(message);
        InboundMessageHandlers handlers = MessagingService.instance().getInbound(peer);
        EmbeddedChannel channel = new EmbeddedChannel();
        try
        {
            FrameDecoder decoder = new FrameDecoderLegacy(GlobalBufferPoolAllocator.instance, VERSION_3014);
            InboundMessageHandler handler = handlers.createHandler(decoder, ConnectionType.SMALL_MESSAGES, channel,
                                                                   VERSION_3014);
            ShareableBytes frame = ShareableBytes.wrap(ByteBuffer.wrap(bytes.toByteArray()));
            assertThat(handler.process(new FrameDecoder.IntactFrame(true, frame))).isTrue();
            assertThat(channel.isOpen()).isTrue();
        }
        finally
        {
            channel.finishAndReleaseAll();
        }
    }

    /** The captured {@code PREPARE_MESSAGE} of the incremental repair, which asks for a failure response. */
    private static Dse51Fixtures.Message prepareMessage() throws Exception
    {
        return DSE51.messages("repair-incremental").stream()
                    .filter(m -> m.name.equals("PREPARE_MESSAGE"))
                    .findFirst().orElseThrow();
    }

    private static Message.Pre40RepairMessage parse(byte[] message, int version) throws Exception
    {
        return Message.serializer.pre40RepairMessage(ByteBuffer.wrap(message), version);
    }

    private void awaitReceived(int count)
    {
        Util.spinAssertEquals(count, received::size, 10);
    }
}
