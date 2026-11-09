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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.google.common.collect.ImmutableList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.net.MessagingService.VERSION_3014;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Golden bytes of the pre-4.0 {@code FWD_TO} param (messaging version 11, the DSE 6 dialect) between DSE 6.9.26 and
 * this software, copied verbatim from the packets captured on a test cluster of two data centers, three nodes each,
 * while the three nodes of {@code dc2} ran this software: the packets seen on node 5, numbered per connection below.
 * <p>
 * A DSE coordinator writes a {@code FWD_TO} value of 128 bytes of which only the first ones are used, the rest are
 * zeros, and lists an id of its own for every target; this software writes the value with nothing after the last
 * target and gives every target the id of the message. Every one of the 48 records of the capture has one of these
 * two layouts.
 */
public class DseForwardToCaptureTest
{
    /**
     * Seen on node 5, message {@code 192.168.57.11 -> 192.168.57.15:7000 #1}: a
     * mutation of the DSE coordinator 192.168.57.11 (message id 228716) to the forwarder 192.168.57.15 of the other data
     * center, which forwards it to 192.168.57.14 (id 228717) and 192.168.57.16 (id 228718). Public for
     * {@code MutationVerbHandlerForwardingTest}, which forwards it.
     */
    public static final String DSE_COORDINATOR_MUTATION =
        "ca552dfa00037d6c3bd728d904c0a8390b000000000000000200064657445f54" +
        "4f000000800000000204c0a8390e00037d6d04c0a8391000037d6e0000000000" +
        "0000000000000000000000000000000000000000000000000000000000000000" +
        "0000000000000000000000000000000000000000000000000000000000000000" +
        "0000000000000000000000000000000000000000000000000000000000000000" +
        "0000000000000743414c5f42414300000001000000004a01c4834b13c80011f1" +
        "a25361ddf55403e5047735950610fd3d87129d1058000002016e017601240006" +
        "63616e61727900080000000000003e39080e37313232306638313a3135393239" +
        "01";

    /**
     * Seen on node 5, message {@code 192.168.57.15 -> 192.168.57.11:7000 #1}: a
     * mutation of the coordinator 192.168.57.15, running this software, (message id 10664) to the DSE forwarder
     * 192.168.57.11 of the other data center, which forwards it to 192.168.57.12 and 192.168.57.13, both with id 10664.
     */
    private static final String UPGRADED_COORDINATOR_MUTATION =
        "ca552dfa000029a83bd72bd304c0a8390f000000000000000200064657445f54" +
        "4f000000160000000204c0a8390c000029a804c0a8390d000029a8000743414c" +
        "5f42414300000001000000004a01c4834b13c80011f1a25361ddf55403e50477" +
        "44d78410fd3d8712a8b381000002016e01760124000663616e61727900080000" +
        "000000003e46080e37313232306638313a313539343201";

    /**
     * Where the creation time, the address and the verb of a pre-4.0 message start (after the magic and the id). The
     * first is public for {@code MutationVerbHandlerForwardingTest}, which sets the time of the captured message.
     */
    public static final int CREATED_AT_OFFSET = 4 + 4;
    private static final int ADDRESS_OFFSET = CREATED_AT_OFFSET + 4;
    private static final int VERB_OFFSET = ADDRESS_OFFSET + 1 + 4;
    /** Where the {@code FWD_TO} value starts: after the verb, the param count, the name (2 + 6) and the length. */
    private static final int FWD_TO_VALUE_OFFSET = VERB_OFFSET + 4 + 4 + 2 + 6 + 4;
    /** The length of the {@code FWD_TO} value DSE writes, and how much of it the two targets use. */
    private static final int DSE_FWD_TO_LENGTH = 128;
    private static final int USED_FWD_TO_LENGTH = 4 + 2 * (1 + 4 + 4);
    /**
     * Where the params of {@link #DSE_COORDINATOR_MUTATION} end: the {@code CAL_BAC} after the padded {@code FWD_TO}.
     * Public, with the message, for {@code MutationVerbHandlerForwardingTest}, which forwards the captured message.
     */
    public static final int DSE_COORDINATOR_PARAMS_END = FWD_TO_VALUE_OFFSET + DSE_FWD_TO_LENGTH + 2 + 7 + 4 + 1;
    /** Where the params of {@link #UPGRADED_COORDINATOR_MUTATION} end: the {@code CAL_BAC} after {@code FWD_TO}. */
    private static final int UPGRADED_COORDINATOR_PARAMS_END = FWD_TO_VALUE_OFFSET + USED_FWD_TO_LENGTH + 2 + 7 + 4 + 1;
    /** The table of the captured mutations, the first thing in their payload. */
    private static final TableId CANARY_TABLE = TableId.fromString("c4834b13-c800-11f1-a253-61ddf55403e5");

    private static InternodeCompatibilityMode modeBefore;
    private static InetAddressAndPort peer;

    @BeforeClass
    public static void dse6Mode() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
        peer = InetAddressAndPort.getByName("127.0.0.42");
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
    }

    @AfterClass
    public static void restoreMode()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
    }

    /** DSE's padded value read by the version-11 deserializer: two targets, an id each, then 106 zeros left unread. */
    @Test
    public void dsePaddedValue() throws Exception
    {
        byte[] packet = Hex.hexToBytes(DSE_COORDINATOR_MUTATION);
        assertThat(ByteBuffer.wrap(packet).getInt(FWD_TO_VALUE_OFFSET - 4)).as("FWD_TO value length").isEqualTo(DSE_FWD_TO_LENGTH);
        byte[] value = Arrays.copyOfRange(packet, FWD_TO_VALUE_OFFSET, FWD_TO_VALUE_OFFSET + DSE_FWD_TO_LENGTH);
        try (DataInputBuffer in = new DataInputBuffer(value))
        {
            ForwardingInfo forwardTo = ForwardingInfo.serializer.deserialize(in, VERSION_3014);
            assertThat(forwardTo.targets).as("targets").containsExactly(InetAddressAndPort.getByName("192.168.57.14"), InetAddressAndPort.getByName("192.168.57.16"));
            assertThat(forwardTo.messageIds).as("an id per target").containsExactly(228717, 228718);
            assertThat(forwardTo.useSameMessageID(228716)).as("same id as the message").isFalse();
            assertThat(value.length - in.available()).as("bytes read").isEqualTo(USED_FWD_TO_LENGTH);
            byte[] padding = new byte[in.available()];
            in.readFully(padding);
            assertThat(padding).as("padding").hasSize(DSE_FWD_TO_LENGTH - USED_FWD_TO_LENGTH).containsOnly(0);
        }
    }

    /**
     * The whole message of the DSE coordinator, framed and read as the inbound path does: its size from the param
     * lengths, the header (FWD_TO and the CAL_BAC that follows the padding), and the payload, which starts with the
     * table id of the mutation.
     */
    @Test
    public void dseMessageWithPaddedValue() throws Exception
    {
        byte[] packet = Hex.hexToBytes(DSE_COORDINATOR_MUTATION);
        Message.Header header = assertHeader(packet, 228716, 228717, 228718);
        assertThat(header.forwardTo().targets).as("targets").containsExactly(InetAddressAndPort.getByName("192.168.57.14"), InetAddressAndPort.getByName("192.168.57.16"));
    }

    /** The message of the upgraded coordinator: no padding, every target with the message's id. */
    @Test
    public void upgradedCoordinatorMessage() throws Exception
    {
        byte[] packet = Hex.hexToBytes(UPGRADED_COORDINATOR_MUTATION);
        Message.Header header = assertHeader(packet, 10664, 10664, 10664);
        assertThat(header.forwardTo().targets).as("targets").containsExactly(InetAddressAndPort.getByName("192.168.57.12"), InetAddressAndPort.getByName("192.168.57.13"));
        assertThat(header.forwardTo().useSameMessageID(10664)).as("same id as the message").isTrue();
    }

    /** The value this software writes for a DSE forwarder at version 11 is the captured one, byte for byte. */
    @Test
    public void upgradedCoordinatorValueBytes() throws Exception
    {
        byte[] packet = Hex.hexToBytes(UPGRADED_COORDINATOR_MUTATION);
        assertThat(ByteBuffer.wrap(packet).getInt(FWD_TO_VALUE_OFFSET - 4)).as("FWD_TO value length").isEqualTo(USED_FWD_TO_LENGTH);
        byte[] captured = Arrays.copyOfRange(packet, FWD_TO_VALUE_OFFSET, FWD_TO_VALUE_OFFSET + USED_FWD_TO_LENGTH);

        ForwardingInfo forwardTo = new ForwardingInfo(ImmutableList.of(InetAddressAndPort.getByName("192.168.57.12"),
                                                                       InetAddressAndPort.getByName("192.168.57.13")),
                                                      new long[]{ 10664, 10664 });
        assertThat(ForwardingInfo.serializer.serializedSize(forwardTo, VERSION_3014)).as("serialized size").isEqualTo(USED_FWD_TO_LENGTH);
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            ForwardingInfo.serializer.serialize(forwardTo, out, VERSION_3014);
            assertThat(out.toByteArray()).as("FWD_TO value").isEqualTo(captured);
        }
    }

    /**
     * The header and params this software writes for a DSE forwarder at version 11 are the captured ones, byte for byte:
     * magic, message id, address, the param count, FWD_TO then CAL_BAC with their lengths and values. Only the creation
     * time and the verb differ: the message is an echo request, whose payload needs no schema; the payload is not compared.
     */
    @Test
    public void upgradedCoordinatorHeaderBytes() throws Exception
    {
        byte[] packet = Hex.hexToBytes(UPGRADED_COORDINATOR_MUTATION);
        ForwardingInfo forwardTo = new ForwardingInfo(ImmutableList.of(InetAddressAndPort.getByName("192.168.57.12"),
                                                                       InetAddressAndPort.getByName("192.168.57.13")),
                                                      new long[]{ 10664, 10664 });
        Message<NoPayload> message = Message.builder(Verb.ECHO_REQ, NoPayload.noPayload)
                                            .withId(10664)
                                            .from(InetAddressAndPort.getByName("192.168.57.15"))
                                            .withFlag(MessageFlag.CALL_BACK_ON_FAILURE)
                                            .withParam(ParamType.FORWARD_TO, forwardTo)
                                            .build();
        byte[] written;
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            Message.serializer.serialize(message, out, VERSION_3014);
            written = out.toByteArray();
        }
        assertThat(written).as("an echo request with no payload").hasSize(UPGRADED_COORDINATOR_PARAMS_END + 4);
        assertThat(Arrays.copyOfRange(written, 0, CREATED_AT_OFFSET)).as("magic and message id")
                                                                       .isEqualTo(Arrays.copyOfRange(packet, 0, CREATED_AT_OFFSET));
        assertThat(Arrays.copyOfRange(written, ADDRESS_OFFSET, VERB_OFFSET)).as("address")
                                                                           .isEqualTo(Arrays.copyOfRange(packet, ADDRESS_OFFSET, VERB_OFFSET));
        assertThat(Arrays.copyOfRange(written, VERB_OFFSET + 4, UPGRADED_COORDINATOR_PARAMS_END)).as("params")
                                                                                                .isEqualTo(Arrays.copyOfRange(packet, VERB_OFFSET + 4, UPGRADED_COORDINATOR_PARAMS_END));
    }

    /** A padded known param followed by a param this software does not know, then CAL_BAC: the unknown one is skipped. */
    @Test
    public void paddedValueFollowedByUnknownParam() throws Exception
    {
        TimeUUID session = TimeUUID.Generator.nextTimeUUID();
        byte[] message = pre40Message(3, out -> {
            out.writeUTF("TraceSession");
            out.writeInt((int) TimeUUID.sizeInBytes() + 3);
            session.serialize(out);
            out.write(new byte[3]);
            out.writeUTF("NOT_A_PARAM");
            out.writeInt(3);
            out.write(new byte[]{ 1, 2, 3 });
            out.writeUTF("CAL_BAC");
            out.writeInt(1);
            out.writeByte(0);
        });
        Message.Header header = Message.serializer.extractHeader(ByteBuffer.wrap(message), peer, approxTime.now(), VERSION_3014);
        assertThat(header.traceSession()).as("TraceSession").isEqualTo(session);
        assertThat(header.callBackOnFailure()).as("CAL_BAC, after the unknown param").isTrue();
    }

    /** A padded FWD_TO followed by FWD_FRM, whose value is a raw address of the declared length. */
    @Test
    public void paddedForwardToFollowedByForwardFrom() throws Exception
    {
        InetAddressAndPort target = InetAddressAndPort.getByName("127.0.0.43");
        InetAddressAndPort respondTo = InetAddressAndPort.getByName("127.0.0.44");
        byte[] message = pre40Message(2, out -> {
            out.writeUTF("FWD_TO");
            out.writeInt(4 + 1 + 4 + 4 + 10);
            out.writeInt(1);
            out.writeByte(4);
            out.write(target.getAddress().getAddress());
            out.writeInt(18);
            out.write(new byte[10]);
            out.writeUTF("FWD_FRM");
            out.writeInt(4);
            out.write(respondTo.getAddress().getAddress());
        });
        Message.Header header = Message.serializer.extractHeader(ByteBuffer.wrap(message), peer, approxTime.now(), VERSION_3014);
        assertThat(header.forwardTo().targets).as("FWD_TO targets").containsExactly(target);
        assertThat(header.forwardTo().messageIds).as("FWD_TO ids").containsExactly(18);
        assertThat(header.respondTo()).as("FWD_FRM").isEqualTo(respondTo);
    }

    /** A negative declared length fails the message, known param or not; the error names at most 64 characters of the name. */
    @Test
    public void negativeDeclaredLength() throws Exception
    {
        char[] longName = new char[1000];
        Arrays.fill(longName, 'x');
        byte[] withLongName = pre40Message(1, out -> {
            out.writeUTF(new String(longName));
            out.writeInt(-1);
        });
        assertThatThrownBy(() -> Message.serializer.extractHeader(ByteBuffer.wrap(withLongName), peer, approxTime.now(), VERSION_3014))
        .isInstanceOf(IOException.class)
        .hasMessage("The pre-4.0 param " + new String(longName, 0, 64) + "... has a negative declared length of -1 bytes");

        for (String alias : new String[]{ "TraceSession", "NOT_A_PARAM" })
        {
            byte[] message = pre40Message(1, out -> {
                out.writeUTF(alias);
                out.writeInt(-1);
            });
            assertThatThrownBy(() -> Message.serializer.extractHeader(ByteBuffer.wrap(message), peer, approxTime.now(), VERSION_3014))
            .isInstanceOf(IOException.class)
            .hasMessage("The pre-4.0 param " + alias + " has a negative declared length of -1 bytes");
        }
    }

    /** From version 4.0 on the params are read as before, in mode NONE and DSE_6: FWD_TO, FWD_FRM and the trace session. */
    @Test
    public void paramsFromVersion40Unchanged() throws Exception
    {
        TimeUUID session = TimeUUID.Generator.nextTimeUUID();
        InetAddressAndPort target = InetAddressAndPort.getByName("127.0.0.43");
        InetAddressAndPort respondTo = InetAddressAndPort.getByName("127.0.0.44");
        Message<NoPayload> message = Message.builder(Verb.ECHO_REQ, NoPayload.noPayload)
                                            .withId(19)
                                            .from(peer)
                                            .withParam(ParamType.FORWARD_TO, new ForwardingInfo(ImmutableList.of(target), new long[]{ 18 }))
                                            .withParam(ParamType.RESPOND_TO, respondTo)
                                            .withParam(ParamType.TRACE_SESSION, session)
                                            .build();
        try
        {
            for (InternodeCompatibilityMode mode : new InternodeCompatibilityMode[]{ InternodeCompatibilityMode.NONE, InternodeCompatibilityMode.DSE_6 })
            {
                DatabaseDescriptor.setInternodeCompatibilityMode(mode);
                for (int version : new int[]{ MessagingService.VERSION_40, MessagingService.current_version })
                {
                    try (DataOutputBuffer out = new DataOutputBuffer())
                    {
                        Message.serializer.serialize(message, out, version);
                        try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
                        {
                            Message<?> received = Message.serializer.deserialize(in, peer, version);
                            assertThat(received.forwardTo().targets).as("FWD_TO targets in %s at %d", mode, version).containsExactly(target);
                            assertThat(received.forwardTo().messageIds).as("FWD_TO ids in %s at %d", mode, version).containsExactly(18);
                            assertThat(received.respondTo()).as("FWD_FRM in %s at %d", mode, version).isEqualTo(respondTo);
                            assertThat(received.traceSession()).as("TraceSession in %s at %d", mode, version).isEqualTo(session);
                            assertThat(in.available()).as("bytes left after the message in %s at %d", mode, version).isZero();
                        }
                    }
                }
            }
        }
        finally
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        }
    }

    /**
     * Any known pre-4.0 param, not only {@code FWD_TO}, may have a value longer than its content: the unread bytes are
     * skipped and the param after it is read where it starts.
     */
    @Test
    public void valueShorterThanItsDeclaredLength() throws Exception
    {
        TimeUUID session = TimeUUID.Generator.nextTimeUUID();
        byte[] message = pre40Message(2, out -> {
            out.writeUTF("TraceSession");
            out.writeInt((int) TimeUUID.sizeInBytes() + 5);
            session.serialize(out);
            out.write(new byte[5]);
            out.writeUTF("CAL_BAC");
            out.writeInt(1);
            out.writeByte(0);
        });
        ByteBuffer buf = ByteBuffer.wrap(message);
        assertThat(Message.serializer.inferMessageSize(buf, 0, message.length, VERSION_3014)).as("message size").isEqualTo(message.length);
        Message.Header header = Message.serializer.extractHeader(buf, peer, approxTime.now(), VERSION_3014);
        assertThat(header.traceSession()).as("TraceSession").isEqualTo(session);
        assertThat(header.callBackOnFailure()).as("CAL_BAC, the param after the padded one").isTrue();

        try (DataInputBuffer in = new DataInputBuffer(message))
        {
            Message<?> received = Message.serializer.deserialize(in, peer, VERSION_3014);
            assertThat(received.traceSession()).as("TraceSession").isEqualTo(session);
            assertThat(received.callBackOnFailure()).as("CAL_BAC, the param after the padded one").isTrue();
            assertThat(in.available()).as("bytes left after the message").isZero();
        }
    }

    /** A serializer that reads more than the declared length fails the message rather than misframe the next param. */
    @Test
    public void valueLongerThanItsDeclaredLength() throws Exception
    {
        byte[] message = pre40Message(2, out -> {
            out.writeUTF("TraceSession");
            out.writeInt(8);
            TimeUUID.Generator.nextTimeUUID().serialize(out);
            out.writeUTF("CAL_BAC");
            out.writeInt(1);
            out.writeByte(0);
        });
        assertThatThrownBy(() -> Message.serializer.extractHeader(ByteBuffer.wrap(message), peer, approxTime.now(), VERSION_3014))
        .isInstanceOf(IOException.class)
        .hasMessage("The serializer of the pre-4.0 param TraceSession read 16 bytes, more than its declared length of 8 bytes");
    }

    /**
     * A serializer that needs more than the declared length and finds the end of the input says so, whether it stopped
     * at the declared length or after it.
     */
    @Test
    public void valueLongerThanItsDeclaredLengthAtTheEndOfTheInput() throws Exception
    {
        // the 16-byte session reads its first long from the 8 bytes of the value; its second finds only the 4 bytes of the payload size
        byte[] atTheLength = pre40Message(1, out -> {
            out.writeUTF("TraceSession");
            out.writeInt(8);
            out.write(new byte[8]);
        });
        assertThat(Message.serializer.inferMessageSize(ByteBuffer.wrap(atTheLength), 0, atTheLength.length, VERSION_3014)).as("message size").isEqualTo(atTheLength.length);
        assertThatThrownBy(() -> Message.serializer.extractHeader(ByteBuffer.wrap(atTheLength), peer, approxTime.now(), VERSION_3014))
        .isInstanceOf(EOFException.class)
        .hasMessage("The serializer of the pre-4.0 param TraceSession needs more than its declared length of 8 bytes: it read 8 bytes, then reached the end of the input");

        // the first long takes the 4 bytes of the value and the 4 of the payload size; the second finds nothing
        byte[] pastTheLength = pre40Message(1, out -> {
            out.writeUTF("TraceSession");
            out.writeInt(4);
            out.write(new byte[4]);
        });
        assertThatThrownBy(() -> Message.serializer.extractHeader(ByteBuffer.wrap(pastTheLength), peer, approxTime.now(), VERSION_3014))
        .isInstanceOf(EOFException.class)
        .hasMessage("The serializer of the pre-4.0 param TraceSession needs more than its declared length of 4 bytes: it read 8 bytes, then reached the end of the input");
    }

    /**
     * The input ends inside a value: while the serializer reads it, whether the failed read wanted bytes of the value or
     * bytes past it, or while the padding after it is skipped.
     */
    @Test
    public void valueCutShortByTheEndOfTheInput() throws Exception
    {
        // the 16-byte session finds only the 8 bytes of the value and the 4 of the payload size
        byte[] inTheValue = pre40Message(1, out -> {
            out.writeUTF("TraceSession");
            out.writeInt(20);
            out.write(new byte[8]);
        });
        assertThatThrownBy(() -> Message.serializer.extractHeader(ByteBuffer.wrap(inTheValue), peer, approxTime.now(), VERSION_3014))
        .isInstanceOf(EOFException.class)
        .hasMessage("The input ended while the serializer of the pre-4.0 param TraceSession was reading its value: it had read 8 of the 20 declared bytes");

        // the 12 declared bytes are all there, the payload size included: the second long of the session wants 4 more
        byte[] pastTheValue = pre40Message(1, out -> {
            out.writeUTF("TraceSession");
            out.writeInt(12);
            out.write(new byte[8]);
        });
        assertThatThrownBy(() -> Message.serializer.extractHeader(ByteBuffer.wrap(pastTheValue), peer, approxTime.now(), VERSION_3014))
        .isInstanceOf(EOFException.class)
        .hasMessage("The input ended while the serializer of the pre-4.0 param TraceSession was reading its value: it had read 8 of the 12 declared bytes");

        // the session is whole; the 8 bytes of padding to skip find only the 4 of the payload size
        byte[] inThePadding = pre40Message(1, out -> {
            out.writeUTF("TraceSession");
            out.writeInt(24);
            TimeUUID.Generator.nextTimeUUID().serialize(out);
        });
        assertThatThrownBy(() -> Message.serializer.extractHeader(ByteBuffer.wrap(inThePadding), peer, approxTime.now(), VERSION_3014))
        .isInstanceOf(EOFException.class)
        .hasMessage("The value of the pre-4.0 param TraceSession is cut short: the input ends before its declared length of 24 bytes (its serializer read 16 bytes)");
    }

    /**
     * The name of a pre-4.0 param has an unsigned short length: an unknown param with a name of more than 32767 bytes is
     * skipped by the size inference, the header extraction and the skip of the header before the payload.
     */
    @Test
    public void longNameOfAnUnknownParam() throws Exception
    {
        char[] name = new char[40000];
        Arrays.fill(name, 'x');
        byte[] message = pre40Message(2, out -> {
            out.writeUTF(new String(name));
            out.writeInt(3);
            out.write(new byte[]{ 1, 2, 3 });
            out.writeUTF("CAL_BAC");
            out.writeInt(1);
            out.writeByte(0);
        });
        ByteBuffer buf = ByteBuffer.wrap(message);
        assertThat(Message.serializer.inferMessageSize(buf, 0, message.length, VERSION_3014)).as("message size").isEqualTo(message.length);
        Message.Header header = Message.serializer.extractHeader(buf, peer, approxTime.now(), VERSION_3014);
        assertThat(header.callBackOnFailure()).as("CAL_BAC, after the long name").isTrue();
        try (DataInputBuffer in = new DataInputBuffer(message))
        {
            Message<?> received = Message.serializer.deserialize(in, header, VERSION_3014);
            assertThat(received.callBackOnFailure()).as("CAL_BAC, after the long name").isTrue();
            assertThat(in.available()).as("bytes left after the message").isZero();
        }
    }

    /** A pre-4.0 echo request of {@link #peer} with no payload and the given params, written by {@code params}. */
    private static byte[] pre40Message(int paramCount, ParamsWriter params) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.writeInt(Message.PROTOCOL_MAGIC);
            out.writeInt(42);
            out.writeInt((int) FBUtilities.now().toEpochMilli());
            byte[] address = peer.getAddress().getAddress();
            out.writeByte(address.length);
            out.write(address);
            out.writeInt(Verb.ECHO_REQ.toPre40Verb().id);
            out.writeInt(paramCount);
            params.write(out);
            out.writeInt(0);
            return out.toByteArray();
        }
    }

    private interface ParamsWriter
    {
        void write(DataOutputBuffer out) throws IOException;
    }

    /**
     * Frames and reads one captured message as {@link InboundMessageHandler} does: the size inferred from the lengths,
     * the header extracted, then the payload read after the header, skipped by the lengths. The payload is a mutation of
     * a table this node does not know: reading it fails on the table id, its first field, which proves where it starts.
     */
    private static Message.Header assertHeader(byte[] packet, long id, long... targetIds) throws Exception
    {
        ByteBuffer buf = ByteBuffer.wrap(packet);
        assertThat(Message.serializer.inferMessageSize(buf, 0, packet.length, VERSION_3014)).as("message size").isEqualTo(packet.length);

        Message.Header header = Message.serializer.extractHeader(buf, peer, approxTime.now(), VERSION_3014);
        assertThat(header.id).as("message id").isEqualTo(id);
        assertThat(header.verb).as("verb").isEqualTo(Verb.MUTATION_REQ);
        assertThat(header.forwardTo().messageIds).as("ids of the targets").containsExactly(targetIds);
        assertThat(header.callBackOnFailure()).as("CAL_BAC, the param after FWD_TO").isTrue();

        try (DataInputBuffer in = new DataInputBuffer(buf, false))
        {
            assertThatThrownBy(() -> Message.serializer.deserialize(in, header, VERSION_3014))
            .isInstanceOfSatisfying(UnknownTableException.class, e -> assertThat(e.id).as("table of the payload").isEqualTo(CANARY_TABLE));
        }
        return header;
    }
}
