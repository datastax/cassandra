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
package org.apache.cassandra.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.IVersionedAsymmetricSerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.tracing.Tracing.TraceType;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FreeRunningClock;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.net.Message.serializer;
import static org.apache.cassandra.net.MessagingService.VERSION_30;
import static org.apache.cassandra.net.MessagingService.VERSION_3014;
import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.apache.cassandra.net.ParamType.RESPOND_TO;
import static org.apache.cassandra.net.ParamType.TRACE_SESSION;
import static org.apache.cassandra.net.ParamType.TRACE_TYPE;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

import static org.junit.Assert.*;

public class MessageTest
{
    /**
     * The pre-4.0 dialect (DSE 6 compatibility mode) carries only the request verbs of Cassandra 3.0, so the pre-4.0
     * cycles use two of them instead of the test verbs: ECHO_REQ without payload, BATCH_REMOVE_REQ with the int payload
     * of _TEST_2 for the duration of the class.
     */
    private static final Verb PRE40_NO_PAYLOAD = Verb.ECHO_REQ;
    private static final Verb PRE40_INT_PAYLOAD = Verb.BATCH_REMOVE_REQ;
    private static Supplier<? extends IVersionedAsymmetricSerializer<?, ?>> pre40IntPayloadSerializer;

    private static final IVersionedSerializer<Integer> INT_SERIALIZER = new IVersionedSerializer<Integer>()
    {
        public void serialize(Integer value, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(value);
        }

        public Integer deserialize(DataInputPlus in, int version) throws IOException
        {
            return in.readInt();
        }

        public long serializedSize(Integer value, int version)
        {
            return 4;
        }
    };

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setCrossNodeTimeout(true);

        Verb._TEST_2.unsafeSetSerializer(() -> INT_SERIALIZER);
        pre40IntPayloadSerializer = PRE40_INT_PAYLOAD.unsafeSetSerializer(() -> INT_SERIALIZER);
    }

    @AfterClass
    public static void tearDownClass() throws Exception
    {
        Verb._TEST_2.unsafeSetSerializer(() -> NoPayload.serializer);
        PRE40_INT_PAYLOAD.unsafeSetSerializer(pre40IntPayloadSerializer);
    }

    /** Runs the task in DSE 6 compatibility mode, the only mode in which the pre-4.0 layouts are spoken. */
    private static void inDse6Mode(ThrowingRunnable task) throws Exception
    {
        InternodeCompatibilityMode before = DatabaseDescriptor.getInternodeCompatibilityMode();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        try
        {
            task.run();
        }
        finally
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(before);
        }
    }

    private interface ThrowingRunnable
    {
        void run() throws Exception;
    }

    @Test
    public void testInferMessageSize() throws Exception
    {
        testInferMessageSize(inferMessageSizeMessage(Verb._TEST_2), VERSION_40);
        // the pre-4.0 layouts are the dialect of the DSE 6 compatibility mode
        Message<Integer> pre40 = inferMessageSizeMessage(PRE40_INT_PAYLOAD);
        inDse6Mode(() -> {
            testInferMessageSize(pre40, VERSION_30);
            testInferMessageSize(pre40, VERSION_3014);
        });
    }

    private static Message<Integer> inferMessageSizeMessage(Verb verb)
    {
        return Message.builder(verb, 37)
                      .withId(1)
                      .from(FBUtilities.getLocalAddressAndPort())
                      .withCreatedAt(approxTime.now())
                      .withExpiresAt(approxTime.now())
                      .withFlag(MessageFlag.CALL_BACK_ON_FAILURE)
                      .withFlag(MessageFlag.TRACK_REPAIRED_DATA)
                      .withParam(TRACE_TYPE, TraceType.QUERY)
                      .withParam(TRACE_SESSION, nextTimeUUID())
                      .build();
    }

    /** With the mode off no peer speaks the pre-4.0 dialect: every entry point of the codec refuses it, as the 4.0 assertions did. */
    @Test
    public void pre40LayoutIsRefusedWithTheModeOff() throws Exception
    {
        assertFalse(DatabaseDescriptor.isDse6CompatibilityMode());
        Message<NoPayload> msg = noPayloadMessage(PRE40_NO_PAYLOAD);
        String refusal = "pre-4.0 serialization version " + VERSION_3014;
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            assertEquals(refusal, assertThrows(AssertionError.class, () -> serializer.serialize(msg, out, VERSION_3014)).getMessage());
            // the size cache has no entry for the version, as before the mode existed
            assertEquals("Unkown serialization version " + VERSION_3014, assertThrows(IllegalStateException.class, () -> msg.serializedSize(VERSION_3014)).getMessage());
        }

        byte[] bytes;
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            inDse6Mode(() -> serializer.serialize(msg, out, VERSION_3014));
            bytes = out.toByteArray();
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        assertEquals(refusal, assertThrows(AssertionError.class, () -> serializer.inferMessageSize(buffer, 0, bytes.length, VERSION_3014)).getMessage());
        assertEquals(refusal, assertThrows(AssertionError.class, () -> serializer.extractHeader(buffer, msg.from(), approxTime.now(), VERSION_3014)).getMessage());
        try (DataInputBuffer in = new DataInputBuffer(bytes))
        {
            assertEquals(refusal, assertThrows(AssertionError.class, () -> serializer.deserialize(in, msg.from(), VERSION_3014)).getMessage());
        }
    }

    /**
     * The lengths and counts of a pre-4.0 message come from the peer: one that no valid message has, an address of
     * neither 4 nor 16 bytes, a negative count or length, is refused when the size is inferred, and lengths that add up
     * to more than the maximum message size (or would overflow an int) fail as an oversized message.
     */
    @Test
    public void pre40LengthsNoMessageHasAreRefused() throws Exception
    {
        Message<NoPayload> msg = noPayloadMessage(PRE40_NO_PAYLOAD);
        inDse6Mode(() -> {
            byte[] valid;
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                serializer.serialize(msg, out, VERSION_3014);
                valid = out.toByteArray();
            }
            assertEquals(valid.length, serializer.inferMessageSize(ByteBuffer.wrap(valid), 0, valid.length, VERSION_3014));

            int addressLengthOffset = 12; // magic, id, creation time
            assertEquals(4, valid[addressLengthOffset]);
            int paramCountOffset = addressLengthOffset + 1 + 4 + 4; // the IPv4 address, the verb
            assertEquals(2, ByteBuffer.wrap(valid).getInt(paramCountOffset)); // CAL_BAC and TraceSession
            int firstParamNameLength = ByteBuffer.wrap(valid).getShort(paramCountOffset + 4);
            int firstParamLengthOffset = paramCountOffset + 4 + 2 + firstParamNameLength;
            int payloadSizeOffset = valid.length - 4; // no payload

            assertPre40Refused(valid, addressLengthOffset, (byte) 5, "Invalid pre-4.0 message: address length 5, neither 4 nor 16");
            assertPre40Refused(valid, addressLengthOffset, (byte) -4, "Invalid pre-4.0 message: address length 252, neither 4 nor 16");
            assertPre40Refused(valid, paramCountOffset, -1, "Invalid pre-4.0 message: param count -1");
            assertPre40Refused(valid, firstParamLengthOffset, -1, "Invalid pre-4.0 message: param length -1");
            assertPre40Refused(valid, payloadSizeOffset, -1, "Invalid pre-4.0 message: payload size -1");

            ByteBuffer oversizedParam = ByteBuffer.wrap(valid.clone()).putInt(firstParamLengthOffset, Integer.MAX_VALUE);
            assertThrows(Message.OversizedMessageException.class, () -> serializer.inferMessageSize(oversizedParam, 0, valid.length, VERSION_3014));
            ByteBuffer oversizedPayload = ByteBuffer.wrap(valid.clone()).putInt(payloadSizeOffset, Integer.MAX_VALUE);
            assertThrows(Message.OversizedMessageException.class, () -> serializer.inferMessageSize(oversizedPayload, 0, valid.length, VERSION_3014));
        });
    }

    private static void assertPre40Refused(byte[] valid, int offset, byte value, String expected)
    {
        byte[] bytes = valid.clone();
        bytes[offset] = value;
        assertPre40Refused(bytes, expected);
    }

    private static void assertPre40Refused(byte[] valid, int offset, int value, String expected)
    {
        byte[] bytes = valid.clone();
        ByteBuffer.wrap(bytes).putInt(offset, value);
        assertPre40Refused(bytes, expected);
    }

    private static void assertPre40Refused(byte[] bytes, String expected)
    {
        Message.InvalidPre40MessageException e = assertThrows(Message.InvalidPre40MessageException.class,
                                                              () -> serializer.inferMessageSize(ByteBuffer.wrap(bytes), 0, bytes.length, VERSION_3014));
        assertEquals(expected, e.getMessage());
    }

    private void testInferMessageSize(Message msg, int version) throws Exception
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            serializer.serialize(msg, out, version);
            assertEquals(msg.serializedSize(version), out.getLength());

            ByteBuffer buffer = out.buffer();

            int payloadSize = (int) msg.verb().serializer().serializedSize(msg.payload, version);
            int serializedSize = msg.serializedSize(version);

            // should return -1 - fail to infer size - for all lengths of buffer until payload length can be read
            for (int limit = 0; limit < serializedSize - payloadSize; limit++)
                assertEquals(-1, serializer.inferMessageSize(buffer, 0, limit, version));

            // once payload size can be read, should correctly infer message size
            for (int limit = serializedSize - payloadSize; limit < serializedSize; limit++)
                assertEquals(serializedSize, serializer.inferMessageSize(buffer, 0, limit, version));
        }
    }

    @Test
    public void testBuilder()
    {
        long id = 1;
        InetAddressAndPort from = FBUtilities.getLocalAddressAndPort();
        long createAtNanos = approxTime.now();
        long expiresAtNanos = createAtNanos + TimeUnit.SECONDS.toNanos(1);
        TraceType traceType = TraceType.QUERY;
        TimeUUID traceSession = nextTimeUUID();

        Message<NoPayload> msg =
            Message.builder(Verb._TEST_1, noPayload)
                   .withId(1)
                   .from(from)
                   .withCreatedAt(createAtNanos)
                   .withExpiresAt(expiresAtNanos)
                   .withFlag(MessageFlag.CALL_BACK_ON_FAILURE)
                   .withParam(TRACE_TYPE, TraceType.QUERY)
                   .withParam(TRACE_SESSION, traceSession)
                   .build();

        assertEquals(id, msg.id());
        assertEquals(from, msg.from());
        assertEquals(createAtNanos, msg.createdAtNanos());
        assertEquals(expiresAtNanos, msg.expiresAtNanos());
        assertTrue(msg.callBackOnFailure());
        assertFalse(msg.trackRepairedData());
        assertEquals(traceType, msg.traceType());
        assertEquals(traceSession, msg.traceSession());
        assertNull(msg.forwardTo());
        assertEquals(from, msg.respondTo());
    }

    @Test
    public void testCycleNoPayload() throws Exception
    {
        testCycle(noPayloadMessage(Verb._TEST_1), VERSION_40);
        testCyclePre40(noPayloadMessage(PRE40_NO_PAYLOAD));
    }

    private static Message<NoPayload> noPayloadMessage(Verb verb)
    {
        return Message.builder(verb, noPayload)
                      .withId(1)
                      .from(FBUtilities.getLocalAddressAndPort())
                      .withCreatedAt(approxTime.now())
                      .withExpiresAt(approxTime.now() + TimeUnit.SECONDS.toNanos(1))
                      .withFlag(MessageFlag.CALL_BACK_ON_FAILURE)
                      .withParam(TRACE_SESSION, nextTimeUUID())
                      .build();
    }

    @Test
    public void testCycleWithPayload() throws Exception
    {
        testCycle(Message.out(Verb._TEST_2, 42), VERSION_40);
        testCycle(Message.outWithFlag(Verb._TEST_2, 42, MessageFlag.CALL_BACK_ON_FAILURE), VERSION_40);
        testCycle(Message.outWithFlags(Verb._TEST_2, 42, MessageFlag.CALL_BACK_ON_FAILURE, MessageFlag.TRACK_REPAIRED_DATA), VERSION_40);
        testCycle(Message.outWithParam(1, Verb._TEST_2, 42, RESPOND_TO, FBUtilities.getBroadcastAddressAndPort()), VERSION_40);
        // testCyclePre40 cycles each message at 4.0 too
        testCyclePre40(Message.out(PRE40_INT_PAYLOAD, 42));
        testCyclePre40(Message.outWithFlag(PRE40_INT_PAYLOAD, 42, MessageFlag.CALL_BACK_ON_FAILURE));
        testCyclePre40(Message.outWithFlags(PRE40_INT_PAYLOAD, 42, MessageFlag.CALL_BACK_ON_FAILURE, MessageFlag.TRACK_REPAIRED_DATA));
        // the pre-4.0 FWD_FRM value is the address alone: the port read back is the default storage port
        testCyclePre40(Message.outWithParam(1, PRE40_INT_PAYLOAD, 42, RESPOND_TO, InetAddressAndPort.getByAddress(FBUtilities.getBroadcastAddressAndPort().getAddress())));
    }

    @Test
    public void testFailureResponse() throws Exception
    {
        long expiresAt = approxTime.now();
        Message<RequestFailureReason> msg = Message.failureResponse(1, expiresAt, RequestFailureReason.INCOMPATIBLE_SCHEMA, Verb.MUTATION_REQ);

        assertEquals(1, msg.id());
        assertEquals(Verb.FAILURE_RSP, msg.verb());
        assertEquals(expiresAt, msg.expiresAtNanos());
        assertEquals(RequestFailureReason.INCOMPATIBLE_SCHEMA, msg.payload);
        assertEquals(Verb.MUTATION_REQ.name(), msg.header.params().get(ParamType.REQUEST_VERB_NAME));
        assertTrue(msg.isFailureResponse());

        // the pre-4.0 dialect only knows the reasons of Cassandra 3.0: the others travel as UNKNOWN (V11SerializationAuditTest)
        testCycle(msg, VERSION_40);
        testCyclePre40(Message.failureResponse(1, expiresAt, RequestFailureReason.READ_TOO_MANY_TOMBSTONES, Verb.MUTATION_REQ));
    }

    @Test
    public void testNonStaticFailureResponse() throws Exception
    {
        Message<String> incomingMessage = Message.builder(Verb.MUTATION_REQ, "some-payload").withId(1).build();
        Message<RequestFailureReason> msg = incomingMessage.failureResponse(RequestFailureReason.INCOMPATIBLE_SCHEMA);

        assertEquals(1, msg.id());
        assertEquals(Verb.FAILURE_RSP, msg.verb());
        assertEquals(incomingMessage.expiresAtNanos(), msg.expiresAtNanos());
        assertEquals(RequestFailureReason.INCOMPATIBLE_SCHEMA, msg.payload);
        assertEquals(Verb.MUTATION_REQ.name(), msg.header.params().get(ParamType.REQUEST_VERB_NAME));
        assertTrue(msg.isFailureResponse());

        testCycle(msg, VERSION_40);
        testCyclePre40(incomingMessage.failureResponse(RequestFailureReason.UNKNOWN));
    }

    @Test
    public void testBuilderAddTraceHeaderWhenTraceSessionPresent()
    {
        Stream.of(TraceType.values()).forEach(this::testAddTraceHeaderWithType);
    }

    @Test
    public void testBuilderNotAddTraceHeaderWithNoTraceSession()
    {
        Message<NoPayload> msg = Message.builder(Verb._TEST_1, noPayload).withTracingParams().build();
        assertNull(msg.header.traceSession());
    }

    @Test
    public void testCustomParams() throws CharacterCodingException, IOException
    {
        long id = 1;
        InetAddressAndPort from = FBUtilities.getLocalAddressAndPort();

        Message<NoPayload> msg =
            Message.builder(Verb._TEST_1, noPayload)
                   .withId(1)
                   .from(from)
                   .withCustomParam("custom1", "custom1value".getBytes(StandardCharsets.UTF_8))
                   .withCustomParam("custom2", "custom2value".getBytes(StandardCharsets.UTF_8))
                   .build();

        assertEquals(id, msg.id());
        assertEquals(from, msg.from());
        assertEquals(2, msg.header.customParams().size());
        assertEquals("custom1value", new String(msg.header.customParams().get("custom1"), StandardCharsets.UTF_8));
        assertEquals("custom2value", new String(msg.header.customParams().get("custom2"), StandardCharsets.UTF_8));

        DataOutputBuffer out = DataOutputBuffer.scratchBuffer.get();
        Message.serializer.serialize(msg, out, VERSION_40);
        DataInputBuffer in = new DataInputBuffer(out.buffer(), true);
        msg = Message.serializer.deserialize(in, from, VERSION_40);

        assertEquals(id, msg.id());
        assertEquals(from, msg.from());
        assertEquals(2, msg.header.customParams().size());
        assertEquals("custom1value", new String(msg.header.customParams().get("custom1"), StandardCharsets.UTF_8));
        assertEquals("custom2value", new String(msg.header.customParams().get("custom2"), StandardCharsets.UTF_8));
    }

    @Test
    public void testResponseWithCustomParams()
    {
        long id = 1;
        InetAddressAndPort from = FBUtilities.getLocalAddressAndPort();

        Message<NoPayload> msg =
                Message.builder(Verb.READ_REQ, noPayload)
                        .withId(1)
                        .from(from)
                        .build();

        Message reply = msg.responseWithBuilder(msg)
                           .withCustomParam("custom1", "custom1value".getBytes(StandardCharsets.UTF_8))
                           .withCustomParam("custom2", "custom2value".getBytes(StandardCharsets.UTF_8))
                           .build();

        assertEquals(id, reply.id());
        assertEquals(from, reply.from());
        assertEquals(2, reply.header.customParams().size());
        assertEquals("custom1value", new String(reply.header.customParams().get("custom1"), StandardCharsets.UTF_8));
        assertEquals("custom2value", new String(reply.header.customParams().get("custom2"), StandardCharsets.UTF_8));
    }

    @Test
    public void testEmptyResponseBuilderWithCustomParams()
    {
        long id = 1;
        InetAddressAndPort from = FBUtilities.getLocalAddressAndPort();

        Message<NoPayload> msg =
        Message.builder(Verb.READ_REQ, noPayload)
               .withId(1)
               .from(from)
               .build();

        Message reply = msg.emptyResponseBuilder()
                           .withCustomParam("custom1", "custom1value".getBytes(StandardCharsets.UTF_8))
                           .withCustomParam("custom2", "custom2value".getBytes(StandardCharsets.UTF_8))
                           .build();

        assertEquals(id, reply.id());
        assertEquals(from, reply.from());
        assertEquals(2, reply.header.customParams().size());
        assertEquals("custom1value", new String(reply.header.customParams().get("custom1"), StandardCharsets.UTF_8));
        assertEquals("custom2value", new String(reply.header.customParams().get("custom2"), StandardCharsets.UTF_8));
    }

    private void testAddTraceHeaderWithType(TraceType traceType)
    {
        try
        {
            TimeUUID sessionId = Tracing.instance.newSession(ClientState.forInternalCalls(), traceType);
            Message<NoPayload> msg = Message.builder(Verb._TEST_1, noPayload).withTracingParams().build();
            assertEquals(sessionId, msg.header.traceSession());
            assertEquals(traceType, msg.header.traceType());
        }
        finally
        {
            Tracing.instance.stopSession();
        }
    }

    /** At every version, the pre-4.0 ones in DSE 6 compatibility mode, the only mode that speaks them. */
    private void testCyclePre40(Message msg) throws Exception
    {
        testCycle(msg, VERSION_40);
        inDse6Mode(() -> {
            testCycle(msg, VERSION_30);
            testCycle(msg, VERSION_3014);
        });
    }

    // serialize (using both variants, all in one or header then rest), verify serialized size, deserialize, compare to the original
    private void testCycle(Message msg, int version) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            serializer.serialize(msg, out, version);
            assertEquals(msg.serializedSize(version), out.getLength());

            // deserialize the message in one go, compare outcomes
            try (DataInputBuffer in = new DataInputBuffer(out.buffer(), true))
            {
                Message msgOut = serializer.deserialize(in, msg.from(), version);
                assertEquals(0, in.available());
                assertMessagesEqual(msg, msgOut);
            }

            // extract header first, then deserialize the rest of the message and compare outcomes
            ByteBuffer buffer = out.buffer();
            try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
            {
                Message.Header headerOut = serializer.extractHeader(buffer, msg.from(), approxTime.now(), version);
                Message msgOut = serializer.deserialize(in, headerOut, version);
                assertEquals(0, in.available());
                assertMessagesEqual(msg, msgOut);
            }
        }
    }

    private static void assertMessagesEqual(Message msg1, Message msg2)
    {
        assertEquals(msg1.id(),                msg2.id());
        assertEquals(msg1.verb(),              msg2.verb());
        assertEquals(msg1.callBackOnFailure(), msg2.callBackOnFailure());
        assertEquals(msg1.trackRepairedData(), msg2.trackRepairedData());
        assertEquals(msg1.traceType(),         msg2.traceType());
        assertEquals(msg1.traceSession(),      msg2.traceSession());
        assertEquals(msg1.respondTo(),         msg2.respondTo());
        assertEquals(msg1.forwardTo(),         msg2.forwardTo());

        Object payload1 = msg1.payload;
        Object payload2 = msg2.payload;

        if (null == payload1)
            assertTrue(payload2 == noPayload || payload2 == null);
        else if (null == payload2)
            assertSame(payload1, noPayload);
        else
            assertEquals(payload1, payload2);
    }

    @Test
    public void testCreationTime()
    {
        long remoteTime = 1632087572480L; // 10111110000000000000000000000000000000000
        long localTime  = 1632087572479L; // 10111101111111111111111111111111111111111
        FreeRunningClock localClock  = new FreeRunningClock(TimeUnit.DAYS.toNanos(1), localTime, 0);

        int remoteCreatedAt = (int) (remoteTime & 0x00000000FFFFFFFFL);

        long localTimeNanos = localClock.now();
        assertTrue( Message.Serializer.calculateCreationTimeNanos(remoteCreatedAt, localClock.translate(), localTimeNanos) > 0);
    }
}
