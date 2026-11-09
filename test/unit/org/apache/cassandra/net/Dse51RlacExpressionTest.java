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
import java.io.IOException;
import java.nio.ByteBuffer;
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

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dsecompat.Dse51Fixtures;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.net.MessagingService.VERSION_3014;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * In DSE 5.1 compatibility mode the reads a DSE 5.1.51 coordinator sends for a table restricted to rows, whose row
 * filter holds DSE 5.1's expression of row-level access control (a user expression, kind 4, under the id DSE registers
 * it with), are refused when they are read: answered with a failure response, as they ask for one ({@code CAL_BAC}),
 * counted, and never served without the filter; the connection reads the next message. The reads are those captured
 * between DSE 5.1.51 nodes ({@code messages/dse-5.1.51/rlac-read.txt}: a {@code READ} and a {@code RANGE_SLICE},
 * whose expression the capture does not tell the version of; by DSE 5.1.51's rule, a cluster of 5.1.51 nodes only, its
 * version 2), and a version 1 expression, which no capture holds, made from the captured {@code READ} by replacing
 * the value with the role alone. This node sends no user expression to a DSE 5.1 node.
 */
public class Dse51RlacExpressionTest
{
    private static final Dse51Fixtures DSE51 = DseFixtures.dse51();
    /** The value of the captured expression: the role and its authentication scheme, each with a 2-byte length. */
    private static final byte[] VERSION_2_VALUE = Hex.hexToBytes("0012" + "0006" + hex("reader") +
                                                               "0008" + hex("INTERNAL"));
    /** The value of a version 1 expression of the same role: the role alone, in UTF-8. */
    private static final byte[] VERSION_1_VALUE = Hex.hexToBytes("0006" + hex("reader"));

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
        SchemaLoader.prepareServer();
        // the user expression of the tests, in the registry of this JVM for every test of the class
        RowFilter.UserExpression.register(TestUserExpression.class, new RowFilter.UserExpression.Deserializer()
        {
            @Override
            protected RowFilter.UserExpression deserialize(DataInputPlus in, int version, TableMetadata metadata)
            {
                throw new UnsupportedOperationException();
            }
        });
        // test_ks.rlac as DSE 5.1.51 defined it, with its id, so that the captured reads find their table
        SchemaLoader.createKeyspace("test_ks", KeyspaceParams.simple(1),
                                    TableMetadata.builder("test_ks", "rlac",
                                                          TableId.fromUUID(Dse51Fixtures.RLAC_TABLE_ID))
                                                 .addPartitionKeyColumn("region", UTF8Type.instance)
                                                 .addClusteringColumn("id", Int32Type.instance)
                                                 .addRegularColumn("v", UTF8Type.instance));
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

    /**
     * The captured reads carry {@code CAL_BAC} and, as the only expression of their row filter, kind 4 with the id 0.
     */
    @Test
    public void theCapturedReadsCarryTheExpressionAndAskForAFailureResponse() throws Throwable
    {
        List<Dse51Fixtures.Message> reads = DSE51.messages("rlac-read");
        assertThat(reads).extracting(m -> m.verb).containsExactly("READ", "RANGE_SLICE");
        for (Dse51Fixtures.Message read : reads)
        {
            InetAddressAndPort from = InetAddressAndPort.getByName(read.from);
            Message.Header header = Message.serializer.extractHeader(ByteBuffer.wrap(read.bytes), from,
                                                                     approxTime.now(), VERSION_3014);
            assertThat(header.callBackOnFailure()).as(read.name).isTrue();
            // the kind, the id 0 under which DSE registers its expression, then its value
            byte[] expression = Hex.hexToBytes("04" + "00000000" + Hex.bytesToHex(VERSION_2_VALUE));
            assertThat(indexOf(read.bytes, expression)).as(read.name).isPositive();
        }
    }

    @Test
    public void theCapturedReadIsAnsweredWithAFailure() throws Throwable
    {
        isAnsweredWithAFailure(DSE51.messages("rlac-read").get(0).bytes);
    }

    @Test
    public void theCapturedRangeReadIsAnsweredWithAFailure() throws Throwable
    {
        isAnsweredWithAFailure(DSE51.messages("rlac-read").get(1).bytes);
    }

    /** DSE 5.1's first version of the expression, which a node writes in a cluster with DSE nodes below 5.1.21. */
    @Test
    public void aVersion1ExpressionIsAnsweredWithAFailure() throws Throwable
    {
        isAnsweredWithAFailure(withValue(DSE51.messages("rlac-read").get(0).bytes, VERSION_1_VALUE));
    }

    /**
     * {@code read}, from the coordinator of the capture, then the captured gossip ACK, in one frame: the read is
     * refused when its row filter is read, answered with a failure response to its id and counted; the ACK after it is
     * read and handed on, the connection open.
     */
    private void isAnsweredWithAFailure(byte[] read) throws Throwable
    {
        InetAddressAndPort coordinator = InetAddressAndPort.getByName(DSE51.messages("rlac-read").get(0).from);
        Message.Header header = Message.serializer.extractHeader(ByteBuffer.wrap(read), coordinator, approxTime.now(),
                                                                 VERSION_3014);
        process(coordinator, List.of(read, DSE51.messages("gossip-digest-ack").get(0).bytes));
        Util.spinAssertEquals(1, received::size, 10);
        assertThat(received.get(0).verb()).isEqualTo(Verb.GOSSIP_DIGEST_ACK);
        assertThat(sent).hasSize(1);
        assertThat(sent.get(0).right).isEqualTo(coordinator);
        assertThat(sent.get(0).left.id()).isEqualTo(header.id);
        assertFailureResponse(sent.get(0).left);
        assertThat(DseCompatRefusals.count(DseCompat.REFUSED_DSE51_ROW_LEVEL_ACCESS_CONTROL)).isEqualTo(1);
        assertThat(DseCompatRefusals.total()).isEqualTo(1);
    }

    /**
     * The row filter refuses the expression at the pre-4.0 versions of the mode, before its bytes; outside the mode the
     * expression is read as before, by the deserializer registered under its id, which here has none: an assertion.
     */
    @Test
    public void theRowFilterRefusesTheExpressionInTheModeOnly() throws Throwable
    {
        TableMetadata rlac = Schema.instance.getTableMetadata("test_ks", "rlac");
        // as the captured reads carry it: the unused flag, one expression, its kind, the id and the value
        byte[] filter = Hex.hexToBytes("00" + "01" + "04" + "00000000" + Hex.bytesToHex(VERSION_2_VALUE));
        try (DataInputBuffer in = new DataInputBuffer(filter))
        {
            assertThatThrownBy(() -> RowFilter.serializer.deserialize(in, VERSION_3014, rlac, false))
            .isInstanceOf(DseRefusedMessageException.class)
            .hasMessage("Received a row filter with a user expression (kind 4) at messaging version 11 on " +
                        "test_ks.rlac: the expression a DSE 5.1 node sends for a read of a table restricted to rows " +
                        "(RESTRICT ROWS), row-level access control this software cannot apply, so the read is " +
                        "refused rather than answered without it; do not run RESTRICT ROWS during the upgrade, and " +
                        "run UNRESTRICT ROWS on the DSE 5.1 nodes for the tables restricted since (never for a view: " +
                        "on DSE 5.1.51 it fails and keeps the node from starting)");
        }
        assertThat(DseCompatRefusals.count(DseCompat.REFUSED_DSE51_ROW_LEVEL_ACCESS_CONTROL)).isEqualTo(1);

        // outside the mode the id is looked up as before: one that no expression type is registered with (the class
        // registers its test expression, which takes the first ids) fails as it did
        byte[] unregistered = Hex.hexToBytes("00" + "01" + "04" + "7fffffff" + Hex.bytesToHex(VERSION_2_VALUE));
        for (InternodeCompatibilityMode mode : List.of(InternodeCompatibilityMode.NONE,
                                                       InternodeCompatibilityMode.DSE_6))
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(mode);
            try (DataInputBuffer in = new DataInputBuffer(unregistered))
            {
                assertThatThrownBy(() -> RowFilter.serializer.deserialize(in, VERSION_3014, rlac, false))
                .as("%s", mode)
                .isInstanceOf(AssertionError.class)
                .hasMessage("No user defined expression type registered with id " + Integer.MAX_VALUE);
            }
        }
        assertThat(DseCompatRefusals.total()).isEqualTo(1);
    }

    /**
     * No user expression is sent to a DSE 5.1 node, which would read it with the deserializer it registered under the
     * id, the one of its row-level access control for the id 0: the serializer refuses it at the pre-4.0 versions of
     * the mode, before writing anything; at a version of this software, or outside the mode, it is written as before.
     */
    @Test
    public void noUserExpressionIsSentToADse51Node() throws Throwable
    {
        TableMetadata rlac = Schema.instance.getTableMetadata("test_ks", "rlac");
        RowFilter.Builder builder = RowFilter.builder(false);
        builder.addUserExpression(new TestUserExpression(rlac));
        RowFilter filter = builder.build();
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            assertThatThrownBy(() -> RowFilter.serializer.serialize(filter, out, VERSION_3014))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("A user expression (" + TestUserExpression.class.getName() + ") cannot be sent at messaging " +
                        "version 11 in internode_compatibility_mode DSE_5_1: a DSE 5.1 node would read it as one of " +
                        "its own expressions");
            assertThat(indexOf(out.toByteArray(), new byte[]{ 4 })).isNegative();
        }
        for (InternodeCompatibilityMode mode : InternodeCompatibilityMode.values())
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(mode);
            int version = mode.isDse51() ? MessagingService.current_version : VERSION_3014;
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                RowFilter.serializer.serialize(filter, out, version);
                assertThat(out.getLength()).as("%s", mode)
                .isEqualTo(RowFilter.serializer.serializedSize(filter, version));
            }
        }
    }

    /**
     * The captured {@code read} with the value of its expression replaced by {@code value}, its payload size updated.
     */
    private static byte[] withValue(byte[] read, byte[] value) throws IOException
    {
        int at = indexOf(read, VERSION_2_VALUE);
        assertThat(at).isPositive();
        assertThat(indexOf(Arrays.copyOfRange(read, at + 1, read.length), VERSION_2_VALUE)).isNegative();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        bytes.write(read, 0, at);
        bytes.write(value);
        bytes.write(read, at + VERSION_2_VALUE.length, read.length - at - VERSION_2_VALUE.length);
        byte[] changed = bytes.toByteArray();
        // the 4-byte payload size just before the payload
        int payloadSize = payloadSize(read);
        int sizeAt = read.length - payloadSize - 4;
        ByteBuffer.wrap(changed).putInt(sizeAt, payloadSize - VERSION_2_VALUE.length + value.length);
        assertThat(payloadSize(changed)).isEqualTo(payloadSize - VERSION_2_VALUE.length + value.length);
        return changed;
    }

    /**
     * The size of the payload a pre-4.0 message ends with, read from the header: the magic, the id and the creation
     * time, the sender with its 1-byte length, the verb, the params (each a name with its 2-byte length and a value
     * with its 4-byte length), then the 4-byte payload size.
     */
    private static int payloadSize(byte[] message)
    {
        ByteBuffer buf = ByteBuffer.wrap(message);
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
            assertThat(in.readInt()).as("payload size").isZero();
            assertThat(in.available()).isZero();
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

    private static int indexOf(byte[] bytes, byte[] part)
    {
        for (int i = 0; i + part.length <= bytes.length; i++)
        {
            if (Arrays.equals(bytes, i, i + part.length, part, 0, part.length))
                return i;
        }
        return -1;
    }

    private static String hex(String text)
    {
        return ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(text));
    }

    /** A user expression of this node, registered for the test; only its framing is written. */
    private static final class TestUserExpression extends RowFilter.UserExpression
    {
        TestUserExpression(TableMetadata metadata)
        {
            super(metadata.getColumn(ByteBufferUtil.bytes("v")), Operator.EQ, ByteBufferUtil.bytes("x"));
        }

        @Override
        public boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row, long nowInSec)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toCQLString(boolean redact)
        {
            return "v = 'x'";
        }

        @Override
        protected void serialize(DataOutputPlus out, int version) throws IOException
        {
            out.writeByte(0);
        }

        @Override
        protected long serializedSize(int version)
        {
            return 1;
        }
    }
}
