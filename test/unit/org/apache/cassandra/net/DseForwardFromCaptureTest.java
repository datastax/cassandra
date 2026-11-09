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
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Hex;

import static org.apache.cassandra.net.MessagingService.VERSION_3014;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Golden bytes of the pre-4.0 {@code FWD_FRM} param (messaging version 11, the DSE 6 dialect), copied verbatim from the
 * packets captured on a DSE 6.9.26 test cluster of two data centers, three nodes each, while the three nodes of
 * {@code dc2} ran this software: the packets seen on nodes 5 and 6, numbered per connection below (node 4 saw the
 * same 32 messages, byte for byte).
 * <p>
 * The DSE nodes 192.168.57.11, .12 and .13 of {@code dc1} coordinate the writes; in {@code dc2} a forwarder sends each
 * write on to the other replicas of its data center with the coordinator's address in {@code FWD_FRM}, a raw IPv4
 * address of 4 bytes with no port. The 64 captured packets are 32 messages, each a whole mutation in one packet, in two
 * layouts:
 * <ul>
 * <li>the DSE forwarder 192.168.57.14 to the upgraded nodes .15 and .16 (16 messages): its own address in the header,
 * two params, {@code FWD_FRM} then {@code CAL_BAC};</li>
 * <li>the upgraded forwarders .15 and .16 to 192.168.57.14 (16 messages): the coordinator's address in the header, one
 * param, {@code FWD_FRM}. The upgraded nodes ran a build that did not yet read every pre-4.0 param within its declared
 * length: reading the padded {@code FWD_TO} of the coordinator's message ({@link DseForwardToCaptureTest}), the
 * forwarder lost the {@code CAL_BAC} after it, so it had no {@code CALL_BACK_ON_FAILURE} to pass on.</li>
 * </ul>
 * The payloads are canary writes of 73 or 74 bytes (the text value has 13 or 14 characters) to two tables. A mutation
 * names its table by id only; the configuration of the cluster's canary writers maps the partition keys to the
 * tables: the default canary writes {@code lab.canary} from 2000000000 (table {@code c4834b13-...}, at
 * {@code LOCAL_QUORUM} during the capture), and the instances {@code quorum} and
 * {@code each-quorum} write {@code paxos_serial.canary} from 2010000000 and 2020000000 (table {@code ee149ec0-...}). The
 * run ids that start the text values ({@code 71220f81} for the first table, {@code 5e5819c7} and {@code 8ac7685f} for
 * the second) are drawn by each canary run and only read off the captured values.
 */
public class DseForwardFromCaptureTest
{
    /** The DSE forwarder of {@code dc2}, the sender of {@link #DSE_FORWARDER}. */
    private static final String DSE_FORWARDER_ADDRESS = "192.168.57.14";

    /**
     * Seen on node 5, message {@code 192.168.57.14 -> 192.168.57.15:7000 #1}: the
     * DSE forwarder 192.168.57.14 sends 192.168.57.15 (message id 228708) the write of the coordinator 192.168.57.11.
     * Public for {@code MutationVerbHandlerForwardingTest}, which hands it to the verb handler.
     */
    public static final String DSE_FORWARDER_MUTATION =
        "ca552dfa00037d643bd7278604c0a8390e000000000000000200074657445f46" +
        "524d00000004c0a8390b000743414c5f42414300000001000000004901ee149e" +
        "c0c80011f191f6dffc55650c1a0477ce2bf510fd3d871297d42c000002016e01" +
        "760124000663616e617279000800000000000020aa080d35653538313963373a" +
        "3833363201";

    /**
     * Seen on node 5, message {@code 192.168.57.15 -> 192.168.57.14:7000 #4}: the
     * upgraded forwarder 192.168.57.15 sends 192.168.57.14 the write of the DSE coordinator 192.168.57.11, the copy of
     * {@link DseForwardToCaptureTest#DSE_COORDINATOR_MUTATION} for that target: the id DSE listed for it in FWD_TO (228717),
     * and the creation time and payload of the coordinator's message. Public for {@code MutationVerbHandlerForwardingTest},
     * which forwards that message again.
     */
    public static final String UPGRADED_FORWARDER_MUTATION =
        "ca552dfa00037d6d3bd728d904c0a8390b000000000000000100074657445f46" +
        "524d00000004c0a8390b0000004a01c4834b13c80011f1a25361ddf55403e504" +
        "7735950610fd3d87129d1058000002016e01760124000663616e617279000800" +
        "00000000003e39080e37313232306638313a313539323901";

    /** Where the creation time of a pre-4.0 message starts, after the magic and the id. */
    private static final int CREATED_AT_OFFSET = DseForwardToCaptureTest.CREATED_AT_OFFSET;
    /**
     * Where the address (an IPv4 one, as in all the captured messages), the verb and the param count of a pre-4.0 message
     * start. Public for {@code MutationVerbHandlerForwardingTest}, which compares the header it forwards with the captured
     * one.
     */
    public static final int ADDRESS_OFFSET = CREATED_AT_OFFSET + 4;
    public static final int VERB_OFFSET = ADDRESS_OFFSET + 1 + 4;
    public static final int PARAM_COUNT_OFFSET = VERB_OFFSET + 4;
    /** Where the {@code FWD_FRM} value starts: after the param count, the name (2 + 7) and the length. */
    private static final int FWD_FRM_VALUE_OFFSET = PARAM_COUNT_OFFSET + 4 + 2 + 7 + 4;
    /** The end of the {@code FWD_FRM} param, which is the end of the params of the upgraded forwarder's message. */
    public static final int FWD_FRM_END = FWD_FRM_VALUE_OFFSET + 4;
    /** The end of the params of the DSE forwarder's message: {@code CAL_BAC} after {@code FWD_FRM}. */
    public static final int DSE_FORWARDER_PARAMS_END = FWD_FRM_END + 2 + 7 + 4 + 1;

    private static final TableId DEFAULT_CANARY = TableId.fromString("c4834b13-c800-11f1-a253-61ddf55403e5");
    private static final TableId PAXOS_SERIAL_CANARY = TableId.fromString("ee149ec0-c800-11f1-91f6-dffc55650c1a");

    /**
     * The messages of the DSE forwarder: from each coordinator to each upgraded target, both tables and both payload
     * sizes. Every one of the 16 messages of this layout differs from these only in its id, creation time and payload.
     */
    private static final List<Captured> DSE_FORWARDER = ImmutableList.of(
        new Captured("node 5, 192.168.57.14 -> 192.168.57.15:7000 #1", DSE_FORWARDER_ADDRESS,
                     DSE_FORWARDER_MUTATION,
                     228708, "192.168.57.11", PAXOS_SERIAL_CANARY, 2010000373, "5e5819c7:8362", 8362),
        new Captured("node 5, 192.168.57.14 -> 192.168.57.15:7000 #3", DSE_FORWARDER_ADDRESS,
                     "ca552dfa00027cb43bd7295404c0a8390e000000000000000200074657445f46" +
                     "524d00000004c0a8390c000743414c5f42414300000001000000004a01c4834b" +
                     "13c80011f1a25361ddf55403e5047735941d10fd3d87129ee45e000002016e01" +
                     "760124000663616e61727900080000000000003e3b080e37313232306638313a" +
                     "313539333101",
                     162996, "192.168.57.12", DEFAULT_CANARY, 2000000029, "71220f81:15931", 15931),
        new Captured("node 5, 192.168.57.14 -> 192.168.57.15:7000 #2", DSE_FORWARDER_ADDRESS,
                     "ca552dfa000271033bd7287604c0a8390e000000000000000200074657445f46" +
                     "524d00000004c0a8390d000743414c5f42414300000001000000004901ee149e" +
                     "c0c80011f191f6dffc55650c1a0477ce2c6b10fd3d87129b8364000002016e01" +
                     "760124000663616e617279000800000000000020ac080d35653538313963373a" +
                     "3833363401",
                     160003, "192.168.57.13", PAXOS_SERIAL_CANARY, 2010000491, "5e5819c7:8364", 8364),
        new Captured("node 6, 192.168.57.14 -> 192.168.57.16:7000 #6", DSE_FORWARDER_ADDRESS,
                     "ca552dfa00037dc63bd72c4b04c0a8390e000000000000000200074657445f46" +
                     "524d00000004c0a8390b000743414c5f42414300000001000000004a01c4834b" +
                     "13c80011f1a25361ddf55403e504773594d310fd3d8712aa8031000002016e01" +
                     "760124000663616e61727900080000000000003e48080e37313232306638313a" +
                     "313539343401",
                     228806, "192.168.57.11", DEFAULT_CANARY, 2000000211, "71220f81:15944", 15944),
        new Captured("node 6, 192.168.57.14 -> 192.168.57.16:7000 #5", DSE_FORWARDER_ADDRESS,
                     "ca552dfa00027ce33bd72aed04c0a8390e000000000000000200074657445f46" +
                     "524d00000004c0a8390c000743414c5f42414300000001000000004a01c4834b" +
                     "13c80011f1a25361ddf55403e5047735950310fd3d8712a520ae000002016e01" +
                     "760124000663616e61727900080000000000003e42080e37313232306638313a" +
                     "313539333801",
                     163043, "192.168.57.12", DEFAULT_CANARY, 2000000259, "71220f81:15938", 15938),
        new Captured("node 6, 192.168.57.14 -> 192.168.57.16:7000 #8", DSE_FORWARDER_ADDRESS,
                     "ca552dfa000271843bd72df904c0a8390e000000000000000200074657445f46" +
                     "524d00000004c0a8390d000743414c5f42414300000001000000004901ee149e" +
                     "c0c80011f191f6dffc55650c1a047866c26c10fd3d8712b10a56000002016e01" +
                     "760124000663616e61727900080000000000001fb5080d38616337363835663a" +
                     "3831313701",
                     160132, "192.168.57.13", PAXOS_SERIAL_CANARY, 2020000364, "8ac7685f:8117", 8117));

    /**
     * The messages of the upgraded forwarders, written by this software before the params were read within their
     * declared length: from each coordinator, from both forwarders, both tables and both payload sizes.
     */
    private static final List<Captured> UPGRADED_FORWARDER = ImmutableList.of(
        new Captured("node 5, 192.168.57.15 -> 192.168.57.14:7000 #4", "192.168.57.15",
                     UPGRADED_FORWARDER_MUTATION,
                     228717, "192.168.57.11", DEFAULT_CANARY, 2000000262, "71220f81:15929", 15929),
        new Captured("node 6, 192.168.57.16 -> 192.168.57.14:7000 #1", "192.168.57.16",
                     "ca552dfa00027c683bd7280904c0a8390c000000000000000100074657445f46" +
                     "524d00000004c0a8390c0000004901ee149ec0c80011f191f6dffc55650c1a04" +
                     "77ce2ab510fd3d871299dfbc000002016e01760124000663616e617279000800" +
                     "000000000020ab080d35653538313963373a3833363301",
                     162920, "192.168.57.12", PAXOS_SERIAL_CANARY, 2010000053, "5e5819c7:8363", 8363),
        new Captured("node 5, 192.168.57.15 -> 192.168.57.14:7000 #2", "192.168.57.15",
                     "ca552dfa000270f23bd7278504c0a8390d000000000000000100074657445f46" +
                     "524d00000004c0a8390d0000004901ee149ec0c80011f191f6dffc55650c1a04" +
                     "7866c2ce10fd3d871297e057000002016e01760124000663616e617279000800" +
                     "00000000001fa6080d38616337363835663a3831303201",
                     159986, "192.168.57.13", PAXOS_SERIAL_CANARY, 2020000462, "8ac7685f:8102", 8102));

    private static InternodeCompatibilityMode modeBefore;
    private static TableMetadata defaultCanary;
    private static TableMetadata paxosSerialCanary;

    @BeforeClass
    public static void defineSchema()
    {
        SchemaLoader.prepareServer();
        defaultCanary = canary("lab", DEFAULT_CANARY);
        paxosSerialCanary = canary("paxos_serial", PAXOS_SERIAL_CANARY);
        SchemaLoader.createKeyspace("lab", KeyspaceParams.simple(1), defaultCanary);
        SchemaLoader.createKeyspace("paxos_serial", KeyspaceParams.simple(1), paxosSerialCanary);
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
    }

    @AfterClass
    public static void restoreMode()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
    }

    /**
     * The table of the cluster's canaries, with the id of the capture: {@code lab.canary} and
     * {@code paxos_serial.canary} were created on that cluster with these columns.
     */
    private static TableMetadata canary(String keyspace, TableId id)
    {
        return TableMetadata.builder(keyspace, "canary")
                            .id(id)
                            .addPartitionKeyColumn("id", Int32Type.instance)
                            .addClusteringColumn("ck", UTF8Type.instance)
                            .addRegularColumn("v", UTF8Type.instance)
                            .addRegularColumn("n", LongType.instance)
                            .build();
    }

    /** The layout of the DSE forwarder's messages: its address in the header, then FWD_FRM (4 bytes) and CAL_BAC. */
    @Test
    public void dseForwarderLayout() throws Exception
    {
        for (Captured captured : DSE_FORWARDER)
        {
            byte[] packet = captured.bytes();
            ByteBuffer buf = ByteBuffer.wrap(packet);
            assertThat(Arrays.copyOfRange(packet, ADDRESS_OFFSET, VERB_OFFSET)).as("address in the header of %s", captured)
                                                                              .isEqualTo(addressField(captured.sender));
            assertThat(buf.getInt(PARAM_COUNT_OFFSET)).as("param count of %s", captured).isEqualTo(2);
            assertThat(Arrays.copyOfRange(packet, PARAM_COUNT_OFFSET + 4, FWD_FRM_VALUE_OFFSET)).as("FWD_FRM name and length of %s", captured)
                                                                                                .isEqualTo(paramNameAndLength("FWD_FRM", 4));
            assertThat(Arrays.copyOfRange(packet, FWD_FRM_VALUE_OFFSET, FWD_FRM_END)).as("FWD_FRM value of %s", captured)
                                                                                     .isEqualTo(InetAddressAndPort.getByName(captured.coordinator).addressBytes);
            assertThat(Arrays.copyOfRange(packet, FWD_FRM_END, DSE_FORWARDER_PARAMS_END - 1)).as("CAL_BAC name and length of %s", captured)
                                                                                             .isEqualTo(paramNameAndLength("CAL_BAC", 1));
            assertThat(packet[DSE_FORWARDER_PARAMS_END - 1]).as("CAL_BAC value of %s", captured).isZero();
            assertThat(buf.getInt(DSE_FORWARDER_PARAMS_END)).as("payload size of %s", captured)
                                                           .isEqualTo(packet.length - DSE_FORWARDER_PARAMS_END - 4);
        }
    }

    /**
     * The messages of the DSE forwarder, framed and read as the inbound path does: the size from the lengths, the
     * header, then the whole message, whose payload is the canary write, read to the last byte of the message.
     */
    @Test
    public void dseForwarderMessages() throws Exception
    {
        for (Captured captured : DSE_FORWARDER)
        {
            Message<Mutation> message = assertMessage(captured);
            assertThat(message.callBackOnFailure()).as("CAL_BAC, the param after FWD_FRM, of %s", captured).isTrue();
        }
    }

    /**
     * The {@code FWD_FRM} value is an address without a port: the reader gives it this node's {@code storage_port}, which
     * is the coordinator's port only when every node listens on the same one, as all six of the captured cluster do, on 7000.
     */
    @Test
    public void forwardFromCarriesNoPort() throws Exception
    {
        byte[] packet = Hex.hexToBytes(DSE_FORWARDER_MUTATION);
        Message.Header header = Message.serializer.extractHeader(ByteBuffer.wrap(packet), InetAddressAndPort.getByName(DSE_FORWARDER_ADDRESS),
                                                                 approxTime.now(), VERSION_3014);
        assertThat(header.respondTo().getHostAddress(false)).as("address of FWD_FRM").isEqualTo("192.168.57.11");
        assertThat(header.respondTo().getPort()).as("port of FWD_FRM").isEqualTo(DatabaseDescriptor.getStoragePort());
    }

    /**
     * This software writes the message of the DSE forwarder, for the same write, id and sender, byte for byte, but for
     * the creation time, which this software sets when it builds a message: the header, FWD_FRM then CAL_BAC, and the
     * mutation.
     */
    @Test
    public void dseForwarderMessagesWrittenByThisSoftware() throws Exception
    {
        for (Captured captured : DSE_FORWARDER)
        {
            byte[] packet = captured.bytes();
            Message<Mutation> received = deserialize(packet, captured.sender);
            Message<Mutation> message = Message.builder(Verb.MUTATION_REQ, received.payload)
                                               .withId(captured.id)
                                               .from(InetAddressAndPort.getByName(captured.sender))
                                               .withFlag(MessageFlag.CALL_BACK_ON_FAILURE)
                                               .withParam(ParamType.RESPOND_TO, InetAddressAndPort.getByName(captured.coordinator))
                                               .build();
            assertThat(message.serializedSize(VERSION_3014)).as("serialized size of %s", captured).isEqualTo(packet.length);
            byte[] written = serialize(message);
            assertThat(Arrays.copyOfRange(written, 0, CREATED_AT_OFFSET)).as("magic and id of %s", captured)
                                                                           .isEqualTo(Arrays.copyOfRange(packet, 0, CREATED_AT_OFFSET));
            assertThat(Arrays.copyOfRange(written, ADDRESS_OFFSET, written.length)).as("all after the creation time of %s", captured)
                                                                                  .isEqualTo(Arrays.copyOfRange(packet, ADDRESS_OFFSET, packet.length));
        }
    }

    /** The FWD_FRM value this software writes at version 11 is the raw address, as DSE's. */
    @Test
    public void forwardFromValueBytes() throws Exception
    {
        byte[] packet = Hex.hexToBytes(DSE_FORWARDER_MUTATION);
        InetAddressAndPort coordinator = InetAddressAndPort.getByName("192.168.57.11");
        assertThat(InetAddressAndPort.FwdFrmSerializer.fwdFrmSerializer.serializedSize(coordinator, VERSION_3014)).as("serialized size").isEqualTo(4);
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            InetAddressAndPort.FwdFrmSerializer.fwdFrmSerializer.serialize(coordinator, out, VERSION_3014);
            assertThat(out.toByteArray()).as("FWD_FRM value").isEqualTo(Arrays.copyOfRange(packet, FWD_FRM_VALUE_OFFSET, FWD_FRM_END));
        }
    }

    /**
     * The layout of the upgraded forwarders' messages, as captured: the coordinator's address in the header, then
     * FWD_FRM alone. They are read as the DSE forwarder's are; they carry no {@code CALL_BACK_ON_FAILURE}.
     */
    @Test
    public void upgradedForwarderMessages() throws Exception
    {
        for (Captured captured : UPGRADED_FORWARDER)
        {
            byte[] packet = captured.bytes();
            ByteBuffer buf = ByteBuffer.wrap(packet);
            assertThat(Arrays.copyOfRange(packet, ADDRESS_OFFSET, VERB_OFFSET)).as("address in the header of %s", captured)
                                                                              .isEqualTo(addressField(captured.coordinator));
            assertThat(buf.getInt(PARAM_COUNT_OFFSET)).as("param count of %s", captured).isEqualTo(1);
            assertThat(Arrays.copyOfRange(packet, PARAM_COUNT_OFFSET + 4, FWD_FRM_VALUE_OFFSET)).as("FWD_FRM name and length of %s", captured)
                                                                                                .isEqualTo(paramNameAndLength("FWD_FRM", 4));
            assertThat(Arrays.copyOfRange(packet, FWD_FRM_VALUE_OFFSET, FWD_FRM_END)).as("FWD_FRM value of %s", captured)
                                                                                     .isEqualTo(InetAddressAndPort.getByName(captured.coordinator).addressBytes);
            assertThat(buf.getInt(FWD_FRM_END)).as("payload size of %s", captured).isEqualTo(packet.length - FWD_FRM_END - 4);

            Message<Mutation> message = assertMessage(captured);
            assertThat(message.callBackOnFailure()).as("CAL_BAC of %s", captured).isFalse();
        }
    }

    /** Mode NONE has no pre-4.0 dialect: the captured message is refused. */
    @Test
    public void refusedInModeNone()
    {
        byte[] packet = Hex.hexToBytes(DSE_FORWARDER_MUTATION);
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        try
        {
            assertThatThrownBy(() -> Message.serializer.inferMessageSize(ByteBuffer.wrap(packet), 0, packet.length, VERSION_3014))
            .isInstanceOf(AssertionError.class)
            .hasMessage("pre-4.0 serialization version 11");
            assertThatThrownBy(() -> deserialize(packet, DSE_FORWARDER_ADDRESS))
            .isInstanceOf(AssertionError.class)
            .hasMessage("pre-4.0 serialization version 11");
        }
        finally
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        }
    }

    /**
     * Frames and reads one captured message from its sender as {@link InboundMessageHandler} does, then reads it whole:
     * the header, with the coordinator in FWD_FRM, and the canary write it carries, which ends where the message ends.
     */
    private static Message<Mutation> assertMessage(Captured captured) throws Exception
    {
        byte[] packet = captured.bytes();
        InetAddressAndPort peer = InetAddressAndPort.getByName(captured.sender);
        InetAddressAndPort coordinator = InetAddressAndPort.getByName(captured.coordinator);
        ByteBuffer buf = ByteBuffer.wrap(packet);
        assertThat(Message.serializer.inferMessageSize(buf, 0, packet.length, VERSION_3014)).as("message size of %s", captured).isEqualTo(packet.length);

        Message.Header header = Message.serializer.extractHeader(buf, peer, approxTime.now(), VERSION_3014);
        assertThat(header.id).as("id of %s", captured).isEqualTo(captured.id);
        assertThat(header.verb).as("verb of %s", captured).isEqualTo(Verb.MUTATION_REQ);
        assertThat(header.from).as("sender of %s", captured).isEqualTo(peer);
        assertThat(header.respondTo()).as("FWD_FRM of %s", captured).isEqualTo(coordinator);
        assertThat(header.forwardTo()).as("FWD_TO of %s", captured).isNull();

        Message<Mutation> message;
        try (DataInputBuffer in = new DataInputBuffer(packet))
        {
            message = Message.serializer.deserialize(in, header, VERSION_3014);
            assertThat(in.available()).as("bytes left after %s", captured).isZero();
        }
        assertThat(message.callBackOnFailure()).as("CAL_BAC of %s, from the header and from the message", captured).isEqualTo(header.callBackOnFailure());
        try (DataInputBuffer in = new DataInputBuffer(packet))
        {
            Message<Mutation> whole = Message.serializer.deserialize(in, peer, VERSION_3014);
            assertThat(whole.from()).as("sender of %s, read with the message", captured).isEqualTo(peer);
            assertThat(whole.respondTo()).as("FWD_FRM of %s, read with the message", captured).isEqualTo(coordinator);
            assertThat(in.available()).as("bytes left after %s, read with its header", captured).isZero();
        }

        Mutation mutation = message.payload;
        TableMetadata table = captured.table.equals(DEFAULT_CANARY) ? defaultCanary : paxosSerialCanary;
        assertThat(mutation.getKeyspaceName()).as("keyspace of %s", captured).isEqualTo(table.keyspace);
        assertThat(mutation.getPartitionUpdates()).as("tables of %s", captured).hasSize(1);
        PartitionUpdate update = mutation.getPartitionUpdate(table);
        assertThat(update).as("update of %s.%s in %s", table.keyspace, table.name, captured).isNotNull();
        assertThat(Int32Type.instance.compose(mutation.key().getKey())).as("id of %s", captured).isEqualTo(captured.key);
        assertThat(update.rowCount()).as("rows of %s", captured).isEqualTo(1);
        Row row = update.rowIterator().next();
        assertThat(UTF8Type.instance.compose(row.clustering().bufferAt(0))).as("ck of %s", captured).isEqualTo("canary");
        assertThat(UTF8Type.instance.compose(row.getCell(table.getColumn(ByteBufferUtil.bytes("v"))).buffer())).as("v of %s", captured).isEqualTo(captured.v);
        assertThat(LongType.instance.compose(row.getCell(table.getColumn(ByteBufferUtil.bytes("n"))).buffer())).as("n of %s", captured).isEqualTo(captured.n);
        return message;
    }

    private static Message<Mutation> deserialize(byte[] packet, String sender) throws Exception
    {
        try (DataInputBuffer in = new DataInputBuffer(packet))
        {
            return Message.serializer.deserialize(in, InetAddressAndPort.getByName(sender), VERSION_3014);
        }
    }

    private static byte[] serialize(Message<?> message) throws Exception
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            Message.serializer.serialize(message, out, VERSION_3014);
            return out.toByteArray();
        }
    }

    /** The name and the declared length of a pre-4.0 param, as they precede its value. */
    private static byte[] paramNameAndLength(String name, int length) throws Exception
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.writeUTF(name);
            out.writeInt(length);
            return out.toByteArray();
        }
    }

    /** The pre-4.0 address field of a header: the length of the address, then the address. */
    private static byte[] addressField(String address) throws Exception
    {
        byte[] bytes = InetAddressAndPort.getByName(address).addressBytes;
        byte[] field = new byte[1 + bytes.length];
        field[0] = (byte) bytes.length;
        System.arraycopy(bytes, 0, field, 1, bytes.length);
        return field;
    }

    /** A captured message, where it was seen, its sender, and what its bytes say. */
    private static final class Captured
    {
        final String record;
        final String sender;
        final String hex;
        final long id;
        final String coordinator;
        final TableId table;
        final int key;
        final String v;
        final long n;

        Captured(String record, String sender, String hex, long id, String coordinator, TableId table, int key, String v, long n)
        {
            this.record = record;
            this.sender = sender;
            this.hex = hex;
            this.id = id;
            this.coordinator = coordinator;
            this.table = table;
            this.key = key;
            this.v = v;
            this.n = n;
        }

        byte[] bytes()
        {
            return Hex.hexToBytes(hex);
        }

        @Override
        public String toString()
        {
            return record;
        }
    }
}
