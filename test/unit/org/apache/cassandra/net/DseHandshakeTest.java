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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.read.ListAppender;
import ch.qos.logback.core.spi.FilterReply;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoop;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.concurrent.Future; // checkstyle: permit this import
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.dsecompat.DseFixtures.DseVersion;
import org.apache.cassandra.dsecompat.DseFixtures.Handshake;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.GossipMessages;
import org.apache.cassandra.gms.HeartBeatState;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.HandshakeProtocol.Accept;
import org.apache.cassandra.net.HandshakeProtocol.ConfirmOutboundPre40;
import org.apache.cassandra.net.HandshakeProtocol.Initiate;
import org.apache.cassandra.net.OutboundConnectionInitiator.Result;
import org.apache.cassandra.net.OutboundConnectionInitiator.Result.MessagingSuccess;
import org.apache.cassandra.net.OutboundConnectionInitiator.SslFallbackConnectionType;
import org.apache.cassandra.net.OutboundConnectionSettings.Framing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

import static org.apache.cassandra.locator.InetAddressAndPort.Serializer.inetAddressAndPortSerializer;
import static org.apache.cassandra.net.ConnectionType.SMALL_MESSAGES;
import static org.apache.cassandra.net.MessagingService.VERSION_30;
import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.apache.cassandra.net.OutboundConnectionInitiator.initiateMessaging;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.STRING;

/**
 * The pre-4.0 handshake of the DSE 6 compatibility mode, against the bytes real DSE nodes exchange
 * (the DSE fixtures), against a fake DSE storage port that answers as DSE's own code does, and over
 * real sockets: DSE's first message is answered with the 3.0.14 dialect and the third message completes
 * the handshake; our first message to a known DSE peer is the pre-4.0 one; a DSE reply to our 4.0-style
 * first message makes us reconnect at the dialect. With the mode off, every pre-4.0 byte sequence is
 * rejected exactly as before.
 */
public class DseHandshakeTest
{
    private static final SocketFactory factory = new SocketFactory();
    /** What a DSE 6.9.7+ node answers when it overrides the requested version with its own: dse(6), relative 6 << 8. */
    private static final int DSE_697_HANDSHAKE_VERSION = 6 << 8;
    /** Flags bits 0-7 of a small-messages connection: category 2, framing CRC (id 2, bit 4). */
    private static final int SMALL_CRC_FLAGS = 0x12;
    /** DSE's first message for a streaming connection. */
    private static final byte[] DSE_FLAGGED_STREAMING = { (byte) 0xCA, 0x55, 0x2D, (byte) 0xFA, 3, 0, (byte) 0xFF, 8 };

    private InternodeCompatibilityMode modeBefore;
    private int minClusterVersionBefore;

    @BeforeClass
    public static void startup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @AfterClass
    public static void cleanup() throws InterruptedException
    {
        factory.shutdownNow();
    }

    @Before
    public void saveMode()
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        // resetting a version does not recompute the minimum when the map becomes empty: restored after each test
        minClusterVersionBefore = MessagingService.instance().versions.minClusterVersion;
    }

    @After
    public void restoreMode()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
        DsePeers.reset();
        MessagingService.instance().versions.minClusterVersion = minClusterVersionBefore;
    }

    private static void dse6()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
    }

    private static void none()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
    }

    private static byte[] bytes(ByteBuf buf)
    {
        byte[] out = new byte[buf.readableBytes()];
        buf.getBytes(buf.readerIndex(), out);
        buf.release();
        return out;
    }

    private static byte[] concat(byte[] a, byte[] b)
    {
        byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    /** The complete 4.0-style first message: magic, flags, our address in the 4.0 layout, CRC of all that. */
    private static byte[] expected40StyleInitiate(int flags, InetAddressAndPort from) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.writeInt(0xCA552DFA);
            out.writeInt(flags);
            out.writeByte(6);
            out.write(from.getAddress().getAddress());
            out.writeShort(from.getPort());
            ByteBuffer body = out.asNewBuffer();
            out.writeInt(Crc.computeCrc32(body, 0, body.limit()));
            return out.toByteArray();
        }
    }

    // ---- message layouts, against the captured bytes ----

    @Test
    public void dseFirstMessageIsDecodedOnlyInDse6Mode() throws Exception
    {
        for (DseVersion version : DseVersion.values())
        {
            for (Handshake captured : DseFixtures.handshakes(version))
            {
                // the first message is followed on the wire by nothing until we answer; add bytes to prove the reader
                // stops after the 8 bytes of the pre-4.0 message
                ByteBuf buf = Unpooled.wrappedBuffer(concat(captured.msg1, new byte[]{ 1, 2, 3 }));
                dse6();
                Initiate initiate = Initiate.maybeDecode(buf);
                assertThat(initiate).as(version + " msg1 in DSE_6").isNotNull();
                assertThat(initiate.isPre40()).isTrue();
                assertThat(initiate.requestMessagingVersion).isEqualTo(DseCompat.DSE_VERSION_BYTE);
                assertThat(initiate.type).isEqualTo(ConnectionType.LEGACY_MESSAGES);
                assertThat(initiate.framing).isEqualTo(Framing.UNPROTECTED);
                assertThat(initiate.from).isNull();
                assertThat(buf.readerIndex()).as("exactly the 8 bytes of the pre-4.0 first message are consumed").isEqualTo(8);

                none();
                ByteBuf again = Unpooled.wrappedBuffer(captured.msg1);
                assertThat(Initiate.maybeDecode(again)).as(version + " msg1 in NONE").isNull();
                assertThat(again.readerIndex()).isEqualTo(0);
            }
        }
    }

    @Test
    public void ourPre40FirstMessageRoundTrips() throws Exception
    {
        dse6();
        Initiate sent = new Initiate(DseCompat.DSE_DIALECT, null, SMALL_MESSAGES, Framing.UNPROTECTED, null);
        byte[] encoded = bytes(sent.encode());
        // magic, then flags: version 11 in bits 8-15, small messages (2) in bits 0-1, no framing bits, no min/max
        assertThat(encoded).containsExactly(0xCA, 0x55, 0x2D, 0xFA, 0x00, 0x00, 0x0B, 0x02);

        Initiate decoded = Initiate.maybeDecode(Unpooled.wrappedBuffer(encoded));
        assertThat(decoded).isEqualTo(sent);
        assertThat(decoded.isPre40()).isTrue();
        assertThat(decoded.requestMessagingVersion).isEqualTo(DseCompat.DSE_DIALECT);

        none();
        assertThat(Initiate.maybeDecode(Unpooled.wrappedBuffer(encoded))).isNull();
    }

    /** DSE's first message with compression (CMP bit, bit 2) selects the legacy LZ4 stream; the streaming bit is kept. */
    @Test
    public void dseFlaggedFirstMessageFlags() throws Exception
    {
        dse6();
        Initiate compressed = Initiate.maybeDecode(Unpooled.wrappedBuffer(new byte[]{ (byte) 0xCA, 0x55, 0x2D, (byte) 0xFA, 6, 0, (byte) 0xFF, 4 }));
        assertThat(compressed.isPre40()).isTrue();
        assertThat(compressed.framing).isEqualTo(Framing.LZ4);
        assertThat(compressed.type).isEqualTo(ConnectionType.LEGACY_MESSAGES);
        Initiate streaming = Initiate.maybeDecode(Unpooled.wrappedBuffer(DSE_FLAGGED_STREAMING));
        assertThat(streaming.isPre40()).isTrue();
        assertThat(streaming.type).isEqualTo(ConnectionType.STREAMING);
    }

    /** Plain pre-4.0 first messages at 10, 11 and 12 (DSE once it has stored OSS_40 for us) are pre-4.0 only in DSE_6 mode. */
    @Test
    public void plainFirstMessagesUpTo12AreDecodedOnlyInDse6Mode() throws Exception
    {
        for (int version = VERSION_30; version <= VERSION_40; version++)
        {
            byte[] encoded = { (byte) 0xCA, 0x55, 0x2D, (byte) 0xFA, 0, 0, (byte) version, 2 };
            dse6();
            Initiate decoded = Initiate.maybeDecode(Unpooled.wrappedBuffer(encoded));
            assertThat(decoded.isPre40()).as("version " + version).isTrue();
            assertThat(decoded.requestMessagingVersion).isEqualTo(version);
            none();
            assertThat(Initiate.maybeDecode(Unpooled.wrappedBuffer(encoded))).as("version " + version).isNull();
        }
    }

    /** The encryption pre-check looks a pre-4.0 peer up by IP address and default storage port, not its ephemeral source port. */
    @Test
    public void pre40EncryptionCheckUsesTheDefaultStoragePort() throws Exception
    {
        InetSocketAddress remote = new InetSocketAddress(InetAddressAndPort.getByName("127.0.0.80").getAddress(), 54321);
        InetAddressAndPort checked = InboundConnectionInitiator.Handler.pre40EncryptionCheckAddress(remote);
        assertThat(checked.getAddress()).isEqualTo(remote.getAddress());
        assertThat(checked.getPort()).isEqualTo(DatabaseDescriptor.getStoragePort());
    }

    /**
     * Where the encryption options require encryption for the peer, an unencrypted pre-4.0 connection is refused: on the
     * socket's IP address before we answer the first message (no second message, the channel closed, nothing
     * recorded), and on the broadcast address the third message names, after our answer.
     */
    @Test
    public void unencryptedPre40ConnectionsAreRefusedWhereEncryptionIsRequired() throws Exception
    {
        dse6();
        InetAddressAndPort namedPeer = InetAddressAndPort.getByAddress(InetAddressAndPort.getByName("127.0.0.13").getAddress());
        byte[] third = bytes(new ConfirmOutboundPre40(DseCompat.DSE_DIALECT, namedPeer).encode());

        // every peer must encrypt: refused before any answer
        Refused before = refusedWithEncryptionFor(peer -> true, plainFirstMessage(DseCompat.DSE_DIALECT), third);
        assertThat(before.replies).isEmpty();
        assertThat(before.open).isFalse();
        assertThat(before.warnings).containsExactly("peer " + ABANDONING_PEER + " attempted to establish an unencrypted pre-4.0 connection");
        assertThat(MessagingService.instance().versions.knows(namedPeer)).isFalse();

        // only the peer the third message names must encrypt: answered, then refused on the third message
        Refused after = refusedWithEncryptionFor(namedPeer::equals, plainFirstMessage(DseCompat.DSE_DIALECT), third);
        assertThat(after.replies).hasSize(1);
        assertThat(after.replies.get(0)).containsExactly(0, 0, 0, DseCompat.DSE_DIALECT);
        assertThat(after.open).isFalse();
        assertThat(after.warnings).containsExactly("peer " + ABANDONING_PEER + " attempted to establish an unencrypted connection (broadcast address " + namedPeer + ')');
        assertThat(MessagingService.instance().versions.knows(namedPeer)).isFalse();

        // neither must: the handshake completes
        Refused accepted = refusedWithEncryptionFor(peer -> false, plainFirstMessage(DseCompat.DSE_DIALECT), third);
        try
        {
            assertThat(accepted.replies).hasSize(1);
            assertThat(accepted.replies.get(0)).containsExactly(0, 0, 0, DseCompat.DSE_DIALECT);
            assertThat(accepted.open).isTrue();
            assertThat(accepted.warnings).isEmpty();
            assertThat(MessagingService.instance().versions.getRaw(namedPeer)).isEqualTo(DseCompat.DSE_DIALECT);
        }
        finally
        {
            MessagingService.instance().versions.reset(namedPeer);
            // the established connection created the inbound handlers of the peer; its channel is closed by now
            MessagingService.instance().removeInbound(namedPeer);
        }
    }

    /**
     * The third message of a pre-4.0 handshake names an address of 4 or 16 bytes, without a port: any other size (the
     * 4.0 layout with a port, or garbage) closes the connection with a warning, and nothing is recorded; a truncated
     * message waits for the rest.
     */
    @Test
    public void thirdMessageWithAnAddressOfAnotherSizeIsRefused() throws Exception
    {
        dse6();
        for (int size : new int[]{ 0, 6, 7, 18, 255 })
        {
            ByteBuffer third = ByteBuffer.allocate(5 + size);
            third.putInt(DseCompat.DSE_DIALECT).put((byte) size);
            Refused refused = refusedWithEncryptionFor(peer -> false, plainFirstMessage(DseCompat.DSE_DIALECT), third.array());
            assertThat(refused.replies).as("size " + size).hasSize(1);
            assertThat(refused.open).as("size " + size).isFalse();
            assertThat(refused.warnings).containsExactly("peer " + ABANDONING_PEER + " sent a third message of the pre-4.0 handshake with an address of " +
                                                         size + " bytes, neither IPv4 nor IPv6; closing");
        }

        // a truncated message: not complete yet, nothing decided
        byte[] whole = bytes(new ConfirmOutboundPre40(DseCompat.DSE_DIALECT, InetAddressAndPort.getByName("127.0.0.14")).encode());
        assertThat(ConfirmOutboundPre40.maybeDecode(Unpooled.wrappedBuffer(Arrays.copyOf(whole, whole.length - 1)))).isNull();
        assertThat(ConfirmOutboundPre40.maybeDecode(Unpooled.wrappedBuffer(Arrays.copyOf(whole, 4)))).isNull();
    }

    /** What our inbound handler did with a pre-4.0 handshake, refused or not. */
    private static final class Refused
    {
        final List<byte[]> replies = new ArrayList<>();
        final List<String> warnings = new ArrayList<>();
        boolean open;
    }

    /** Feeds the messages to our inbound handler, with encryption options that require encryption for the given peers. */
    private static Refused refusedWithEncryptionFor(Predicate<InetAddressAndPort> mustEncrypt, byte[]... received)
    {
        ServerEncryptionOptions encryption = new ServerEncryptionOptions()
        {
            @Override
            public boolean shouldEncrypt(InetAddressAndPort endpoint)
            {
                return mustEncrypt.test(endpoint);
            }
        };
        InboundConnectionSettings settings = new InboundConnectionSettings().withBindAddress(FBUtilities.getLocalAddressAndPort())
                                                                            .withEncryption(encryption)
                                                                            .withDefaults();
        Logger initiatorLogger = (Logger) LoggerFactory.getLogger(InboundConnectionInitiator.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        initiatorLogger.addAppender(appender);
        InboundChannel channel = new InboundChannel();
        Refused result = new Refused();
        try
        {
            channel.pipeline().addLast("handshake", new InboundConnectionInitiator.Handler(settings));
            for (byte[] bytes : received)
            {
                // a refused connection is closed: what the peer sends after that never reaches the handler
                if (!channel.isOpen())
                    break;
                channel.writeInbound(Unpooled.wrappedBuffer(bytes));
            }
            ByteBuf reply;
            while ((reply = channel.readOutbound()) != null)
                result.replies.add(bytes(reply));
            result.open = channel.isOpen();
            for (ILoggingEvent event : appender.list)
                if (event.getLevel() == Level.WARN && event.getFormattedMessage().contains(ABANDONING_PEER.toString()))
                    result.warnings.add(event.getFormattedMessage());
        }
        finally
        {
            channel.finishAndReleaseAll();
            initiatorLogger.detachAppender(appender);
        }
        return result;
    }

    /** In DSE_6 mode the 4.0-style first message carries the requested version in the legacy byte; with NONE, min as before. */
    @Test
    public void our40StyleFirstMessageCarriesTheRequestedVersionOnlyInDse6Mode() throws Exception
    {
        InetAddressAndPort from = InetAddressAndPort.getByName("127.0.0.1:7012");
        AcceptVersions accept = new AcceptVersions(VERSION_40, current_version);

        dse6();
        Initiate dse = new Initiate(current_version, accept, SMALL_MESSAGES, Framing.CRC, from);
        byte[] dseBytes = bytes(dse.encode());
        assertThat(dseBytes).containsExactly(expected40StyleInitiate(SMALL_CRC_FLAGS | current_version << 8 | VERSION_40 << 16 | current_version << 24, from));
        // what a 4.0+ node of this software makes of it: the same message, the legacy byte kept
        Initiate decoded = Initiate.maybeDecode(Unpooled.wrappedBuffer(dseBytes));
        assertThat(decoded).isEqualTo(dse);
        assertThat(decoded.acceptVersions).isEqualTo(accept);
        none();
        assertThat(Initiate.maybeDecode(Unpooled.wrappedBuffer(dseBytes))).isEqualTo(dse);

        // NONE: the byte is acceptVersions.min, exactly as before this mode existed
        Initiate plain = new Initiate(accept, SMALL_MESSAGES, Framing.CRC, from);
        assertThat(bytes(plain.encode())).containsExactly(expected40StyleInitiate(SMALL_CRC_FLAGS | VERSION_40 << 8 | VERSION_40 << 16 | current_version << 24, from));
    }

    @Test
    public void ourSecondMessageToDseIsTheDialectOnly()
    {
        byte[] reply = bytes(Accept.respondPre40(DseCompat.DSE_DIALECT, Unpooled.buffer(4).alloc()));
        assertThat(reply).containsExactly(0, 0, 0, DseCompat.DSE_DIALECT);
    }

    @Test
    public void thirdMessageOfDseIsDecodedAsVersionAndAddress() throws Exception
    {
        dse6(); // the pre-4.0 address layout is read in DSE 6 compatibility mode only
        for (DseVersion version : DseVersion.values())
        {
            for (Handshake captured : DseFixtures.handshakes(version))
            {
                ByteBuf buf = Unpooled.wrappedBuffer(captured.msg3);
                ConfirmOutboundPre40 confirm = ConfirmOutboundPre40.maybeDecode(buf);
                assertThat(confirm).isNotNull();
                // DSE to DSE: its own version, relative << 8; the address is the connecting node, without port
                assertThat(confirm.maxMessagingVersion).isEqualTo(version.relativeMessagingVersion << 8);
                assertThat(confirm.from.getAddress().getHostAddress()).isEqualTo(captured.client);
                // the reader stops after the address: DSE's parameters block is left untouched
                int consumed = 4 + 1 + confirm.from.getAddress().getAddress().length;
                assertThat(buf.readerIndex()).isEqualTo(consumed);
                assertThat(captured.msg3.length).as("the capture holds a parameters block after the address").isGreaterThan(consumed);
            }
        }
        // what we send: the dialect and our address, 9 bytes for IPv4, decodable by the same reader
        InetAddressAndPort us = InetAddressAndPort.getByName("192.168.57.21:7000");
        byte[] ours = bytes(new ConfirmOutboundPre40(DseCompat.DSE_DIALECT, us).encode());
        assertThat(ours).containsExactly(0, 0, 0, 11, 4, 192, 168, 57, 21);
        ConfirmOutboundPre40 decoded = ConfirmOutboundPre40.maybeDecode(Unpooled.wrappedBuffer(ours));
        assertThat(decoded.maxMessagingVersion).isEqualTo(DseCompat.DSE_DIALECT);
        assertThat(decoded.from.getAddress()).isEqualTo(us.getAddress());
    }

    @Test
    public void dseSecondMessageToOur40StyleFirstMessageMeansReconnectAtTheDialect() throws Exception
    {
        byte[] hcdReply = { 0, 0, 0, 11 };  // a peer already at the dialect

        dse6();
        for (DseVersion version : DseVersion.values())
        {
            for (Handshake captured : DseFixtures.handshakes(version))
            {
                // the captured second message: relative << 8
                assertThat(ByteBuffer.wrap(captured.msg2).getInt()).isEqualTo(version.relativeMessagingVersion << 8);
                Accept accept = Accept.maybeDecode(Unpooled.wrappedBuffer(captured.msg2), current_version);
                assertThat(accept).isNotNull();
                assertThat(accept.useMessagingVersion).isEqualTo(0);
                assertThat(accept.maxMessagingVersion).isEqualTo(DseCompat.DSE_DIALECT);

                none();
                assertThat(Accept.maybeDecode(Unpooled.wrappedBuffer(captured.msg2), current_version)).as("4 bytes are 'not yet' with NONE").isNull();
                dse6();
            }
        }
        Accept fromHcd = Accept.maybeDecode(Unpooled.wrappedBuffer(hcdReply), current_version);
        assertThat(fromHcd.useMessagingVersion).isEqualTo(0);
        assertThat(fromHcd.maxMessagingVersion).isEqualTo(DseCompat.DSE_DIALECT);
        // and to our pre-4.0 first message, the peer echoes the dialect
        Accept echoed = Accept.maybeDecode(Unpooled.wrappedBuffer(hcdReply), DseCompat.DSE_DIALECT);
        assertThat(echoed.useMessagingVersion).isEqualTo(0);
        assertThat(echoed.maxMessagingVersion).isEqualTo(DseCompat.DSE_DIALECT);

        none();
        assertThat(Accept.maybeDecode(Unpooled.wrappedBuffer(hcdReply), current_version)).isNull();
    }

    /** A real 12-byte 4.0-style Accept is decoded the same way in both modes, whatever its first int (12 or more). */
    @Test
    public void fortyStyleAcceptIsUnchangedInDse6Mode() throws Exception
    {
        for (int peerMax : new int[]{ VERSION_40, MessagingService.VERSION_50, current_version })
        {
            byte[] encoded = bytes(new Accept(VERSION_40, peerMax).encode(Unpooled.buffer(12).alloc()));
            assertThat(encoded).hasSize(12);
            for (InternodeCompatibilityMode mode : InternodeCompatibilityMode.values())
            {
                DatabaseDescriptor.setInternodeCompatibilityMode(mode);
                // only 4 bytes so far: a 4.0 reply is not complete yet in either mode
                assertThat(Accept.maybeDecode(Unpooled.wrappedBuffer(encoded, 0, 4), current_version)).as(mode + " " + peerMax).isNull();
                ByteBuf buf = Unpooled.wrappedBuffer(encoded);
                Accept accept = Accept.maybeDecode(buf, current_version);
                assertThat(accept.useMessagingVersion).as(mode + " " + peerMax).isEqualTo(VERSION_40);
                assertThat(accept.maxMessagingVersion).isEqualTo(peerMax);
                assertThat(buf.readerIndex()).isEqualTo(12);
            }
        }
    }

    /** The pre-4.0 address layout (size 4 or 16, no port) is read only in DSE_6 mode; with NONE it is refused as before. */
    @Test
    public void pre40AddressLayoutOnlyInDse6Mode() throws Exception
    {
        byte[] pre40 = { 4, 10, 0, 0, 7 };
        dse6();
        try (DataInputBuffer in = new DataInputBuffer(pre40))
        {
            assertThat(inetAddressAndPortSerializer.deserialize(in, VERSION_30).getAddress().getHostAddress()).isEqualTo("10.0.0.7");
        }
        assertThat(inetAddressAndPortSerializer.extract(ByteBuffer.wrap(pre40), 0).getAddress().getHostAddress()).isEqualTo("10.0.0.7");

        none();
        try (DataInputBuffer in = new DataInputBuffer(pre40))
        {
            assertThatThrownBy(() -> inetAddressAndPortSerializer.deserialize(in, VERSION_40))
            .isInstanceOf(AssertionError.class).hasMessage("pre-4.0 serialization size 4");
        }
        assertThatThrownBy(() -> inetAddressAndPortSerializer.extract(ByteBuffer.wrap(pre40), 0))
        .isInstanceOf(AssertionError.class).hasMessage("Unexpected pre-4.0 InetAddressAndPort size 4");
    }

    // ---- over real sockets, against our own inbound side ----

    private static InboundSockets openInbound() throws Exception
    {
        InboundSockets inbound = new InboundSockets(new InboundConnectionSettings().withAcceptMessaging(null));
        inbound.open();
        return inbound;
    }

    private static InetAddressAndPort bindAddress(InboundSockets inbound)
    {
        return inbound.sockets().stream().map(s -> s.settings.bindAddress).findFirst().get();
    }

    private static void awaitKnown(InetAddressAndPort peer)
    {
        Util.spinAssertEquals("pipeline set up for the DSE peer", true, () -> MessagingService.instance().versions.knows(peer), 10, TimeUnit.SECONDS);
    }

    /**
     * A peer sending DSE's flagged first message connects to us: that first message, our 4-byte answer, its third message,
     * the connection is established at 11, as with DSE 6.9.26 (DSE 6.8.65 reconnects at 11 instead of sending the third
     * message).
     */
    @Test
    public void dseNodeConnectsToUs() throws Exception
    {
        dse6();
        InboundSockets inbound = openInbound();
        // the third message carries the address only: the peer is recorded with the default storage port
        InetAddressAndPort dsePeer = InetAddressAndPort.getByAddress(InetAddressAndPort.getByName("127.0.0.77").getAddress());
        CountDownLatch delivered = new CountDownLatch(1);
        Predicate<Message<?>> sink = message -> {
            if (message.verb() != Verb.ECHO_REQ || !message.from().equals(dsePeer))
                return true;
            delivered.countDown();
            return false;
        };
        MessagingService.instance().inboundSink.add(sink);
        try
        {
            InetAddressAndPort endpoint = bindAddress(inbound);
            Handshake captured = DseFixtures.handshakes(DseVersion.DSE_6_9_26).get(0);
            try (Socket socket = new Socket(endpoint.getAddress(), endpoint.getPort()))
            {
                socket.setSoTimeout(10_000);
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                DataInputStream in = new DataInputStream(socket.getInputStream());
                out.write(captured.msg1); // CA 55 2D FA 06 00 FF 00
                out.flush();
                byte[] reply = new byte[4];
                in.readFully(reply);
                assertThat(reply).containsExactly(0, 0, 0, DseCompat.DSE_DIALECT);
                // a peer that confirms at the version we answered after a flagged first message, with its address, as
                // DSE 6.9.26 does; not what DSE 6.8.65 does, which closes this socket and reconnects at 11 (see
                // InboundConnectionInitiator.Handler.isAbandonedByDse)
                // DSE writes its first message right after the third message: one read can carry both, and the bytes
                // after the third message must reach the messaging pipeline
                byte[] third = bytes(new ConfirmOutboundPre40(DseCompat.DSE_DIALECT, dsePeer).encode());
                out.write(concat(third, echoAt11(dsePeer)));
                out.flush();
                awaitKnown(dsePeer);
                assertThat(MessagingService.instance().versions.getRaw(dsePeer)).isEqualTo(DseCompat.DSE_DIALECT);
                assertThat(delivered.await(10, TimeUnit.SECONDS)).as("the message sent with the third message").isTrue();
            }
        }
        finally
        {
            MessagingService.instance().inboundSink.remove(sink);
            MessagingService.instance().versions.reset(dsePeer);
            inbound.close().await(1L, TimeUnit.SECONDS);
        }
    }

    /** An echo request from {@code from}, serialized at the dialect as DSE writes it. */
    private static byte[] echoAt11(InetAddressAndPort from) throws IOException
    {
        Message<NoPayload> echo = Message.builder(Verb.ECHO_REQ, noPayload).from(from).build();
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            Message.serializer.serialize(echo, out, DseCompat.DSE_DIALECT);
            return out.toByteArray();
        }
    }

    /** A pre-4.0 handshake with our inbound side: the given first message, our answer, then the third message naming {@code peer}. */
    private static void pre40HandshakeFrom(InetAddressAndPort endpoint, byte[] firstMessage, InetAddressAndPort peer) throws Exception
    {
        try (Socket socket = new Socket(endpoint.getAddress(), endpoint.getPort()))
        {
            socket.setSoTimeout(10_000);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            out.write(firstMessage);
            out.flush();
            assertThat(new DataInputStream(socket.getInputStream()).readInt()).isEqualTo(DseCompat.DSE_DIALECT);
            out.write(bytes(new ConfirmOutboundPre40(DseCompat.DSE_DIALECT, peer).encode()));
            out.flush();
            awaitKnown(peer);
        }
        MessagingService.instance().versions.reset(peer);
    }

    private static byte[] plainFirstMessage(int version)
    {
        return new byte[]{ (byte) 0xCA, 0x55, 0x2D, (byte) 0xFA, 0, 0, (byte) version, 2 };
    }

    /**
     * The first message tells a DSE node from HCD 1.2 at the dialect, for the ordinals of gossip: DSE's flagged one and a
     * pre-4.0 one requesting 12 (DSE once it knows our NET_VERSION) identify the peer when a third message follows on the
     * same socket, a plain one at 11 does not. A DSE node identified once stays identified when it reconnects with a plain
     * message at 11, which it sends once it has stored 11 for us. DSE 6.9.26 sends that third message after a request at
     * 12 or 255; DSE 6.8.65 never does (it closes the socket and reconnects at 11,
     * {@link #dseNodeReconnectingAt11IsIdentifiedByGossip}).
     */
    @Test
    public void inboundFirstMessageIdentifiesDseNodes() throws Exception
    {
        dse6();
        InboundSockets inbound = openInbound();
        InetAddressAndPort flagged = InetAddressAndPort.getByAddress(InetAddressAndPort.getByName("127.0.0.81").getAddress());
        InetAddressAndPort requesting12 = InetAddressAndPort.getByAddress(InetAddressAndPort.getByName("127.0.0.82").getAddress());
        InetAddressAndPort hcd = InetAddressAndPort.getByAddress(InetAddressAndPort.getByName("127.0.0.83").getAddress());
        try
        {
            InetAddressAndPort endpoint = bindAddress(inbound);
            pre40HandshakeFrom(endpoint, DseFixtures.handshakes(DseVersion.DSE_6_9_26).get(0).msg1, flagged);
            pre40HandshakeFrom(endpoint, plainFirstMessage(VERSION_40), requesting12);
            pre40HandshakeFrom(endpoint, plainFirstMessage(DseCompat.DSE_DIALECT), hcd);
            assertThat(DsePeers.isDse(flagged)).isTrue();
            assertThat(DsePeers.isDse(requesting12)).isTrue();
            assertThat(DsePeers.isDse(hcd)).isFalse();

            // the DSE node reconnects with a plain first message at 11
            pre40HandshakeFrom(endpoint, plainFirstMessage(DseCompat.DSE_DIALECT), flagged);
            assertThat(DsePeers.isDse(flagged)).isTrue();
        }
        finally
        {
            inbound.close().await(1L, TimeUnit.SECONDS);
        }
    }

    /**
     * What DSE 6.8.65 does: its flagged first message, our answer 11, and the socket closed without a third
     * message; then a new socket with a plain first message at 11, which completes. The handshakes identify nothing (the
     * second looks exactly like HCD 1.2's); the DSE version the node gossips as its NET_VERSION identifies it, and the
     * identification survives its next reconnection at 11.
     */
    @Test
    public void dseNodeReconnectingAt11IsIdentifiedByGossip() throws Exception
    {
        dse6();
        InboundSockets inbound = openInbound();
        InetAddressAndPort dseNode = InetAddressAndPort.getByAddress(InetAddressAndPort.getByName("127.0.0.84").getAddress());
        try
        {
            InetAddressAndPort endpoint = bindAddress(inbound);
            try (Socket socket = new Socket(endpoint.getAddress(), endpoint.getPort()))
            {
                socket.setSoTimeout(10_000);
                socket.getOutputStream().write(flaggedMsg1(DseVersion.DSE_6_8_65));
                socket.getOutputStream().flush();
                assertThat(new DataInputStream(socket.getInputStream()).readInt()).isEqualTo(DseCompat.DSE_DIALECT);
            }
            pre40HandshakeFrom(endpoint, plainFirstMessage(DseCompat.DSE_DIALECT), dseNode);
            assertThat(DsePeers.snapshot()).as("identified by the handshakes").isEmpty();

            // its first gossip message at the dialect carries its own state, with its DSE version as its NET_VERSION
            EndpointState state = new EndpointState(new HeartBeatState(1, 1));
            state.addApplicationState(ApplicationState.NET_VERSION,
                                      VersionedValue.unsafeMakeVersionedValue(String.valueOf(DseVersion.DSE_6_8_65.relativeMessagingVersion << 8), 1));
            GossipMessages.receivedAck2(dseNode, Collections.singletonMap(dseNode, state), DseCompat.DSE_DIALECT);
            assertThat(DsePeers.isDse(dseNode)).as("identified by its gossiped NET_VERSION").isTrue();

            pre40HandshakeFrom(endpoint, plainFirstMessage(DseCompat.DSE_DIALECT), dseNode);
            assertThat(DsePeers.isDse(dseNode)).as("still identified after reconnecting at 11").isTrue();
        }
        finally
        {
            inbound.close().await(1L, TimeUnit.SECONDS);
        }
    }

    /** A plain pre-4.0 first message at 10, 11 or 12 is answered with 11; with the mode off, 12 gets no answer. */
    @Test
    public void plainFirstMessagesUpTo12AreAnsweredWithTheDialect() throws Exception
    {
        for (int version = VERSION_30; version <= VERSION_40; version++)
        {
            dse6();
            InboundSockets inbound = openInbound();
            try
            {
                InetAddressAndPort endpoint = bindAddress(inbound);
                try (Socket socket = new Socket(endpoint.getAddress(), endpoint.getPort()))
                {
                    socket.setSoTimeout(10_000);
                    socket.getOutputStream().write(new byte[]{ (byte) 0xCA, 0x55, 0x2D, (byte) 0xFA, 0, 0, (byte) version, 2 });
                    socket.getOutputStream().flush();
                    assertThat(new DataInputStream(socket.getInputStream()).readInt()).as("version " + version).isEqualTo(DseCompat.DSE_DIALECT);
                }
            }
            finally
            {
                inbound.close().await(1L, TimeUnit.SECONDS);
            }
        }

        none();
        InboundSockets inbound = openInbound();
        try
        {
            InetAddressAndPort endpoint = bindAddress(inbound);
            try (Socket socket = new Socket(endpoint.getAddress(), endpoint.getPort()))
            {
                socket.setSoTimeout(2_000);
                socket.getOutputStream().write(new byte[]{ (byte) 0xCA, 0x55, 0x2D, (byte) 0xFA, 0, 0, (byte) VERSION_40, 2 });
                socket.getOutputStream().flush();
                assertThatThrownBy(() -> socket.getInputStream().read()).isInstanceOf(SocketTimeoutException.class);
            }
        }
        finally
        {
            inbound.close().await(1L, TimeUnit.SECONDS);
        }
    }

    /** A third message at any version other than the one we answered (11) closes the connection; the peer is not recorded. */
    @Test
    public void thirdMessageAtAnotherVersionIsRefused() throws Exception
    {
        dse6();
        InetAddressAndPort dsePeer = InetAddressAndPort.getByAddress(InetAddressAndPort.getByName("127.0.0.78").getAddress());
        Handshake captured = DseFixtures.handshakes(DseVersion.DSE_6_9_26).get(0);
        for (int confirmed : new int[]{ VERSION_30, DSE_697_HANDSHAKE_VERSION })
        {
            InboundSockets inbound = openInbound();
            try
            {
                InetAddressAndPort endpoint = bindAddress(inbound);
                try (Socket socket = new Socket(endpoint.getAddress(), endpoint.getPort()))
                {
                    socket.setSoTimeout(10_000);
                    DataInputStream in = new DataInputStream(socket.getInputStream());
                    socket.getOutputStream().write(captured.msg1);
                    socket.getOutputStream().flush();
                    assertThat(in.readInt()).isEqualTo(DseCompat.DSE_DIALECT);
                    socket.getOutputStream().write(bytes(new ConfirmOutboundPre40(confirmed, dsePeer).encode()));
                    socket.getOutputStream().flush();
                    assertThat(in.read()).as("closed after a third message at " + confirmed).isEqualTo(-1);
                }
                assertThat(MessagingService.instance().versions.knows(dsePeer)).isFalse();
            }
            finally
            {
                MessagingService.instance().versions.reset(dsePeer);
                inbound.close().await(1L, TimeUnit.SECONDS);
            }
        }
    }

    /** A pre-4.0 first message below 3.0 is closed without an answer, as DSE itself does. */
    @Test
    public void obsoleteVersionIsClosedWithoutAnAnswer() throws Exception
    {
        dse6();
        InboundSockets inbound = openInbound();
        try
        {
            InetAddressAndPort endpoint = bindAddress(inbound);
            Initiate obsolete = new Initiate(VERSION_30 - 1, null, SMALL_MESSAGES, Framing.UNPROTECTED, null);
            try (Socket socket = new Socket(endpoint.getAddress(), endpoint.getPort()))
            {
                socket.setSoTimeout(10_000);
                socket.getOutputStream().write(bytes(obsolete.encode()));
                socket.getOutputStream().flush();
                assertThat(socket.getInputStream().read()).as("closed without a second message").isEqualTo(-1);
            }
        }
        finally
        {
            inbound.close().await(1L, TimeUnit.SECONDS);
        }
    }

    /**
     * The same first message with the mode off gets no answer at all: the inbound handler keeps waiting for a 4.0-style
     * message that never decodes, even once DSE's third message follows, until the handshake timeout closes the socket
     * (60 seconds here, not waited for); the peer is never recorded.
     */
    @Test
    public void dseNodeIsRefusedWithTheModeOff() throws Exception
    {
        none();
        InboundSockets inbound = openInbound();
        InetAddressAndPort dsePeer = InetAddressAndPort.getByAddress(InetAddressAndPort.getByName("127.0.0.79").getAddress());
        try
        {
            InetAddressAndPort endpoint = bindAddress(inbound);
            Handshake captured = DseFixtures.handshakes(DseVersion.DSE_6_9_26).get(0);
            try (Socket socket = new Socket(endpoint.getAddress(), endpoint.getPort()))
            {
                socket.setSoTimeout(2_000);
                socket.getOutputStream().write(captured.msg1);
                socket.getOutputStream().flush();
                assertThatThrownBy(() -> socket.getInputStream().read()).as("no second message, socket still open")
                                                                        .isInstanceOf(SocketTimeoutException.class);
                socket.getOutputStream().write(new byte[]{ 0, 0, 0, 11, 4, 127, 0, 0, 79 }); // DSE's third message at 11
                socket.getOutputStream().flush();
                assertThatThrownBy(() -> socket.getInputStream().read()).as("still silent after a third message")
                                                                        .isInstanceOf(SocketTimeoutException.class);
                assertThat(MessagingService.instance().versions.knows(dsePeer)).isFalse();
            }
        }
        finally
        {
            inbound.close().await(1L, TimeUnit.SECONDS);
        }
    }

    /** We connect to a peer we know speaks the dialect: pre-4.0 first message, 4-byte answer, third message, success at 11. */
    @Test
    public void weConnectToADseNodeAtTheDialect() throws Exception
    {
        dse6();
        InboundSockets inbound = openInbound();
        try
        {
            InetAddressAndPort endpoint = bindAddress(inbound);
            Result<MessagingSuccess> result = connect(endpoint, DseCompat.DSE_DIALECT).get(20, TimeUnit.SECONDS);
            assertThat(result.outcome).isEqualTo(Result.Outcome.SUCCESS);
            assertThat(result.success().messagingVersion).isEqualTo(DseCompat.DSE_DIALECT);
            result.success().channel.close();
        }
        finally
        {
            inbound.close().await(1L, TimeUnit.SECONDS);
        }
    }

    /** The 4.0-style handshake between two nodes of this software is unchanged by the mode. */
    @Test
    public void fortyStyleHandshakeUnchangedInDse6Mode() throws Exception
    {
        dse6();
        InboundSockets inbound = openInbound();
        try
        {
            InetAddressAndPort endpoint = bindAddress(inbound);
            Result<MessagingSuccess> result = connect(endpoint, current_version).get(20, TimeUnit.SECONDS);
            assertThat(result.outcome).isEqualTo(Result.Outcome.SUCCESS);
            assertThat(result.success().messagingVersion).isEqualTo(current_version);
            result.success().channel.close();
        }
        finally
        {
            inbound.close().await(1L, TimeUnit.SECONDS);
        }
    }

    @Test
    public void streamingFromADsePeerIsRefused() throws Exception
    {
        dse6();
        InboundSockets inbound = openInbound();
        try
        {
            InetAddressAndPort endpoint = bindAddress(inbound);
            // pre-4.0 first messages with the streaming bit (bit 3) set: plain at 11, and DSE's own
            byte[] plain = bytes(new Initiate(DseCompat.DSE_DIALECT, null, ConnectionType.STREAMING, Framing.UNPROTECTED, null).encode());
            for (byte[] streaming : new byte[][]{ plain, DSE_FLAGGED_STREAMING })
            {
                try (Socket socket = new Socket(endpoint.getAddress(), endpoint.getPort()))
                {
                    socket.setSoTimeout(10_000);
                    socket.getOutputStream().write(streaming);
                    socket.getOutputStream().flush();
                    assertThat(socket.getInputStream().read()).as("closed without a reply").isEqualTo(-1);
                }
            }
        }
        finally
        {
            inbound.close().await(1L, TimeUnit.SECONDS);
        }
    }

    // ---- over real sockets, against a fake DSE 6.9.7+ storage port ----

    private static OutboundConnectionSettings settings(InetAddressAndPort endpoint)
    {
        return new OutboundConnectionSettings(endpoint).withFraming(Framing.CRC).withDefaults(ConnectionCategory.MESSAGING);
    }

    /** Connects as {@link OutboundConnection} does, requesting {@code requestMessagingVersion}. */
    private static Future<Result<MessagingSuccess>> connect(InetAddressAndPort endpoint, int requestMessagingVersion)
    {
        EventLoop eventLoop = factory.defaultGroup().next();
        return initiateMessaging(eventLoop, SMALL_MESSAGES, SslFallbackConnectionType.SERVER_CONFIG, settings(endpoint),
                                 requestMessagingVersion, AsyncPromise.withExecutor(eventLoop));
    }

    /**
     * The storage port of a DSE 6.9.7+ node, answering the first message of a non-DSE peer as {@code handleStart}
     * does: the version byte (bits 8-15 of the flags) below 10 is closed without an answer, 10 to 12 are
     * echoed, anything higher is overridden with DSE's own version (1536), and so is another DSE node's first message.
     * A pre-4.0 first message answered with its own version, or with a DSE version that maps to it (a fixed answer: DSE
     * 6.8.65 answers 1024 to a request at 11), is followed by our third message, which is recorded. DSE
     * reads only the 8 bytes of magic and flags as the first message; here the rest of a 4.0-style one (address and
     * CRC, which DSE would take for a third message) is read too, so the test can assert the whole message.
     * <p>
     * A socket whose handshake completed stays open until {@link #close}, as an established connection to DSE does, and
     * the server socket stops listening once the last connection is accepted. Closing the established socket at once
     * would race with the message the test enqueued: if the socket closed before the message reached it, the
     * connection would reconnect into a server socket that still listens but accepts no more (the TCP connection
     * completes in its backlog), and that handshake would wait for an answer that never comes, longer than the test's
     * 30 s wait for {@code OutboundConnection.close}.
     */
    private static final class FakeDse implements AutoCloseable
    {
        final ServerSocket server;
        final Integer fixedAnswer;
        final List<byte[]> firstMessages = new CopyOnWriteArrayList<>();
        final List<Integer> answers = new CopyOnWriteArrayList<>();
        final List<byte[]> thirdMessages = new CopyOnWriteArrayList<>();
        /** The sockets whose handshake completed, closed by {@link #close}. */
        final List<Socket> established = new CopyOnWriteArrayList<>();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread thread;
        /** Run once the first connection's first message is read, before it is answered; null for nothing. */
        volatile Runnable beforeFirstAnswer;
        /** Run before every answer, after {@link #beforeFirstAnswer}; null for nothing. */
        volatile Runnable beforeEachAnswer;
        /** When each answer was written, in {@code nanoTime}. */
        final List<Long> answeredAtNanos = new CopyOnWriteArrayList<>();
        /**
         * Set by {@link #close} before it closes the server socket: any exception the thread raises from then on (the
         * accept that closing ends, a read reaching its timeout) is not recorded; {@code close} reports a thread that did
         * not stop instead.
         */
        volatile boolean closed;

        FakeDse(int connections) throws IOException
        {
            this(connections, null);
        }

        /** @param fixedAnswer the 4-byte second message to send whatever the first one says, or null for DSE's own answers */
        FakeDse(int connections, Integer fixedAnswer) throws IOException
        {
            this.server = new ServerSocket(0, connections, InetAddressAndPort.getByName("127.0.0.1").getAddress());
            this.fixedAnswer = fixedAnswer;
            this.thread = new Thread(() -> {
                try
                {
                    for (int i = 0; i < connections; i++)
                    {
                        Socket socket = server.accept();
                        boolean served = false;
                        boolean kept = false;
                        try
                        {
                            if (i == connections - 1)
                                server.close();
                            kept = serve(socket);
                            served = true;
                        }
                        finally
                        {
                            if (kept)
                                established.add(socket);
                            else
                                closeUnkept(socket, served);
                        }
                    }
                }
                catch (Throwable t)
                {
                    if (!closed)
                        failure.compareAndSet(null, t);
                }
            }, "fake-dse");
            thread.start();
        }

        InetAddressAndPort endpoint()
        {
            return InetAddressAndPort.getByAddressOverrideDefaults(server.getInetAddress(), server.getLocalPort());
        }

        /**
         * Closes a socket that is not kept. A failure to close is recorded when the socket was served, and ignored when
         * serving it failed, so that it never hides that failure.
         */
        private void closeUnkept(Socket socket, boolean served)
        {
            try
            {
                socket.close();
            }
            catch (IOException e)
            {
                if (served)
                    failure.compareAndSet(null, e);
            }
        }

        static int dseAnswer(int flags)
        {
            int requested = (flags >>> 8) & 0xFF;
            int min = (flags >>> 16) & 0xFF;
            if (requested == DseCompat.DSE_VERSION_BYTE && min == 0)
                return DSE_697_HANDSHAKE_VERSION;  // another DSE node: dse(relative), answered with the local version
            if (requested < VERSION_30)
                return -1;                          // MessagingVersion.from(oss(n)) is null: close without msg2
            if (requested <= VERSION_40)
                return requested;                   // oss(10), oss(11), oss(12): exact match, echoed
            return DSE_697_HANDSHAKE_VERSION;       // oss(13..254): no exact match, overridden with the local version
        }

        /** @return whether the handshake completed, so that the socket stays open as an established connection */
        private boolean serve(Socket socket) throws IOException
        {
            socket.setSoTimeout(10_000);
            DataInputStream in = new DataInputStream(socket.getInputStream());
            ByteArrayOutputStream first = new ByteArrayOutputStream();
            byte[] head = new byte[8];
            in.readFully(head);
            first.write(head);
            int flags = ByteBuffer.wrap(head, 4, 4).getInt();
            boolean fortyStyle = (flags >>> 16) != 0;
            if (fortyStyle)
            {
                int size = in.readUnsignedByte();
                byte[] rest = new byte[size + 4];
                in.readFully(rest);
                first.write(size);
                first.write(rest);
            }
            firstMessages.add(first.toByteArray());

            int answer = fixedAnswer != null ? fixedAnswer : dseAnswer(flags);
            Runnable hook = beforeFirstAnswer;
            if (hook != null && firstMessages.size() == 1)
                hook.run();
            Runnable eachHook = beforeEachAnswer;
            if (eachHook != null)
                eachHook.run();
            answers.add(answer);
            if (answer < 0)
                return false;
            new DataOutputStream(socket.getOutputStream()).writeInt(answer);
            socket.getOutputStream().flush();
            answeredAtNanos.add(System.nanoTime());

            if (fortyStyle)
                return false; // DSE reads our address and CRC as a third message, fails and closes

            int requested = (flags >>> 8) & 0xFF;
            // the rule of DseCompat.isDseHandshakeVersion written out on purpose: a wrong production rule fails the tests
            if (answer == requested || (requested == DseCompat.DSE_DIALECT && answer >= 256 && answer <= 255 << 8 && (answer & 0xFF) == 0))
            {
                // the requested version, or at the dialect a DSE version (relative << 8, relative 1 to 255), is followed
                // by our third message: version and address in the 3.0 layout
                byte[] version = new byte[4];
                in.readFully(version);
                int size = in.readUnsignedByte();
                byte[] address = new byte[size];
                in.readFully(address);
                thirdMessages.add(concat(concat(version, new byte[]{ (byte) size }), address));
                return true;
            }
            else
            {
                // wait for the client to give up on this socket
                try
                {
                    while (in.read() >= 0) {}
                }
                catch (SocketTimeoutException | EOFException ignore)
                {
                }
                return false;
            }
        }

        @Override
        public void close() throws Exception
        {
            thread.join(15_000);
            // read before the server socket is closed, which would end an accept still waiting for a connection
            boolean stopped = !thread.isAlive();
            closed = true;
            server.close();
            // a thread still serving may add a socket after the loop below, closed by nobody: a second join narrows that
            // window, and the assertion fails the test anyway
            if (!stopped)
                thread.join(15_000);
            for (Socket socket : established)
            {
                try
                {
                    socket.close();
                }
                catch (IOException e)
                {
                    failure.compareAndSet(null, e);
                }
            }
            assertThat(stopped).as("the fake DSE node served its connections within 15 s").isTrue();
            assertThat(failure.get()).isNull();
        }
    }

    /**
     * Our 4.0-style first message to a DSE node we do not know: the legacy byte carries the current version, which DSE
     * overrides with its own (1536), and we retry at the dialect; the retry is then answered by an echo of 11 and our
     * third message completes the handshake.
     */
    @Test
    public void dseAnswerToOur40StyleMessageMakesUsRetryThenConnectAtTheDialect() throws Exception
    {
        dse6();
        try (FakeDse dse = new FakeDse(2))
        {
            OutboundConnectionSettings settings = settings(dse.endpoint());
            // an unknown peer: OutboundConnection requests the current version (EndpointMessagingVersions.get)
            Result<MessagingSuccess> first = connect(dse.endpoint(), current_version).get(20, TimeUnit.SECONDS);
            assertThat(first.outcome).isEqualTo(Result.Outcome.RETRY);
            assertThat(first.retry().withMessagingVersion).isEqualTo(DseCompat.DSE_DIALECT);

            byte[] sent = dse.firstMessages.get(0);
            assertThat(sent).containsExactly(expected40StyleInitiate(SMALL_CRC_FLAGS | current_version << 8 | VERSION_40 << 16 | current_version << 24, settings.from));
            assertThat(sent[6] & 0xFF).as("the version byte DSE decodes").isEqualTo(current_version);
            assertThat(dse.answers.get(0)).isEqualTo(DSE_697_HANDSHAKE_VERSION);

            // the reconnection OutboundConnection makes with the version the retry carries
            Result<MessagingSuccess> second = connect(dse.endpoint(), first.retry().withMessagingVersion).get(20, TimeUnit.SECONDS);
            assertThat(second.outcome).isEqualTo(Result.Outcome.SUCCESS);
            assertThat(second.success().messagingVersion).isEqualTo(DseCompat.DSE_DIALECT);
            assertThat(dse.firstMessages.get(1)).containsExactly(0xCA, 0x55, 0x2D, 0xFA, 0x00, 0x00, 0x0B, SMALL_CRC_FLAGS);
            assertThat(dse.answers.get(1)).isEqualTo(DseCompat.DSE_DIALECT);
            second.success().channel.close();
            // the DSE version in the first answer identified the peer as a DSE node, for the ordinals of gossip
            assertThat(DsePeers.snapshot()).containsEntry(settings.to, "handshake reply with the DSE messaging version " + DSE_697_HANDSHAKE_VERSION);
            dse.thread.join(10_000);
            byte[] address = settings.from.getAddress().getAddress();
            assertThat(dse.thirdMessages).hasSize(1);
            assertThat(dse.thirdMessages.get(0)).containsExactly(concat(new byte[]{ 0, 0, 0, 11, 4 }, address));
        }
    }

    /**
     * A pre-4.0 peer that answers our 4.0-style first message with 11 (HCD 1.2 at the dialect) is retried at the dialect
     * like DSE, but is not identified as a DSE node: gossip with it keeps this software's ordinals; nor is a peer
     * answering 256 or more with a value that is not a DSE version ({@code relative << 8}), which is retried at the
     * dialect too.
     */
    @Test
    public void pre40AnswerWithoutADseVersionDoesNotIdentifyADseNode() throws Exception
    {
        dse6();
        for (int answer : new int[]{ DseCompat.DSE_DIALECT, 300, 1025 })
        {
            DsePeers.reset();
            try (FakeDse peer = new FakeDse(1, answer))
            {
                Result<MessagingSuccess> result = connect(peer.endpoint(), current_version).get(20, TimeUnit.SECONDS);
                assertThat(result.outcome).as("answer " + answer).isEqualTo(Result.Outcome.RETRY);
                assertThat(result.retry().withMessagingVersion).as("answer " + answer).isEqualTo(DseCompat.DSE_DIALECT);
                assertThat(DsePeers.isDse(peer.endpoint())).as("answer " + answer).isFalse();
            }
        }
    }

    /** A peer stored at 12 in DSE_6 mode: the legacy byte is raised to 13, which DSE overrides with 1536, never echoes. */
    @Test
    public void legacyByteIsNever12InDse6Mode() throws Exception
    {
        dse6();
        try (FakeDse dse = new FakeDse(1))
        {
            OutboundConnectionSettings settings = settings(dse.endpoint());
            Result<MessagingSuccess> result = connect(dse.endpoint(), VERSION_40).get(20, TimeUnit.SECONDS);
            assertThat(result.outcome).isEqualTo(Result.Outcome.RETRY);
            assertThat(result.retry().withMessagingVersion).isEqualTo(DseCompat.DSE_DIALECT);
            int version50 = MessagingService.VERSION_50;
            assertThat(dse.firstMessages.get(0)).containsExactly(expected40StyleInitiate(SMALL_CRC_FLAGS | version50 << 8 | VERSION_40 << 16 | current_version << 24, settings.from));
            assertThat(dse.answers.get(0)).isEqualTo(DSE_697_HANDSHAKE_VERSION);
        }
    }

    /**
     * A pre-4.0 first message answered with another version is final: incompatible, never a retry, reported with the
     * version the peer sent, and identifies no DSE node. That includes a DSE version answering a request at 10, since it
     * maps to the dialect (11), and an answer to a request at 11 of 256 or more that is not a DSE version
     * ({@code relative << 8}, relative 1 to 255).
     */
    @Test
    public void pre40RequestAnsweredWithAnotherVersionIsIncompatible() throws Exception
    {
        dse6();
        int[][] requestedAndAnswer = { { DseCompat.DSE_DIALECT, VERSION_30 },
                                       { DseCompat.DSE_DIALECT, VERSION_40 },
                                       { VERSION_30, DseCompat.DSE_681_VERSION },
                                       { DseCompat.DSE_DIALECT, 300 },
                                       { DseCompat.DSE_DIALECT, 1025 },
                                       { DseCompat.DSE_DIALECT, 0x10000 },
                                       { DseCompat.DSE_DIALECT, Integer.MAX_VALUE },
                                       { DseCompat.DSE_DIALECT, 255 },
                                       { DseCompat.DSE_DIALECT, 0 } };
        for (int[] pair : requestedAndAnswer)
        {
            int requested = pair[0];
            int answer = pair[1];
            DsePeers.reset();
            try (FakeDse dse = new FakeDse(1, answer))
            {
                Result<MessagingSuccess> result = connect(dse.endpoint(), requested).get(20, TimeUnit.SECONDS);
                String what = "requested " + requested + ", answer " + answer;
                assertThat(result.outcome).as(what).isEqualTo(Result.Outcome.INCOMPATIBLE);
                assertThat(result.incompatible().maxMessagingVersion).as(what).isEqualTo(answer);
                assertThat(dse.firstMessages.get(0)).as(what).containsExactly(0xCA, 0x55, 0x2D, 0xFA, 0x00, 0x00, requested, SMALL_CRC_FLAGS);
                assertThat(dse.thirdMessages).as(what).isEmpty();
                assertThat(DsePeers.isDse(dse.endpoint())).as(what).isFalse();
            }
        }
    }

    /**
     * A DSE version answering our pre-4.0 first message at the dialect is accepted, and our third message completes the
     * handshake at the dialect: DSE 6.8.65 answers its own version (1024) there instead of echoing 11 (observed with
     * DSE 6.8.65); any other DSE version (relative 1 to 255) is accepted as well. Such an answer identifies a DSE node.
     */
    @Test
    public void dseVersionAnsweringOurPre40MessageIsAccepted() throws Exception
    {
        dse6();
        for (int answer : new int[]{ DseCompat.DSE_681_VERSION, DSE_697_HANDSHAKE_VERSION, DseCompat.DSE_60_VERSION, 0xFF00 })
        {
            DsePeers.reset();
            try (FakeDse dse = new FakeDse(1, answer))
            {
                OutboundConnectionSettings settings = settings(dse.endpoint());
                Result<MessagingSuccess> result = connect(dse.endpoint(), DseCompat.DSE_DIALECT).get(20, TimeUnit.SECONDS);
                String what = "answer " + answer;
                assertThat(result.outcome).as(what).isEqualTo(Result.Outcome.SUCCESS);
                assertThat(result.success().messagingVersion).as(what).isEqualTo(DseCompat.DSE_DIALECT);
                assertThat(dse.firstMessages.get(0)).as(what).containsExactly(0xCA, 0x55, 0x2D, 0xFA, 0x00, 0x00, 0x0B, SMALL_CRC_FLAGS);
                result.success().channel.close();
                dse.thread.join(10_000);
                byte[] address = settings.from.getAddress().getAddress();
                assertThat(dse.thirdMessages).as(what).hasSize(1);
                assertThat(dse.thirdMessages.get(0)).as(what).containsExactly(concat(new byte[]{ 0, 0, 0, 11, 4 }, address));
                assertThat(DsePeers.snapshot()).as(what).containsEntry(settings.to, "pre-4.0 handshake reply with the DSE messaging version " + answer);
            }
        }
    }

    /**
     * With the mode off our first message is the 4.0-style one with acceptVersions.min (12) in the legacy byte, as
     * before, even for a peer stored at 11; DSE echoes 12 (as the source of DSE 6.9 has it), which a 4.0 reader takes
     * for the start of a 12-byte Accept, and the handshake fails once DSE closes the socket.
     */
    @Test
    public void dseAnswerIsAFailureWithTheModeOff() throws Exception
    {
        none();
        for (int requested : new int[]{ current_version, DseCompat.DSE_DIALECT })
        {
            try (FakeDse dse = new FakeDse(1))
            {
                OutboundConnectionSettings settings = settings(dse.endpoint());
                Future<Result<MessagingSuccess>> future = connect(dse.endpoint(), requested);
                // 4 bytes of 12 are not an Accept: the handshake fails when DSE closes the socket
                assertThatThrownBy(() -> future.get(20, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class)
                                                                          .hasCauseInstanceOf(ClosedChannelException.class);
                assertThat(dse.firstMessages.get(0)).containsExactly(expected40StyleInitiate(SMALL_CRC_FLAGS | VERSION_40 << 8 | VERSION_40 << 16 | current_version << 24, settings.from));
                assertThat(dse.answers.get(0)).isEqualTo(VERSION_40);
            }
        }
    }

    // ---- the first message OutboundConnection sends, known and unknown peers ----

    private static OutboundConnection outbound(InetAddressAndPort endpoint)
    {
        return outbound(endpoint, SMALL_MESSAGES);
    }

    private static OutboundConnection outbound(InetAddressAndPort endpoint, ConnectionType type)
    {
        OutboundConnectionSettings template = new OutboundConnectionSettings(endpoint).withFraming(Framing.CRC)
                                                                                      .withDefaultReserveLimits()
                                                                                      .withSocketFactory(factory)
                                                                                      .withDefaults(ConnectionCategory.MESSAGING);
        ResourceLimits.EndpointAndGlobal reserve = new ResourceLimits.EndpointAndGlobal(new ResourceLimits.Concurrent(template.applicationSendQueueReserveEndpointCapacityInBytes),
                                                                                         template.applicationSendQueueReserveGlobalCapacityInBytes);
        return new OutboundConnection(type, template, reserve);
    }

    /**
     * A peer stored with a DSE 6 release is seeded at the dialect, and the first message the connection sends it is the
     * 8-byte pre-4.0 one at 11, laid out as DSE's own first message (same magic, same length, the version where DSE puts
     * 0xFF), so that DSE never sees the 19-byte 4.0-style probe it would read the rest of as a third message. DSE 6.9.26
     * echoes 11; DSE 6.8.65 answers with its own version instead (observed with DSE 6.8.65; the value is the 4-byte
     * answer of its captured handshake with another DSE node, 1024, its answer to our message at 11 being known from the
     * upgraded node's log, not from a capture), which identifies it as a DSE node and ends its presumption. Either way
     * the connection is established at the dialect on that first socket, with no retry and no refusal.
     */
    @Test
    public void knownDsePeerIsFirstSentThePre40MessageNeverTheProbe() throws Exception
    {
        dse6();
        for (DseVersion version : DseVersion.values())
        {
            Handshake captured = DseFixtures.handshakes(version).get(0);
            int ownVersion = ByteBuffer.wrap(captured.msg2).getInt();
            assertThat(ownVersion).as(version + ": the captured answer").isEqualTo(version.relativeMessagingVersion << 8);
            // DSE 6.8.65 answers with its own version, DSE 6.9.26 echoes (null: the fake's own answer, the echo)
            Integer fixedAnswer = version == DseVersion.DSE_6_8_65 ? Integer.valueOf(ownVersion) : null;
            int expectedAnswer = fixedAnswer != null ? fixedAnswer : DseCompat.DSE_DIALECT;
            DsePeers.reset();
            try (FakeDse dse = new FakeDse(1, fixedAnswer))
            {
                InetAddressAndPort endpoint = dse.endpoint();
                assertThat(DseDialectVersions.seed(Stream.of(DseDialectVersionsTest.peer(endpoint, version.releaseVersion)), MessagingService.instance().versions))
                .containsExactly(endpoint);
                assertThat(DsePeers.isPresumed(endpoint)).as(version + ": presumed once seeded").isTrue();
                OutboundConnection outbound = outbound(endpoint);
                try
                {
                    outbound.enqueue(Message.out(Verb.ECHO_REQ, noPayload));
                    dse.thread.join(20_000);
                    assertThat(dse.firstMessages).as(version + ": a single connection").hasSize(1);
                    byte[] sent = dse.firstMessages.get(0);
                    assertThat(sent).containsExactly(0xCA, 0x55, 0x2D, 0xFA, 0x00, 0x00, 0x0B, SMALL_CRC_FLAGS);
                    assertThat(sent).as(version + ": the length of DSE's own first message").hasSameSizeAs(captured.msg1);
                    assertThat(Arrays.copyOf(sent, 4)).as(version + ": the magic").containsExactly(Arrays.copyOf(captured.msg1, 4));
                    assertThat(captured.msg1[6] & 0xFF).as(version + ": where DSE puts 0xFF").isEqualTo(DseCompat.DSE_VERSION_BYTE);
                    assertThat(sent[6] & 0xFF).isEqualTo(DseCompat.DSE_DIALECT);
                    assertThat(dse.answers).as(version + ": the answer").containsExactly(expectedAnswer);
                    // our third message: version and address, laid out as the start of DSE's own (4-byte version, address size, address)
                    byte[] address = outbound.settings().from.getAddress().getAddress();
                    assertThat(dse.thirdMessages).hasSize(1);
                    assertThat(dse.thirdMessages.get(0)).containsExactly(concat(new byte[]{ 0, 0, 0, 11, 4 }, address));
                    assertThat(captured.msg3[4]).as(version + ": address size in DSE's third message").isEqualTo((byte) address.length);
                    // our third message is flushed before the event loop completes the connection: the count may lag
                    Util.spinAssertEquals(1L, outbound::successfulConnections, 10);
                    assertThat(outbound.connectionAttempts()).as(version + ": no retry").isEqualTo(1);
                    assertThat(outbound.messagingVersion()).isEqualTo(DseCompat.DSE_DIALECT);
                    assertThat(DsePeers.isDse(endpoint)).as(version + ": identified by the answer").isEqualTo(fixedAnswer != null);
                    assertThat(DsePeers.isPresumed(endpoint)).as(version + ": still presumed").isEqualTo(fixedAnswer == null);
                }
                finally
                {
                    outbound.close(false).get(30, TimeUnit.SECONDS);
                    MessagingService.instance().versions.reset(endpoint);
                }
            }
        }
    }

    /**
     * A peer the node knows nothing of, or knows at 4.0 or later, still gets the 4.0-style probe (as observed with DSE
     * 6.8.65): DSE overrides the version with its own and the connection retries with the pre-4.0 first message. At the
     * level of OutboundConnection, the answer in the dialect is also recorded by the node (a node rolled back to DSE,
     * whose stale version was 4.0 or later), so that no later attempt corrects itself back to the probe: exactly two
     * attempts, one connection.
     */
    @Test
    public void unknownOrStalePeerIsProbedOnceThenRecordedAtTheDialect() throws Exception
    {
        dse6();
        for (boolean stale : new boolean[]{ false, true })
        {
            try (FakeDse dse = new FakeDse(2))
            {
                InetAddressAndPort endpoint = dse.endpoint();
                if (stale)
                    MessagingService.instance().versions.set(endpoint, current_version);
                OutboundConnection outbound = outbound(endpoint);
                try
                {
                    outbound.enqueue(Message.out(Verb.ECHO_REQ, noPayload));
                    dse.thread.join(20_000);
                    assertThat(dse.firstMessages).as("stale " + stale).hasSize(2);
                    assertThat(dse.firstMessages.get(0)).hasSize(19)
                                                       .containsExactly(expected40StyleInitiate(SMALL_CRC_FLAGS | current_version << 8 | VERSION_40 << 16 | current_version << 24,
                                                                                                outbound.settings().from));
                    assertThat(dse.firstMessages.get(1)).containsExactly(0xCA, 0x55, 0x2D, 0xFA, 0x00, 0x00, 0x0B, SMALL_CRC_FLAGS);
                    assertThat(dse.answers).containsExactly(DSE_697_HANDSHAKE_VERSION, DseCompat.DSE_DIALECT);
                    Util.spinAssertEquals(1L, outbound::successfulConnections, 10);
                    assertThat(outbound.connectionAttempts()).isEqualTo(2);
                    assertThat(MessagingService.instance().versions.getRaw(endpoint)).isEqualTo(DseCompat.DSE_DIALECT);
                }
                finally
                {
                    outbound.close(false).get(30, TimeUnit.SECONDS);
                    MessagingService.instance().versions.reset(endpoint);
                }
            }
        }
    }

    /**
     * The peer's version rises to 4.0 or later while a connection is being made in the dialect (the interrupt of the
     * raise reaches only established connections): the connection completed in the dialect is dropped and made again
     * with the 4.0-style handshake. Here the peer is a node rolled back to DSE after all, which answers the probe with its
     * own version: it is then recorded at the dialect and the third connection, in the dialect, is kept. Three attempts,
     * no loop.
     */
    @Test
    public void aConnectionMadeInTheDialectAfterTheVersionRoseIsMadeAgain() throws Exception
    {
        dse6();
        try (FakeDse dse = new FakeDse(3))
        {
            InetAddressAndPort endpoint = dse.endpoint();
            assertThat(DseDialectVersions.seed(Stream.of(DseDialectVersionsTest.peer(endpoint, DseVersion.DSE_6_9_26.releaseVersion)),
                                               MessagingService.instance().versions))
            .containsExactly(endpoint);
            // the version rises (gossip, or a 4.0-style handshake) while the first connection is being made
            dse.beforeFirstAnswer = () -> MessagingService.instance().versions.set(endpoint, current_version);
            OutboundConnection outbound = outbound(endpoint);
            try
            {
                outbound.enqueue(Message.out(Verb.ECHO_REQ, noPayload));
                dse.thread.join(20_000);
                byte[] pre40 = { (byte) 0xCA, 0x55, 0x2D, (byte) 0xFA, 0x00, 0x00, 0x0B, SMALL_CRC_FLAGS };
                assertThat(dse.firstMessages).hasSize(3);
                assertThat(dse.firstMessages.get(0)).containsExactly(pre40);
                // the copy corrected to the node's version: the probe carries the current version in the legacy byte
                assertThat(dse.firstMessages.get(1)).containsExactly(expected40StyleInitiate(SMALL_CRC_FLAGS | current_version << 8 | VERSION_40 << 16 | current_version << 24,
                                                                                             outbound.settings().from));
                assertThat(dse.firstMessages.get(2)).containsExactly(pre40);
                assertThat(dse.answers).containsExactly(DseCompat.DSE_DIALECT, DSE_697_HANDSHAKE_VERSION, DseCompat.DSE_DIALECT);
                Util.spinAssertEquals(1L, outbound::successfulConnections, 10);
                assertThat(outbound.connectionAttempts()).isEqualTo(3);
                assertThat(outbound.messagingVersion()).isEqualTo(DseCompat.DSE_DIALECT);
                assertThat(MessagingService.instance().versions.getRaw(endpoint)).isEqualTo(DseCompat.DSE_DIALECT);
            }
            finally
            {
                outbound.close(false).get(30, TimeUnit.SECONDS);
                MessagingService.instance().versions.reset(endpoint);
                DsePeers.reset();
            }
        }
    }

    /**
     * The peer's version keeps rising to 4.0 or later before each answer, while the peer answers the probe in the
     * dialect (its version going back and forth): the first connection completed in a superseded dialect is made again
     * at once, the second one only after the delay of a failed attempt, so that the connection cannot spin.
     */
    @Test
    public void aConnectionSupersededTwiceIsMadeAgainOnlyAfterADelay() throws Exception
    {
        dse6();
        try (FakeDse dse = new FakeDse(3))
        {
            InetAddressAndPort endpoint = dse.endpoint();
            assertThat(DseDialectVersions.seed(Stream.of(DseDialectVersionsTest.peer(endpoint, DseVersion.DSE_6_9_26.releaseVersion)),
                                               MessagingService.instance().versions))
            .containsExactly(endpoint);
            dse.beforeEachAnswer = () -> MessagingService.instance().versions.set(endpoint, current_version);
            // the delay of a first failed attempt is half the minimum RPC timeout: 100 ms instead of 10 s here
            long readRpcTimeoutBefore = DatabaseDescriptor.getReadRpcTimeout(TimeUnit.MILLISECONDS);
            DatabaseDescriptor.setReadRpcTimeout(200);
            try
            {
                OutboundConnection outbound = outbound(endpoint);
                try
                {
                    outbound.enqueue(Message.out(Verb.ECHO_REQ, noPayload));
                    dse.thread.join(20_000);
                    // in the dialect (superseded, made again at once), the probe (answered in the dialect), in the
                    // dialect again (superseded a second time)
                    assertThat(dse.answers).containsExactly(DseCompat.DSE_DIALECT, DSE_697_HANDSHAKE_VERSION, DseCompat.DSE_DIALECT);
                    Util.spinAssertEquals(4L, outbound::connectionAttempts, 10);
                    long delayMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - dse.answeredAtNanos.get(2));
                    assertThat(delayMillis).as("the fourth attempt waits for the retry delay").isGreaterThanOrEqualTo(90);
                    assertThat(outbound.successfulConnections()).isZero();
                }
                finally
                {
                    outbound.close(false).get(30, TimeUnit.SECONDS);
                }
            }
            finally
            {
                DatabaseDescriptor.setReadRpcTimeout(readRpcTimeoutBefore);
                MessagingService.instance().versions.reset(endpoint);
                DsePeers.reset();
            }
        }
    }

    /** A peer stored with the release of this software is not seeded: the connection makes the 4.0-style handshake, at the current version. */
    @Test
    public void upgradedPeerIsConnectedToWithThe40StyleHandshake() throws Exception
    {
        dse6();
        InboundSockets inbound = openInbound();
        InetAddressAndPort endpoint = bindAddress(inbound);
        try
        {
            // the inbound handshakes of other tests record this node's own address, which the inbound sockets bind to
            MessagingService.instance().versions.reset(endpoint);
            assertThat(DseDialectVersions.seed(Stream.of(DseDialectVersionsTest.peer(endpoint, "5.0.7.0")), MessagingService.instance().versions)).isEmpty();
            assertThat(MessagingService.instance().versions.knows(endpoint)).isFalse();
            OutboundConnection outbound = outbound(endpoint);
            try
            {
                outbound.enqueue(Message.out(Verb.ECHO_REQ, noPayload));
                Util.spinAssertEquals(1L, outbound::successfulConnections, 20);
                assertThat(outbound.messagingVersion()).isEqualTo(current_version);
            }
            finally
            {
                outbound.close(false).get(30, TimeUnit.SECONDS);
            }
        }
        finally
        {
            inbound.close().await(1L, TimeUnit.SECONDS);
            MessagingService.instance().versions.reset(endpoint);
        }
    }

    /**
     * Two connection types of a pool to a DSE node this node knows nothing of: the first one probes, DSE answers with
     * its version (which identifies it) and the node records the dialect; the second one, whose copy of the versions was
     * taken before, goes straight to the pre-4.0 first message instead of probing DSE a second time.
     */
    @Test
    public void onlyOneConnectionTypeProbesADseNode() throws Exception
    {
        dse6();
        try (FakeDse dse = new FakeDse(3))
        {
            InetAddressAndPort endpoint = dse.endpoint();
            OutboundConnection small = outbound(endpoint);
            OutboundConnection large = outbound(endpoint, ConnectionType.LARGE_MESSAGES);
            try
            {
                small.enqueue(Message.out(Verb.ECHO_REQ, noPayload));
                Util.spinAssertEquals(1L, small::successfulConnections, 20);
                large.enqueue(Message.out(Verb.ECHO_REQ, noPayload));
                dse.thread.join(20_000);
                assertThat(dse.firstMessages).hasSize(3);
                assertThat(dse.firstMessages.get(0)).hasSize(19);
                assertThat(dse.firstMessages.get(1)).hasSize(8);
                byte[] third = dse.firstMessages.get(2);
                assertThat(third).as("the large connection's first message").hasSize(8);
                assertThat(third[6] & 0xFF).isEqualTo(DseCompat.DSE_DIALECT);
                assertThat(dse.answers).containsExactly(DSE_697_HANDSHAKE_VERSION, DseCompat.DSE_DIALECT, DseCompat.DSE_DIALECT);
                assertThat(large.connectionAttempts()).isEqualTo(1);
                assertThat(DsePeers.isDse(endpoint)).isTrue();
            }
            finally
            {
                small.close(false).get(30, TimeUnit.SECONDS);
                large.close(false).get(30, TimeUnit.SECONDS);
                MessagingService.instance().versions.reset(endpoint);
            }
        }
    }

    // ---- the handshake timeout, on our inbound handler in an embedded channel ----

    /** The remote end of the connection: an arbitrary peer address with an ephemeral source port. */
    private static final InetSocketAddress ABANDONING_PEER = new InetSocketAddress("127.0.0.12", 45265);
    /** DSE's plain pre-4.0 first message once it stored 12 (our gossiped {@code NET_VERSION}) for us: version byte 12. */
    private static final byte[] PLAIN_PRE40_AT_12 = plainPre40At(DseCompat.NET_VERSION_PUBLISHED);

    /**
     * A plain pre-4.0 first message at the given version, in the layout of our own pre-4.0 encoder (small messages, no
     * framing bits); DSE's own plain first message is not among the fixtures.
     */
    private static byte[] plainPre40At(int version)
    {
        return new byte[]{ (byte) 0xCA, 0x55, 0x2D, (byte) 0xFA, 0, 0, (byte) version, 2 };
    }

    /** The settings of our storage port, completed as {@link InboundSockets} does. */
    private static InboundConnectionSettings inboundSettings()
    {
        return new InboundConnectionSettings().withBindAddress(FBUtilities.getLocalAddressAndPort()).withDefaults();
    }

    /** An embedded channel whose ends are socket addresses, as the inbound handler expects: our storage port, the peer. */
    private static final class InboundChannel extends EmbeddedChannel
    {
        @Override
        protected SocketAddress localAddress0()
        {
            InetAddressAndPort local = FBUtilities.getLocalAddressAndPort();
            return new InetSocketAddress(local.getAddress(), local.getPort());
        }

        @Override
        protected SocketAddress remoteAddress0()
        {
            return ABANDONING_PEER;
        }
    }

    /** What the inbound handler did with a connection that never completed the handshake. */
    private static final class TimedOut
    {
        final List<byte[]> replies = new ArrayList<>();
        final List<ILoggingEvent> log = new ArrayList<>();
        /** The log lines before the handshake timeout was due. */
        final List<ILoggingEvent> logBeforeTimeout = new ArrayList<>();
        final boolean peerClosed;
        boolean openBeforeTimeout;
        boolean openAfterTimeout;

        TimedOut(boolean peerClosed)
        {
            this.peerClosed = peerClosed;
        }

        List<ILoggingEvent> at(Level level)
        {
            return log.stream().filter(e -> e.getLevel() == level).collect(Collectors.toList());
        }
    }

    /** {@link #timeOutAfter(boolean, byte[]...)} with a peer that keeps the socket open. */
    private static TimedOut timeOutAfter(byte[]... received)
    {
        return timeOutAfter(false, received);
    }

    /**
     * Feeds the given bytes to our inbound handshake handler, closes the channel from the peer's side if asked (as the
     * transport does when the peer closes: the channel becomes inactive and the handlers are removed, the scheduled
     * handshake timeout is kept unless the handler cancels it), then lets the handshake timeout expire and runs it; returns
     * the replies, the log lines of {@link InboundConnectionInitiator} before the timeout was due and in all, and whether
     * the channel was open before and after the timeout.
     */
    private static TimedOut timeOutAfter(boolean peerCloses, byte[]... received)
    {
        Logger initiatorLogger = (Logger) LoggerFactory.getLogger(InboundConnectionInitiator.class);
        Level levelBefore = initiatorLogger.getLevel();
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        // only the lines about this channel: the handshake timeouts of the real sockets of other tests may fire meanwhile
        appender.addFilter(new Filter<>()
        {
            @Override
            public FilterReply decide(ILoggingEvent event)
            {
                return event.getFormattedMessage().contains(ABANDONING_PEER.toString()) ? FilterReply.NEUTRAL : FilterReply.DENY;
            }
        });
        appender.start();
        initiatorLogger.addAppender(appender);
        initiatorLogger.setLevel(Level.DEBUG);
        InboundChannel channel = new InboundChannel();
        TimedOut result = new TimedOut(peerCloses);
        try
        {
            channel.freezeTime();
            channel.pipeline().addLast("handshake", new InboundConnectionInitiator.Handler(inboundSettings()));
            for (byte[] bytes : received)
                channel.writeInbound(Unpooled.wrappedBuffer(bytes));
            ByteBuf reply;
            while ((reply = channel.readOutbound()) != null)
                result.replies.add(bytes(reply)); // bytes() releases the reply
            if (peerCloses)
                channel.unsafe().close(channel.voidPromise()); // not EmbeddedChannel.close(), which cancels scheduled tasks
            result.openBeforeTimeout = channel.isOpen();
            result.logBeforeTimeout.addAll(appender.list);
            channel.advanceTimeBy(HandshakeProtocol.TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            channel.runScheduledPendingTasks();
            result.openAfterTimeout = channel.isOpen();
            result.log.addAll(appender.list);
        }
        finally
        {
            channel.finishAndReleaseAll();
            initiatorLogger.setLevel(levelBefore);
            initiatorLogger.detachAppender(appender);
        }
        return result;
    }

    private static void assertTimeoutError(TimedOut timedOut)
    {
        assertThat(timedOut.openBeforeTimeout).isEqualTo(!timedOut.peerClosed);
        assertThat(timedOut.openAfterTimeout).isFalse();
        assertThat(timedOut.logBeforeTimeout).extracting(ILoggingEvent::getLevel).noneMatch(Level.ERROR::equals);
        assertThat(timedOut.at(Level.ERROR)).extracting(ILoggingEvent::getFormattedMessage)
                                            .singleElement(STRING)
                                            .startsWith("Timeout handshaking with " + ABANDONING_PEER + " (on ");
        assertThat(timedOut.log).extracting(ILoggingEvent::getFormattedMessage).noneMatch(m -> m.contains("third message"));
    }

    /** What a DSE 6.8.65 node gives up, in the debug lines: a socket answered below 12, or the one of its own first message. */
    private static String givesUp(int requested)
    {
        return requested == DseCompat.DSE_VERSION_BYTE
               ? "the socket of its own flagged first message once answered with a plain pre-4.0 version"
               : "a socket answered below the version it stored for us";
    }

    /**
     * No error, no warning, one answer of 11 and the channel closed at the end; a socket left open is closed by the timeout
     * with its debug line, a socket the peer closed cancels the timeout with the other debug line, logged when the socket
     * closed, and nothing is logged when the timeout would have fired.
     */
    private static void assertQuietAbandonedSocket(TimedOut timedOut, int requested)
    {
        assertThat(timedOut.replies).hasSize(1);
        assertThat(timedOut.replies.get(0)).containsExactly(0, 0, 0, DseCompat.DSE_DIALECT);
        assertThat(timedOut.openBeforeTimeout).isEqualTo(!timedOut.peerClosed);
        assertThat(timedOut.openAfterTimeout).isFalse();
        assertThat(timedOut.at(Level.ERROR)).isEmpty();
        assertThat(timedOut.at(Level.WARN)).isEmpty();
        String expected = timedOut.peerClosed
                          ? "the connection from peer " + ABANDONING_PEER + " (on " + inboundSettings().bindAddress + ") closed without " +
                            "the third message of the pre-4.0 handshake (requested version " + requested + ", answered 11): expected from a " +
                            "DSE 6.8.65 node, which gives up " + givesUp(requested) + " and reconnects at 11 on a new one; handshake timeout cancelled"
                          : "peer " + ABANDONING_PEER + " did not send the third message of the pre-4.0 handshake " +
                            "(requested version " + requested + ", answered 11) within " + HandshakeProtocol.TIMEOUT_MILLIS + " ms of the " +
                            "connection (on " + inboundSettings().bindAddress + "): expected from a DSE 6.8.65 node, which gives up " +
                            givesUp(requested) + " and reconnects at 11 on a new one; closing the socket";
        assertThat(timedOut.at(Level.DEBUG)).extracting(ILoggingEvent::getFormattedMessage)
                                            .filteredOn(m -> m.contains("third message"))
                                            .singleElement(STRING)
                                            .isEqualTo(expected);
        // a socket the peer closed is logged when it closes and its cancelled timeout logs nothing; an open one is logged
        // by the timeout
        List<String> loggedByTheTimeout = timedOut.log.subList(timedOut.logBeforeTimeout.size(), timedOut.log.size())
                                                      .stream().map(ILoggingEvent::getFormattedMessage).collect(Collectors.toList());
        if (timedOut.peerClosed)
            assertThat(loggedByTheTimeout).isEmpty();
        else
            assertThat(loggedByTheTimeout).containsExactly(expected);
    }

    /**
     * DSE's plain pre-4.0 first message at 12 answered with 11 and no third message: DSE gave the socket up to reconnect at
     * 11 on a new one (observed with DSE 6.8.65; DSE 6.9.26 sends the third message instead), so the timeout closes the
     * socket quietly, with a debug line and no error.
     */
    @Test
    public void socketAbandonedByDseAfterOurDowngradeIsClosedQuietly()
    {
        dse6();
        assertQuietAbandonedSocket(timeOutAfter(PLAIN_PRE40_AT_12), DseCompat.NET_VERSION_PUBLISHED);
    }

    /**
     * The same socket closed by DSE instead of left open (DSE's code reads a disconnect): the close cancels the timeout
     * with a debug line, and nothing is logged when the timeout would have fired.
     */
    @Test
    public void socketClosedByDseAfterOurDowngradeIsQuietToo()
    {
        dse6();
        assertQuietAbandonedSocket(timeOutAfter(true, PLAIN_PRE40_AT_12), DseCompat.NET_VERSION_PUBLISHED);
    }

    /**
     * DSE's own flagged first message answered with 11 and no third message: DSE 6.8.65 gives that socket up and reconnects
     * at 11 on a new one (observed with DSE 6.8.65; DSE 6.9.26 sends the third message instead), so a socket left open
     * is closed quietly by the timeout. A socket left open was not observed (DSE 6.8.65 closes it at once); this
     * covers a DSE version that keeps it open.
     */
    @Test
    public void socketAbandonedByDseAfterItsFlaggedRequestIsClosedQuietly() throws Exception
    {
        dse6();
        for (DseVersion version : DseVersion.values())
            assertQuietAbandonedSocket(timeOutAfter(flaggedMsg1(version)), DseCompat.DSE_VERSION_BYTE);
    }

    /**
     * The same socket closed by DSE at once, as observed with DSE 6.8.65 (the sockets of the flagged first messages never
     * appear in {@code ss -tn} on our node): the close cancels the timeout with a debug line and no late timeout error follows.
     */
    @Test
    public void socketClosedByDseAfterItsFlaggedRequestIsQuietAndCancelsTheTimeout() throws Exception
    {
        dse6();
        for (DseVersion version : DseVersion.values())
            assertQuietAbandonedSocket(timeOutAfter(true, flaggedMsg1(version)), DseCompat.DSE_VERSION_BYTE);
    }

    /** DSE's flagged first message as captured from the given version. */
    private static byte[] flaggedMsg1(DseVersion version) throws IOException
    {
        byte[] flagged = DseFixtures.handshakes(version).get(0).msg1;
        assertThat(flagged[6]).as("DSE's marker in the version byte, " + version).isEqualTo((byte) DseCompat.DSE_VERSION_BYTE);
        return flagged;
    }

    /**
     * A plain pre-4.0 first message at 10 (a 3.0 peer) or at the version we answer (11), then silence: nothing explains it,
     * the timeout is an error, whether the peer left the socket open or closed it (the timeout is not cancelled then,
     * as upstream).
     */
    @Test
    public void missingThirdMessageAfterARequestAt10Or11IsAnError()
    {
        dse6();
        for (boolean peerCloses : new boolean[]{ false, true })
        {
            for (int version : new int[]{ VERSION_30, DseCompat.DSE_DIALECT })
            {
                TimedOut timedOut = timeOutAfter(peerCloses, plainPre40At(version));
                String as = "version " + version + ", peer closes: " + peerCloses;
                assertThat(timedOut.replies).as(as).hasSize(1);
                assertThat(timedOut.replies.get(0)).as(as).containsExactly(0, 0, 0, DseCompat.DSE_DIALECT);
                assertTimeoutError(timedOut);
            }
        }
    }

    /**
     * Part of a third message after the downgrade, or after DSE's flagged first message, means the peer did not give the
     * socket up: the timeout is an error, also when the peer closed the socket after the partial message.
     */
    @Test
    public void partialThirdMessageAfterOurDowngradeIsAnError() throws Exception
    {
        dse6();
        for (byte[] msg1 : new byte[][]{ PLAIN_PRE40_AT_12, flaggedMsg1(DseVersion.DSE_6_8_65) })
        {
            for (boolean peerCloses : new boolean[]{ false, true })
            {
                // in two reads, and in one read as TCP may deliver them
                for (byte[][] received : new byte[][][]{ { msg1, new byte[]{ 0, 0 } }, { concat(msg1, new byte[]{ 0, 0 }) } })
                {
                    TimedOut timedOut = timeOutAfter(peerCloses, received);
                    String as = "requested " + (msg1[6] & 0xFF) + ", peer closes: " + peerCloses + ", reads: " + received.length;
                    assertThat(timedOut.replies).as(as).hasSize(1);
                    assertThat(timedOut.replies.get(0)).as(as).containsExactly(0, 0, 0, DseCompat.DSE_DIALECT);
                    assertTimeoutError(timedOut);
                }
            }
        }
    }

    /**
     * Part of DSE's flagged first message, then silence or a close: we never answered, so nothing explains it and the
     * timeout is an error, also when the peer closed the socket.
     */
    @Test
    public void partialFirstMessageIsAnError() throws Exception
    {
        dse6();
        byte[] partial = Arrays.copyOf(flaggedMsg1(DseVersion.DSE_6_8_65), 4);
        for (boolean peerCloses : new boolean[]{ false, true })
        {
            TimedOut timedOut = timeOutAfter(peerCloses, partial);
            assertThat(timedOut.replies).as("peer closes: " + peerCloses).isEmpty();
            assertTimeoutError(timedOut);
        }
    }

    /**
     * With the mode off the same first messages are never answered and their timeout is the error it always was, also when
     * the peer closed the socket before it fired; nothing else is logged, before or after the timeout.
     */
    @Test
    public void handshakeTimeoutIsAnErrorWithTheModeOff() throws Exception
    {
        none();
        for (byte[] msg1 : new byte[][]{ PLAIN_PRE40_AT_12, flaggedMsg1(DseVersion.DSE_6_8_65) })
        {
            for (boolean peerCloses : new boolean[]{ false, true })
            {
                TimedOut timedOut = timeOutAfter(peerCloses, msg1);
                String as = "requested " + (msg1[6] & 0xFF) + ", peer closes: " + peerCloses;
                assertThat(timedOut.replies).as(as).isEmpty();
                assertTimeoutError(timedOut);
                assertThat(timedOut.logBeforeTimeout).as(as).isEmpty();
                assertThat(timedOut.log).as(as).hasSize(1);
            }
        }
    }
}
