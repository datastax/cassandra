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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.memory.BufferPools;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.locator.InetAddressAndPort.Serializer.inetAddressAndPortSerializer;
import static org.apache.cassandra.net.MessagingService.VERSION_30;
import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.net.Message.validateLegacyProtocolMagic;
import static org.apache.cassandra.net.Crc.*;
import static org.apache.cassandra.net.OutboundConnectionSettings.*;

/**
 * Messages for the handshake phase of the internode protocol.
 *
 * The modern handshake is composed of 2 messages: Initiate and Accept
 * <p>
 * The legacy handshake is composed of 3 messages, the first being sent by the initiator of the connection. The other
 * side then answer with the 2nd message. At that point, if a version mismatch is detected by the connection initiator,
 * it will simply disconnect and reconnect with a more appropriate version. But if the version is acceptable, the connection
 * initiator sends the third message of the protocol, after which it considers the connection ready.
 */
class HandshakeProtocol
{
    static final long TIMEOUT_MILLIS = 3 * DatabaseDescriptor.getRpcTimeout(MILLISECONDS);

    /**
     * The initial message sent when a node creates a new connection to a remote peer. This message contains:
     *   1) the {@link Message#PROTOCOL_MAGIC} number (4 bytes).
     *   2) the connection flags (4 bytes), which encodes:
     *      - the version the initiator thinks should be used for the connection (in practice, either the initiator
     *        version if it's the first time we connect to that remote since startup, or the last version known for that
     *        peer otherwise).
     *      - the "mode" of the connection: whether it is for streaming or for messaging.
     *      - whether compression should be used or not (if it is, compression is enabled _after_ the last message of the
     *        handshake has been sent).
     *   3) the connection initiator's broadcast address
     *   4) a CRC protecting the message from corruption
     * <p>
     * More precisely, connection flags:
     * <pre>
     * {@code
     *                      1 1 1 1 1 1 1 1 1 1 2 2 2 2 2 2 2 2 2 2 3 3
     *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * |C C C M C      |    REQUEST    |      MIN      |      MAX      |
     * |A A M O R      |    VERSION    |   SUPPORTED   |   SUPPORTED   |
     * |T T P D C      |  (DEPRECATED) |    VERSION    |    VERSION    |
     * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
     * }
     * </pre>
     * CAT - QOS category, 2 bits: SMALL, LARGE, URGENT, or LEGACY (unset)
     * CMP - compression enabled bit
     * MOD - connection mode; if the bit is on, the connection is for streaming; if the bit is off, it is for inter-node messaging.
     * CRC - crc enabled bit
     * VERSION - {@link org.apache.cassandra.net.MessagingService#current_version}
     */
    static class Initiate
    {
        /** Contains the PROTOCOL_MAGIC (int) and the flags (int). */
        private static final int MIN_LENGTH = 8;
        private static final int MAX_LENGTH = 12 + InetAddressAndPort.Serializer.MAXIMUM_SIZE;

        /**
         * The version a pre-4.0 peer reads from the flags (bits 8-15); 4.0+ peers only check that it is not below 4.0.
         * <ul>
         *   <li>With {@code internode_compatibility_mode: NONE} it is always {@code acceptVersions.min}, as before.</li>
         *   <li>In DSE 6 compatibility mode a 4.0-style first message carries the version we request, raised to at
         *       least 13 ({@code VERSION_50}; the current version for an unknown peer, as HCD 1.2 does), so that a DSE
         *       peer answers with its own version ({@code relative << 8}) and we reconnect in the dialect:
         *       DSE echoes 12 as a 4-byte 12 (as the source of DSE 6.9 has it), which a 4.0 reader cannot tell from the
         *       start of a 12-byte Accept.</li>
         *   <li>Pre-4.0 first messages carry only this and no accepted range: {@link #acceptVersions} is null for them.
         *       They exist only in DSE 6 compatibility mode: DSE sends one with {@link DseCompat#DSE_VERSION_BYTE},
         *       and we send one at {@link DseCompat#DSE_DIALECT} to a peer we know to speak the dialect.</li>
         * </ul>
         */
        final int requestMessagingVersion;
        // the messagingVersion bounds the sender will accept to initiate a connection;
        // if the remote peer supports any, the newest supported version will be selected; otherwise the nearest supported version
        final AcceptVersions acceptVersions;
        final ConnectionType type;
        final Framing framing;
        final InetAddressAndPort from;

        Initiate(AcceptVersions acceptVersions, ConnectionType type, Framing framing, InetAddressAndPort from)
        {
            this(acceptVersions.min, acceptVersions, type, framing, from);
        }

        Initiate(int requestMessagingVersion, AcceptVersions acceptVersions, ConnectionType type, Framing framing, InetAddressAndPort from)
        {
            this.requestMessagingVersion = requestMessagingVersion;
            this.acceptVersions = acceptVersions;
            this.type = type;
            this.framing = framing;
            this.from = from;
        }

        /** A pre-4.0 first message: requested version only, no accepted range, no address, no CRC. */
        boolean isPre40()
        {
            return acceptVersions == null;
        }

        private int encodeFlags()
        {
            int flags = 0;
            if (type.isMessaging())
                flags |= type.twoBitID();
            if (type.isStreaming())
                flags |= 1 << 3;

            // framing id is split over 2nd and 4th bits, for backwards compatibility
            flags |= ((framing.id & 1) << 2) | ((framing.id & 2) << 3);
            flags |= (requestMessagingVersion << 8); // legacy (pre40)
            if (isPre40())
                return flags;
            flags |= (acceptVersions.min << 16);
            flags |= (acceptVersions.max << 24);
            return flags;
        }

        ByteBuf encode()
        {
            ByteBuffer buffer = BufferPools.forNetworking().get(MAX_LENGTH, BufferType.OFF_HEAP);
            try (DataOutputBufferFixed out = new DataOutputBufferFixed(buffer))
            {
                out.writeInt(Message.PROTOCOL_MAGIC);
                out.writeInt(encodeFlags());
                if (!isPre40())
                {
                    inetAddressAndPortSerializer.serialize(from, out, acceptVersions.min);
                    out.writeInt(computeCrc32(buffer, 0, buffer.position()));
                }
                buffer.flip();
                return GlobalBufferPoolAllocator.wrap(buffer);
            }
            catch (IOException e)
            {
                throw new IllegalStateException(e);
            }
        }

        static Initiate maybeDecode(ByteBuf buf) throws IOException
        {
            if (buf.readableBytes() < MIN_LENGTH)
                return null;

            ByteBuffer nio = buf.nioBuffer();
            int start = nio.position();
            try (DataInputBuffer in = new DataInputBuffer(nio, false))
            {
                validateLegacyProtocolMagic(in.readInt());
                int flags = in.readInt();

                int requestedMessagingVersion = getBits(flags, 8, 8);
                int minMessagingVersion = getBits(flags, 16, 8);
                int maxMessagingVersion = getBits(flags, 24, 8);

                int framingBits = getBits(flags, 2, 1) | (getBits(flags, 4, 1) << 1);

                // DSE 6 compatibility mode: DSE's first message (0xFF and its relative version), or a plain pre-4.0 one
                // (a DSE peer reconnecting with the version it learnt, or an HCD 1.2 node forced to the 3.0.14 dialect);
                // both are 8 bytes, without address or CRC, and are answered with a 4-byte version
                if (DseCompat.enabled() &&
                    (DseCompat.isDseFlaggedInitiate(requestedMessagingVersion, minMessagingVersion, maxMessagingVersion) ||
                     DseCompat.isPre40Initiate(requestedMessagingVersion, minMessagingVersion, maxMessagingVersion)))
                {
                    ConnectionType pre40Type = getBits(flags, 3, 1) == 1 ? ConnectionType.STREAMING : ConnectionType.fromId(getBits(flags, 0, 2));
                    Framing pre40Framing = Framing.forId(framingBits);
                    buf.skipBytes(nio.position() - start);
                    return new Initiate(requestedMessagingVersion, null, pre40Type, pre40Framing, null);
                }

                // legacy pre40 messagingVersion flag
                if (requestedMessagingVersion < VERSION_40)
                    return null;

                // 5.0+ does not support pre40
                if (maxMessagingVersion < MessagingService.VERSION_40)
                    return null;

                Framing framing = Framing.forId(framingBits);

                boolean isStream = getBits(flags, 3, 1) == 1;

                ConnectionType type = isStream
                                    ? ConnectionType.STREAMING
                                    : ConnectionType.fromId(getBits(flags, 0, 2));

                InetAddressAndPort from = inetAddressAndPortSerializer.deserialize(in, minMessagingVersion);

                int computed = computeCrc32(nio, start, nio.position());
                int read = in.readInt();
                if (read != computed)
                    throw new InvalidCrc(read, computed);

                buf.skipBytes(nio.position() - start);
                return new Initiate(requestedMessagingVersion, new AcceptVersions(minMessagingVersion, maxMessagingVersion), type, framing, from);

            }
            catch (EOFException e)
            {
                return null;
            }
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof Initiate))
                return false;

            Initiate that = (Initiate)other;
            return    this.type == that.type
                   && this.framing == that.framing
                   && this.requestMessagingVersion == that.requestMessagingVersion
                   && Objects.equals(this.acceptVersions, that.acceptVersions);
        }

        @Override
        public String toString()
        {
            if (isPre40())
                return String.format("Initiate(pre-4.0, request: %d, type: %s, framing: %s)", requestMessagingVersion, type, framing);
            return String.format("Initiate(min: %d, max: %d, type: %s, framing: %s, from: %s)",
                                 acceptVersions.min,
                                 acceptVersions.max,
                                 type, framing, from);
        }
    }


    /**
     * The second message of the handshake, sent by the node receiving the {@link Initiate} back to the
     * connection initiator.
     *
     * This message contains
     *   1) the messaging version of the peer sending this message
     *   2) the negotiated messaging version if one could be accepted by both peers,
     *      or if not the closest version that this peer could support to the ones requested
     *   3) a CRC protecting the integrity of the message
     *
     */
    static class Accept
    {
        /** The messaging version sent by the receiving peer (int). */
        private static final int MAX_LENGTH = 12;

        final int useMessagingVersion;
        final int maxMessagingVersion;
        /**
         * DSE 6 compatibility mode: the 4 bytes of a pre-4.0 reply as the peer sent them, before a DSE version is mapped
         * to the dialect in {@link #maxMessagingVersion}; 0 for a 4.0-style reply.
         */
        final int pre40PeerVersion;

        Accept(int useMessagingVersion, int maxMessagingVersion)
        {
            this(useMessagingVersion, maxMessagingVersion, 0);
        }

        private Accept(int useMessagingVersion, int maxMessagingVersion, int pre40PeerVersion)
        {
            this.useMessagingVersion = useMessagingVersion;
            this.maxMessagingVersion = maxMessagingVersion;
            this.pre40PeerVersion = pre40PeerVersion;
        }

        ByteBuf encode(ByteBufAllocator allocator)
        {
            ByteBuf buffer = allocator.directBuffer(MAX_LENGTH);
            buffer.clear();
            buffer.writeInt(maxMessagingVersion);
            buffer.writeInt(useMessagingVersion);
            buffer.writeInt(computeCrc32(buffer, 0, 8));
            return buffer;
        }

        /**
         * The pre-4.0 form of this message: the 4-byte version only, no CRC. Sent, in DSE 6 compatibility mode, to a
         * peer whose first message was pre-4.0 (see {@link Initiate#isPre40()}).
         */
        static ByteBuf respondPre40(int messagingVersion, ByteBufAllocator allocator)
        {
            ByteBuf buffer = allocator.directBuffer(4);
            buffer.clear();
            buffer.writeInt(messagingVersion);
            return buffer;
        }

        static Accept maybeDecode(ByteBuf in) throws InvalidCrc
        {
            return maybeDecode(in, VERSION_40);
        }

        /**
         * @param requestMessagingVersion the version our first message requested; below 4.0 it was a pre-4.0 message
         *                                and the peer answers with 4 bytes only (DSE 6 compatibility mode)
         * @return an {@link Accept} whose {@code useMessagingVersion} is 0 for a pre-4.0 reply: {@code maxMessagingVersion}
         *         is then the dialect to use with that peer, or to reconnect with
         */
        static Accept maybeDecode(ByteBuf in, int requestMessagingVersion) throws InvalidCrc
        {
            int readerIndex = in.readerIndex();
            if (in.readableBytes() < 4)
                return null;
            int maxMessagingVersion = in.readInt();
            int useMessagingVersion = 0;

            if (DseCompat.enabled() && (requestMessagingVersion < VERSION_40
                                        || maxMessagingVersion < VERSION_40
                                        || maxMessagingVersion >= DseCompat.DSE_HANDSHAKE_MIN))
            {
                // a pre-4.0 peer answers with its version only: a 3.x/HCD 1.2 value (10, 11), or a DSE version
                // (relative << 8, at least 256) that means "speak the 3.0.14 dialect with me"
                return new Accept(useMessagingVersion, DseCompat.dialectFor(maxMessagingVersion), maxMessagingVersion);
            }

            // pre-4.0 not supported, close the connection
            if (maxMessagingVersion < VERSION_40)
                return null;

            if (in.readableBytes() < 8)
            {
                in.readerIndex(readerIndex);
                return null;
            }
            useMessagingVersion = in.readInt();

            // verify crc
            int computed = computeCrc32(in, readerIndex, readerIndex + 8);
            int read = in.readInt();
            if (read != computed)
                throw new InvalidCrc(read, computed);

            return new Accept(useMessagingVersion, maxMessagingVersion);
        }

        @Override
        public boolean equals(Object other)
        {
            return other instanceof Accept
                   && this.useMessagingVersion == ((Accept) other).useMessagingVersion
                   && this.maxMessagingVersion == ((Accept) other).maxMessagingVersion;
        }

        @Override
        public String toString()
        {
            if (pre40PeerVersion != 0)
                return String.format("Accept(pre-4.0, peer: %d, max: %d)", pre40PeerVersion, maxMessagingVersion);
            return String.format("Accept(use: %d, max: %d)", useMessagingVersion, maxMessagingVersion);
        }
    }

    /**
     * The third message of the handshake, sent by pre-4.0 peers on reception of a pre-4.0 {@link Accept}; DSE 6
     * compatibility mode only. It contains:
     *   1) the connection initiator's messaging version (4 bytes): in DSE 6 mode always {@link DseCompat#DSE_DIALECT},
     *      whichever product sends it, because that is what we answered with;
     *   2) the connection initiator's broadcast address as encoded by {@link InetAddressAndPort.Serializer} at
     *      version 3.0: 5 bytes for an IPv4 address, 17 for an IPv6 one, no port. Any other size of address is refused
     *      ({@link InvalidPre40Address}): the peer may not pick the port it is registered under.
     * DSE appends its own parameters block after that when talking to another DSE node; at version 11 it does not, and
     * nothing after the address is read here.
     * <p>
     * This message concludes the legacy handshake protocol.
     */
    static class ConfirmOutboundPre40
    {
        private static final int MAX_LENGTH = 4 + InetAddressAndPort.Serializer.MAXIMUM_SIZE;

        final int maxMessagingVersion;
        final InetAddressAndPort from;

        ConfirmOutboundPre40(int maxMessagingVersion, InetAddressAndPort from)
        {
            this.maxMessagingVersion = maxMessagingVersion;
            this.from = from;
        }

        ByteBuf encode()
        {
            ByteBuffer buffer = BufferPools.forNetworking().get(MAX_LENGTH, BufferType.OFF_HEAP);
            try (DataOutputBufferFixed out = new DataOutputBufferFixed(buffer))
            {
                out.writeInt(maxMessagingVersion);
                // pre-4.0 nodes should only receive the address, never port, and it's ok to hardcode VERSION_30
                inetAddressAndPortSerializer.serialize(from, out, VERSION_30);
                buffer.flip();
                return GlobalBufferPoolAllocator.wrap(buffer);
            }
            catch (IOException e)
            {
                throw new IllegalStateException(e);
            }
        }

        /**
         * @return the third message, or null while the buffer does not hold all of it
         * @throws InvalidPre40Address when the size of the address is neither 4 (IPv4) nor 16 (IPv6)
         */
        static ConfirmOutboundPre40 maybeDecode(ByteBuf in) throws InvalidPre40Address
        {
            // the version (4 bytes) and the size of the address (1 byte), then the address
            if (in.readableBytes() < 5)
                return null;
            int readerIndex = in.readerIndex();
            int addressSize = in.getUnsignedByte(readerIndex + 4);
            if (addressSize != 4 && addressSize != 16)
                throw new InvalidPre40Address(addressSize);
            if (in.readableBytes() < 5 + addressSize)
                return null;

            ByteBuffer nio = in.nioBuffer();
            int start = nio.position();
            try (DataInputBuffer input = new DataInputBuffer(nio, false))
            {
                int version = input.readInt();
                // the address is always in the 3.0 layout here, whatever version number precedes it (DSE writes its own)
                InetAddressAndPort address = inetAddressAndPortSerializer.deserialize(input, VERSION_30);
                in.skipBytes(nio.position() - start);
                return new ConfirmOutboundPre40(version, address);
            }
            catch (IOException e)
            {
                // the bytes were counted above
                throw new IllegalStateException(e);
            }
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof ConfirmOutboundPre40))
                return false;

            ConfirmOutboundPre40 that = (ConfirmOutboundPre40) other;
            return this.maxMessagingVersion == that.maxMessagingVersion
                   && Objects.equals(this.from, that.from);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(maxMessagingVersion, from);
        }

        @Override
        public String toString()
        {
            return String.format("ConfirmOutboundPre40(maxMessagingVersion: %d; address: %s)", maxMessagingVersion, from);
        }
    }

    /** The third message of a pre-4.0 handshake names an address that is neither IPv4 nor IPv6 (DSE 6 compatibility mode). */
    static class InvalidPre40Address extends Exception
    {
        final int size;

        InvalidPre40Address(int size)
        {
            super("address of " + size + " bytes in the third message of the pre-4.0 handshake, neither 4 nor 16");
            this.size = size;
        }
    }

    private static int getBits(int packed, int start, int count)
    {
        return (packed >>> start) & ~(-1 << count);
    }

}
