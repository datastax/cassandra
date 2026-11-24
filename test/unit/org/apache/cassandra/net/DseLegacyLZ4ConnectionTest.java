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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoop;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4Factory;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.dsecompat.DseFixtures.DseVersion;
import org.apache.cassandra.io.IVersionedAsymmetricSerializer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.HandshakeProtocol.ConfirmOutboundPre40;
import org.apache.cassandra.net.OutboundConnectionInitiator.Result;
import org.apache.cassandra.net.OutboundConnectionInitiator.Result.MessagingSuccess;
import org.apache.cassandra.net.OutboundConnectionInitiator.SslFallbackConnectionType;
import org.apache.cassandra.net.OutboundConnectionSettings.Framing;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

import static org.apache.cassandra.net.ConnectionType.SMALL_MESSAGES;
import static org.apache.cassandra.net.LegacyLZ4FramingTest.LEGACY_VERB;
import static org.apache.cassandra.net.LegacyLZ4FramingTest.lz4JavaStream;
import static org.apache.cassandra.net.LegacyLZ4FramingTest.writeInChunks;
import static org.apache.cassandra.net.LegacyLZ4FramingTest.xxhash32;
import static org.apache.cassandra.net.MessagingService.VERSION_3014;
import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.net.OutboundConnectionInitiator.initiateMessaging;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Internode compression with a DSE 6 peer over real sockets, in both directions: a connection negotiated with the
 * compression bit of the pre-4.0 first message carries, after the third message, the LZ4 block stream that lz4-java
 * and Netty write by default, with the xxHash32 checksum (what DSE 6.8.65 was observed writing towards a peer at the
 * 3.0.14 dialect, with {@code internode_compression: all} between the products). A DSE node connecting to us has its stream
 * decompressed and handed to the pre-4.0 message decoder; when we connect to a DSE node with
 * {@code internode_compression: all}, our first message asks for compression and what we write is read back by
 * lz4-java's reader.
 */
public class DseLegacyLZ4ConnectionTest
{
    private static final SocketFactory factory = new SocketFactory();
    /** The compression bit (bit 2) of the flags of a pre-4.0 first message. */
    private static final int CMP = 1 << 2;
    /** The end of the log line of an inbound connection terminated by an unexpected exception. */
    private static final String TERMINATED = "unexpected exception caught while processing inbound messages; terminating connection";

    private static Supplier<? extends IVersionedAsymmetricSerializer<?, ?>> legacyVerbSerializer;

    private InternodeCompatibilityMode modeBefore;
    private Config.InternodeCompression compressionBefore;

    @BeforeClass
    public static void startup() throws NoSuchFieldException, IllegalAccessException
    {
        DatabaseDescriptor.daemonInitialization();
        legacyVerbSerializer = LegacyLZ4FramingTest.setTestSerializer();
    }

    @AfterClass
    public static void cleanup() throws InterruptedException, NoSuchFieldException, IllegalAccessException
    {
        LEGACY_VERB.unsafeSetSerializer(legacyVerbSerializer);
        factory.shutdownNow();
    }

    @Before
    public void dse6()
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        compressionBefore = DatabaseDescriptor.internodeCompression();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
    }

    @After
    public void restore()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
        DatabaseDescriptor.setInternodeCompression(compressionBefore);
    }

    private static byte[] bytes(ByteBuf buf)
    {
        byte[] out = new byte[buf.readableBytes()];
        buf.readBytes(out);
        buf.release();
        return out;
    }

    /**
     * A DSE node with {@code internode_compression: all} connects to us: its first message (captured from DSE 6.8.65
     * and 6.9.26) carries the compression bit, so the connection is set up at 11 with the legacy LZ4 framing, and
     * after the third message the node writes messages as an LZ4 block stream, here lz4-java's (64 KiB blocks, as DSE's
     * code has it; the xxHash32 checksum, as a trace of DSE 6.8.65 shows), flushed at arbitrary points. The stream is
     * decompressed block by block and handed to the pre-4.0 message decoder, which delivers every message.
     */
    @Test
    public void dseNodeSendsUsCompressedMessages() throws Exception
    {
        List<Message<?>> received = new CopyOnWriteArrayList<>();
        Predicate<Message<?>> sink = message -> {
            if (message.verb() != LEGACY_VERB)
                return true;
            received.add(message);
            return false;
        };
        Logger handlerLogger = (Logger) LoggerFactory.getLogger(InboundMessageHandler.class);
        Logger initiatorLogger = (Logger) LoggerFactory.getLogger(InboundConnectionInitiator.class);
        ListAppender<ILoggingEvent> log = new ListAppender<>();
        log.start();
        handlerLogger.addAppender(log);
        initiatorLogger.addAppender(log);
        MessagingService.instance().inboundSink.add(sink);

        InboundSockets inbound = new InboundSockets(new InboundConnectionSettings().withAcceptMessaging(null));
        inbound.open();
        try
        {
            InetAddressAndPort endpoint = inbound.sockets().stream().map(s -> s.settings.bindAddress).findFirst().get();
            int peerAddress = 78;
            for (DseVersion version : DseVersion.values())
            {
                InetAddressAndPort dsePeer = InetAddressAndPort.getByName("127.0.0." + peerAddress++);
                received.clear();
                try
                {
                    dseNodeSendsUsCompressedMessages(endpoint, version, dsePeer, received, log);
                }
                finally
                {
                    MessagingService.instance().versions.reset(dsePeer);
                }
            }
        }
        finally
        {
            MessagingService.instance().inboundSink.remove(sink);
            handlerLogger.detachAppender(log);
            initiatorLogger.detachAppender(log);
            inbound.close().await(1L, TimeUnit.SECONDS);
        }
    }

    private static void dseNodeSendsUsCompressedMessages(InetAddressAndPort endpoint, DseVersion version, InetAddressAndPort dsePeer,
                                                         List<Message<?>> received, ListAppender<ILoggingEvent> log) throws Exception
    {
        long seed = new SecureRandom().nextLong();
        Random random = new Random(seed);
        String context = version + ", seed " + seed;
        LegacyLZ4FramingTest.Messages messages = new LegacyLZ4FramingTest.Messages(random, 40, 0.1f, VERSION_3014);
        byte[] msg1 = DseFixtures.handshakes(version).get(0).msg1.clone();
        assertThat(msg1[7] & CMP).as("captured without compression").isZero();
        msg1[7] |= CMP; // CA 55 2D FA <relative version> 00 FF 04
        try (Socket socket = new Socket(endpoint.getAddress(), endpoint.getPort()))
        {
            socket.setSoTimeout(10_000);
            OutputStream out = socket.getOutputStream();
            DataInputStream in = new DataInputStream(socket.getInputStream());
            out.write(msg1);
            out.flush();
            byte[] reply = new byte[4];
            in.readFully(reply);
            assertThat(reply).containsExactly(0, 0, 0, DseCompat.DSE_DIALECT);
            out.write(bytes(new ConfirmOutboundPre40(DseCompat.DSE_DIALECT, dsePeer).encode()));
            out.flush();
            // the pipeline is the legacy LZ4 one, not the unframed legacy one (logged once it is installed)
            Util.spinAssertEquals("legacy LZ4 pipeline, " + context, 1,
                                  () -> events(log, dsePeer, "messaging connection established, version = 11, framing = LZ4").size(), 10, TimeUnit.SECONDS);

            // DSE starts compressing with the first byte after the third message; the stream is never finished while
            // the connection lives, so its end block is not written
            writeInChunks(random, messages.stream, lz4JavaStream(out, xxhash32()));

            Util.spinAssertEquals("every message delivered, " + context, messages.payloads.size(), received::size, 20, TimeUnit.SECONDS);
            assertThat(received.stream().map(m -> (byte[]) m.payload)).as(context).containsExactlyInAnyOrderElementsOf(messages.payloads);
            assertThat(events(log, dsePeer, TERMINATED)).as(context).isEmpty();
        }
    }

    /** The events logged about the connections of {@code peer} whose message ends with {@code suffix}. */
    private static List<ILoggingEvent> events(ListAppender<ILoggingEvent> log, InetAddressAndPort peer, String suffix)
    {
        String prefix = peer.toString();
        List<ILoggingEvent> result = new ArrayList<>();
        for (ILoggingEvent event : log.list.toArray(new ILoggingEvent[0]))
        {
            String message = event.getFormattedMessage();
            if (message.startsWith(prefix) && message.contains(suffix))
                result.add(event);
        }
        return result;
    }

    /**
     * With {@code internode_compression: all} we connect to a DSE node: the pre-4.0 first message at the dialect
     * carries the compression bit, the legacy LZ4 encoder is installed after the third message, and the payloads we
     * write, small and large, are read back by lz4-java's block stream reader verifying the xxHash32 checksum.
     */
    @Test
    public void weSendCompressedMessagesToADseNode() throws Exception
    {
        DatabaseDescriptor.setInternodeCompression(Config.InternodeCompression.all);
        long seed = new SecureRandom().nextLong();
        Random random = new Random(seed);
        byte[] stream = new LegacyLZ4FramingTest.Messages(random, 40, 0.2f, VERSION_40).stream;

        try (FakeCompressingDse dse = new FakeCompressingDse(stream.length))
        {
            OutboundConnectionSettings settings = new OutboundConnectionSettings(dse.endpoint()).withDefaults(ConnectionCategory.MESSAGING);
            assertThat(settings.framing).isEqualTo(Framing.LZ4);
            EventLoop eventLoop = factory.defaultGroup().next();
            Result<MessagingSuccess> result = initiateMessaging(eventLoop, SMALL_MESSAGES, SslFallbackConnectionType.SERVER_CONFIG, settings,
                                                                DseCompat.DSE_DIALECT, AsyncPromise.withExecutor(eventLoop)).get(20, TimeUnit.SECONDS);
            assertThat(result.outcome).as("seed %d", seed).isEqualTo(Result.Outcome.SUCCESS);
            MessagingSuccess success = result.success();
            try
            {
                assertThat(success.messagingVersion).isEqualTo(DseCompat.DSE_DIALECT);
                Channel channel = success.channel;
                assertThat(channel.pipeline().get("frameEncoder")).isSameAs(FrameEncoderLegacyLZ4.instance);
                ChannelFuture lastWrite = null;
                for (int offset = 0; offset < stream.length; )
                {
                    int length = Math.min(stream.length - offset, 1 + random.nextInt(FrameEncoder.Payload.MAX_SIZE));
                    FrameEncoder.Payload payload = success.allocator.allocate(true, length);
                    payload.buffer.put(stream, offset, length);
                    payload.finish();
                    lastWrite = channel.writeAndFlush(payload);
                    offset += length;
                }
                // writes complete in order: the last one reports a failure of any of them
                assertThat(lastWrite.await(20, TimeUnit.SECONDS)).as("seed %d", seed).isTrue();
                assertThat(lastWrite.cause()).as("seed %d", seed).isNull();
                dse.await(seed);
            }
            finally
            {
                success.channel.close();
            }

            assertThat(ByteBuffer.wrap(dse.firstMessage.get(), 0, 4).getInt()).as("magic").isEqualTo(Message.PROTOCOL_MAGIC);
            int flags = ByteBuffer.wrap(dse.firstMessage.get(), 4, 4).getInt();
            assertThat(flags & CMP).as("compression requested").isEqualTo(CMP);
            assertThat((flags >>> 8) & 0xFF).isEqualTo(DseCompat.DSE_DIALECT);
            assertThat(flags >>> 16).as("a pre-4.0 first message").isZero();
            assertThat(dse.received.get()).as("seed %d", seed).isEqualTo(stream);
        }
    }

    /**
     * The storage port of a DSE node reading a compressed connection: it answers a pre-4.0 first message with the
     * requested version, reads the third message, then decompresses the rest with lz4-java's block stream reader,
     * which verifies the xxHash32 checksum of every block, until it has the expected number of bytes.
     */
    private static final class FakeCompressingDse implements AutoCloseable
    {
        final ServerSocket server;
        final AtomicReference<byte[]> firstMessage = new AtomicReference<>();
        final AtomicReference<byte[]> received = new AtomicReference<>();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread thread;

        FakeCompressingDse(int expectedBytes) throws IOException
        {
            server = new ServerSocket(0, 1, InetAddressAndPort.getByName("127.0.0.1").getAddress());
            thread = new Thread(() -> {
                try (Socket socket = server.accept())
                {
                    socket.setSoTimeout(20_000);
                    DataInputStream in = new DataInputStream(socket.getInputStream());
                    byte[] first = new byte[8];
                    in.readFully(first);
                    firstMessage.set(first);
                    new DataOutputStream(socket.getOutputStream()).writeInt((first[6] & 0xFF));
                    socket.getOutputStream().flush();
                    // the third message: version, then the address with a 1-byte length
                    in.readInt();
                    in.readFully(new byte[in.readUnsignedByte()]);
                    DataInputStream lz4 = new DataInputStream(new LZ4BlockInputStream(in, LZ4Factory.fastestInstance().fastDecompressor(), xxhash32()));
                    byte[] bytes = new byte[expectedBytes];
                    lz4.readFully(bytes);
                    received.set(bytes);
                }
                catch (Throwable t)
                {
                    failure.set(t);
                }
            }, "fake-compressing-dse");
            thread.start();
        }

        InetAddressAndPort endpoint()
        {
            return InetAddressAndPort.getByAddressOverrideDefaults(server.getInetAddress(), server.getLocalPort());
        }

        void await(long seed) throws InterruptedException
        {
            thread.join(30_000);
            assertThat(thread.isAlive()).as("seed %d", seed).isFalse();
            assertThat(failure.get()).as("seed %d", seed).isNull();
        }

        @Override
        public void close() throws Exception
        {
            server.close();
            thread.join(30_000);
        }
    }
}
