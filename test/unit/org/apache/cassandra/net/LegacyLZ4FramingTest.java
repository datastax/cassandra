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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.compression.Lz4FrameDecoder;
import io.netty.handler.codec.compression.Lz4FrameEncoder;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.io.IVersionedAsymmetricSerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.memory.BufferPools;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.net.MessagingService.VERSION_3014;
import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.net.OutboundConnections.LARGE_MESSAGE_THRESHOLD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The legacy LZ4 block stream of the DSE 6 compatibility mode, end to end through the real inbound pipeline:
 * {@code FrameDecoderLegacyLZ4.LZ4Decoder} hands each decompressed block to {@link FrameDecoderLegacy}, which turns
 * the stream of messages into frames. The streams come from our {@link FrameEncoderLegacyLZ4} and from the two
 * independent writers of the format, lz4-java's {@link LZ4BlockOutputStream} (the pre-4.0 Cassandra writer) and
 * Netty's {@link Lz4FrameEncoder}; both carry the xxHash32 checksum with seed {@code 0x9747B28C}, keeping its low 28
 * bits, which the decoder verifies. The bytes are fed in arbitrary chunks and carry many messages, some of them
 * large. The opposite direction checks that both readers accept what our encoder writes.
 * <p>
 * The messages are serialized at 4.0 and at the 3.0.14 dialect, whose layouts {@link FrameDecoderLegacy} both reads.
 */
public class LegacyLZ4FramingTest
{
    private static final Logger logger = LoggerFactory.getLogger(LegacyLZ4FramingTest.class);

    /** The seed of the xxHash32 checksum of the pre-4.0 LZ4 block stream (lz4-java's and Netty's default). */
    static final int XXHASH_SEED = 0x9747B28C;
    /** The block size of lz4-java's {@link LZ4BlockOutputStream} and of Netty's {@link Lz4FrameEncoder} by default. */
    static final int DEFAULT_WRITER_BLOCK_SIZE = 1 << 16;

    private static final int ITERATIONS = 25;

    /**
     * The verb of the test messages: the pre-4.0 dialect carries only the request verbs of Cassandra 3.0, so one of
     * them carries a length-prefixed byte array for the duration of a test class.
     */
    static final Verb LEGACY_VERB = Verb.ECHO_REQ;
    private static Supplier<? extends IVersionedAsymmetricSerializer<?, ?>> legacyVerbSerializer;

    private InternodeCompatibilityMode modeBefore;

    @BeforeClass
    public static void begin() throws NoSuchFieldException, IllegalAccessException
    {
        DatabaseDescriptor.daemonInitialization();
        legacyVerbSerializer = setTestSerializer();
    }

    @AfterClass
    public static void end() throws NoSuchFieldException, IllegalAccessException
    {
        LEGACY_VERB.unsafeSetSerializer(legacyVerbSerializer);
    }

    @Before
    public void dse6()
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        // the legacy pipeline exists only in DSE 6 compatibility mode
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
    }

    @After
    public void restoreMode()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
    }

    /** Makes {@link #LEGACY_VERB} carry a length-prefixed byte array; returns its serializer, to restore it. */
    static Supplier<? extends IVersionedAsymmetricSerializer<?, ?>> setTestSerializer() throws NoSuchFieldException, IllegalAccessException
    {
        return LEGACY_VERB.unsafeSetSerializer(() -> new IVersionedSerializer<byte[]>()
        {
            public void serialize(byte[] t, DataOutputPlus out, int version) throws IOException
            {
                out.writeUnsignedVInt32(t.length);
                out.write(t);
            }

            public byte[] deserialize(DataInputPlus in, int version) throws IOException
            {
                byte[] r = new byte[in.readUnsignedVInt32()];
                in.readFully(r);
                return r;
            }

            public long serializedSize(byte[] t, int version)
            {
                return VIntCoding.computeUnsignedVIntSize(t.length) + t.length;
            }
        });
    }

    // ---- the messages ----

    static final class Messages
    {
        final List<byte[]> payloads = new ArrayList<>();
        final List<byte[]> serialized = new ArrayList<>();
        final byte[] stream;

        Messages(Random random, int count, float largeRatio, int version)
        {
            ByteArrayOutputStream all = new ByteArrayOutputStream();
            for (int i = 0; i < count; i++)
            {
                byte[] payload = random.nextFloat() < largeRatio
                                 ? FramingTest.randomishBytes(random, LARGE_MESSAGE_THRESHOLD + 1, 3 * LARGE_MESSAGE_THRESHOLD)
                                 : FramingTest.randomishBytes(random, 1, 1 << 13);
                // a quarter of the payloads do not compress, so that the writers also emit uncompressed blocks
                if (random.nextInt(4) == 0)
                    random.nextBytes(payload);
                byte[] message = serialize(Message.out(LEGACY_VERB, payload), version);
                payloads.add(payload);
                serialized.add(message);
                all.write(message, 0, message.length);
            }
            stream = all.toByteArray();
        }
    }

    static byte[] serialize(Message<byte[]> message, int version)
    {
        try (DataOutputBuffer out = new DataOutputBuffer(message.serializedSize(version)))
        {
            Message.serializer.serialize(message, out, version);
            return out.toByteArray();
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    // ---- the writers of the legacy LZ4 block stream ----

    /** Our encoder, fed payloads of arbitrary sizes as the outbound connection does (up to several LZ4 blocks each). */
    static byte[] encodeWithOurEncoder(Random random, byte[] stream)
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int offset = 0; offset < stream.length; )
        {
            int length = Math.min(stream.length - offset, 1 + random.nextInt(3 * LegacyLZ4Constants.DEFAULT_BLOCK_LENGTH));
            byte[] frame = encodePayload(stream, offset, length);
            out.write(frame, 0, frame.length);
            offset += length;
        }
        return out.toByteArray();
    }

    /** One payload through our encoder: as many LZ4 blocks as it takes 32 KiB blocks to hold it. */
    private static byte[] encodePayload(byte[] bytes, int offset, int length)
    {
        FrameEncoder encoder = FrameEncoderLegacyLZ4.instance;
        FrameEncoder.Payload payload = encoder.allocator().allocate(true, length);
        payload.buffer.put(bytes, offset, length);
        payload.finish();
        ByteBuf frame = encoder.encode(true, payload.buffer);
        try
        {
            byte[] result = new byte[frame.readableBytes()];
            frame.readBytes(result);
            return result;
        }
        finally
        {
            frame.release();
        }
    }

    /** lz4-java's block stream, flushed at arbitrary points and finished with its empty end block. */
    static byte[] encodeWithLz4Java(Random random, byte[] stream, Checksum checksum) throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (LZ4BlockOutputStream lz4 = lz4JavaStream(out, checksum))
        {
            writeInChunks(random, stream, lz4);
        }
        return out.toByteArray();
    }

    static LZ4BlockOutputStream lz4JavaStream(OutputStream out, Checksum checksum)
    {
        return new LZ4BlockOutputStream(out, DEFAULT_WRITER_BLOCK_SIZE, LZ4Factory.fastestInstance().fastCompressor(), checksum, true);
    }

    static void writeInChunks(Random random, byte[] stream, OutputStream out) throws IOException
    {
        for (int offset = 0; offset < stream.length; )
        {
            int length = Math.min(stream.length - offset, 1 + random.nextInt(2 * DEFAULT_WRITER_BLOCK_SIZE));
            out.write(stream, offset, length);
            if (random.nextBoolean())
                out.flush();
            offset += length;
        }
        out.flush();
    }

    /** The xxHash32 checksum of the pre-4.0 stream, as lz4-java and Netty compute it by default. */
    static Checksum xxhash32()
    {
        return XXHashFactory.fastestInstance().newStreamingHash32(XXHASH_SEED).asChecksum();
    }

    /** Netty's LZ4 frame encoder, flushed after every write of arbitrary size, then closed (writing its end block). */
    private static byte[] encodeWithNetty(Random random, byte[] stream, Lz4FrameEncoder encoder)
    {
        EmbeddedChannel channel = new EmbeddedChannel(encoder);
        for (int offset = 0; offset < stream.length; )
        {
            int length = Math.min(stream.length - offset, 1 + random.nextInt(2 * DEFAULT_WRITER_BLOCK_SIZE));
            assertThat(channel.writeOutbound(Unpooled.wrappedBuffer(stream, offset, length))).isTrue();
            offset += length;
        }
        channel.close();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (ByteBuf buf = channel.readOutbound(); buf != null; buf = channel.readOutbound())
        {
            byte[] bytes = new byte[buf.readableBytes()];
            buf.readBytes(bytes);
            buf.release();
            out.write(bytes, 0, bytes.length);
        }
        return out.toByteArray();
    }

    // ---- the reader: the real legacy LZ4 inbound pipeline ----

    /** A frame as {@link FrameDecoderLegacy} delivered it: where it starts in the stream, its length, and its kind. */
    private static final class DeliveredFrame
    {
        final int offset;
        final int length;
        final boolean isSelfContained;

        DeliveredFrame(int offset, int length, boolean isSelfContained)
        {
            this.offset = offset;
            this.length = length;
            this.isSelfContained = isSelfContained;
        }
    }

    /**
     * {@code legacyLZ4Decoder → frameDecoderNone}, as {@code InboundConnectionInitiator} installs them for a pre-4.0
     * peer with compression, with a spy between the two recording the decompressed blocks the LZ4 decoder hands on.
     */
    static final class LegacyLZ4Pipeline implements AutoCloseable
    {
        final EmbeddedChannel channel = new EmbeddedChannel();
        final FrameDecoderLegacyLZ4 decoder;
        final List<ShareableBytes> handedOn = new ArrayList<>();
        private final ByteArrayOutputStream delivered = new ByteArrayOutputStream();
        private final List<DeliveredFrame> frames = new ArrayList<>();

        LegacyLZ4Pipeline(int version)
        {
            decoder = new FrameDecoderLegacyLZ4(GlobalBufferPoolAllocator.instance, version);
            decoder.addLastTo(channel.pipeline());
            assertThat(channel.pipeline().names()).startsWith("legacyLZ4Decoder", "frameDecoderNone");
            channel.pipeline().addBefore("frameDecoderNone", "spy", new ChannelInboundHandlerAdapter()
            {
                @Override
                public void channelRead(ChannelHandlerContext ctx, Object msg)
                {
                    assertThat(msg).isInstanceOf(ShareableBytes.class);
                    handedOn.add((ShareableBytes) msg);
                    ctx.fireChannelRead(msg);
                }
            });
            decoder.activate(frame -> {
                assertThat(frame).isInstanceOf(FrameDecoder.IntactFrame.class);
                FrameDecoder.IntactFrame intact = (FrameDecoder.IntactFrame) frame;
                byte[] bytes = new byte[intact.contents.remaining()];
                intact.contents.get().duplicate().get(bytes);
                frames.add(new DeliveredFrame(delivered.size(), bytes.length, intact.isSelfContained));
                delivered.write(bytes, 0, bytes.length);
                intact.consume();
                return true;
            });
        }

        /** Feeds the compressed stream in chunks of arbitrary sizes, as the network would deliver them. */
        void feed(Random random, byte[] compressed)
        {
            for (int offset = 0; offset < compressed.length; )
            {
                int length = Math.min(compressed.length - offset, chunkLength(random));
                ByteBuffer buffer = BufferPools.forNetworking().getAtLeast(length, BufferType.OFF_HEAP);
                buffer.put(compressed, offset, length);
                buffer.flip();
                channel.writeInbound(GlobalBufferPoolAllocator.wrap(buffer));
                offset += length;
            }
        }

        private static int chunkLength(Random random)
        {
            switch (random.nextInt(4))
            {
                case 0: return 1 + random.nextInt(LegacyLZ4Constants.HEADER_LENGTH + 2); // splits the block header
                case 1: return 1 + random.nextInt(1 << 10);
                case 2: return 1 + random.nextInt(1 << 16);
                default: return 1 + random.nextInt(1 << 18);
            }
        }

        byte[] delivered()
        {
            return delivered.toByteArray();
        }

        /** The frames carrying a part of a large message. */
        long partialFrames()
        {
            return frames.stream().filter(f -> !f.isSelfContained).count();
        }

        /**
         * Everything fed was delivered and is released: the frames follow the message boundaries, nothing is stashed
         * or queued, and every block the LZ4 decoder handed on was released exactly once (a second release throws).
         */
        void assertDeliveredExactly(Messages messages, int version) throws IOException
        {
            assertThat(delivered()).isEqualTo(messages.stream);

            int[] starts = new int[messages.serialized.size() + 1];
            for (int i = 0; i < messages.serialized.size(); i++)
                starts[i + 1] = starts[i] + messages.serialized.get(i).length;
            for (DeliveredFrame frame : frames)
            {
                assertThat(frame.length).isPositive();
                int first = Arrays.binarySearch(starts, frame.offset);
                if (frame.isSelfContained)
                {
                    // one or more whole messages, none of them large
                    assertThat(first).as("a self-contained frame starts a message").isNotNegative();
                    int end = Arrays.binarySearch(starts, frame.offset + frame.length);
                    assertThat(end).as("and ends one").isNotNegative();
                    for (int message = first; message < end; message++)
                        assertThat(messages.serialized.get(message).length)
                        .as("message %d in the self-contained frame at %d", message, frame.offset)
                        .isLessThanOrEqualTo(LARGE_MESSAGE_THRESHOLD);
                }
                else
                {
                    // a part of exactly one large message
                    int message = first >= 0 ? first : -first - 2;
                    assertThat(messages.serialized.get(message).length).isGreaterThan(LARGE_MESSAGE_THRESHOLD);
                    assertThat(frame.offset + frame.length).isLessThanOrEqualTo(starts[message + 1]);
                }
            }

            InetAddressAndPort peer = FBUtilities.getBroadcastAddressAndPort();
            try (DataInputBuffer in = new DataInputBuffer(delivered()))
            {
                for (byte[] payload : messages.payloads)
                {
                    Message<byte[]> message = Message.serializer.deserialize(in, peer, version);
                    assertThat(message.verb()).isEqualTo(LEGACY_VERB);
                    assertThat(message.payload).isEqualTo(payload);
                }
                assertThat(in.available()).isZero();
            }

            assertThat(decoder.stash).isNull();
            assertThat(decoder.frames).isEmpty();
            assertThat(handedOn).isNotEmpty().allMatch(ShareableBytes::isReleased);
        }

        @Override
        public void close()
        {
            channel.close();
        }
    }

    // ---- the tests ----

    private interface Writer
    {
        byte[] write(Random random, byte[] stream) throws IOException;
    }

    private static void throughTheLegacyPipeline(String writerName, int version, Writer writer) throws IOException
    {
        SecureRandom seeds = new SecureRandom();
        int[] blockKinds = new int[2];
        for (float largeRatio : new float[]{ 0.0f, 0.2f, 1.0f })
        {
            for (int i = 0; i < ITERATIONS; i++)
            {
                long seed = seeds.nextLong();
                Random random = new Random(seed);
                Messages messages = new Messages(random, 1 + random.nextInt(largeRatio > 0.5f ? 12 : 64), largeRatio, version);
                byte[] compressed = writer.write(random, messages.stream);
                try (LegacyLZ4Pipeline pipeline = new LegacyLZ4Pipeline(version))
                {
                    countBlockKinds(compressed, blockKinds);
                    pipeline.feed(random, compressed);
                    pipeline.assertDeliveredExactly(messages, version);
                    if (largeRatio == 1.0f)
                        assertThat(pipeline.partialFrames()).as("large messages are delivered in parts").isPositive();
                }
                catch (Throwable t)
                {
                    logger.error("writer {}, version {}, large ratio {}, seed {}", writerName, version, largeRatio, seed);
                    throw t;
                }
            }
        }
        assertThat(blockKinds[0]).as("%s wrote compressed blocks", writerName).isPositive();
        assertThat(blockKinds[1]).as("%s wrote uncompressed blocks", writerName).isPositive();
    }

    /**
     * Walks the block headers of a legacy LZ4 stream, counting the compressed blocks into {@code counts[0]} and the
     * uncompressed ones with data into {@code counts[1]}.
     */
    private static void countBlockKinds(byte[] stream, int[] counts)
    {
        ByteBuffer bigEndian = ByteBuffer.wrap(stream);
        ByteBuffer littleEndian = ByteBuffer.wrap(stream).order(ByteOrder.LITTLE_ENDIAN);
        int offset = 0;
        while (offset < stream.length)
        {
            assertThat(bigEndian.getLong(offset + LegacyLZ4Constants.MAGIC_NUMBER_OFFSET)).isEqualTo(LegacyLZ4Constants.MAGIC_NUMBER);
            int blockType = stream[offset + LegacyLZ4Constants.TOKEN_OFFSET] & 0xF0;
            int compressedLength = littleEndian.getInt(offset + LegacyLZ4Constants.COMPRESSED_LENGTH_OFFSET);
            if (blockType == LegacyLZ4Constants.BLOCK_TYPE_COMPRESSED)
                counts[0]++;
            else if (compressedLength > 0)
                counts[1]++;
            offset += LegacyLZ4Constants.HEADER_LENGTH + compressedLength;
        }
        assertThat(offset).isEqualTo(stream.length);
    }

    private static final Writer OUR_ENCODER = LegacyLZ4FramingTest::encodeWithOurEncoder;
    /** The format of the pre-4.0 Cassandra writer: lz4-java's block stream with its default xxHash32 checksum. */
    private static final Writer LZ4_JAVA = (random, stream) -> encodeWithLz4Java(random, stream, xxhash32());
    /** Netty's LZ4 frame encoder with its default checksum, xxHash32 with the same seed. */
    private static final Writer NETTY = (random, stream) -> encodeWithNetty(random, stream, new Lz4FrameEncoder());
    /** Netty's LZ4 frame encoder given lz4-java's xxHash32 checksum explicitly. */
    private static final Writer NETTY_XXHASH32 = (random, stream) -> encodeWithNetty(random, stream, new Lz4FrameEncoder(LZ4Factory.fastestInstance(), false,
                                                                                                                        DEFAULT_WRITER_BLOCK_SIZE, xxhash32()));

    @Test
    public void ourEncoderThroughTheLegacyPipeline() throws IOException
    {
        throughTheLegacyPipeline("FrameEncoderLegacyLZ4", VERSION_40, OUR_ENCODER);
    }

    @Test
    public void ourEncoderThroughTheLegacyPipelineAtTheDialect() throws IOException
    {
        throughTheLegacyPipeline("FrameEncoderLegacyLZ4", VERSION_3014, OUR_ENCODER);
    }

    @Test
    public void lz4JavaBlockStreamThroughTheLegacyPipeline() throws IOException
    {
        throughTheLegacyPipeline("LZ4BlockOutputStream", VERSION_40, LZ4_JAVA);
    }

    @Test
    public void lz4JavaBlockStreamThroughTheLegacyPipelineAtTheDialect() throws IOException
    {
        throughTheLegacyPipeline("LZ4BlockOutputStream", VERSION_3014, LZ4_JAVA);
    }

    /**
     * Netty's LZ4 frame encoder writes the same format; as DSE's code reads (not checked on the
     * wire), DSE 6 builds its compressed connections on it, with xxHash32 for a peer below its own 6.8 protocol, the
     * checksum DSE 6.8.65 was observed writing at the dialect.
     */
    @Test
    public void nettyLz4FrameEncoderThroughTheLegacyPipeline() throws IOException
    {
        throughTheLegacyPipeline("Lz4FrameEncoder", VERSION_40, NETTY);
        throughTheLegacyPipeline("Lz4FrameEncoder(xxhash32)", VERSION_40, NETTY_XXHASH32);
    }

    @Test
    public void nettyLz4FrameEncoderThroughTheLegacyPipelineAtTheDialect() throws IOException
    {
        throughTheLegacyPipeline("Lz4FrameEncoder", VERSION_3014, NETTY);
        throughTheLegacyPipeline("Lz4FrameEncoder(xxhash32)", VERSION_3014, NETTY_XXHASH32);
    }

    /** A stream of nothing but end blocks (compressed and uncompressed lengths zero) delivers nothing and releases everything. */
    @Test
    public void emptyBlocksDeliverNothing() throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (int i = 0; i < 3; i++)
            lz4JavaStream(out, xxhash32()).close();
        byte[] endBlocks = out.toByteArray();
        assertThat(endBlocks).hasSize(3 * LegacyLZ4Constants.HEADER_LENGTH);
        try (LegacyLZ4Pipeline pipeline = new LegacyLZ4Pipeline(VERSION_40))
        {
            pipeline.feed(new Random(1), endBlocks);
            assertThat(pipeline.delivered()).isEmpty();
            assertThat(pipeline.handedOn).hasSize(3).allMatch(ShareableBytes::isReleased);
            assertThat(pipeline.decoder.stash).isNull();
            assertThat(pipeline.decoder.frames).isEmpty();
        }
    }

    /**
     * A fatal error on the first frame of a read that carries several LZ4 blocks: the handler after the decoder
     * discards it and closes the channel, as {@link InboundMessageHandler} does, while the LZ4 decoder still hands on
     * the other blocks of the read. They are dropped (released unread) instead of being decoded into frames that
     * nothing would ever release, and the close reaches the handlers after the decoder.
     */
    @Test
    public void blocksAfterAFatalErrorAreDroppedAndTheCloseReachesTheHandler() throws IOException
    {
        // one message per LZ4 block, so that each block after the failure would decode into a whole frame
        long seed = new SecureRandom().nextLong();
        Random random = new Random(seed);
        Messages messages = new Messages(random, 16, 0.0f, VERSION_40);
        ByteArrayOutputStream blocks = new ByteArrayOutputStream();
        for (byte[] message : messages.serialized)
        {
            byte[] block = encodePayload(message, 0, message.length);
            blocks.write(block, 0, block.length);
        }
        byte[] compressed = blocks.toByteArray();

        EmbeddedChannel channel = new EmbeddedChannel();
        try
        {
            assertFatalErrorDropsTheRestOfTheRead(channel, messages, compressed, seed);
        }
        finally
        {
            channel.finishAndReleaseAll();
        }
    }

    private static void assertFatalErrorDropsTheRestOfTheRead(EmbeddedChannel channel, Messages messages, byte[] compressed, long seed)
    {
        FrameDecoderLegacyLZ4 decoder = new FrameDecoderLegacyLZ4(GlobalBufferPoolAllocator.instance, VERSION_40);
        decoder.addLastTo(channel.pipeline());
        List<ShareableBytes> handedOn = new ArrayList<>();
        channel.pipeline().addBefore("frameDecoderNone", "spy", new ChannelInboundHandlerAdapter()
        {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg)
            {
                handedOn.add((ShareableBytes) msg);
                ctx.fireChannelRead(msg);
            }
        });
        List<Throwable> caught = new ArrayList<>();
        List<Boolean> inactive = new ArrayList<>();
        channel.pipeline().addLast("handler", new ChannelInboundHandlerAdapter()
        {
            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            {
                caught.add(cause);
                decoder.discard();
                ctx.channel().close();
            }

            @Override
            public void channelInactive(ChannelHandlerContext ctx)
            {
                inactive.add(true);
            }
        });
        decoder.activate(frame -> { throw new IOException("fatal"); });

        // one read carrying every block
        ByteBuffer buffer = BufferPools.forNetworking().getAtLeast(compressed.length, BufferType.OFF_HEAP);
        buffer.put(compressed);
        buffer.flip();
        channel.writeInbound(GlobalBufferPoolAllocator.wrap(buffer));
        channel.runPendingTasks();

        assertThat(handedOn).as("one block per message, all in the read, seed %d", seed).hasSize(messages.serialized.size());
        assertThat(caught).as("seed %d", seed).hasSize(1);
        assertThat(caught.get(0)).hasMessage("fatal");
        assertThat(decoder.frames).as("nothing queued after the failure, seed %d", seed).isEmpty();
        assertThat(decoder.stash).isNull();
        assertThat(handedOn).allMatch(ShareableBytes::isReleased);
        assertThat(channel.isActive()).isFalse();
        assertThat(inactive).as("the close reached the handler, seed %d", seed).containsExactly(true);
    }

    /**
     * Only the pipeline of {@link FrameDecoderLegacyLZ4} hands {@link ShareableBytes} to a frame decoder: every other
     * frame decoder, including the unframed legacy one, still refuses them, in either mode.
     */
    @Test
    public void otherFrameDecodersStillRefuseShareableBytes()
    {
        for (InternodeCompatibilityMode mode : InternodeCompatibilityMode.values())
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(mode);
            BufferPoolAllocator allocator = GlobalBufferPoolAllocator.instance;
            for (FrameDecoder decoder : new FrameDecoder[]{ FrameDecoderCrc.create(allocator), FrameDecoderLZ4.fast(allocator),
                                                            new FrameDecoderUnprotected(allocator), new FrameDecoderLegacy(allocator, VERSION_40) })
            {
                EmbeddedChannel channel = new EmbeddedChannel();
                try
                {
                    decoder.addLastTo(channel.pipeline());
                    // a heap buffer outside the pool: the refusal does not take ownership of it
                    ShareableBytes bytes = ShareableBytes.wrap(ByteBuffer.allocate(16));
                    assertThatThrownBy(() -> channel.writeInbound(bytes))
                    .as("%s in %s", decoder.getClass().getSimpleName(), mode)
                    .isInstanceOf(IllegalArgumentException.class);
                    assertThat(bytes.isReleased()).isFalse();
                }
                finally
                {
                    channel.close();
                }
            }
        }
    }

    /**
     * A block whose header or data is corrupt is refused before anything is delivered, and closing the channel releases
     * what the decoders hold: a bad magic number, a bad block type, a compressed length above 32 MiB, an uncompressed
     * length above what the token allows, lengths that do not match, data that does not decompress, data that
     * decompresses to fewer bytes than the header says.
     */
    @Test
    public void corruptBlocksAreRefused() throws IOException
    {
        long seed = new SecureRandom().nextLong();
        Random random = new Random(seed);
        byte[] data = new byte[1 << 12];
        Arrays.fill(data, (byte) 7); // compresses well: the block is a compressed one
        byte[] block = encodePayload(data, 0, data.length);
        int token = block[LegacyLZ4Constants.TOKEN_OFFSET] & 0xFF;
        assertThat(token & 0xF0).isEqualTo(LegacyLZ4Constants.BLOCK_TYPE_COMPRESSED);
        int compressedLength = littleEndianInt(block, LegacyLZ4Constants.COMPRESSED_LENGTH_OFFSET);

        assertCorrupt(random, seed, corrupt(block, b -> b[LegacyLZ4Constants.MAGIC_NUMBER_OFFSET] ^= 1),
                      "Invalid magic number at the beginning of an LZ4 block: ");
        assertCorrupt(random, seed, corrupt(block, b -> b[LegacyLZ4Constants.TOKEN_OFFSET] = (byte) (0x30 | (token & 0x0F))),
                      "Invalid block type encountered: 48");
        assertCorrupt(random, seed, corrupt(block, b -> putLittleEndianInt(b, LegacyLZ4Constants.COMPRESSED_LENGTH_OFFSET, LegacyLZ4Constants.MAX_BLOCK_LENGTH + 1)),
                      "Invalid compressedLength: ");
        int maxUncompressed = 1 << ((token & 0x0F) + 10);
        assertCorrupt(random, seed, corrupt(block, b -> putLittleEndianInt(b, LegacyLZ4Constants.UNCOMPRESSED_LENGTH_OFFSET, maxUncompressed + 1)),
                      "Invalid uncompressedLength: ");
        assertCorrupt(random, seed, corrupt(block, b -> b[LegacyLZ4Constants.TOKEN_OFFSET] = (byte) (LegacyLZ4Constants.BLOCK_TYPE_NON_COMPRESSED | (token & 0x0F))),
                      "Stream corrupted: compressedLength(" + compressedLength + ") and decompressedLength(" + data.length + ") mismatch");
        assertCorrupt(random, seed, corrupt(block, b -> Arrays.fill(b, LegacyLZ4Constants.HEADER_LENGTH, b.length, (byte) 0xFF)), null);
        assertCorrupt(random, seed, corrupt(block, b -> putLittleEndianInt(b, LegacyLZ4Constants.UNCOMPRESSED_LENGTH_OFFSET, data.length + 1)),
                      "Stream corrupted: " + data.length + " bytes decompressed, " + (data.length + 1) + " expected");
    }

    private interface Corruption
    {
        void apply(byte[] block);
    }

    private static byte[] corrupt(byte[] block, Corruption corruption)
    {
        byte[] corrupt = block.clone();
        corruption.apply(corrupt);
        return corrupt;
    }

    private static int littleEndianInt(byte[] bytes, int offset)
    {
        return ByteBuffer.wrap(bytes, offset, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    private static void putLittleEndianInt(byte[] bytes, int offset, int value)
    {
        ByteBuffer.wrap(bytes, offset, 4).order(ByteOrder.LITTLE_ENDIAN).putInt(value);
    }

    /** {@code message} null: data that does not decompress, which lz4-java or the length check refuses. */
    private static void assertCorrupt(Random random, long seed, byte[] stream, String message)
    {
        LegacyLZ4Pipeline pipeline = new LegacyLZ4Pipeline(VERSION_40);
        try
        {
            if (message == null)
                assertThatThrownBy(() -> pipeline.feed(random, stream)).as("seed %d", seed).isInstanceOfAny(LZ4Exception.class, IOException.class);
            else
                assertThatThrownBy(() -> pipeline.feed(random, stream)).as("seed %d", seed).hasMessageStartingWith(message);
            assertThat(pipeline.delivered()).isEmpty();
        }
        finally
        {
            pipeline.close();
        }
        assertThat(pipeline.handedOn).allMatch(ShareableBytes::isReleased);
        assertThat(pipeline.decoder.stash).isNull();
        assertThat(pipeline.decoder.frames).isEmpty();
    }

    /**
     * A block stream without the xxHash32 checksum (zero in its place), or with another checksum, is refused as
     * corrupt. CRC32 is the checksum DSE's code gives DSE 6.8 between peers at its own 6.8 protocol (not checked on the
     * wire), never at the 3.0.14 dialect.
     */
    @Test
    public void streamsWithoutTheXxHash32ChecksumAreRefused() throws IOException
    {
        long seed = new SecureRandom().nextLong();
        Random random = new Random(seed);
        byte[] data = FramingTest.randomishBytes(random, 1 << 10, 1 << 12);
        byte[] noChecksum = encodeWithLz4Java(random, data, new ZeroChecksum());
        byte[] crc32 = encodeWithNetty(random, data, new Lz4FrameEncoder(LZ4Factory.fastestInstance(), false, DEFAULT_WRITER_BLOCK_SIZE, new CRC32()));
        for (byte[] compressed : new byte[][]{ noChecksum, crc32 })
        {
            try (LegacyLZ4Pipeline pipeline = new LegacyLZ4Pipeline(VERSION_40))
            {
                assertThatThrownBy(() -> pipeline.feed(random, compressed))
                .as("seed %d", seed)
                .isInstanceOf(IOException.class)
                .hasMessageStartingWith("Invalid checksum detected: ");
                assertThat(pipeline.delivered()).isEmpty();
            }
        }
    }

    /** No checksum: lz4-java writes the value of the {@link Checksum} it is given, here always zero. */
    private static final class ZeroChecksum implements Checksum
    {
        public void update(int b) {}
        public void update(byte[] b, int off, int len) {}
        public long getValue() { return 0; }
        public void reset() {}
    }

    /** Up to 256 KiB, half of the time incompressible, so that our encoder writes both kinds of blocks. */
    private static byte[] someData(Random random)
    {
        byte[] data = FramingTest.randomishBytes(random, 1, 1 << 18);
        if (random.nextBoolean())
            random.nextBytes(data);
        return data;
    }

    /** What our encoder writes is read back by lz4-java's block stream reader, which verifies the checksum. */
    @Test
    public void ourEncoderIsReadByLz4Java() throws IOException
    {
        SecureRandom seeds = new SecureRandom();
        for (int i = 0; i < ITERATIONS; i++)
        {
            long seed = seeds.nextLong();
            Random random = new Random(seed);
            byte[] data = someData(random);
            byte[] compressed = encodeWithOurEncoder(random, data);
            byte[] read = new byte[data.length];
            try (DataInputStream in = new DataInputStream(new LZ4BlockInputStream(new ByteArrayInputStream(compressed),
                                                                                  LZ4Factory.fastestInstance().fastDecompressor(),
                                                                                  xxhash32())))
            {
                in.readFully(read);
            }
            assertThat(read).as("seed %d", seed).isEqualTo(data);
        }
    }

    /**
     * What our encoder writes is read back by Netty's LZ4 frame decoder, which verifies the checksum; as DSE's code reads,
     * it is the reader of DSE 6, and DSE 6.8.65 was observed reading the compressed connections of this software.
     */
    @Test
    public void ourEncoderIsReadByNetty()
    {
        SecureRandom seeds = new SecureRandom();
        for (int i = 0; i < ITERATIONS; i++)
        {
            long seed = seeds.nextLong();
            Random random = new Random(seed);
            byte[] data = someData(random);
            byte[] compressed = encodeWithOurEncoder(random, data);
            EmbeddedChannel channel = new EmbeddedChannel(new Lz4FrameDecoder(LZ4Factory.fastestInstance(), xxhash32()));
            ByteArrayOutputStream read = new ByteArrayOutputStream();
            try
            {
                for (int offset = 0; offset < compressed.length; )
                {
                    int length = Math.min(compressed.length - offset, 1 + random.nextInt(1 << 15));
                    channel.writeInbound(Unpooled.wrappedBuffer(compressed, offset, length));
                    offset += length;
                }
                for (ByteBuf buf = channel.readInbound(); buf != null; buf = channel.readInbound())
                {
                    byte[] bytes = new byte[buf.readableBytes()];
                    buf.readBytes(bytes);
                    buf.release();
                    read.write(bytes, 0, bytes.length);
                }
            }
            finally
            {
                channel.finishAndReleaseAll();
            }
            assertThat(read.toByteArray()).as("seed %d", seed).isEqualTo(data);
        }
    }
}
