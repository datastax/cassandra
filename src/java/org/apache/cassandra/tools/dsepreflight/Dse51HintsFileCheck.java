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

package org.apache.cassandra.tools.dsepreflight;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.zip.CRC32;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.hints.Dse51HintsCompression;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JsonUtils;

/**
 * Reads a hints file of version 1 to its end as DSE 5.1.51 frames it, for the rollback check
 * ({@link Dse51RollbackInspection}): its name against its descriptor, the compression of its descriptor, and each entry
 * (its size and the checksum of the size, the hint and its checksum), through the decompressor the descriptor names,
 * without decoding the mutations. What DSE 5.1.51 does with the flaws it finds:
 * <ul>
 *     <li>a name other than the one its descriptor gives: the file is not found at its dispatch, by the name built from
 *     the descriptor, so it stays out of the dispatch queue, never delivered;</li>
 *     <li>a compression DSE 5.1.51 cannot build ({@link Dse51HintsCompression}): the first dispatch after each start
 *     fails, and the file stays out of the queue, never delivered;</li>
 *     <li>a size whose checksum does not match, zeros after the last entry (a file padded or preallocated) among them,
 *     or a compressed block that cannot be read: the file is set aside as corrupt and the error handed to the disk
 *     failure policy, {@code stop} in the {@code cassandra.yaml} DSE 5.1.51 ships, which stops the node's transports;
 *     or, for a runtime error of the decompressor (a negative or truncated length in the block), the file is left out
 *     of the dispatch queue until the next start, never delivered (inferred from the code of its reader);</li>
 *     <li>a partial entry at the end (a file cut short) or a hint whose checksum does not match: those hints only are
 *     dropped, with a warning, the others delivered.</li>
 * </ul>
 * The first three are {@link Outcome#damaged}, the last {@link Outcome#truncated}. The compression is read as DSE
 * 5.1.51 reads it: {@code crc_check_chance} is removed (DSE 5.1.51 removes it with a warning) and an option of the
 * class without a value is its default. A descriptor with an {@code encryption} parameter is not read: neither DSE
 * 5.1.51's writer nor this software's writes one, so the check cannot vouch for such a file, and counts it as damaged.
 * A file the check cannot read here, though it may be sound, is {@link Outcome#unverified}: a compressor of DSE 5.1.51
 * this software does not have (the encrypting ones of {@code system_info_encryption}), one that cannot be built or run
 * in this JVM, a block larger than this JVM can hold.
 */
public final class Dse51HintsFileCheck
{
    /** The size of the block header of a compressed hints file: the uncompressed size, then the compressed size. */
    private static final int BLOCK_HEADER_SIZE = 8;

    /** The compressors of DSE 5.1.51 that encrypt, which this software does not have. */
    private static final Set<String> DSE51_ENCRYPTING_COMPRESSORS = Set.of("EncryptingLZ4Compressor",
                                                                           "EncryptingSnappyCompressor",
                                                                           "EncryptingDeflateCompressor",
                                                                           "Encryptor");

    /** Builds the compressor of a file; replaced by tests to fail as a compressor this JVM cannot load. */
    @VisibleForTesting
    static Function<ParameterizedClass, ICompressor> compressorFactory = CompressionParams::createCompressor;

    /** The largest block, compressed and uncompressed together, this JVM reads: a quarter of its heap; for tests. */
    @VisibleForTesting
    static long maxBlockSize = Runtime.getRuntime().maxMemory() / 4;

    private Dse51HintsFileCheck()
    {
    }

    /** What the read of a file found. */
    public static final class Outcome
    {
        /** why DSE 5.1.51 would set the file aside as corrupt or never deliver it; null when it would not */
        @Nullable
        public final String damaged;
        /** why this check could not read the file to its end here, the file perhaps sound; null when it could */
        @Nullable
        public final String unverified;
        /** the hints DSE 5.1.51 would drop alone, delivering the others; null when none */
        @Nullable
        public final String truncated;
        /** the entries read whole */
        public final int hints;
        /** the compressed blocks read whole, 0 for a file that is not compressed */
        public final int blocks;

        private Outcome(@Nullable String damaged, @Nullable String unverified, @Nullable String truncated, int hints,
                        int blocks)
        {
            this.damaged = damaged;
            this.unverified = unverified;
            this.truncated = truncated;
            this.hints = hints;
            this.blocks = blocks;
        }

        private static Outcome damaged(String reason)
        {
            return new Outcome(reason, null, null, 0, 0);
        }

        private static Outcome unverified(String reason)
        {
            return new Outcome(null, reason, null, 0, 0);
        }
    }

    /** A flaw of a compressed block, which DSE 5.1.51 meets as a read error of the file. */
    private static final class DamagedBlock extends IOException
    {
        DamagedBlock(String message)
        {
            super(message);
        }
    }

    /** A block this JVM cannot hold or decompress, so the file is not read to its end here. */
    private static final class UnreadableBlock extends IOException
    {
        UnreadableBlock(String message)
        {
            super(message);
        }
    }

    /**
     * Reads the file to its end. Its descriptor is read again: the caller has checked its checksums and its version.
     *
     * @throws IOException when the file cannot be read
     */
    public static Outcome check(Path file) throws IOException
    {
        long fileSize = Files.size(file);
        try (InputStream stream = Files.newInputStream(file, StandardOpenOption.READ);
             DataInputStream in = new DataInputStream(new BufferedInputStream(stream)))
        {
            int version = in.readInt();
            long timestamp = in.readLong();
            UUID hostId = new UUID(in.readLong(), in.readLong());
            int length = in.readInt();
            in.readInt(); // the checksum of the fields above
            byte[] parameters = new byte[length];
            in.readFully(parameters);
            in.readInt(); // the checksum of the parameters
            long descriptorSize = 4 + 8 + 16 + 4 + 4 + length + 4;

            String expected = hostId + "-" + timestamp + '-' + version + ".hints";
            if (!expected.equals(file.getFileName().toString()))
                return Outcome.damaged("its name is not " + expected + ", the one its descriptor gives, which DSE " +
                                       "5.1.51 opens it by");

            Map<String, Object> params = JsonUtils.fromJsonMap(parameters);
            if (params.containsKey("encryption"))
                return Outcome.damaged("its descriptor has an encryption parameter, which neither DSE 5.1.51's " +
                                       "writer nor this software's writes, so this check does not read the file");
            ICompressor compressor = null;
            Object compression = params.get("compression");
            if (compression != null)
            {
                if (!(compression instanceof Map))
                    return Outcome.damaged("its compression parameter " + compression + " is not a class with its " +
                                           "options");
                Map<?, ?> map = (Map<?, ?>) compression;
                Object className = map.get(ParameterizedClass.CLASS_NAME);
                Object options = map.get(ParameterizedClass.PARAMETERS);
                if (options != null && !(options instanceof Map))
                    return Outcome.damaged("the options " + options + " of its compression are not a map");
                Map<String, String> textOptions = new LinkedHashMap<>();
                if (options != null)
                    ((Map<?, ?>) options).forEach((k, v) -> textOptions.put(k == null ? null : String.valueOf(k),
                                                                             v == null ? null : String.valueOf(v)));
                // as DSE 5.1.51 reads them: crc_check_chance removed, with a warning
                textOptions.remove(Dse51HintsCompression.CRC_CHECK_CHANCE);
                String name = className == null ? null : String.valueOf(className);
                String simpleName = name == null || name.isEmpty() ? null : Dse51HintsCompression.simpleName(name);
                if (simpleName != null && DSE51_ENCRYPTING_COMPRESSORS.contains(simpleName))
                    return Outcome.unverified("its compressor " + name + " is DSE 5.1.51's own, which encrypts the " +
                                              "file and which this software does not have");
                String refusal = Dse51HintsCompression.refusal(name, textOptions);
                if (refusal != null)
                    return Outcome.damaged("its compression " + compression + " is not one DSE 5.1.51 can build: " +
                                           refusal);
                // an option of the class without a value is its default in DSE 5.1.51, as if absent
                textOptions.values().removeIf(Objects::isNull);
                try
                {
                    compressor = compressorFactory.apply(new ParameterizedClass(name, textOptions));
                }
                catch (RuntimeException | LinkageError e)
                {
                    return Outcome.unverified("its compressor " + name + " cannot be built here to read it (" + e +
                                              ')');
                }
            }

            Blocks blocks = compressor == null ? null : new Blocks(in, compressor, descriptorSize, fileSize);
            return readEntries(new DataInputStream(blocks == null ? in : blocks), blocks);
        }
    }

    /** The entries, as DSE 5.1.51's reader frames them, through the blocks of a compressed file if not null. */
    private static Outcome readEntries(DataInputStream in, @Nullable Blocks blocks) throws IOException
    {
        long position = 0;
        int hints = 0;
        int hintMismatches = 0;
        byte[] chunk = new byte[64 * 1024];
        try
        {
            while (true)
            {
                int first = in.read();
                if (first < 0)
                    break;
                long entryStart = position;
                try
                {
                    int size = (first << 24) | (in.readUnsignedByte() << 16) | (in.readUnsignedByte() << 8) |
                               in.readUnsignedByte();
                    int sizeChecksum = in.readInt();
                    CRC32 crc = new CRC32();
                    FBUtilities.updateChecksumInt(crc, size);
                    if ((int) crc.getValue() != sizeChecksum)
                        return new Outcome(size == 0 && sizeChecksum == 0
                                           ? "zeros after its last entry, at byte " + entryStart + " of its " +
                                             "entries, which DSE 5.1.51 reads as a size whose checksum does not match"
                                           : "an entry size whose checksum does not match, at byte " + entryStart +
                                             " of its entries",
                                           null, mismatches(hintMismatches), hints, blockCount(blocks));
                    if (size < 0)
                        return new Outcome("a negative entry size, at byte " + entryStart + " of its entries", null,
                                           mismatches(hintMismatches), hints, blockCount(blocks));
                    position += 8;
                    int left = size;
                    while (left > 0)
                    {
                        int read = Math.min(left, chunk.length);
                        in.readFully(chunk, 0, read);
                        crc.update(chunk, 0, read);
                        left -= read;
                    }
                    position += size;
                    if ((int) crc.getValue() != in.readInt())
                        hintMismatches++;
                    position += 4;
                    hints++;
                }
                catch (EOFException e)
                {
                    String truncated = "a partial entry at its end, at byte " + entryStart + " of its entries";
                    if (hintMismatches > 0)
                        truncated += ", and " + mismatches(hintMismatches);
                    return new Outcome(null, null, truncated, hints, blockCount(blocks));
                }
            }
        }
        catch (DamagedBlock e)
        {
            return new Outcome(e.getMessage(), null, mismatches(hintMismatches), hints, blockCount(blocks));
        }
        catch (UnreadableBlock e)
        {
            return new Outcome(null, e.getMessage(), mismatches(hintMismatches), hints, blockCount(blocks));
        }
        return new Outcome(null, null, mismatches(hintMismatches), hints, blockCount(blocks));
    }

    private static int blockCount(@Nullable Blocks blocks)
    {
        return blocks == null ? 0 : blocks.blocksRead;
    }

    @Nullable
    private static String mismatches(int count)
    {
        if (count == 0)
            return null;
        return count + (count == 1 ? " hint whose checksum does not match" : " hints whose checksums do not match");
    }

    /**
     * The uncompressed bytes of a compressed hints file, block by block: each block is its uncompressed size, its
     * compressed size, then the compressed bytes, as {@code CompressedHintsWriter} writes it and DSE 5.1.51 reads it.
     * A block is a write buffer of the writer, or one hint larger than it, up to the largest hint the node's hints
     * buffer takes, which this tool does not know: a block whose compressed and uncompressed bytes together exceed a
     * quarter of the heap, or that this JVM cannot allocate, is not read, the file not verified.
     */
    private static final class Blocks extends InputStream
    {
        private final DataInputStream file;
        private final ICompressor compressor;
        private final long fileSize;
        private long filePosition;
        /** the blocks read whole */
        private int blocksRead;
        private byte[] block = new byte[0];
        private int blockPosition;

        Blocks(DataInputStream file, ICompressor compressor, long descriptorSize, long fileSize)
        {
            this.file = file;
            this.compressor = compressor;
            this.filePosition = descriptorSize;
            this.fileSize = fileSize;
        }

        @Override
        public int read() throws IOException
        {
            if (blockPosition == block.length && !nextBlock())
                return -1;
            return block[blockPosition++] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException
        {
            if (len == 0)
                return 0;
            if (blockPosition == block.length && !nextBlock())
                return -1;
            int read = Math.min(len, block.length - blockPosition);
            System.arraycopy(block, blockPosition, b, off, read);
            blockPosition += read;
            return read;
        }

        private boolean nextBlock() throws IOException
        {
            // an empty block reads as no data, so the next one is read
            while (true)
            {
                if (filePosition == fileSize)
                    return false;
                long start = filePosition;
                if (fileSize - filePosition < BLOCK_HEADER_SIZE)
                    throw new DamagedBlock("a compressed block cut short, at byte " + start + " of the file");
                int uncompressedSize = file.readInt();
                int compressedSize = file.readInt();
                filePosition += BLOCK_HEADER_SIZE;
                if (uncompressedSize == 0 && compressedSize == 0)
                    throw new DamagedBlock("zeros after its last compressed block, at byte " + start + " of the file");
                if (uncompressedSize < 0 || compressedSize <= 0)
                    throw new DamagedBlock("a compressed block with the sizes " + uncompressedSize + " and " +
                                           compressedSize + ", at byte " + start + " of the file");
                if (fileSize - filePosition < compressedSize)
                    throw new DamagedBlock("a compressed block cut short, at byte " + start + " of the file");
                String tooLarge = "a compressed block of " + compressedSize + " bytes, " + uncompressedSize +
                                  " uncompressed, at byte " + start + " of the file, above what this JVM can hold";
                if ((long) uncompressedSize + compressedSize > maxBlockSize)
                    throw new UnreadableBlock(tooLarge);
                byte[] compressed;
                byte[] uncompressed;
                try
                {
                    compressed = new byte[compressedSize];
                    uncompressed = new byte[uncompressedSize];
                }
                catch (OutOfMemoryError e)
                {
                    throw new UnreadableBlock(tooLarge);
                }
                file.readFully(compressed);
                filePosition += compressedSize;
                int produced;
                try
                {
                    // a block that declares no uncompressed byte still goes through the decompressor, as in DSE 5.1.51
                    produced = compressor.uncompress(compressed, 0, compressedSize, uncompressed, 0);
                }
                catch (IOException | RuntimeException e)
                {
                    throw new DamagedBlock("a compressed block that cannot be decompressed, at byte " + start +
                                           " of the file (" + e.getMessage() + ')');
                }
                catch (LinkageError e)
                {
                    throw new UnreadableBlock("a compressed block this JVM cannot decompress, at byte " + start +
                                              " of the file (" + e + ')');
                }
                blocksRead++;
                if (produced == 0)
                    continue;
                block = produced == uncompressedSize ? uncompressed : Arrays.copyOf(uncompressed, produced);
                blockPosition = 0;
                return true;
            }
        }
    }
}
