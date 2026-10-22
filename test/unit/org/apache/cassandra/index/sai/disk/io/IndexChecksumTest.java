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
package org.apache.cassandra.index.sai.disk.io;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.SequentialWriterOption;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Regression tests for two defects in {@link IndexFileUtils.IncrementalChecksumSequentialWriter#writeMostSignificantBytes}.
 *
 * <p>Defect 1 – Double-counting at buffer boundaries: when fewer than 8 bytes remain the parent
 * delegates to overridden primitives that already update the checksum, so a second
 * {@code addMsbToChecksum} call double-counts those bytes.
 *
 * <p>Defect 2 – Truncation for widths 5–7: {@code addMsbToChecksum} cast the shifted long to
 * {@code int}, silently dropping the high bits needed for 40-, 48-, and 56-bit values.
 */
public class IndexChecksumTest
{
    static
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    // Byte layout (MSB-first): 01 23 45 67 89 AB CD EF
    private static final long REGISTER = 0x0123456789ABCDEFL;

    // Byte layout (MSB-first): FF EE DD CC BB AA 99 88 — high bit of MSB byte is set
    private static final long REGISTER_HIGH_BIT = 0xFFEEDDCCBBAA9988L;

    private static final Version TEST_VERSION = Version.LATEST;

    private static SequentialWriterOption option(int bufferSize, BufferType bufferType)
    {
        return SequentialWriterOption.newBuilder()
                                     .bufferSize(bufferSize)
                                     .bufferType(bufferType)
                                     .finishOnClose(true)
                                     .build();
    }

    private static long crc32(byte[] data)
    {
        Checksum crc = new CRC32();
        crc.update(data, 0, data.length);
        return crc.getValue();
    }

    /** Returns the top {@code bytes} bytes of {@code register} in big-endian order. */
    private static byte[] expectedMsbBytes(long register, int bytes)
    {
        byte[] result = new byte[bytes];
        for (int i = 0; i < bytes; i++)
            result[i] = (byte) (register >>> (8 * (8 - 1 - i)));
        return result;
    }

    /**
     * Writes {@code width} MSBs of {@code register} with {@code remaining} bytes free in the
     * buffer, then asserts payload bytes and checksum match a reference CRC32.
     */
    private void assertMsbWrite(int bufferSize, BufferType bufferType,
                                 int remaining, int width, long register) throws IOException
    {
        Path tmpFile = tmp.newFile().toPath();
        File file = new File(tmpFile);

        SequentialWriterOption opt = option(bufferSize, bufferType);
        IndexFileUtils.IncrementalChecksumSequentialWriter writer =
                new IndexFileUtils.IncrementalChecksumSequentialWriter(file, opt, TEST_VERSION, false);

        int padBytes = bufferSize - remaining;
        for (int i = 0; i < padBytes; i++)
            writer.writeByte(0xAA);

        writer.writeMostSignificantBytes(register, width);
        long reportedChecksum = writer.getChecksum();
        writer.close();

        byte[] fileBytes = Files.readAllBytes(tmpFile);

        byte[] expected = expectedMsbBytes(register, width);
        byte[] actual = new byte[width];
        System.arraycopy(fileBytes, padBytes, actual, 0, width);
        assertArrayEquals(
                String.format("Payload mismatch: bufType=%s remaining=%d width=%d register=%016X",
                              bufferType, remaining, width, register),
                expected, actual);

        long expectedChecksum = crc32(fileBytes);
        assertEquals(
                String.format("Checksum mismatch: bufType=%s remaining=%d width=%d register=%016X " +
                              "expected=%X reported=%X",
                              bufferType, remaining, width, register, expectedChecksum, reportedChecksum),
                expectedChecksum, reportedChecksum);
    }

    // --- Defect 1: double-counting at buffer boundaries ---
    // Sweep all widths 1–8 × remaining 0–8 for both buffer types.

    @Test
    public void testBoundaryDoubleCounting_heapBuffer() throws IOException
    {
        runBoundarySweep(BufferType.ON_HEAP);
    }

    @Test
    public void testBoundaryDoubleCounting_directBuffer() throws IOException
    {
        runBoundarySweep(BufferType.OFF_HEAP);
    }

    private void runBoundarySweep(BufferType bufferType) throws IOException
    {
        // bufferSize must be >= 8 so the pad fits (max pad = bufferSize - 0 = bufferSize).
        // 16 bytes is sufficient: max pad = 8, max write = 8, writer flushes as needed.
        int bufferSize = 16;

        for (int remaining = 0; remaining <= 8; remaining++)
        {
            for (int width = 1; width <= 8; width++)
            {
                assertMsbWrite(bufferSize, bufferType, remaining, width, REGISTER);
                assertMsbWrite(bufferSize, bufferType, remaining, width, REGISTER_HIGH_BIT);
            }
        }
    }

    // --- Defect 2: int truncation for widths 5–7 ---
    // Use a large buffer (remaining >= 8) to take the direct-buffer path.

    @Test
    public void testTruncation_width5_heapBuffer() throws IOException
    {
        runTruncationTest(5, BufferType.ON_HEAP);
    }

    @Test
    public void testTruncation_width5_directBuffer() throws IOException
    {
        runTruncationTest(5, BufferType.OFF_HEAP);
    }

    @Test
    public void testTruncation_width6_heapBuffer() throws IOException
    {
        runTruncationTest(6, BufferType.ON_HEAP);
    }

    @Test
    public void testTruncation_width6_directBuffer() throws IOException
    {
        runTruncationTest(6, BufferType.OFF_HEAP);
    }

    @Test
    public void testTruncation_width7_heapBuffer() throws IOException
    {
        runTruncationTest(7, BufferType.ON_HEAP);
    }

    @Test
    public void testTruncation_width7_directBuffer() throws IOException
    {
        runTruncationTest(7, BufferType.OFF_HEAP);
    }

    private void runTruncationTest(int width, BufferType bufferType) throws IOException
    {
        int bufferSize = 256; // remaining >= 8: direct-buffer path
        assertMsbWrite(bufferSize, bufferType, bufferSize, width, REGISTER);
        assertMsbWrite(bufferSize, bufferType, bufferSize, width, REGISTER_HIGH_BIT);
    }

    // --- End-to-end: SAI header + compact writes + footer checksum ---
    // Verifies the stored footer checksum equals CRC32 over all preceding bytes.

    @Test
    public void testFooterChecksumAfterCompactWrite_heapBuffer() throws IOException
    {
        runFooterChecksumTest(BufferType.ON_HEAP);
    }

    @Test
    public void testFooterChecksumAfterCompactWrite_directBuffer() throws IOException
    {
        runFooterChecksumTest(BufferType.OFF_HEAP);
    }

    private void runFooterChecksumTest(BufferType bufferType) throws IOException
    {
        // bufferSize=16: after the 7-byte header, 9 bytes remain, putting widths 5–7
        // across both defect zones (boundary and truncation).
        int bufferSize = 16;

        Path tmpFile = tmp.newFile().toPath();
        File file = new File(tmpFile);

        SequentialWriterOption opt = option(bufferSize, bufferType);
        IndexFileUtils.IncrementalChecksumSequentialWriter writer =
                new IndexFileUtils.IncrementalChecksumSequentialWriter(file, opt, TEST_VERSION, false);

        IndexOutputWriter indexOutput = new IndexOutputWriter(writer, ByteOrder.BIG_ENDIAN, TEST_VERSION);

        SAICodecUtils.writeHeader(indexOutput);

        for (int width = 5; width <= 7; width++)
            writer.writeMostSignificantBytes(REGISTER, width);

        SAICodecUtils.writeFooter(indexOutput);
        indexOutput.close();

        byte[] fileBytes = Files.readAllBytes(tmpFile);

        // Footer layout: ... | 8-byte checksum (last field)
        int checksumOffset = fileBytes.length - 8;

        long storedChecksum = 0;
        for (int i = 0; i < 8; i++)
            storedChecksum = (storedChecksum << 8) | (fileBytes[checksumOffset + i] & 0xFF);

        long referenceChecksum = crc32(java.util.Arrays.copyOf(fileBytes, checksumOffset));

        assertEquals(
                String.format("Footer checksum mismatch: bufType=%s stored=%X reference=%X",
                              bufferType, storedChecksum, referenceChecksum),
                referenceChecksum, storedChecksum);
    }

    // --- All widths 1–8 with ample buffer (direct-buffer path) ---
    // Isolates truncation defect independent of boundary behavior.

    @Test
    public void testAllWidths_ampleBuf_heapBuffer() throws IOException
    {
        runAllWidthsWithAmpleBuffer(BufferType.ON_HEAP);
    }

    @Test
    public void testAllWidths_ampleBuf_directBuffer() throws IOException
    {
        runAllWidthsWithAmpleBuffer(BufferType.OFF_HEAP);
    }

    private void runAllWidthsWithAmpleBuffer(BufferType bufferType) throws IOException
    {
        int bufferSize = 256;
        for (int width = 1; width <= 8; width++)
        {
            assertMsbWrite(bufferSize, bufferType, bufferSize, width, REGISTER);
            assertMsbWrite(bufferSize, bufferType, bufferSize, width, REGISTER_HIGH_BIT);
        }
    }
}
