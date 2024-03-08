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

package org.apache.cassandra.index.sai.disk.oldlucene;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteOrder;

public class ByteArrayIndexInputTest {

    private ByteArrayIndexInput inputBE;
    private ByteArrayIndexInput inputLE;
    private final byte[] testData = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

    // Reset the input streams before each test
    @Before
    public void setUp() {
        inputBE = new ByteArrayIndexInput("BigEndianTest", testData, ByteOrder.BIG_ENDIAN);
        inputLE = new ByteArrayIndexInput("LittleEndianTest", testData, ByteOrder.LITTLE_ENDIAN);
    }

    @Test
    public void testReadByte() {
        assertEquals(0, inputBE.readByte());
        assertEquals(1, inputBE.readByte());

        assertEquals(0, inputLE.readByte());
        assertEquals(1, inputLE.readByte());
    }

    @Test
    public void testReadShort() {
        var beExpected = (short) 1;
        var leExpected = (short) 256;
        assertEquals(beExpected, inputBE.readShort());
        assertEquals(leExpected, inputLE.readShort());
        assertEquals(beExpected, Short.reverseBytes(leExpected));
    }

    @Test
    public void testReadInt() {
        var beExpected = 66051;
        var leExpected = 50462976;
        assertEquals(beExpected, inputBE.readInt());
        assertEquals(leExpected, inputLE.readInt());
        assertEquals(beExpected, Integer.reverseBytes(leExpected));
    }

    @Test
    public void testReadLong() {
        var beExpected = 283686952306183L;
        var leExpected = 506097522914230528L;
        assertEquals(beExpected, inputBE.readLong());
        assertEquals(leExpected, inputLE.readLong());
        assertEquals(beExpected, Long.reverseBytes(leExpected));
    }

    @Test
    public void testReadByteWithPosition() throws IOException
    {
        assertEquals(2, inputBE.readByte(2));
        assertEquals(2, inputLE.readByte(2));
    }

    @Test
    public void testReadShortWithPosition() throws IOException {
        var beExpected = (short) 515;
        var leExpected = (short) 770;
        assertEquals(beExpected, inputBE.readShort(2));
        assertEquals(leExpected, inputLE.readShort(2));
        assertEquals(beExpected, Short.reverseBytes(leExpected));
    }

    @Test
    public void testReadIntWithPosition() throws IOException {
        var beExpected = 33752069;
        var leExpected = 84148994;
        assertEquals(beExpected, inputBE.readInt(2));
        assertEquals(leExpected, inputLE.readInt(2));
        assertEquals(beExpected, Integer.reverseBytes(leExpected));
    }

    @Test
    public void testReadLongWithPosition() throws IOException {
        var beExpected = 144964032628459529L;
        var leExpected = 650777868590383874L;
        assertEquals(beExpected, inputBE.readLong(2));
        assertEquals(leExpected, inputLE.readLong(2));
        assertEquals(beExpected, Long.reverseBytes(leExpected));
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testReadShortBeyondEndWithPosition() throws IOException {
        inputBE.readShort(testData.length); // Attempt to read beyond the available data
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testReadIntBeyondEndWithPosition() throws IOException {
        inputBE.readInt(testData.length - 1); // Attempt to read beyond the available data
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testReadLongBeyondEndWithPosition() throws IOException {
        inputBE.readLong(testData.length - 3); // Attempt to read beyond the available data
    }

    @Test
    public void testSeekAndGetPosition() throws EOFException {
        inputBE.seek(2);
        assertEquals(2, inputBE.getFilePointer());

        inputLE.seek(4);
        assertEquals(4, inputLE.getFilePointer());
    }

    @Test(expected = EOFException.class)
    public void testSeekBeyondLengthBE() throws EOFException {
        inputBE.seek(testData.length + 1);
    }

    @Test(expected = EOFException.class)
    public void testSeekBeyondLengthLE() throws EOFException {
        inputLE.seek(testData.length + 1);
    }

    @Test
    public void testLength() {
        assertEquals(testData.length, inputBE.length());
        assertEquals(testData.length, inputLE.length());
    }

    @Test
    public void testReadBytes() {
        byte[] buffer = new byte[4];
        inputBE.readBytes(buffer, 0, buffer.length);
        assertArrayEquals(new byte[]{0, 1, 2, 3}, buffer);

        inputLE.readBytes(buffer, 0, buffer.length);
        assertArrayEquals(new byte[]{0, 1, 2, 3}, buffer);
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testReadBeyondEnd() {
        byte[] buffer = new byte[11];
        inputBE.readBytes(buffer, 0, buffer.length);
    }

    @Test
    public void testClose() {
        inputBE.close();
        try {
            inputBE.readByte();
            fail("Should throw a NullPointerException after close");
        } catch (NullPointerException e) {
            // expected
        }
    }

    @Test
    public void testCloneAndSlice() throws EOFException {
        ByteArrayIndexInput clone = (ByteArrayIndexInput) inputBE.clone();
        ByteArrayIndexInput slice = inputBE.slice("slice", 2, 4);

        clone.seek(2);
        assertEquals(2, clone.readByte());

        assertEquals(2, slice.readByte());
        assertEquals(4, slice.length());
    }
}
