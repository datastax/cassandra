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

package org.apache.cassandra.utils;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

// We use the same values to test as in AWS S3 SDK tests.
// See https://github.com/aws/aws-sdk-java-v2/blob/0c5e331bd2544e7d43fabd3db046a95b94d0a2dd/core/checksums/src/test/java/software/amazon/awssdk/checksums/internal/Crc64NvmeChecksumTest.java
public class PureJavaCRC64NVMETest
{

    private PureJavaCRC64NVME crc64NVME;
    private static final String TEST_STRING = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    @Before
    public void setUp()
    {
        crc64NVME = new PureJavaCRC64NVME();
    }

    @Test
    public void validateCrcChecksumValues()
    {
        byte[] bytes = TEST_STRING.getBytes(StandardCharsets.UTF_8);
        crc64NVME.update(bytes, 0, bytes.length);
        assertThat(getAsString(getAsBytes(crc64NVME.getValue()))).isEqualTo("0000000000000000000000008b8f30cfc6f16409");
    }

    @Test
    public void validateEncodedBase64ForCrc()
    {
        crc64NVME.update("abc".getBytes(StandardCharsets.UTF_8));
        String toBase64 = toBase64(crc64NVME.getValue());
        assertThat(toBase64).isEqualTo("BeXKuz/B+us=");
    }

    @Test
    public void validateEncodedBase64ForCrcSingleByte()
    {
        for (byte value : "abc".getBytes(StandardCharsets.UTF_8))
        {
            crc64NVME.update(value);
        }
        String toBase64 = toBase64(crc64NVME.getValue());
        assertThat(toBase64).isEqualTo("BeXKuz/B+us=");
    }

    @Test
    public void validateResetForCrc() {
        crc64NVME.update("beta".getBytes(StandardCharsets.UTF_8));
        crc64NVME.reset();
        crc64NVME.update("alpha".getBytes(StandardCharsets.UTF_8));
        String toBase64 = toBase64(crc64NVME.getValue());
        // Checksum of "alpha"
        assertThat(toBase64).isEqualTo("Ehnh98TMQlQ=");
    }

    private String toBase64(long value)
    {
        byte[] bytes = getAsBytes(value);
        return Base64.getEncoder().encodeToString(bytes);
    }

    private byte[] getAsBytes(long value)
    {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(value);
        return buffer.array();
    }

    private String getAsString(byte[] checksumBytes) {
        return String.format("%040x", new BigInteger(1, checksumBytes));
    }
}
