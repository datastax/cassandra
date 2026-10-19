/*
 * Copyright DataStax, Inc.
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

package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;

import org.junit.Test;

import org.assertj.core.api.Assertions;

public class RedactionUtilTest
{
    @Test
    public void testRedactByteBuffer()
    {
        assertRedactByteBuffer(null, "?");
        assertRedactByteBuffer(ByteBuffer.allocate(0), "?");
        assertRedactByteBuffer(ByteBuffer.allocate(1), "?");
        assertRedactByteBuffer(ByteBuffer.allocate(100), "?");
        assertRedactByteBuffer(ByteBuffer.allocate(101), "?[>100B]");
        assertRedactByteBuffer(ByteBuffer.allocate(1024), "?[>100B]");
        assertRedactByteBuffer(ByteBuffer.allocate(1025), "?[>1KiB]");
        assertRedactByteBuffer(ByteBuffer.allocate(10 * 1024), "?[>1KiB]");
        assertRedactByteBuffer(ByteBuffer.allocate(10 * 1024 + 1), "?[>10KiB]");
        // we don't want to keep testing allocating giant buffers, the test for the size alone should get us covered
    }

    private static void assertRedactByteBuffer(ByteBuffer bytes, String expectedForVariableLength)
    {
        Assertions.assertThat(RedactionUtil.redact(bytes, true)).isEqualTo("?");
        Assertions.assertThat(RedactionUtil.redact(bytes, false)).isEqualTo(expectedForVariableLength);
    }

    @Test
    public void testRedactSize()
    {
        // invalid size
        Assertions.assertThatThrownBy(() -> RedactionUtil.redact(-1)).isInstanceOf(AssertionError.class);

        // byte range
        assertRedactSize(0, "?");
        assertRedactSize(1, "?");
        assertRedactSize(100, "?");
        assertRedactSize(101, "?[>100B]");

        // KiB range
        int unit = 1024;
        assertRedactSize(unit, "?[>100B]");
        assertRedactSize(unit + 1, "?[>1KiB]");
        assertRedactSize(10 * unit, "?[>1KiB]");
        assertRedactSize(10 * unit + 1, "?[>10KiB]");
        assertRedactSize(100 * unit, "?[>10KiB]");
        assertRedactSize(100 * unit + 1, "?[>100KiB]");

        // MiB range
        unit *= 1024;
        assertRedactSize(unit, "?[>100KiB]");
        assertRedactSize(unit + 1, "?[>1MiB]");
        assertRedactSize(10 * unit, "?[>1MiB]");
        assertRedactSize(10 * unit + 1, "?[>10MiB]");
        assertRedactSize(100 * unit, "?[>10MiB]");
        assertRedactSize(100 * unit + 1, "?[>100MiB]");

        // GiB range
        unit *= 1024;
        assertRedactSize(unit, "?[>100MiB]");
        assertRedactSize(unit + 1, "?[>1GiB]");
        assertRedactSize(Integer.MAX_VALUE, "?[>1GiB]");
    }

    private static void assertRedactSize(int size, String expected)
    {
        Assertions.assertThat(RedactionUtil.redact(size)).isEqualTo(expected);
    }
}
