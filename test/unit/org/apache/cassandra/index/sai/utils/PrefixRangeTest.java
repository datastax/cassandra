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
package org.apache.cassandra.index.sai.utils;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests the range of the terms starting with a prefix, used by the estimates and index selection of LIKE patterns.
 */
public class PrefixRangeTest
{
    @Test
    public void upperBound()
    {
        assertArrayEquals(new byte[]{ 0x62 }, PrefixRange.upperBound(new byte[]{ 0x61 }));
        assertArrayEquals(new byte[]{ 0x61, 0x63 }, PrefixRange.upperBound(new byte[]{ 0x61, 0x62 }));
        // trailing 0xFF bytes cannot be incremented and are dropped
        assertArrayEquals(new byte[]{ 0x62 }, PrefixRange.upperBound(new byte[]{ 0x61, (byte) 0xFF }));
        assertArrayEquals(new byte[]{ 0x62 }, PrefixRange.upperBound(new byte[]{ 0x61, (byte) 0xFF, (byte) 0xFF }));
        // no upper bound exists for an empty or all-0xFF prefix
        assertNull(PrefixRange.upperBound(new byte[0]));
        assertNull(PrefixRange.upperBound(new byte[]{ (byte) 0xFF, (byte) 0xFF }));

        // sanity: the bound is exclusive and sorts after every extension of the prefix
        byte[] prefix = utf8("ab");
        byte[] upper = PrefixRange.upperBound(prefix);
        assertTrue(Arrays.compareUnsigned(prefix, upper) < 0);
        assertTrue(Arrays.compareUnsigned(utf8("ab￿￿"), upper) < 0);
        assertTrue(Arrays.compareUnsigned(utf8("ac"), upper) >= 0);
    }

    private static byte[] utf8(String s)
    {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
