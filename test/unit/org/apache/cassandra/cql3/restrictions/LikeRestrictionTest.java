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

package org.apache.cassandra.cql3.restrictions;

import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.Pair;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

/**
 * Tests how {@code SingleColumnRestriction.LikeRestriction#makeSpecific} classifies a bound LIKE pattern: every
 * {@code '%'} is a wildcard, wherever it appears in the pattern.
 */
public class LikeRestrictionTest
{
    @Test
    public void testLiteralWithLeadingAndTrailingWildcards()
    {
        assertSpecific("abc", Operator.LIKE_MATCHES, "abc");
        assertSpecific("abc%", Operator.LIKE_PREFIX, "abc");
        assertSpecific("%abc", Operator.LIKE_SUFFIX, "abc");
        assertSpecific("%abc%", Operator.LIKE_CONTAINS, "abc");

        // runs of leading/trailing wildcards are wildcards too
        assertSpecific("abc%%", Operator.LIKE_PREFIX, "abc");
        assertSpecific("%%abc", Operator.LIKE_SUFFIX, "abc");
        assertSpecific("%%%abc%%", Operator.LIKE_CONTAINS, "abc");

        // '_' is a literal character, and so are multi-byte characters
        assertSpecific("a_c%", Operator.LIKE_PREFIX, "a_c");
        assertSpecific("%日本語", Operator.LIKE_SUFFIX, "日本語");
    }

    @Test
    public void testInnerWildcardsMakeGenericPatterns()
    {
        // any inner wildcard makes the whole (untrimmed) pattern a generic LIKE_MATCHES pattern, with its runs of
        // wildcards collapsed
        assertSpecific("a%b", Operator.LIKE_MATCHES, "a%b");
        assertSpecific("a%b%", Operator.LIKE_MATCHES, "a%b%");
        assertSpecific("%a%b", Operator.LIKE_MATCHES, "%a%b");
        assertSpecific("%a%b%", Operator.LIKE_MATCHES, "%a%b%");
        assertSpecific("a%%b", Operator.LIKE_MATCHES, "a%b");
        assertSpecific("%%a%b%%", Operator.LIKE_MATCHES, "%a%b%");
        assertSpecific("%%%a%%%%b%c%%_%%", Operator.LIKE_MATCHES, "%a%b%c%_%");
        assertSpecific("a" + "%".repeat(9000) + "日", Operator.LIKE_MATCHES, "a%日");
    }

    @Test
    public void testEmptyPatterns()
    {
        for (String pattern : new String[]{ "", "%", "%%", "%%%" })
            assertThatThrownBy(() -> makeSpecific(pattern)).isInstanceOf(InvalidRequestException.class)
                                                           .hasMessageContaining("LIKE value can't be empty");
    }

    @Test
    public void testBufferWithNonZeroPosition()
    {
        // the bound value may be a slice of a larger buffer
        ByteBuffer buffer = UTF8Type.instance.decompose("xx%abc%");
        buffer.position(2);
        Pair<Operator, ByteBuffer> specific = SingleColumnRestriction.LikeRestriction.makeSpecific(buffer);
        assertEquals(Operator.LIKE_CONTAINS, specific.left);
        assertEquals("abc", UTF8Type.instance.compose(specific.right));

        buffer = UTF8Type.instance.decompose("xx%a%b");
        buffer.position(2);
        specific = SingleColumnRestriction.LikeRestriction.makeSpecific(buffer);
        assertEquals(Operator.LIKE_MATCHES, specific.left);
        assertEquals("%a%b", UTF8Type.instance.compose(specific.right));

        buffer = UTF8Type.instance.decompose("%%%a%%b");
        buffer.position(2);
        specific = SingleColumnRestriction.LikeRestriction.makeSpecific(buffer);
        assertEquals(Operator.LIKE_MATCHES, specific.left);
        assertEquals("%a%b", UTF8Type.instance.compose(specific.right));
    }

    private static Pair<Operator, ByteBuffer> makeSpecific(String pattern)
    {
        return SingleColumnRestriction.LikeRestriction.makeSpecific(UTF8Type.instance.decompose(pattern));
    }

    private static void assertSpecific(String pattern, Operator expectedOperator, String expectedValue)
    {
        Pair<Operator, ByteBuffer> specific = makeSpecific(pattern);
        assertEquals(pattern, expectedOperator, specific.left);
        assertEquals(pattern, expectedValue, UTF8Type.instance.compose(specific.right));
    }
}
