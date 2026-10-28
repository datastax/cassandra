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
 * {@code '%'} and {@code '_'} is a wildcard, wherever it appears in the pattern, unless it is escaped.
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

        // multi-byte characters, and backslashes without an ESCAPE clause, are literals
        assertSpecific("%日本語", Operator.LIKE_SUFFIX, "日本語");
        assertSpecific("a\\c%", Operator.LIKE_PREFIX, "a\\c");
        // a wildcard-free pattern is in internal form: the backslash is escaped
        assertSpecific("a\\c", Operator.LIKE_MATCHES, "a\\\\c");
    }

    @Test
    public void testUnderscoreMakesGenericPatterns()
    {
        assertSpecific("a_c%", Operator.LIKE_MATCHES, "a_c%");
        assertSpecific("_", Operator.LIKE_MATCHES, "_");
        assertSpecific("%%_%%", Operator.LIKE_MATCHES, "%_%");
        assertSpecific("%a_", Operator.LIKE_MATCHES, "%a_");
    }

    @Test
    public void testEscapes()
    {
        // escaped wildcards are literals, so the leading/trailing ones don't select an operator
        assertSpecific("a!_c%", "!", Operator.LIKE_PREFIX, "a_c");
        assertSpecific("%a!%", "!", Operator.LIKE_SUFFIX, "a%");
        assertSpecific("%!%%", "!", Operator.LIKE_CONTAINS, "%");
        assertSpecific("a!!b%", "!", Operator.LIKE_PREFIX, "a!b");
        // in a generic or wildcard-free pattern, they are escaped with the internal escape character
        assertSpecific("a!_c", "!", Operator.LIKE_MATCHES, "a\\_c");
        assertSpecific("a!%_\\%", "!", Operator.LIKE_MATCHES, "a\\%_\\\\%");
        assertSpecific("a\\_%", "\\", Operator.LIKE_PREFIX, "a_");
        assertSpecific("a\\\\_", "\\", Operator.LIKE_MATCHES, "a\\\\_");
        // multi-byte escape characters
        assertSpecific("日%日日%", "日", Operator.LIKE_PREFIX, "%日");
        assertSpecific("é_日_", "é", Operator.LIKE_MATCHES, "\\_日_");
        // without an escape character, there is nothing to escape
        assertSpecific("a!_c", Operator.LIKE_MATCHES, "a!_c");
        // escaped wildcards don't make a pattern empty
        assertSpecific("!%", "!", Operator.LIKE_MATCHES, "\\%");

        for (String pattern : new String[]{ "a!", "a!b", "a!日", "!" })
            assertThatThrownBy(() -> makeSpecific(pattern, "!")).isInstanceOf(InvalidRequestException.class)
                                                                .hasMessageContaining("escape character");
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

    private static Pair<Operator, ByteBuffer> makeSpecific(String pattern, String escape)
    {
        return SingleColumnRestriction.LikeRestriction.makeSpecific(UTF8Type.instance.decompose(pattern),
                                                                    UTF8Type.instance.decompose(escape));
    }

    private static void assertSpecific(String pattern, Operator expectedOperator, String expectedValue)
    {
        assertSpecific(makeSpecific(pattern), pattern, expectedOperator, expectedValue);
    }

    private static void assertSpecific(String pattern, String escape, Operator expectedOperator, String expectedValue)
    {
        assertSpecific(makeSpecific(pattern, escape), pattern + " ESCAPE " + escape, expectedOperator, expectedValue);
    }

    private static void assertSpecific(Pair<Operator, ByteBuffer> specific, String message, Operator expectedOperator, String expectedValue)
    {
        assertEquals(message, expectedOperator, specific.left);
        assertEquals(message, expectedValue, UTF8Type.instance.compose(specific.right));
    }
}
