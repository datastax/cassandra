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

package org.apache.cassandra.cql3;

import java.util.Arrays;

/**
 * A reference implementation of the semantics of the CQL {@code LIKE} operators for tests, independent of
 * {@link LikePatterns} and of the trie patterns: it works on the characters (code points) of strings, and matches the
 * generic patterns by dynamic programming.
 */
public final class LikeReference
{
    private static final int ANY_STRING = -1;
    private static final int ANY_CHAR = -2;

    private LikeReference()
    {
    }

    /**
     * Whether the candidate matches the value of the given {@code LIKE} operator: a literal for the prefix, suffix
     * and contains operators, and a pattern in internal form (see {@link LikePatterns#ESCAPE}) for
     * {@link Operator#LIKE_MATCHES}.
     */
    public static boolean matches(Operator operator, String value, String candidate)
    {
        switch (operator)
        {
            case LIKE_PREFIX:
                return candidate.startsWith(value);
            case LIKE_SUFFIX:
                return candidate.endsWith(value);
            case LIKE_CONTAINS:
                return candidate.contains(value);
            case LIKE_MATCHES:
                return matchesPattern(value, candidate);
            default:
                throw new IllegalArgumentException("Not a LIKE operator: " + operator);
        }
    }

    /**
     * Whether the candidate matches the given pattern in internal form: {@code %} matches any (possibly empty)
     * sequence of characters, {@code _} any single character, and {@link LikePatterns#ESCAPE} makes the character after
     * it literal.
     */
    public static boolean matchesPattern(String pattern, String candidate)
    {
        int[] tokens = tokens(pattern);
        int[] chars = candidate.codePoints().toArray();
        // matches[i][j]: whether the tokens from i match the characters from j
        boolean[][] matches = new boolean[tokens.length + 1][chars.length + 1];
        matches[tokens.length][chars.length] = true;
        for (int i = tokens.length - 1; i >= 0; --i)
        {
            for (int j = chars.length; j >= 0; --j)
            {
                int token = tokens[i];
                if (token == ANY_STRING)
                    matches[i][j] = matches[i + 1][j] || j < chars.length && matches[i][j + 1];
                else if (token == ANY_CHAR)
                    matches[i][j] = j < chars.length && matches[i + 1][j + 1];
                else
                    matches[i][j] = j < chars.length && chars[j] == token && matches[i + 1][j + 1];
            }
        }
        return matches[0][0];
    }

    private static int[] tokens(String pattern)
    {
        int[] tokens = new int[pattern.length()];
        int count = 0;
        for (int i = 0; i < pattern.length(); i = pattern.offsetByCodePoints(i, 1))
        {
            int c = pattern.codePointAt(i);
            if (c == LikePatterns.ESCAPE)
            {
                i = pattern.offsetByCodePoints(i, 1);
                if (i >= pattern.length())
                    throw new IllegalArgumentException("Pattern ends with an escape: " + pattern);
                tokens[count++] = pattern.codePointAt(i);
            }
            else if (c == '%')
                tokens[count++] = ANY_STRING;
            else if (c == '_')
                tokens[count++] = ANY_CHAR;
            else
                tokens[count++] = c;
        }
        return Arrays.copyOf(tokens, count);
    }
}
