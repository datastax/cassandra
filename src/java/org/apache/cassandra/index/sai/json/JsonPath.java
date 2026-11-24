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

package org.apache.cassandra.index.sai.json;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * A path into a JSON document, limited to the subset {@code $}, {@code .name}, {@code ['quoted name']} and
 * {@code [n]} (n >= 0). Equality and {@link #toString()} are based on the normalized form, so
 * {@code $['a'].b} and {@code $.a.b} are the same path.
 */
public final class JsonPath
{
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    /**
     * A single step in a JSON path, representing either a named field or an array index.
     */
    public sealed interface Step permits Field, Index
    {
    }

    /**
     * A field navigation step representing property access by field name.
     */
    public record Field(String name) implements Step
    {
    }

    /**
     * An array index navigation step representing element access by 0-based integer index.
     */
    public record Index(int index) implements Step
    {
    }

    private final List<Step> steps;

    /**
     * Constructs an immutable JsonPath from the given list of navigation steps.
     */
    private JsonPath(List<Step> steps)
    {
        this.steps = Collections.unmodifiableList(steps);
    }

    /**
     * Parses a string representation of a JSON path into a normalized {@link JsonPath} instance.
     *
     * @param path the JSON path string starting with '$'
     * @return the parsed and normalized JsonPath
     * @throws IllegalArgumentException if the path is not in the supported subset; the message names the position
     */
    public static JsonPath parse(String path)
    {
        if (path == null || path.isEmpty() || path.charAt(0) != '$')
            throw error(path, 0, "path must start with '$'");

        List<Step> steps = new ArrayList<>();
        int pos = 1;
        int len = path.length();
        while (pos < len)
        {
            char c = path.charAt(pos);
            if (c == '.')
            {
                int start = ++pos;
                while (pos < len && isIdentifierChar(path.charAt(pos), pos == start))
                    pos++;
                if (pos == start)
                    throw error(path, start, "expected field name after '.'");
                steps.add(new Field(path.substring(start, pos)));
            }
            else if (c == '[')
            {
                pos++;
                if (pos >= len)
                    throw error(path, pos, "unterminated '['");
                if (path.charAt(pos) == '\'')
                {
                    StringBuilder name = new StringBuilder();
                    pos = parseQuoted(path, pos + 1, name);
                    steps.add(new Field(name.toString()));
                }
                else
                {
                    int start = pos;
                    while (pos < len && path.charAt(pos) >= '0' && path.charAt(pos) <= '9')
                        pos++;
                    if (pos == start)
                        throw error(path, start, "expected quoted name or non-negative index");
                    try
                    {
                        steps.add(new Index(Integer.parseInt(path.substring(start, pos))));
                    }
                    catch (NumberFormatException e)
                    {
                        throw error(path, start, "index out of range");
                    }
                }
                if (pos >= len || path.charAt(pos) != ']')
                    throw error(path, pos, "expected ']'");
                pos++;
            }
            else
            {
                throw error(path, pos, "unexpected character '" + c + "'");
            }
        }
        return new JsonPath(steps);
    }

    /**
     * Parses a single-quoted field name starting after the opening quote; returns the position after the closing quote.
     */
    private static int parseQuoted(String path, int pos, StringBuilder name)
    {
        int len = path.length();
        while (pos < len)
        {
            char c = path.charAt(pos);
            if (c == '\'')
                return pos + 1;
            if (c == '\\')
            {
                pos++;
                if (pos >= len || (path.charAt(pos) != '\\' && path.charAt(pos) != '\''))
                    throw error(path, pos, "invalid escape, only \\\\ and \\' are supported");
                c = path.charAt(pos);
            }
            name.append(c);
            pos++;
        }
        throw error(path, len, "unterminated quoted name");
    }

    /**
     * Determines whether a character is valid in an unquoted JSON path field identifier.
     */
    private static boolean isIdentifierChar(char c, boolean first)
    {
        return c == '_' || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (!first && c >= '0' && c <= '9');
    }

    /**
     * Constructs a descriptive IllegalArgumentException for JSON path parsing failures.
     */
    private static IllegalArgumentException error(String path, int pos, String message)
    {
        return new IllegalArgumentException(String.format("Invalid JSON path '%s' at position %d: %s", path, pos, message));
    }

    /**
     * Returns the list of parsed navigation steps that comprise this JSON path.
     */
    public List<Step> steps()
    {
        return steps;
    }

    /**
     * Formats this JSON path in its canonical, normalized string representation.
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("$");
        for (Step step : steps)
        {
            if (step instanceof Index i)
            {
                sb.append('[').append(i.index()).append(']');
            }
            else
            {
                String name = ((Field) step).name();
                if (IDENTIFIER.matcher(name).matches())
                    sb.append('.').append(name);
                else
                    sb.append("['").append(name.replace("\\", "\\\\").replace("'", "\\'")).append("']");
            }
        }
        return sb.toString();
    }

    /**
     * Checks equality against another object based on the list of navigation steps.
     */
    @Override
    public boolean equals(Object o)
    {
        return this == o || (o instanceof JsonPath && steps.equals(((JsonPath) o).steps));
    }

    /**
     * Computes the hash code based on the navigation steps.
     */
    @Override
    public int hashCode()
    {
        return steps.hashCode();
    }
}
