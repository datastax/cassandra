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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.JsonUtils;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * Extracts the terms of one {@link JsonPath} from a UTF-8 JSON document, serialized as the given target type.
 * <p>
 * This is the single source of truth for both indexing and post-filtering, so it is pure and deterministic and
 * never throws on document content: anything that does not fit yields no terms.
 * <ul>
 *   <li>missing node, null or object: no terms</li>
 *   <li>scalar: one term if it fits the type</li>
 *   <li>array: one term per scalar element that fits; nested arrays and objects are skipped</li>
 * </ul>
 * Arrays in the middle of a path are addressed with {@code [n]} and are never fanned out.
 */
public final class JsonPathExtractor
{
    private static final Logger logger = LoggerFactory.getLogger(JsonPathExtractor.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    private final JsonPath path;
    private final AbstractType<?> type;

    /**
     * Constructs a JsonPathExtractor for the specified JSON path and target Cassandra type.
     *
     * @param path the JSON path to extract
     * @param type the target Cassandra data type to validate and decompose terms into
     * @throws IllegalArgumentException if the type is not supported for JSON path indexing
     */
    public JsonPathExtractor(JsonPath path, AbstractType<?> type)
    {
        if (!isSupportedType(type))
            throw new IllegalArgumentException("Unsupported type for JSON path index: " + type);
        this.path = path;
        this.type = type;
    }

    /**
     * Checks whether the given Cassandra type is supported as a JSON path index target type.
     */
    public static boolean isSupportedType(AbstractType<?> type)
    {
        return type == UTF8Type.instance || type == AsciiType.instance || type == Int32Type.instance
               || type == LongType.instance || type == DoubleType.instance || type == BooleanType.instance;
    }

    /**
     * Returns the JSON path being extracted.
     */
    public JsonPath path()
    {
        return path;
    }

    /**
     * Returns the target Cassandra data type for extracted terms.
     */
    public AbstractType<?> type()
    {
        return type;
    }

    /**
     * Extracts byte buffer terms matching the configured path and type from the given UTF-8 JSON byte buffer.
     *
     * @param jsonUtf8 the raw UTF-8 JSON buffer
     * @return iterator of decomposed ByteBuffer terms, never null; empty when nothing fits or the document is malformed
     */
    public Iterator<ByteBuffer> extract(ByteBuffer jsonUtf8)
    {
        if (jsonUtf8 == null || !jsonUtf8.hasRemaining())
            return Collections.emptyIterator();

        JsonNode node;
        try
        {
            byte[] bytes = new byte[jsonUtf8.remaining()];
            jsonUtf8.duplicate().get(bytes);
            node = JsonUtils.JSON_OBJECT_MAPPER.readTree(bytes);
        }
        catch (IOException | RuntimeException e)
        {
            noSpamLogger.warn("Ignoring malformed JSON document while extracting path {}: {}", path, e.getMessage());
            return Collections.emptyIterator();
        }

        for (JsonPath.Step step : path.steps())
        {
            if (node == null)
                break;
            if (step instanceof JsonPath.Field field)
                node = node.isObject() ? node.get(field.name()) : null;
            else
                node = node.isArray() ? node.get(((JsonPath.Index) step).index()) : null;
        }
        if (node == null)
            return Collections.emptyIterator();

        List<ByteBuffer> terms = new ArrayList<>();
        if (node.isArray())
        {
            for (JsonNode element : node)
                addIfFits(element, terms);
        }
        else
        {
            addIfFits(node, terms);
        }
        return terms.iterator();
    }

    /**
     * Converts a scalar JSON node to a ByteBuffer term and appends it to the terms list if it conforms to the target type.
     */
    private void addIfFits(JsonNode node, List<ByteBuffer> terms)
    {
        if (node.isTextual())
        {
            String s = node.textValue();
            if (type == UTF8Type.instance)
                terms.add(UTF8Type.instance.decompose(s));
            else if (type == AsciiType.instance && isAscii(s))
                terms.add(AsciiType.instance.decompose(s));
        }
        else if (node.isNumber())
        {
            if (type == DoubleType.instance)
            {
                terms.add(DoubleType.instance.decompose(node.doubleValue()));
            }
            else if (type == Int32Type.instance || type == LongType.instance)
            {
                try
                {
                    if (type == Int32Type.instance)
                        terms.add(Int32Type.instance.decompose(node.decimalValue().intValueExact()));
                    else
                        terms.add(LongType.instance.decompose(node.decimalValue().longValueExact()));
                }
                catch (ArithmeticException e)
                {
                    // fractional or out of range: does not fit
                }
            }
        }
        else if (node.isBoolean() && type == BooleanType.instance)
        {
            terms.add(BooleanType.instance.decompose(node.booleanValue()));
        }
    }

    /**
     * Checks if all characters in the string are within the standard 7-bit ASCII range (0-127).
     */
    private static boolean isAscii(String s)
    {
        for (int i = 0; i < s.length(); i++)
            if (s.charAt(i) > 127)
                return false;
        return true;
    }
}
