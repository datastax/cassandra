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

import java.util.Map;
import java.util.Optional;

import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.TargetParser;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.analyzer.NonTokenizingOptions;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;

/**
 * Helpers for SAI indexes defined over a JSON path of a text/ascii column via the
 * {@code json_path} and {@code json_type} index options.
 */
public final class JsonPathIndexes
{
    public static final String JSON_PATH = "json_path";
    public static final String JSON_TYPE = "json_type";

    private JsonPathIndexes()
    {
    }

    /**
     * Determines whether the given index options map specifies a JSON path index.
     */
    public static boolean isJsonPathIndex(Map<String, String> options)
    {
        return options.containsKey(JSON_PATH) || options.containsKey(JSON_TYPE);
    }

    /**
     * Parses and returns the {@link JsonPath} specified in the index options.
     *
     * @throws InvalidRequestException if json_path is missing or invalid
     */
    public static JsonPath path(Map<String, String> options)
    {
        String value = options.get(JSON_PATH);
        if (value == null)
            throw new InvalidRequestException("Option '" + JSON_PATH + "' is required when '" + JSON_TYPE + "' is specified");
        try
        {
            return JsonPath.parse(value);
        }
        catch (IllegalArgumentException e)
        {
            throw new InvalidRequestException("Invalid '" + JSON_PATH + "' option '" + value + "': " + e.getMessage());
        }
    }

    /**
     * Resolves the target Cassandra {@link AbstractType} specified in the json_type index option.
     *
     * @throws InvalidRequestException if json_type is missing or unsupported
     */
    public static AbstractType<?> type(Map<String, String> options)
    {
        String value = options.get(JSON_TYPE);
        if (value == null)
            throw new InvalidRequestException("Option '" + JSON_TYPE + "' is required when '" + JSON_PATH + "' is specified");
        switch (value.trim().toLowerCase())
        {
            case "text": return UTF8Type.instance;
            case "ascii": return AsciiType.instance;
            case "int": return Int32Type.instance;
            case "bigint": return LongType.instance;
            case "double": return DoubleType.instance;
            case "boolean": return BooleanType.instance;
            default:
                throw new InvalidRequestException("Unsupported '" + JSON_TYPE + "' option '" + value +
                                                  "'; supported values are text, ascii, int, bigint, double, boolean");
        }
    }

    /**
     * Validates that the JSON index options are well-formed and compatible with the target column and settings.
     */
    public static void validate(Map<String, String> options, Pair<ColumnMetadata, IndexTarget.Type> target)
    {
        path(options);
        type(options);
        if ((target.right != IndexTarget.Type.VALUES && target.right != IndexTarget.Type.SIMPLE)
            || !(target.left.type instanceof UTF8Type || target.left.type instanceof AsciiType))
            throw new InvalidRequestException("Options '" + JSON_PATH + "' and '" + JSON_TYPE +
                                              "' require a plain column target of type text or ascii");
        if (AbstractAnalyzer.isAnalyzed(options)
            || options.containsKey(NonTokenizingOptions.CASE_SENSITIVE)
            || options.containsKey(NonTokenizingOptions.NORMALIZE)
            || options.containsKey(NonTokenizingOptions.ASCII))
            throw new InvalidRequestException("Options '" + JSON_PATH + "' and '" + JSON_TYPE +
                                              "' cannot be combined with analyzer or case_sensitive/normalize/ascii options");
    }

    /**
     * Returns the normalized JSON path string from index options, or null if not a JSON path index.
     */
    public static String normalizedPathOrNull(Map<String, String> options)
    {
        if (!options.containsKey(JSON_PATH))
            return null;
        try
        {
            return JsonPath.parse(options.get(JSON_PATH)).toString();
        }
        catch (IllegalArgumentException e)
        {
            return options.get(JSON_PATH);
        }
    }

    /**
     * Looks up any existing StorageAttachedIndex on the given column matching the specified JSON path and returns its indexed type.
     */
    public static Optional<AbstractType<?>> findIndexedType(TableMetadata table, ColumnMetadata column, JsonPath path)
    {
        for (IndexMetadata index : table.indexes)
        {
            if (!index.getIndexClassName().equals(StorageAttachedIndex.class.getName()) || !isJsonPathIndex(index.options))
                continue;
            Pair<ColumnMetadata, IndexTarget.Type> target = TargetParser.parse(table, index);
            if (target == null || !target.left.equals(column))
                continue;
            if (path.equals(path(index.options)))
                return Optional.of(type(index.options));
        }
        return Optional.empty();
    }
}
