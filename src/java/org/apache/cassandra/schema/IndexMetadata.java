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

package org.apache.cassandra.schema;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnknownIndexException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDSerializer;

import static org.apache.cassandra.cql3.statements.schema.IndexAttributes.KW_KEY_COMPRESSION;
import static org.apache.cassandra.cql3.statements.schema.IndexAttributes.KW_VALUE_COMPRESSION;

import static org.apache.cassandra.schema.SchemaConstants.PATTERN_NON_WORD_CHAR;
import static org.apache.cassandra.schema.SchemaConstants.isValidName;

/**
 * An immutable representation of secondary index metadata.
 */
public final class IndexMetadata
{
    private static final Logger logger = LoggerFactory.getLogger(IndexMetadata.class);

    public static final Serializer serializer = new Serializer();

    /**
     * A mapping of user-friendly index names to their fully qualified index class names.
     */
    private static final Map<String, String> indexNameAliases = new ConcurrentHashMap<>();

    static
    {
        indexNameAliases.put(StorageAttachedIndex.class.getSimpleName(), StorageAttachedIndex.class.getCanonicalName());
    }

    public enum Kind
    {
        KEYS, CUSTOM, COMPOSITES
    }

    // UUID for serialization. This is a deterministic UUID generated from the index name
    // Both the id and name are guaranteed unique per keyspace.
    public final UUID id;
    public final String name;
    public final Kind kind;

    /**
     * Compression parameters to use for compressing primary keys of the table the index points to.
     * Shared by all indexes on the same table.
     */
    public final CompressionParams keyCompression;

    /** Compression parameters to use for compressing values (terms) of the indexed column */
    public final CompressionParams valueCompression;

    /** Other index-specific options */
    public final Map<String, String> options;

    private IndexMetadata(String name,
                          Map<String, String> options,
                          Kind kind,
                          CompressionParams keyCompression,
                          CompressionParams valueCompression)
    {
        this.id = UUID.nameUUIDFromBytes(name.getBytes());
        this.name = name;
        this.options = options == null ? ImmutableMap.of() : ImmutableMap.copyOf(options);
        this.kind = kind;
        this.keyCompression = keyCompression;
        this.valueCompression = valueCompression;
    }

    public static IndexMetadata fromSchemaMetadata(String name, Kind kind, Map<String, String> options)
    {
        return new IndexMetadata(name, options, kind, CompressionParams.noCompression(), CompressionParams.noCompression());
    }

    public static IndexMetadata fromSchemaMetadata(String name,
                                                   Kind kind,
                                                   Map<String, String> options,
                                                   CompressionParams keyCompression,
                                                   CompressionParams valueCompression)
    {
        return new IndexMetadata(name, options, kind, keyCompression, valueCompression);
    }

    public static IndexMetadata fromIndexTargets(List<IndexTarget> targets,
                                                 String name,
                                                 Kind kind,
                                                 Map<String, String> options)
    {
        return fromIndexTargets(targets, name, kind, options,
                                CompressionParams.noCompression(), CompressionParams.noCompression());
    }

    public static IndexMetadata fromIndexTargets(List<IndexTarget> targets,
                                                 String name,
                                                 Kind kind,
                                                 Map<String, String> options,
                                                 CompressionParams keyCompression,
                                                 CompressionParams valueCompression)
    {
        Map<String, String> newOptions = new HashMap<>(options);
        newOptions.put(IndexTarget.TARGET_OPTION_NAME, targets.stream()
                                                              .map(IndexTarget::asCqlString)
                                                              .collect(Collectors.joining(", ")));
        return new IndexMetadata(name, newOptions, kind, keyCompression, valueCompression);
    }

    public static String generateDefaultIndexName(String table, ColumnIdentifier column)
    {
        return PATTERN_NON_WORD_CHAR.matcher(table + '_' + column.toString() + "_idx").replaceAll("");
    }

    public static String generateDefaultIndexName(String table)
    {
        return PATTERN_NON_WORD_CHAR.matcher(table + '_' + "idx").replaceAll("");
    }

    /**
     * Creates a copy IndexMetadata with keyCompression parameter set to given value
     */
    public IndexMetadata withKeyCompression(CompressionParams keyCompression)
    {
        return fromSchemaMetadata(name, kind, options, keyCompression, valueCompression);
    }

    public void validate(TableMetadata table)
    {
        if (!isValidName(name, true))
            throw new ConfigurationException("Illegal index name " + name);

        if (kind == null)
            throw new ConfigurationException("Index kind is null for index " + name);

        if (kind == Kind.CUSTOM)
        {
            if (options == null || !options.containsKey(IndexTarget.CUSTOM_INDEX_OPTION_NAME))
                throw new ConfigurationException(String.format("Required option missing for index %s : %s",
                                                               name, IndexTarget.CUSTOM_INDEX_OPTION_NAME));
            // Find any aliases to the fully qualified index class name:
            String className = expandAliases(options.get(IndexTarget.CUSTOM_INDEX_OPTION_NAME));

            Class<Index> indexerClass = FBUtilities.classForName(className, "custom indexer");
            if (!Index.class.isAssignableFrom(indexerClass))
                throw new ConfigurationException(String.format("Specified Indexer class (%s) does not implement the Indexer interface", className));
            validateCustomIndexOptions(table, indexerClass);
        }
    }

    public String getIndexClassName()
    {
        if (isCustom())
            return expandAliases(options.get(IndexTarget.CUSTOM_INDEX_OPTION_NAME));
        return CassandraIndex.class.getName();
    }

    public static String expandAliases(String className)
    {
        return indexNameAliases.getOrDefault(className, className);
    }

    private void validateCustomIndexOptions(TableMetadata table, Class<? extends Index> indexerClass)
    {
        try
        {
            Map<String, String> filteredOptions = Maps.filterKeys(options, key -> !key.equals(IndexTarget.CUSTOM_INDEX_OPTION_NAME));

            if (filteredOptions.isEmpty())
                return;

            Map<?, ?> unknownOptions;
            try
            {
                unknownOptions = (Map) indexerClass.getMethod("validateOptions", IndexMetadata.class, TableMetadata.class).invoke(null, this, table);
            }
            catch (NoSuchMethodException e)
            {
                try
                {
                    unknownOptions = (Map) indexerClass.getMethod("validateOptions", Map.class, TableMetadata.class).invoke(null, filteredOptions, table);
                }
                catch (NoSuchMethodException e2)
                {
                    unknownOptions = (Map) indexerClass.getMethod("validateOptions", Map.class).invoke(null, filteredOptions);
                }
            }

            unknownOptions.remove(IndexTarget.CUSTOM_INDEX_OPTION_NAME);
            if (!unknownOptions.isEmpty())
                throw new ConfigurationException(String.format("Properties specified %s are not understood by %s", unknownOptions.keySet(), indexerClass.getSimpleName()));
        }
        catch (NoSuchMethodException e)
        {
            logger.info("Indexer {} does not have a static validateOptions method. Validation ignored",
                        indexerClass.getName());
        }
        catch (InvocationTargetException e)
        {
            if (e.getTargetException() instanceof RequestValidationException)
                throw (RequestValidationException) e.getTargetException();
            if (e.getTargetException() instanceof ConfigurationException)
                throw (ConfigurationException) e.getTargetException();
            throw new ConfigurationException("Failed to validate custom indexer options: " + options, e);
        }
        catch (ConfigurationException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            throw new ConfigurationException("Failed to validate custom indexer options: " + options);
        }
    }

    public boolean isCustom()
    {
        return kind == Kind.CUSTOM;
    }

    public boolean isKeys()
    {
        return kind == Kind.KEYS;
    }

    public boolean isComposites()
    {
        return kind == Kind.COMPOSITES;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(id, name, kind, options, keyCompression, valueCompression);
    }

    public boolean equalsWithoutName(IndexMetadata other)
    {
        return Objects.equal(kind, other.kind)
               && Objects.equal(options, other.options)
               && Objects.equal(keyCompression, other.keyCompression)
               && Objects.equal(valueCompression, other.valueCompression);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
            return true;

        if (!(obj instanceof IndexMetadata))
            return false;

        IndexMetadata other = (IndexMetadata) obj;

        return Objects.equal(id, other.id) && Objects.equal(name, other.name) && equalsWithoutName(other);
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder(this)
               .append("id", id.toString())
               .append("name", name)
               .append("kind", kind)
               .append("options", options)
               .append("keyCompression", keyCompression)
               .append("valueCompression", valueCompression)
               .build();
    }

    public String toCqlString(TableMetadata table, boolean ifNotExists)
    {
        CqlBuilder builder = new CqlBuilder();
        appendCqlTo(builder, table, ifNotExists);
        return builder.toString();
    }

    /**
     * Appends to the specified builder the CQL used to create this index.
     * @param builder the builder to which the CQL myst be appended
     * @param table the parent table
     * @param ifNotExists includes "IF NOT EXISTS" into statement
     */
    public void appendCqlTo(CqlBuilder builder, TableMetadata table, boolean ifNotExists)
    {
        if (isCustom())
        {
            Map<String, String> copyOptions = new HashMap<>(options);

            builder.append("CREATE CUSTOM INDEX ");

            if (ifNotExists)
            {
                builder.append("IF NOT EXISTS ");
            }

            builder.appendQuotingIfNeeded(name)
                   .append(" ON ")
                   .append(table.toString())
                   .append(" (")
                   .append(copyOptions.remove(IndexTarget.TARGET_OPTION_NAME))
                   .append(") USING ")
                   .appendWithSingleQuotes(copyOptions.remove(IndexTarget.CUSTOM_INDEX_OPTION_NAME));


            builder.appendOptions(b -> {
                b.append("options", copyOptions);
                if (valueCompression.isEnabled())
                    b.append(KW_VALUE_COMPRESSION, valueCompression.asMap());
                if (keyCompression.isEnabled())
                    b.append(KW_KEY_COMPRESSION, keyCompression.asMap());
            });
        }
        else
        {
            builder.append("CREATE INDEX ");

            if (ifNotExists)
            {
                builder.append("IF NOT EXISTS ");
            }

            builder.appendQuotingIfNeeded(name)
                   .append(" ON ")
                   .append(table.toString())
                   .append(" (")
                   .append(options.get(IndexTarget.TARGET_OPTION_NAME))
                   .append(')');
        }
        builder.append(';');
    }

    public static class Serializer
    {
        public void serialize(IndexMetadata metadata, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(metadata.id, out, version);
        }

        public IndexMetadata deserialize(DataInputPlus in, int version, TableMetadata table) throws IOException
        {
            UUID id = UUIDSerializer.serializer.deserialize(in, version);
            return table.indexes.get(id).orElseThrow(() -> new UnknownIndexException(table, id));
        }

        public long serializedSize(IndexMetadata metadata, int version)
        {
            return UUIDSerializer.serializer.serializedSize(metadata.id, version);
        }
    }
}
