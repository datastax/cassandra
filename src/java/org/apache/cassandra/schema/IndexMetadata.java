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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
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
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sasi.SASIIndex;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDSerializer;

import javax.annotation.Nullable;

import static org.apache.cassandra.schema.SchemaConstants.PATTERN_NON_WORD_CHAR;
import static org.apache.cassandra.schema.SchemaConstants.isValidName;

/**
 * An immutable representation of secondary index metadata.
 */
public final class IndexMetadata
{
    private static final Logger logger = LoggerFactory.getLogger(IndexMetadata.class);

    public static final Serializer serializer = new Serializer();

    static final String INDEX_POSTFIX = "_idx";
    /**
     * A mapping of user-friendly index names to their fully qualified index class names.
     */
    private static final Map<String, String> indexNameAliases = new ConcurrentHashMap<>();

    static
    {
        indexNameAliases.put(StorageAttachedIndex.NAME, StorageAttachedIndex.class.getCanonicalName());
        indexNameAliases.put(StorageAttachedIndex.class.getSimpleName().toLowerCase(), StorageAttachedIndex.class.getCanonicalName());
        indexNameAliases.put(SASIIndex.class.getSimpleName(), SASIIndex.class.getCanonicalName());
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
    public final Map<String, String> options;

    private IndexMetadata(String name,
                          Map<String, String> options,
                          Kind kind)
    {
        this.id = UUID.nameUUIDFromBytes(name.getBytes());
        this.name = name;
        this.options = options == null ? ImmutableMap.of() : ImmutableMap.copyOf(options);
        this.kind = kind;
    }

    public static IndexMetadata fromSchemaMetadata(String name, Kind kind, Map<String, String> options)
    {
        return new IndexMetadata(name, options, kind);
    }

    public static IndexMetadata fromIndexTargets(List<IndexTarget> targets,
                                                 String name,
                                                 Kind kind,
                                                 Map<String, String> options)
    {
        Map<String, String> newOptions = new HashMap<>(options);
        newOptions.put(IndexTarget.TARGET_OPTION_NAME, targets.stream()
                                                              .map(IndexTarget::asCqlString)
                                                              .collect(Collectors.joining(", ")));
        return new IndexMetadata(name, newOptions, kind);
    }

    /**
     * Generates a default index name from the table and column names.
     * Characters other than alphanumeric and underscore are removed.
     * Long index names are truncated to fit the length allowing constructing filenames.
     *
     * @param table  the table name
     * @param column the column identifier. Can be null if the index is not column specific.
     * @return the generated index name
     */
    public static String generateDefaultIndexName(String table, @Nullable ColumnIdentifier column)
    {
        String indexNameUncleaned = table;
        if (column != null)
            indexNameUncleaned += '_' + column.toString();
        String indexNameUntrimmed = PATTERN_NON_WORD_CHAR.matcher(indexNameUncleaned).replaceAll("");
        String indexNameTrimmed = indexNameUntrimmed
                                  .substring(0,
                                             Math.min(calculateGeneratedIndexNameMaxLength(),
                                                      indexNameUntrimmed.length()));
        return indexNameTrimmed + INDEX_POSTFIX;
    }

    /**
     * Calculates the maximum length of the generated index name to fit file names.
     * It includes the generated suffixes in account.
     * The calculation depends on how index implements file names construciton from index names.
     * This needs to be addressed, see CNDB-13240.
     *
     * @return the allowed length of the generated index name
     */
    private static int calculateGeneratedIndexNameMaxLength()
    {
        // Speculative assumption that uniqueness breaker will fit into 999.
        // The value is used for trimming the index name if needed.
        // Introducing validation of index name length is TODO for CNDB-13198.
        int uniquenessSuffixLength = 4;
        int indexNameAddition = uniquenessSuffixLength + INDEX_POSTFIX.length();
        int allowedIndexNameLength = Version.calculateIndexNameAllowedLength();

        assert allowedIndexNameLength >= indexNameAddition : "cannot happen with current implementation as allowedIndexNameLength is approximately 255 - ~76. However, allowedIndexNameLength was " + allowedIndexNameLength + " and  indexNameAddition was " + indexNameAddition;

        return allowedIndexNameLength - indexNameAddition;
    }

    public void validate(TableMetadata table)
    {
        if (!isValidName(name, true))
            throw new ConfigurationException(String.format("Index name must not be empty, or contain non-alphanumeric-underscore characters (got \"%s\")",
                                                           name));

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
            validateCustomIndexOptions(table, indexerClass, options);
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
        return indexNameAliases.getOrDefault(className.toLowerCase(), className);
    }

    private void validateCustomIndexOptions(TableMetadata table, Class<? extends Index> indexerClass, Map<String, String> options)
    {
        try
        {
            Map<String, String> filteredOptions = Maps.filterKeys(options, key -> !key.equals(IndexTarget.CUSTOM_INDEX_OPTION_NAME));

            if (filteredOptions.isEmpty())
                return;

            Map<?, ?> unknownOptions;
            try
            {
                unknownOptions = (Map) indexerClass.getMethod("validateOptions", Map.class, TableMetadata.class).invoke(null, filteredOptions, table);
            }
            catch (NoSuchMethodException e)
            {
                unknownOptions = (Map) indexerClass.getMethod("validateOptions", Map.class).invoke(null, filteredOptions);
            }

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
        return Objects.hashCode(id, name, kind, options);
    }

    public boolean equalsWithoutName(IndexMetadata other)
    {
        return Objects.equal(kind, other.kind)
               && Objects.equal(options, other.options);
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

    /**
     * @param metadata the index metadata to join
     * @return a comma-separated list of alphabetically sorted unqualified index names
     */
    public static String joinNames(Iterable<IndexMetadata> metadata)
    {
        TreeSet<String> sortedNames = new TreeSet<>();
        for (IndexMetadata indexMetadata : metadata)
            sortedNames.add(indexMetadata.name);
        return String.join(",", sortedNames);
    }

    public static Set<String> toNames(Set<IndexMetadata> indexes)
    {
        Set<String> included = new HashSet<>(indexes.size());
        for (IndexMetadata i : indexes)
            included.add(i.name);
        return included;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder(this)
               .append("id", id.toString())
               .append("name", name)
               .append("kind", kind)
               .append("options", options)
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

            if (!copyOptions.isEmpty())
                builder.append(" WITH OPTIONS = ")
                       .append(copyOptions);
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
