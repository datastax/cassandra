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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnknownIndexException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDSerializer;

import static org.apache.cassandra.index.sai.disk.format.Version.SAI_DESCRIPTOR;
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

    public static String generateDefaultIndexName(int keyspaceNameLength, String table, ColumnIdentifier column)
    {

        String indexNameUntruncated = PATTERN_NON_WORD_CHAR.matcher(table + '_' + column.toString()).replaceAll("");

        String indexNameTrimmed = indexNameUntruncated
                                  .substring(0,
                                             Math.min(calculateMaxGeneratedIndexNameLength(keyspaceNameLength, table.length()),
                                                      indexNameUntruncated.length()));
        return indexNameTrimmed + INDEX_POSTFIX;
    }

    private static int calculateIndexAllowedLength(int keyspaceNameLength, int tableNameLength)
    {

        int addedLength1 = getAddedLengthFromIndexContextFullName(keyspaceNameLength, tableNameLength);
        int addedLength2 = getAddedLengthFromDescriptorAndVersion();

        int maxAddedLength = Math.max(addedLength1, addedLength2);

        assert maxAddedLength <= SchemaConstants.NAME_LENGTH : "Index name additions are too long";

        return SchemaConstants.NAME_LENGTH - maxAddedLength;
    }

    private static int calculateMaxGeneratedIndexNameLength(int keyspaceNameLength, int tableNameLength)
    {
        int uniquenessSuffixLength = 3;
        int indexNameAddition = uniquenessSuffixLength + INDEX_POSTFIX.length();
        int allowedIndexNameLength = calculateIndexAllowedLength(keyspaceNameLength, tableNameLength);
        if (allowedIndexNameLength < indexNameAddition)
            throw new InvalidQueryException(String.format("Cannot generate index name to fit file names, since the addition take the allowed length %s.", SchemaConstants.NAME_LENGTH));
        return allowedIndexNameLength - indexNameAddition;
    }

    /**
     * Calculates the length of the added prefixes and suffixes from Descriptor constructor
     * and Version.stargazerFileNameFormat.
     *
     * @return the length of the added prefixes and suffixes
     */
    private static int getAddedLengthFromDescriptorAndVersion()
    {
        int separatorLength = 1;
        // Prefixes and suffixes constructed by Version.stargazerFileNameFormat
        int versionNameLength = Version.latest().toString().length();
        int generationLength = 1;
        int fileExtensionLength = 3;
        int addedLength2 = SAI_DESCRIPTOR.length()
                           + versionNameLength
                           + generationLength
                           + fileExtensionLength
                           + IndexComponentType.KD_TREE_POSTING_LISTS.representation.length()
                           + separatorLength * 4;
        // Prefixes from Descriptor constructor
        int indexVersionLength = 2;
        int tableIdLength = 32;
        addedLength2 += indexVersionLength
                        + SSTableFormat.Type.BTI.name().length()
                        + tableIdLength
                        + separatorLength * 2;
        return addedLength2;
    }

    /**
     * Lengths of prefixes and suffixes from IndexContext.getFullName
     *
     * @param keyspaceNameLength the length of the keyspace name
     * @param tableNameLength    the length of the table name
     * @return the length of the added prefixes and suffixes
     */
    private static int getAddedLengthFromIndexContextFullName(int keyspaceNameLength, int tableNameLength)
    {
        int separatorLength = 1;
        int addedLength1 = keyspaceNameLength + tableNameLength + separatorLength * 2;
        if (addedLength1 > SchemaConstants.NAME_LENGTH)
            throw new InvalidQueryException(String.format("Prefix of keyspace and table names together are too long for an index file name: %s. Max length is %s",
                                                          addedLength1,
                                                          SchemaConstants.NAME_LENGTH));
        return addedLength1;
    }

    @VisibleForTesting
    public static String generateDefaultIndexName(String table, ColumnIdentifier column)
    {
        return generateDefaultIndexName(0, table, column);
    }

    public static String generateDefaultIndexName(String table)
    {
        return PATTERN_NON_WORD_CHAR.matcher(table + "_idx").replaceAll("");
    }

    public void validate(TableMetadata table)
    {
        if (!isValidName(name))
            throw new ConfigurationException(String.format("Keyspace name must not be empty, more than %s characters long, "
                                                           + "or contain non-alphanumeric-underscore characters (got \"%s\")",
                                                           SchemaConstants.NAME_LENGTH,
                                                           name));

        if (name.length() > calculateIndexAllowedLength(table.keyspace.length(), table.name.length()))
            throw new ConfigurationException(String.format("Index name %s is too long to be part of constructed file names. Together with added prefixes and suffixes it must fit %s characters.",
                                                           name,
                                                           SchemaConstants.NAME_LENGTH));

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
        return indexNameAliases.getOrDefault(className, className);
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
