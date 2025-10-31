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
package org.apache.cassandra.cql3.statements.schema;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.StreamSupport;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.statements.RawKeyspaceAwareStatement;
import org.apache.cassandra.cql3.statements.schema.IndexTarget.Type;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.guardrails.Threshold;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;
import org.apache.cassandra.transport.Event.SchemaChange.Target;
import org.apache.cassandra.utils.FBUtilities;

import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Iterables.tryFind;
import static org.apache.cassandra.config.CassandraRelevantProperties.INDEX_UNKNOWN_IGNORE;

public final class CreateIndexStatement extends AlterSchemaStatement
{
    private static final Logger logger = LoggerFactory.getLogger(CreateIndexStatement.class);

    public static final String KEYSPACE_DOES_NOT_EXIST = "Keyspace '%s' doesn't exist";
    public static final String TABLE_DOES_NOT_EXIST = "Table '%s' doesn't exist";
    public static final String COUNTER_TABLES_NOT_SUPPORTED = "Secondary indexes on counter tables aren't supported";
    public static final String MATERIALIZED_VIEWS_NOT_SUPPORTED = "Secondary indexes on materialized views aren't supported";
    public static final String TRANSIENTLY_REPLICATED_KEYSPACE_NOT_SUPPORTED = "Secondary indexes are not supported on transiently replicated keyspaces";
    public static final String CUSTOM_CREATE_WITHOUT_COLUMN = "Only CUSTOM indexes can be created without specifying a target column";
    public static final String CUSTOM_MULTIPLE_COLUMNS = "Only CUSTOM indexes support multiple columns";
    public static final String DUPLICATE_TARGET_COLUMN = "Duplicate column '%s' in index target list";
    public static final String COLUMN_DOES_NOT_EXIST = "Column '%s' doesn't exist";
    public static final String INVALID_CUSTOM_INDEX_TARGET = "Column '%s' is longer than the permissible name length of %d characters or" +
                                                             " contains non-alphanumeric-underscore characters";
    public static final String COLLECTIONS_WITH_DURATIONS_NOT_SUPPORTED = "Secondary indexes are not supported on collections containing durations";
    public static final String TUPLES_WITH_DURATIONS_NOT_SUPPORTED = "Secondary indexes are not supported on tuples containing durations";
    public static final String DURATIONS_NOT_SUPPORTED = "Secondary indexes are not supported on duration columns";
    public static final String UDTS_WITH_DURATIONS_NOT_SUPPORTED = "Secondary indexes are not supported on UDTs containing durations";
    public static final String PRIMARY_KEY_IN_COMPACT_STORAGE = "Secondary indexes are not supported on PRIMARY KEY columns in COMPACT STORAGE tables";
    public static final String COMPACT_COLUMN_IN_COMPACT_STORAGE = "Secondary indexes are not supported on compact value column of COMPACT STORAGE tables";
    public static final String ONLY_PARTITION_KEY = "Cannot create secondary index on the only partition key column %s";
    public static final String CREATE_ON_FROZEN_COLUMN = "Cannot create %s() index on frozen column %s. Frozen collections are immutable and must be fully " +
                                                         "indexed by using the 'full(%s)' modifier";
    public static final String FULL_ON_FROZEN_COLLECTIONS = "full() indexes can only be created on frozen collections";
    public static final String NON_COLLECTION_SIMPLE_INDEX = "Cannot create %s() index on %s. Non-collection columns only support simple indexes";
    public static final String CREATE_WITH_NON_MAP_TYPE = "Cannot create index on %s of column %s with non-map type";
    public static final String CREATE_ON_NON_FROZEN_UDT = "Cannot create index on non-frozen UDT column %s";
    public static final String INDEX_ALREADY_EXISTS = "Index '%s' already exists";
    public static final String INDEX_DUPLICATE_OF_EXISTING = "Index %s is a duplicate of existing index %s";
    public static final String KEYSPACE_DOES_NOT_MATCH_TABLE = "Keyspace name '%s' doesn't match table name '%s'";
    public static final String KEYSPACE_DOES_NOT_MATCH_INDEX = "Keyspace name '%s' doesn't match index name '%s'";
    public static final String MUST_SPECIFY_INDEX_IMPLEMENTATION = "Must specify index implementation via USING";

    private final String indexName;
    private final String tableName;
    private final List<IndexTarget.Raw> rawIndexTargets;
    private final IndexAttributes attrs;
    private final boolean ifNotExists;

    private ClientState state;
    private static final String DSE_INDEX_WARNING = "Index %s was not created. DSE custom index (%s) is not " +
                                                    "supported. Consult the docs on alternatives (SAI indexes, " +
                                                    "Secondary Indexes).";

    @VisibleForTesting
    public static final Set<String> DSE_INDEXES = ImmutableSet.of(
        "com.datastax.bdp.cassandra.index.solr.SolrSecondaryIndex",
        "com.datastax.bdp.cassandra.index.solr.ThriftSolrSecondaryIndex",
        "com.datastax.bdp.cassandra.index.solr.Cql3SolrSecondaryIndex",
        "com.datastax.bdp.search.solr.ThriftSolrSecondaryIndex",
        "com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex"
    );

    public CreateIndexStatement(String queryString,
                                String keyspaceName,
                                String tableName,
                                String indexName,
                                List<IndexTarget.Raw> rawIndexTargets,
                                IndexAttributes attrs,
                                boolean ifNotExists)
    {
        super(queryString, keyspaceName);
        this.tableName = tableName;
        this.indexName = indexName;
        this.rawIndexTargets = rawIndexTargets;
        this.attrs = attrs;
        this.ifNotExists = ifNotExists;
    }

    @Override
    public void validate(ClientState state)
    {
        super.validate(state);

        // Check the length of a valid index name.
        // Non-valid indexes are validated in IndexMetadata#validate.
        if (!state.isInternal
            && SchemaConstants.isValidName(indexName, true)
            && indexName.length() > SchemaConstants.INDEX_NAME_LENGTH)

            throw ire("Index name shouldn't be more than %s characters long (got %s chars for %s)",
                      SchemaConstants.INDEX_NAME_LENGTH, indexName.length(), indexName);

        // save the query state to use it for guardrails validation in #apply
        this.state = state;
    }

    public Keyspaces apply(Keyspaces schema)
    {
        if (isDseIndexCreateStatement())
        {
            // DSE indexes are not supported. The index is not created, the attempt is ignored (doesn't cause error),
            // a meaningfull warning is returned instead.
            return schema;
        }

        attrs.maybeApplyDefaultIndex();
        attrs.validate();

        Guardrails.createSecondaryIndexesEnabled.ensureEnabled("Creating secondary indexes", state);

        KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);
        if (null == keyspace)
            throw ire(KEYSPACE_DOES_NOT_EXIST, keyspaceName);

        TableMetadata table = keyspace.getTableOrViewNullable(tableName);
        if (null == table)
            throw ire(TABLE_DOES_NOT_EXIST, tableName);

        if (null != indexName && keyspace.hasIndex(indexName))
        {
            if (ifNotExists)
                return schema;

            throw ire(INDEX_ALREADY_EXISTS, indexName);
        }

        if (table.isCounter())
            throw ire(COUNTER_TABLES_NOT_SUPPORTED);

        if (table.isView())
            throw ire(MATERIALIZED_VIEWS_NOT_SUPPORTED);

        if (keyspace.createReplicationStrategy().hasTransientReplicas())
            throw new InvalidRequestException(TRANSIENTLY_REPLICATED_KEYSPACE_NOT_SUPPORTED);

        List<IndexTarget> indexTargets = Lists.newArrayList(transform(rawIndexTargets, t -> t.prepare(table)));

        if (indexTargets.isEmpty() && !attrs.isCustom)
            throw ire(CUSTOM_CREATE_WITHOUT_COLUMN);

        if (indexTargets.size() > 1)
        {
            if (!attrs.isCustom)
                throw ire(CUSTOM_MULTIPLE_COLUMNS);

            Set<ColumnIdentifier> columns = new HashSet<>();
            for (IndexTarget target : indexTargets)
                if (!columns.add(target.column))
                    throw ire(DUPLICATE_TARGET_COLUMN, target.column);
        }

        IndexMetadata.Kind kind = attrs.isCustom ? IndexMetadata.Kind.CUSTOM : IndexMetadata.Kind.COMPOSITES;

        indexTargets.forEach(t -> validateIndexTarget(table, kind, t));

        String name = null == indexName ? generateIndexName(keyspace, indexTargets) : indexName;

        Map<String, String> options = attrs.isCustom ? attrs.getOptions() : Collections.emptyMap();

        IndexMetadata index = IndexMetadata.fromIndexTargets(indexTargets, name, kind, options);

        String className = index.getIndexClassName();
        if (isUnknownCustomIndexCreateStatement(className) && INDEX_UNKNOWN_IGNORE.getBoolean())
        {
            logger.error("Cannot find index type {}, but '{}' is true so ignoring index {} creation",
                         className, INDEX_UNKNOWN_IGNORE.getKey(), indexName);
            return schema;
        }

        IndexGuardrails guardRails = IndexGuardrails.forClassName(className);
        String indexDescription = indexName == null ? String.format("on table %s", table.name) : String.format("%s on table %s", indexName, table.name);

        // Guardrail to limit number of secondary indexes (per table)
        if (guardRails.hasPerTableThreshold())
        {
            long indexesOnSameTable = table.indexes.stream().filter(other -> className.equals(other.getIndexClassName())).count();
            guardRails.perTableThreshold.guard(indexesOnSameTable + 1, indexDescription,false, state);
        }

        // Guardrail to limit number of secondary indexes (total)
        if (guardRails.hasTotalThreshold())
        {
            long indexesOnAllTables = StreamSupport.stream(Keyspace.all().spliterator(), false).flatMap(ks -> ks.getColumnFamilyStores().stream())
                                                   .flatMap(ks -> ks.indexManager.listIndexes().stream())
                                                   .map(i -> i.getIndexMetadata().getIndexClassName())
                                                   .filter(className::equals).count();
            guardRails.totalThreshold.guard(indexesOnAllTables + 1, indexDescription, false, state);
        }

        // check to disallow creation of an index which duplicates an existing one in all but name
        IndexMetadata equalIndex = tryFind(table.indexes, i -> i.equalsWithoutName(index)).orNull();
        if (null != equalIndex)
        {
            if (ifNotExists)
                return schema;

            throw ire(INDEX_DUPLICATE_OF_EXISTING, index.name, equalIndex.name);
        }

        TableMetadata newTable = table.withSwapped(table.indexes.with(index));
        newTable.validate();

        return schema.withAddedOrUpdated(keyspace.withSwapped(keyspace.tables.withSwapped(newTable)));
    }

    @Override
    Set<String> clientWarnings(KeyspacesDiff diff)
    {
        if (isDseIndexCreateStatement())
            return ImmutableSet.of(String.format(DSE_INDEX_WARNING, indexName, attrs.customClass));

        return ImmutableSet.of();
    }

    private boolean isDseIndexCreateStatement()
    {
        return DSE_INDEXES.contains(attrs.customClass);
    }

    private boolean isUnknownCustomIndexCreateStatement(String className)
    {
        try
        {
            // mimic what IndexMetadata.validate(..) does
            FBUtilities.classForName(className, "custom indexer");
            return false;
        }
        catch (ConfigurationException ex)
        {
            return true;
        }
    }

    private void validateIndexTarget(TableMetadata table, IndexMetadata.Kind kind, IndexTarget target)
    {
        ColumnMetadata column = table.getColumn(target.column);

        if (null == column)
            throw ire(COLUMN_DOES_NOT_EXIST, target.column);

        AbstractType<?> baseType = column.type.unwrap();

        if (column.type.referencesDuration())
        {
            if (column.type.isCollection())
                throw ire(COLLECTIONS_WITH_DURATIONS_NOT_SUPPORTED);

            if (column.type.isTuple())
                throw ire(TUPLES_WITH_DURATIONS_NOT_SUPPORTED);

            if (column.type.isUDT())
                throw  ire(UDTS_WITH_DURATIONS_NOT_SUPPORTED);

            throw ire(DURATIONS_NOT_SUPPORTED);
        }

        if (table.isCompactTable())
        {
            TableMetadata.CompactTableMetadata compactTable = (TableMetadata.CompactTableMetadata) table;
            if (column.isPrimaryKeyColumn())
                throw new InvalidRequestException(PRIMARY_KEY_IN_COMPACT_STORAGE);
            if (compactTable.compactValueColumn.equals(column))
                throw new InvalidRequestException(COMPACT_COLUMN_IN_COMPACT_STORAGE);
        }

        if (column.isPartitionKey() && table.partitionKeyColumns().size() == 1)
            throw ire(ONLY_PARTITION_KEY, column);

        if (baseType.isCollection() && !baseType.isMultiCell() && target.type != Type.FULL)
            throw ire(CREATE_ON_FROZEN_COLUMN, target.type, column, column.name.toCQLString());

        if ((!baseType.isCollection() || baseType.isMultiCell) && target.type == Type.FULL)
            throw ire(FULL_ON_FROZEN_COLLECTIONS);

        if (!baseType.isCollection() && target.type != Type.SIMPLE)
            throw ire(NON_COLLECTION_SIMPLE_INDEX, target.type, column);

        if (!(baseType instanceof MapType && baseType.isMultiCell()) && (target.type == Type.KEYS || target.type == Type.KEYS_AND_VALUES))
            throw ire(CREATE_WITH_NON_MAP_TYPE, target.type, column);

        if (column.type.isUDT() && column.type.isMultiCell())
            throw ire(CREATE_ON_NON_FROZEN_UDT, column);
    }

    private String generateIndexName(KeyspaceMetadata keyspace, List<IndexTarget> targets)
    {
        String baseName = targets.size() == 1
                        ? IndexMetadata.generateDefaultIndexName(tableName, targets.get(0).column)
                        : IndexMetadata.generateDefaultIndexName(tableName, null);
        return keyspace.findAvailableIndexName(baseName);
    }

    SchemaChange schemaChangeEvent(KeyspacesDiff diff)
    {
        return new SchemaChange(Change.UPDATED, Target.TABLE, keyspaceName, tableName);
    }

    public void authorize(ClientState client)
    {
        client.ensureTablePermission(keyspaceName, tableName, Permission.ALTER);
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.CREATE_INDEX, keyspaceName, indexName);
    }

    public String toString()
    {
        return String.format("%s (%s, %s)", getClass().getSimpleName(), keyspaceName, indexName);
    }

    public static final class Raw extends RawKeyspaceAwareStatement<CreateIndexStatement>
    {
        private final QualifiedName tableName;
        private final QualifiedName indexName;
        private final List<IndexTarget.Raw> rawIndexTargets;
        private final IndexAttributes attrs;
        private final boolean ifNotExists;

        public Raw(QualifiedName tableName,
                   QualifiedName indexName,
                   List<IndexTarget.Raw> rawIndexTargets,
                   IndexAttributes attrs,
                   boolean ifNotExists)
        {
            this.tableName = tableName;
            this.indexName = indexName;
            this.rawIndexTargets = rawIndexTargets;
            this.attrs = attrs;
            this.ifNotExists = ifNotExists;
        }

        @Override
        public CreateIndexStatement prepare(ClientState state, UnaryOperator<String> keyspaceMapper)
        {
            String keyspaceName = tableName.hasKeyspace()
                                ? tableName.getKeyspace()
                                : indexName.hasKeyspace() ? indexName.getKeyspace() : state.getKeyspace();

            if (tableName.hasKeyspace() && !keyspaceName.equals(tableName.getKeyspace()))
                throw ire(KEYSPACE_DOES_NOT_MATCH_TABLE, keyspaceName, tableName);

            if (indexName.hasKeyspace() && !keyspaceName.equals(indexName.getKeyspace()))
                throw ire(KEYSPACE_DOES_NOT_MATCH_INDEX, keyspaceName, tableName);

            // Set the configured default 2i implementation if one isn't specified with USING:
            if (attrs.customClass == null)
            {
                if (DatabaseDescriptor.getDefaultSecondaryIndexEnabled())
                    attrs.customClass = DatabaseDescriptor.getDefaultSecondaryIndex();
                else
                    // However, operators may require an implementation be specified
                    throw ire(MUST_SPECIFY_INDEX_IMPLEMENTATION);
            }

            // If we explicitly specify the index type "legacy_local_table", we can just clear the custom class, and the
            // non-custom 2i creation process will begin. Otherwise, if an index type has been specified with
            // USING, make sure the appropriate custom index is created.
            if (attrs.customClass != null)
            {
                boolean isLegacyLocalTable = attrs.customClass.equalsIgnoreCase(CassandraIndex.NAME);
                if (isLegacyLocalTable)
                    attrs.customClass = null;
                else
                    attrs.isCustom = true;
            }

            return new CreateIndexStatement(rawCQLStatement, keyspaceMapper.apply(keyspaceName), tableName.getName(),
                                            indexName.getName(), rawIndexTargets, attrs, ifNotExists);
        }
    }

    enum IndexGuardrails
    {
        LEGACY(Guardrails.secondaryIndexesPerTable, null),
        SAI(Guardrails.saiIndexesPerTable, Guardrails.saiIndexesTotal),
        UNKNOWN(null, null);

        final Threshold perTableThreshold;
        final Threshold totalThreshold;

        IndexGuardrails(Threshold perTableThreshold, Threshold totalThreshold)
        {
            this.perTableThreshold = perTableThreshold;
            this.totalThreshold = totalThreshold;
        }

        boolean hasPerTableThreshold()
        {
            return perTableThreshold != null;
        }

        boolean hasTotalThreshold()
        {
            return totalThreshold != null;
        }

        static IndexGuardrails forClassName(String className)
        {
            switch (className)
            {
                case "org.apache.cassandra.index.internal.CassandraIndex":
                    return IndexGuardrails.LEGACY;
                case "org.apache.cassandra.index.sai.StorageAttachedIndex":
                    return IndexGuardrails.SAI;
                default:
                    return IndexGuardrails.UNKNOWN;
            }
        }

    }
}
