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
package org.apache.cassandra.cql3.statements;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.conditions.ColumnCondition;
import org.apache.cassandra.cql3.conditions.Conditions;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.filter.IndexHints;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.sensors.SensorsCustomParams;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.sensors.Context;
import org.apache.cassandra.sensors.RequestSensors;
import org.apache.cassandra.sensors.RequestTracker;
import org.apache.cassandra.sensors.Type;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkContainsNoDuplicates;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;

/**
 * An <code>UPDATE</code> statement parsed from a CQL query statement.
 *
 */
public class UpdateStatement extends ModificationStatement
{
    private static final Constants.Value EMPTY = new Constants.Value(ByteBufferUtil.EMPTY_BYTE_BUFFER);

    private UpdateStatement(String queryString,
                            StatementType type,
                            VariableSpecifications bindVariables,
                            TableMetadata metadata,
                            Operations operations,
                            StatementRestrictions restrictions,
                            Conditions conditions,
                            Attributes attrs)
    {
        super(queryString, type, bindVariables, metadata, operations, restrictions, conditions, attrs);
    }

    @Override
    public void addUpdateForKey(PartitionUpdate.Builder updateBuilder, Clustering<?> clustering, UpdateParameters params)
    {
        if (updatesRegularRows())
        {
            params.newRow(clustering);

            // We update the row timestamp only on INSERT (#6782)
            // Further, COMPACT tables semantic differs from "CQL3" ones in that a row exists only if it has
            // a non-null column, so we don't want to set the row timestamp for them.
            if (type.isInsert() && !metadata.isCompactTable())
                params.addPrimaryKeyLivenessInfo();

            List<Operation> updates = getRegularOperations();

            // For compact table, we don't accept an insert/update that only sets the PK unless the is no
            // declared non-PK columns (which we recognize because in that case
            // the compact value is of type "EmptyType").
            if (metadata().isCompactTable() && updates.isEmpty())
            {
                TableMetadata.CompactTableMetadata metadata = (TableMetadata.CompactTableMetadata) metadata();
                RequestValidations.checkTrue(metadata.hasEmptyCompactValue(),
                                             "Column %s is mandatory for this COMPACT STORAGE table",
                                             metadata.compactValueColumn);

                updates = Collections.singletonList(new Constants.Setter(metadata.compactValueColumn, EMPTY));
            }

            for (int i = 0, isize = updates.size(); i < isize; i++)
                updates.get(i).execute(updateBuilder.partitionKey(), params);

            updateBuilder.add(params.buildRow());
        }

        if (updatesStaticRow())
        {
            params.newRow(Clustering.STATIC_CLUSTERING);
            List<Operation> staticOps = getStaticOperations();
            for (int i = 0, isize = staticOps.size(); i < isize; i++)
                staticOps.get(i).execute(updateBuilder.partitionKey(), params);
            updateBuilder.add(params.buildRow());
        }
    }

    @Override
    public void addUpdateForKey(PartitionUpdate.Builder update, Slice slice, UpdateParameters params)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultMessage execute(QueryState state, QueryOptions options, long queryStartNanoTime)
    {
        ResultMessage result = super.execute(state, options, queryStartNanoTime);

        if (result == null)
            result = new ResultMessage.Void();

        RequestSensors sensors = RequestTracker.instance.get();
        Context context = Context.from(this.metadata());
        SensorsCustomParams.addSensorToCQLResponse(result, options.getProtocolVersion(), sensors, context, Type.WRITE_BYTES);
        // CAS updates incorporate read sensors
        SensorsCustomParams.addSensorToCQLResponse(result, options.getProtocolVersion(), sensors, context, Type.READ_BYTES);

        return result;
    }

    public static class ParsedInsert extends ModificationStatement.Parsed
    {
        private final List<ColumnIdentifier> columnNames;
        private final List<Term.Raw> columnValues;

        /**
         * A parsed <code>INSERT</code> statement.
         *
         * @param name column family being operated on
         * @param attrs additional attributes for statement (CL, timestamp, timeToLive)
         * @param columnNames list of column names
         * @param columnValues list of column values (corresponds to names)
         * @param ifNotExists true if an IF NOT EXISTS condition was specified, false otherwise
         */
        public ParsedInsert(QualifiedName name,
                            Attributes.Raw attrs,
                            List<ColumnIdentifier> columnNames,
                            List<Term.Raw> columnValues,
                            boolean ifNotExists)
        {
            super(name, StatementType.INSERT, attrs, null, ifNotExists, false);
            this.columnNames = columnNames;
            this.columnValues = columnValues;
        }

        @Override
        protected ModificationStatement prepareInternal(TableMetadata metadata,
                                                        VariableSpecifications bindVariables,
                                                        Conditions conditions,
                                                        Attributes attrs)
        {

            // Created from an INSERT
            checkFalse(metadata.isCounter(), "INSERT statements are not allowed on counter tables, use UPDATE instead");

            checkFalse(columnNames == null, "Column names for INSERT must be provided when using VALUES");
            checkFalse(columnNames.isEmpty(), "No columns provided to INSERT");
            checkFalse(columnNames.size() != columnValues.size(), "Unmatched column names/values");
            checkContainsNoDuplicates(columnNames, "The column names contains duplicates");

            WhereClause.Builder whereClause = new WhereClause.Builder();
            Operations operations = new Operations(type);
            boolean hasClusteringColumnsSet = false;

            for (int i = 0; i < columnNames.size(); i++)
            {
                ColumnMetadata def = metadata.getExistingColumn(columnNames.get(i));

                if (def.isClusteringColumn())
                    hasClusteringColumnsSet = true;

                Term.Raw value = columnValues.get(i);

                if (def.isPrimaryKeyColumn())
                {
                    whereClause.add(new SingleColumnRelation(columnNames.get(i), Operator.EQ, value));
                }
                else
                {
                    Operation operation = new Operation.SetValue(value).prepare(metadata, def);
                    operation.collectMarkerSpecification(bindVariables);
                    operations.add(operation);
                }
            }

            boolean applyOnlyToStaticColumns = !hasClusteringColumnsSet && appliesOnlyToStaticColumns(operations, conditions);

            StatementRestrictions restrictions = StatementRestrictions.create(type,
                                                                              metadata,
                                                                              whereClause.build(),
                                                                              bindVariables,
                                                                              Collections.emptyList(),
                                                                              IndexHints.NONE,
                                                                              applyOnlyToStaticColumns,
                                                                              false,
                                                                              false);

            return new UpdateStatement(rawCQLStatement,
                                       type,
                                       bindVariables,
                                       metadata,
                                       operations,
                                       restrictions,
                                       conditions,
                                       attrs);
        }
    }

    /**
     * A parsed INSERT JSON statement.
     */
    public static class ParsedInsertJson extends ModificationStatement.Parsed
    {
        private final Json.Raw jsonValue;
        private final boolean defaultUnset;

        public ParsedInsertJson(QualifiedName name, Attributes.Raw attrs, Json.Raw jsonValue, boolean defaultUnset, boolean ifNotExists)
        {
            super(name, StatementType.INSERT, attrs, null, ifNotExists, false);
            this.jsonValue = jsonValue;
            this.defaultUnset = defaultUnset;
        }

        @Override
        protected ModificationStatement prepareInternal(TableMetadata metadata,
                                                        VariableSpecifications bindVariables,
                                                        Conditions conditions,
                                                        Attributes attrs)
        {
            checkFalse(metadata.isCounter(), "INSERT statements are not allowed on counter tables, use UPDATE instead");

            Collection<ColumnMetadata> defs = metadata.columns();
            Json.Prepared prepared = jsonValue.prepareAndCollectMarkers(metadata, defs, bindVariables);

            WhereClause.Builder whereClause = new WhereClause.Builder();
            Operations operations = new Operations(type);
            boolean hasClusteringColumnsSet = false;

            for (ColumnMetadata def : defs)
            {
                if (def.isClusteringColumn())
                    hasClusteringColumnsSet = true;

                Term.Raw raw = prepared.getRawTermForColumn(def, defaultUnset);
                if (def.isPrimaryKeyColumn())
                {
                    whereClause.add(new SingleColumnRelation(def.name, Operator.EQ, raw));
                }
                else
                {
                    Operation operation = new Operation.SetValue(raw).prepare(metadata, def);
                    operation.collectMarkerSpecification(bindVariables);
                    operations.add(operation);
                }
            }

            boolean applyOnlyToStaticColumns = !hasClusteringColumnsSet && appliesOnlyToStaticColumns(operations, conditions);

            StatementRestrictions restrictions = StatementRestrictions.create(type,
                                                                              metadata,
                                                                              whereClause.build(),
                                                                              bindVariables,
                                                                              Collections.emptyList(),
                                                                              IndexHints.NONE,
                                                                              applyOnlyToStaticColumns,
                                                                              false,
                                                                              false);

            return new UpdateStatement(rawCQLStatement,
                                       type,
                                       bindVariables,
                                       metadata,
                                       operations,
                                       restrictions,
                                       conditions,
                                       attrs);
        }
    }

    public static class ParsedUpdate extends ModificationStatement.Parsed
    {
        // Provided for an UPDATE
        private final List<Pair<ColumnIdentifier, Operation.RawUpdate>> updates;
        private final WhereClause whereClause;

        /**
         * Creates a new UpdateStatement from a column family name, columns map, consistency
         * level, and key term.
         *
         * @param name column family being operated on
         * @param attrs additional attributes for statement (timestamp, timeToLive)
         * @param updates a map of column operations to perform
         * @param whereClause the where clause
         * @param ifExists flag to check if row exists
         * */
        public ParsedUpdate(QualifiedName name,
                            Attributes.Raw attrs,
                            List<Pair<ColumnIdentifier, Operation.RawUpdate>> updates,
                            WhereClause whereClause,
                            List<Pair<ColumnIdentifier, ColumnCondition.Raw>> conditions,
                            boolean ifExists)
        {
            super(name, StatementType.UPDATE, attrs, conditions, false, ifExists);
            this.updates = updates;
            this.whereClause = whereClause;
        }

        @Override
        protected ModificationStatement prepareInternal(TableMetadata metadata,
                                                        VariableSpecifications bindVariables,
                                                        Conditions conditions,
                                                        Attributes attrs)
        {
            Operations operations = new Operations(type);

            for (Pair<ColumnIdentifier, Operation.RawUpdate> entry : updates)
            {
                ColumnMetadata def = metadata.getExistingColumn(entry.left);

                checkFalse(def.isPrimaryKeyColumn(), "PRIMARY KEY part %s found in SET part", def.name);

                Operation operation = entry.right.prepare(metadata, def);
                operation.collectMarkerSpecification(bindVariables);
                operations.add(operation);
            }

            StatementRestrictions restrictions = newRestrictions(metadata, bindVariables, operations, whereClause, conditions);

            return new UpdateStatement(rawCQLStatement,
                                       type,
                                       bindVariables,
                                       metadata,
                                       operations,
                                       restrictions,
                                       conditions,
                                       attrs);
        }
    }
    
    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.UPDATE, keyspace(), columnFamily());
    }
}
