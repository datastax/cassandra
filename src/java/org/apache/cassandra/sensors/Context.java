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

package org.apache.cassandra.sensors;

import java.util.Objects;
import java.util.Optional;

import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Represents the context for a (group of) {@link Sensor}(s).
 *
 * <p>Two kinds of context exist:
 * <ul>
 *   <li><b>Table context</b> — identifies a specific keyspace, table, and table-id. Created via
 *       {@link #Context(String, String, String)}, {@link #from(TableMetadata)}, etc. All sensors
 *       that track per-table measurements (bytes, execution time, per-table costs) use this.</li>
 *   <li><b>Request context</b> — carries no keyspace or table identity; instead it carries a
 *       {@link #getRequestOwner() request owner} derived from the {@link RequestSensors} instance.
 *       Created via {@link #from(RequestSensors)}. Used e.g. for the {@link Type#TOTAL_COST}
 *       sensor that aggregates cost across all tables touched by one coordinator request.
 *       Two request contexts with the same owner are considered equal.</li>
 * </ul>
 * <p>For table contexts, {@link #getKeyspace()}, {@link #getTable()}, and {@link #getTableId()} always
 * return a non-empty {@link Optional}. For request contexts they always return {@link Optional#empty()}.
 */
public class Context
{
    private final String keyspace;
    private final String table;
    private final String tableId;
    private final String requestOwner;

    private final int hashCode;

    public Context(String keyspace, String table, String tableId)
    {
        Objects.requireNonNull(keyspace, "keyspace must not be null");
        Objects.requireNonNull(table, "table must not be null");
        Objects.requireNonNull(tableId, "tableId must not be null");
        this.keyspace = keyspace;
        this.table = table;
        this.tableId = tableId;
        this.requestOwner = null;
        this.hashCode = Objects.hash(keyspace, table, tableId, null);
    }

    private Context(String requestOwner)
    {
        this.keyspace = null;
        this.table = null;
        this.tableId = null;
        this.requestOwner = requestOwner;
        this.hashCode = Objects.hash(null, null, null, requestOwner);
    }

    /**
     * Returns {@code true} if this is a request-level context (no keyspace/table identity).
     */
    public boolean isRequestContext()
    {
        return keyspace == null;
    }

    /**
     * Returns the keyspace name, or {@link Optional#empty()} for a {@link #from(RequestSensors)} context.
     */
    public Optional<String> getKeyspace()
    {
        return Optional.ofNullable(keyspace);
    }

    /**
     * Returns the table name, or {@link Optional#empty()} for a {@link #from(RequestSensors)} context.
     */
    public Optional<String> getTable()
    {
        return Optional.ofNullable(table);
    }

    /**
     * Returns the table id, or {@link Optional#empty()} for a {@link #from(RequestSensors)} context.
     */
    public Optional<String> getTableId()
    {
        return Optional.ofNullable(tableId);
    }

    /**
     * Returns the request owner, or {@link Optional#empty()} for a table-level context.
     */
    public Optional<String> getRequestOwner()
    {
        return Optional.ofNullable(requestOwner);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Context context = (Context) o;
        return Objects.equals(keyspace, context.keyspace)
               && Objects.equals(table, context.table)
               && Objects.equals(tableId, context.tableId)
               && Objects.equals(requestOwner, context.requestOwner);
    }

    @Override
    public int hashCode()
    {
        return hashCode;
    }

    @Override
    public String toString()
    {
        if (isRequestContext())
            return "Context{requestOwner='" + requestOwner + "'}";
        return "Context{" +
               "keyspace='" + keyspace + '\'' +
               ", table='" + table + '\'' +
               ", tableId='" + tableId + '\'' +
               '}';
    }

    /**
     * Returns a request-level context owned by the given {@link RequestSensors} instance.
     * Two calls with sensors that return the same {@link RequestSensors#getRequestOwner()} value
     * produce equal contexts and map to the same sensor in the {@link SensorsRegistry}.
     */
    public static Context from(RequestSensors sensors)
    {
        return new Context(sensors.getRequestOwner());
    }

    /**
     * Creates a table-level context from the table metadata of the given {@link ReadCommand}.
     *
     * @param command the read command whose table metadata is used to build the context
     * @return a table-level context identifying the command's keyspace, table name, and table id
     */
    public static Context from(ReadCommand command)
    {
        return from(command.metadata());
    }

    /**
     * Creates a table-level context from the given {@link TableMetadata}.
     *
     * @param table the table metadata used to build the context
     * @return a table-level context identifying the keyspace, table name, and table id
     */
    public static Context from(TableMetadata table)
    {
        return new Context(table.keyspace, table.name, table.id.toString());
    }

    /**
     * Creates a table-level context from the given SAI {@link IndexContext}.
     *
     * @param indexContext the index context used to build the context
     * @return a table-level context identifying the index's keyspace, table name, and table id
     */
    public static Context from(IndexContext indexContext)
    {
        return new Context(indexContext.getKeyspace(), indexContext.getTable(), indexContext.getTableId().toString());
    }
}
