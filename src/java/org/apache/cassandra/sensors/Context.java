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
 *   <li><b>Request context</b> — carries no keyspace or table identity. Created via
 *       {@link #request()}. Used i.e. for the {@link Type#TOTAL_COST} sensor that
 *       aggregates cost across all tables touched by one coordinator request.</li>
 * </ul>
 */
public class Context
{
    /** Singleton request-level context. */
    private static final Context REQUEST_CONTEXT = new Context(null, null, null);

    private final String keyspace;
    private final String table;
    private final String tableId;

    private final int hashCode;

    public Context(String keyspace, String table, String tableId)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.tableId = tableId;
        this.hashCode = Objects.hash(keyspace, table, tableId);
    }

    /**
     * Returns the singleton request-level context, used for sensors that spans an entire coordinator request rather
     * than a specific table.
     */
    public static Context request()
    {
        return REQUEST_CONTEXT;
    }

    /**
     * Returns {@code true} if this is the request-level context (no keyspace/table identity).
     */
    public boolean isRequestContext()
    {
        return keyspace == null;
    }

    /**
     * Returns the keyspace name, or {@link Optional#empty()} for a {@link #request()} context.
     */
    public Optional<String> getKeyspace()
    {
        return Optional.ofNullable(keyspace);
    }

    /**
     * Returns the table name, or {@link Optional#empty()} for a {@link #request()} context.
     */
    public Optional<String> getTable()
    {
        return Optional.ofNullable(table);
    }

    /**
     * Returns the table id, or {@link Optional#empty()} for a {@link #request()} context.
     */
    public Optional<String> getTableId()
    {
        return Optional.ofNullable(tableId);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Context context = (Context) o;
        return Objects.equals(keyspace, context.keyspace) && Objects.equals(table, context.table) && Objects.equals(tableId, context.tableId);
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
            return "Context{request}";
        return "Context{" +
               "keyspace='" + keyspace + '\'' +
               ", table='" + table + '\'' +
               ", tableId='" + tableId + '\'' +
               '}';
    }

    public static Context from(ReadCommand command)
    {
        return from(command.metadata());
    }

    public static Context from(TableMetadata table)
    {
        return new Context(table.keyspace, table.name, table.id.toString());
    }

    public static Context from(IndexContext indexContext)
    {
        return new Context(indexContext.getKeyspace(), indexContext.getTable(), indexContext.getTableId().toString());
    }
}
