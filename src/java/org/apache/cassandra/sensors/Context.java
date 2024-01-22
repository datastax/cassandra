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

import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Represents the context for a (group of) {@link Sensor}(s), made up of:
 * <ul>
 *     <li>The keyspace the sensor refers to.</li>
 *     <li>The table the sensor refers to.</li>
 *     <li>The related table id.</li>
 * </ul>
 */
public class Context
{
    private final String keyspace;
    private final String table;
    private final String tableId;

    public Context(String keyspace, String table, String tableId)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.tableId = tableId;
    }

    public String getKeyspace()
    {
        return keyspace;
    }

    public String getTable()
    {
        return table;
    }

    public String getTableId()
    {
        return tableId;
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
        return Objects.hash(keyspace, table, tableId);
    }

    @Override
    public String toString()
    {
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
}
