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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;

import org.apache.cassandra.db.SystemKeyspace;

/**
 * Maintains the definition of local keyspaces (local system keyspaces) for {@link Schema}.
 */
public class LocalKeyspaces
{
    private final Schema manager;

    private final Map<String, KeyspaceMetadata> localSystemKeyspaces = new ConcurrentHashMap<>();
    private final Map<TableId, TableMetadata> localSystemTables = new ConcurrentHashMap<>();

    LocalKeyspaces(Schema manager)
    {
        this.manager = manager;
    }

    /**
     * Adds the hard-coded definitions for all local system keyspaces and virtual keyspaces.
     * <p>
     * This isn't done in the constructor mainly because this part is skipped when running in 'client' mode.
     */
    public void loadHardCodedDefinitions()
    {
        load(SchemaKeyspace.metadata());
        load(SystemKeyspace.metadata());
    }

    @Nullable
    public KeyspaceMetadata getNullable(String keyspaceName)
    {
        return localSystemKeyspaces.get(keyspaceName);
    }

    @Nullable
    public TableMetadata getTableOrViewNullable(TableId tableId)
    {
        return localSystemTables.get(tableId);
    }

    Collection<KeyspaceMetadata> localSystemKeyspaces()
    {
        return localSystemKeyspaces.values();
    }

    private void load(KeyspaceMetadata localKeyspace)
    {
        localSystemKeyspaces.put(localKeyspace.name, localKeyspace);
        for (TableMetadata tableMetadata : localKeyspace.tablesAndViews())
            localSystemTables.put(tableMetadata.id, tableMetadata);

        manager.addNewRefs(localKeyspace);
    }
}
