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

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.transport.Event;

public class DropCDCStatement extends SchemaAlteringStatement
{
    private final String name;

    public DropCDCStatement(String name)
    {
        super();
        this.name = name;
    }

    @Override
    public String keyspace()
    {
        return name;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.hasKeyspaceAccess(name, Permission.ALTER);
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(name);
        if (ksm == null)
            throw new InvalidRequestException("Unknown keyspace " + name);
        if (Schema.isSystemKeyspace(ksm.name))
            throw new InvalidRequestException("Cannot alter system keyspace");

        // Confirm we have CDC logs on this ks
        if (!ksm.params.hasCDCEnabled())
        {
            throw new InvalidRequestException("Cannot drop CDC Datacenters from Keyspace without CDC configured.");
        }
    }

    public Event.SchemaChange announceMigration(boolean isLocalOnly) throws RequestValidationException
    {
        KeyspaceMetadata oldKsm = Schema.instance.getKSMetaData(name);
        // In the (very) unlikely case the keyspace was dropped since validate()
        if (oldKsm == null)
            throw new InvalidRequestException("Unknown keyspace " + name);

        KeyspaceParams newParams = new KeyspaceParams(oldKsm.params.durableWrites, oldKsm.params.replication);
        KeyspaceMetadata newKsm = oldKsm.withSwapped(newParams);
        MigrationManager.announceKeyspaceUpdate(newKsm, isLocalOnly);
        return new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, keyspace());
    }
}
