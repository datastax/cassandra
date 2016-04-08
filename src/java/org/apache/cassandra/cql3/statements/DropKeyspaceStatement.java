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
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.transport.Event;

public class DropKeyspaceStatement extends SchemaAlteringStatement
{
    private final String name;
    private final boolean ifExists;

    public DropKeyspaceStatement(String name, boolean ifExists)
    {
        super();
        this.name = name;
        this.ifExists = ifExists;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.hasKeyspaceAccess(name, Permission.DROP);
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        // If we have CDC enabled on this keyspace, don't allow dropping until CDC is dropped.
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(name);
        if (ksm == null && !ifExists)
            throw new InvalidRequestException("Unknown keyspace " + name);
        else if (ksm == null && ifExists)
        {
            ClientWarn.instance.warn(String.format("Keyspace %s not found. Ignoring request.", name));
            return;
        }
        ThriftValidation.validateKeyspaceNotSystem(name);

        if (ksm.params.getCDCDataCenters().size() != 0)
            throw new InvalidRequestException("Cannot drop keyspace with active CDC log. Remove CDC log before dropping Keyspace.");
    }

    @Override
    public String keyspace()
    {
        return name;
    }

    public Event.SchemaChange announceMigration(boolean isLocalOnly) throws ConfigurationException
    {
        KeyspaceMetadata oldKsm = Schema.instance.getKSMetaData(name);
        if (ifExists && oldKsm == null)
            return null;

        MigrationManager.announceKeyspaceDrop(name, isLocalOnly);
        return new Event.SchemaChange(Event.SchemaChange.Change.DROPPED, keyspace());
    }
}
