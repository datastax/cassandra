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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * An immutable class representing keyspace parameters (durability and replication).
 */
public final class KeyspaceParams
{
    public static final boolean DEFAULT_DURABLE_WRITES = true;

    public enum Option
    {
        DURABLE_WRITES,
        REPLICATION,
        CDC_DATACENTERS;

        @Override
        public String toString()
        {
            return name().toLowerCase();
        }
    }

    public final boolean durableWrites;
    public final ReplicationParams replication;
    private final Set<String> cdc_datacenters;

    public KeyspaceParams(boolean durableWrites, ReplicationParams replication)
    {
        this(durableWrites, replication, new HashSet<>());
    }

    public KeyspaceParams(boolean durableWrites, ReplicationParams replication, Set<String> cdc_datacenters)
    {
        this.durableWrites = durableWrites;
        this.replication = replication;
        assert cdc_datacenters != null : "Don't allow null in cdc_datacenters.";
        this.cdc_datacenters = cdc_datacenters;
    }

    public static KeyspaceParams create(boolean durableWrites, Map<String, String> replication)
    {
        return create(durableWrites, replication, new HashSet<>());
    }

    public static KeyspaceParams create(boolean durableWrites, Map<String, String> replication, Set<String> cdc_datacenters)
    {
        return new KeyspaceParams(durableWrites, ReplicationParams.fromMap(replication), cdc_datacenters);
    }

    // The various static builders are used in schema table creation and testing and thus have no support for CDC included
    public static KeyspaceParams local()
    {
        return new KeyspaceParams(true, ReplicationParams.local());
    }

    public static KeyspaceParams simple(int replicationFactor)
    {
        return new KeyspaceParams(true, ReplicationParams.simple(replicationFactor));
    }

    public static KeyspaceParams simpleTransient(int replicationFactor)
    {
        return new KeyspaceParams(false, ReplicationParams.simple(replicationFactor));
    }

    /**
     * NetworkTopology keyspace - used for testing. No CDC support.
     */
    public static KeyspaceParams nts(Object... args)
    {
        return new KeyspaceParams(true, ReplicationParams.nts(args));
    }

    public void validate(String name)
    {
        replication.validate(name);

        // Can't have a CDC datacenter that doesn't exist for the keyspace.
        Set<String> dcs = replication.getReplicationDataCenters();

        // In the event we're not nts and have only a single datacenter, we don't pass in a DC name in params so we cannot
        // confirm that the name they entered is correct. That being said, we also don't really care. If it's a single
        // datacenter and you provide a single CDC DC name, that's good enough for us.

        if (dcs.size() == 0 && cdc_datacenters.size() > 1)
        {
            throw new ConfigurationException("Single datacenter for replication needs 0 or 1 CDC datacenters specified.");
        }
        else if (dcs.size() != 0 && cdc_datacenters.size() > 0)
        {
            for (String s : cdc_datacenters)
            {
                if (s.equals(""))
                    throw new ConfigurationException("Cannot have empty CDC DataCenter.");
                if (!dcs.contains(s))
                    throw new ConfigurationException(String.format("CDC DataCenter for unknown DC added. Unknown DC: %s. Known DataCenters: %s",
                                                                   cdc_datacenters, dcs.toString()));
            }
        }
    }

    public Set<String> getCDCDataCenters()
    {
        return cdc_datacenters;
    }

    public boolean hasCDCEnabled(String datacenter)
    {
        return cdc_datacenters.contains(datacenter);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof KeyspaceParams))
            return false;

        KeyspaceParams p = (KeyspaceParams) o;

        return durableWrites == p.durableWrites && replication.equals(p.replication) && cdc_datacenters.equals(p.cdc_datacenters);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(durableWrites, replication, cdc_datacenters);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add(Option.DURABLE_WRITES.toString(), durableWrites)
                          .add(Option.REPLICATION.toString(), replication)
                      .add(Option.CDC_DATACENTERS.toString(), cdc_datacenters)
                          .toString();
    }
}
