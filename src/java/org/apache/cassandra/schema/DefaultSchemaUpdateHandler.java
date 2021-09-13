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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;

@NotThreadSafe
public class DefaultSchemaUpdateHandler implements SchemaUpdateHandler, IEndpointStateChangeSubscriber
{
    private final static Logger logger = LoggerFactory.getLogger(DefaultSchemaUpdateHandler.class);

    private final MigrationCoordinator migrationCoordinator;
    private final Clock clock;
    private final boolean requireSchemas;

    public DefaultSchemaUpdateHandler()
    {
        this(MigrationCoordinator.instance,
             !CassandraRelevantProperties.BOOTSTRAP_SKIP_SCHEMA_CHECK.getBoolean(),
             Clock.systemDefaultZone());
    }

    public DefaultSchemaUpdateHandler(MigrationCoordinator migrationCoordinator, boolean requireSchemas, Clock clock)
    {
        this.migrationCoordinator = migrationCoordinator;
        this.requireSchemas = requireSchemas;
        this.clock = clock;
        Gossiper.instance.register(this);
    }

    @Override
    public void start()
    {
        migrationCoordinator.start();
    }

    @Override
    public SchemaManager schema()
    {
        return SchemaManager.instance;
    }

    @Override
    public boolean waitUntilReady(Duration timeout)
    {
        logger.debug("Waiting for schema to be ready (max {})", timeout);
        Instant deadline = clock.instant().plus(timeout);

        while (schema().isEmpty() && clock.instant().isBefore(deadline))
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

        boolean schemasReceived = migrationCoordinator.awaitSchemaRequests(Math.max(0, Duration.between(clock.instant(), deadline).toMillis()));

        if (schemasReceived)
            return true;

        logger.warn("There are nodes in the cluster with a different schema version than us, from which we did not merge schemas: " +
                    "our version: ({}), outstanding versions -> endpoints: {}. Use -D{}}=true to ignore this, " +
                    "-D{}=<ep1[,epN]> to skip specific endpoints, or -D{}=<ver1[,verN]> to skip specific schema versions",
                    schema().getVersion(),
                    migrationCoordinator.outstandingVersions(),
                    CassandraRelevantProperties.BOOTSTRAP_SKIP_SCHEMA_CHECK.getKey(),
                    MigrationCoordinator.IGNORED_ENDPOINTS_PROP, MigrationCoordinator.IGNORED_VERSIONS_PROP);

        if (requireSchemas)
        {
            logger.error("Didn't receive schemas for all known versions within the {}. Use -D{}=true to skip this check.",
                         timeout, CassandraRelevantProperties.BOOTSTRAP_SKIP_SCHEMA_CHECK.getKey());

            return false;
        }

        return true;
    }

    @Override
    public void onRemove(InetAddressAndPort endpoint)
    {
        migrationCoordinator.removeAndIgnoreEndpoint(endpoint);
    }

    @Override
    public void onChange(InetAddressAndPort endpoint, ApplicationState state, VersionedValue value)
    {
        if (state == ApplicationState.SCHEMA)
        {
            EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
            if (epState != null && !Gossiper.instance.isDeadState(epState) && StorageService.instance.getTokenMetadata().isMember(endpoint))
            {
                migrationCoordinator.reportEndpointVersion(endpoint, UUID.fromString(value.value));
            }
        }
    }
}
