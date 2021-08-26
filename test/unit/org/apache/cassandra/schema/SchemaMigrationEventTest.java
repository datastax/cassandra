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

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.TestCase;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertFalse;

public class SchemaMigrationEventTest extends TestCase
{
    @Test
    public void testSchemaMigrationEventCreation() throws UnknownHostException
    {
        SchemaLoader.prepareServer();
        SchemaMigrationEvent e1 = new SchemaMigrationEvent(SchemaMigrationEvent.MigrationManagerEventType.TASK_CREATED, null, null);

        InetAddressAndPort liveEndpoint = InetAddressAndPort.getByName("127.0.0.1");
        SchemaMigrationEvent e2 = new SchemaMigrationEvent(SchemaMigrationEvent.MigrationManagerEventType.TASK_CREATED, liveEndpoint, null);

        InetAddressAndPort deadEndpoint = InetAddressAndPort.getByName("127.0.0.2");
        markNodeAsDead(deadEndpoint, new RandomPartitioner.BigIntegerToken("3"));
        SchemaMigrationEvent e3 = new SchemaMigrationEvent(SchemaMigrationEvent.MigrationManagerEventType.TASK_CREATED, deadEndpoint, null);
    }

    private void markNodeAsDead(InetAddressAndPort address, Token token)
    {
        IPartitioner partitioner = new RandomPartitioner();
        UUID hostId = UUID.randomUUID();
        Gossiper.instance.initializeNodeUnsafe(address, hostId, MessagingService.current_version, 1);
        Gossiper.instance.injectApplicationState(address, ApplicationState.TOKENS, new VersionedValue.VersionedValueFactory(partitioner).tokens(Collections.singleton(token)));
        StorageService.instance.onChange(address,
                                         ApplicationState.STATUS_WITH_PORT,
                                         new VersionedValue.VersionedValueFactory(partitioner).normal(Collections.singleton(token)));
        EndpointState endpointState = Gossiper.instance.getEndpointStateForEndpoint(address);
        Gossiper.runInGossipStageBlocking(() -> Gossiper.instance.markDead(address, endpointState));
        IFailureDetector.instance.report(address);
        IFailureDetector.instance.interpret(address);
        assertFalse("Node not convicted", IFailureDetector.instance.isAlive(address));
    }
}