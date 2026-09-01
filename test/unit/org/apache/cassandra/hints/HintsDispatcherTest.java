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
package org.apache.cassandra.hints;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.RateLimiter;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MockMessagingService;
import org.apache.cassandra.net.MockMessagingSpy;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.net.MockMessagingService.verb;
import static org.apache.cassandra.net.Verb.HINT_REQ;
import static org.apache.cassandra.net.Verb.HINT_RSP;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class HintsDispatcherTest
{
    private static final String KEYSPACE = "hints_dispatcher_test";
    private static final String MATCHING_VERSION_TABLE = "matching_version";
    private static final String DIFFERENT_VERSION_TABLE = "different_version";
    private static final String LIVE_TABLE = "live_table";
    private static final UUID HOST_ID = UUID.randomUUID();
    private static InetAddressAndPort address;

    @BeforeClass
    public static void defineSchema()
    {
        SchemaLoader.prepareServer();
        address = FBUtilities.getBroadcastAddressAndPort();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, MATCHING_VERSION_TABLE),
                                    SchemaLoader.standardCFMD(KEYSPACE, DIFFERENT_VERSION_TABLE),
                                    SchemaLoader.standardCFMD(KEYSPACE, LIVE_TABLE));
    }

    @After
    public void cleanup()
    {
        MockMessagingService.cleanup();
    }

    @Test
    public void dispatchesEncodedHintForDroppedTableWhenVersionsMatch() throws Exception
    {
        File directory = new File(Files.createTempDirectory(null));
        try
        {
            File file = writeHint(directory, MATCHING_VERSION_TABLE);
            SchemaTestUtil.announceTableDrop(KEYSPACE, MATCHING_VERSION_TABLE);
            MockMessagingSpy spy = respondingSpy();

            try (HintsDispatcher dispatcher = dispatcher(file, MessagingService.VERSION_DS_12))
            {
                assertTrue(dispatcher.dispatch());
            }

            Message<?> message = spy.captureMessageOut().get(1, TimeUnit.SECONDS);
            assertTrue(message.payload instanceof HintMessage.Encoded);
        }
        finally
        {
            directory.deleteRecursive();
        }
    }

    @Test
    public void skipsDroppedTableHintWhenVersionsDiffer() throws Exception
    {
        File directory = new File(Files.createTempDirectory(null));
        try
        {
            File file = writeHint(directory, DIFFERENT_VERSION_TABLE);
            SchemaTestUtil.announceTableDrop(KEYSPACE, DIFFERENT_VERSION_TABLE);
            MockMessagingSpy spy = respondingSpy();

            try (HintsDispatcher dispatcher = dispatcher(file, MessagingService.VERSION_DS_11))
            {
                assertTrue(dispatcher.dispatch());
            }

            spy.interceptNoMsg(100, TimeUnit.MILLISECONDS).get(1, TimeUnit.SECONDS);
        }
        finally
        {
            directory.deleteRecursive();
        }
    }

    @Test
    public void dispatchesDecodedHintWhenVersionsDiffer() throws Exception
    {
        File directory = new File(Files.createTempDirectory(null));
        try
        {
            File file = writeHint(directory, LIVE_TABLE);
            MockMessagingSpy spy = respondingSpy();

            try (HintsDispatcher dispatcher = dispatcher(file, MessagingService.VERSION_DS_11))
            {
                assertTrue(dispatcher.dispatch());
            }

            Message<?> message = spy.captureMessageOut().get(1, TimeUnit.SECONDS);
            assertTrue(message.payload instanceof HintMessage);
            assertNotNull(((HintMessage) message.payload).hint());
        }
        finally
        {
            directory.deleteRecursive();
        }
    }

    private static File writeHint(File directory, String tableName) throws Exception
    {
        HintsDescriptor descriptor = new HintsDescriptor(HOST_ID,
                                                         HintsDescriptor.VERSION_DS_12,
                                                         System.currentTimeMillis(),
                                                         ImmutableMap.of());
        TableMetadata table = Schema.instance.getTableMetadata(KEYSPACE, tableName);
        long now = System.currentTimeMillis();
        Mutation mutation = new RowUpdateBuilder(table, TimeUnit.MILLISECONDS.toMicros(now), bytes("key"))
                            .clustering("column")
                            .add("val", "value")
                            .build();

        try (HintsWriter writer = HintsWriter.create(directory, descriptor);
             HintsWriter.Session session = writer.newSession(ByteBuffer.allocate(1024)))
        {
            session.append(Hint.create(mutation, now));
        }
        return new File(directory, descriptor.fileName());
    }

    private static HintsDispatcher dispatcher(File file, int messagingVersion)
    {
        return HintsDispatcher.create(file,
                                      RateLimiter.create(1_000_000_000),
                                      address,
                                      HOST_ID,
                                      messagingVersion,
                                      () -> false);
    }

    private static MockMessagingSpy respondingSpy()
    {
        return MockMessagingService.when(verb(HINT_REQ))
                                   .respond(Message.internalResponse(HINT_RSP, NoPayload.noPayload));
    }
}
