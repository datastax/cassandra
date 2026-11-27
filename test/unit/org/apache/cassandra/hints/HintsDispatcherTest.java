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
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.RateLimiter;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.io.util.DataOutputBuffer;
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
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.apache.cassandra.net.MockMessagingService.verb;
import static org.apache.cassandra.net.Verb.HINT_REQ;
import static org.apache.cassandra.net.Verb.HINT_RSP;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
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
        return writeHint(directory, descriptor, tableName);
    }

    private static File writeHint(File directory, HintsDescriptor descriptor, String tableName) throws Exception
    {
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

    /**
     * In the DSE 5.1 storage mode a hints file is of version 1, its hints at messaging version 11: a DSE 5.1 target, at
     * 11, gets the stored bytes as they are; a target of this software, whose dispatch version the storage mode caps at
     * 12, gets each hint decoded at 11 and encoded again.
     */
    @Test
    public void inTheDse51ModeAFileOfVersion1IsSentAsStoredAt11AndDecodedFor12() throws Exception
    {
        StorageCompatibilityMode storageBefore = DatabaseDescriptor.getStorageCompatibilityMode();
        InternodeCompatibilityMode internodeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.DSE_5_1);
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        File directory = new File(Files.createTempDirectory(null));
        try
        {
            HintsDescriptor descriptor = new HintsDescriptor(HOST_ID, System.currentTimeMillis());
            assertEquals(HintsDescriptor.VERSION_30, descriptor.version);
            File file = writeHint(directory, descriptor, LIVE_TABLE);

            MockMessagingSpy stored = respondingSpy();
            try (HintsDispatcher dispatcher = dispatcher(file, MessagingService.VERSION_3014))
            {
                assertTrue(dispatcher.dispatch());
            }
            // the stored hint, as its entry in the file holds it
            byte[] record = DseFixtures.hintsRecords(file.toPath()).get(0);
            Message<?> atEleven = stored.captureMessageOut().get(1, TimeUnit.SECONDS);
            assertTrue(atEleven.payload instanceof HintMessage.Encoded);
            // the message carries the host id, the size of the hint and its bytes as stored
            byte[] sent;
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                HintMessage.serializer.serialize((HintMessage.Encoded) atEleven.payload, out,
                                                 MessagingService.VERSION_3014);
                sent = out.toByteArray();
            }
            assertArrayEquals(record, Arrays.copyOfRange(sent, sent.length - record.length, sent.length));
            MockMessagingService.cleanup();

            MockMessagingSpy decoded = respondingSpy();
            try (HintsDispatcher dispatcher = dispatcher(file, MessagingService.VERSION_40))
            {
                assertTrue(dispatcher.dispatch());
            }
            Message<?> atTwelve = decoded.captureMessageOut().get(1, TimeUnit.SECONDS);
            assertTrue(atTwelve.payload instanceof HintMessage);
            Hint hint = ((HintMessage) atTwelve.payload).hint();
            assertNotNull(hint);
            // decoded from the bytes stored at 11: the same hint, encoded again when sent at 12
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                Hint.serializer.serialize(hint, out, MessagingService.VERSION_3014);
                assertArrayEquals(record, out.toByteArray());
            }
        }
        finally
        {
            DatabaseDescriptor.setStorageCompatibilityMode(storageBefore);
            DatabaseDescriptor.setInternodeCompatibilityMode(internodeBefore);
            directory.deleteRecursive();
        }
    }
}