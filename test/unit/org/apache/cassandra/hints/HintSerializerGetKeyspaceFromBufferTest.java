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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;


public class HintSerializerGetKeyspaceFromBufferTest
{
    private static final String KEYSPACE = "hint_serializer_keyspace_test";
    private static final String TABLE = "table_0";

    @BeforeClass
    public static void setup()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, TABLE));
    }

    private static ByteBuffer serializeHint(Hint hint) throws IOException
    {
        try (DataOutputBuffer dob = new DataOutputBuffer())
        {
            Hint.serializer.serialize(hint, dob, MessagingService.current_version);
            return dob.buffer();
        }
    }

    private static Hint createHint(String rowKey)
    {
        long now = FBUtilities.timestampMicros();
        TableMetadata table = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        Mutation mutation = new RowUpdateBuilder(table, now, bytes(rowKey))
                            .clustering("col")
                            .add("val", "v")
                            .build();
        return Hint.create(mutation, now / 1000);
    }

    /**
     * A hint serialized at messaging version 11, as the DSE 5.1 compatibility mode writes them (its mutation's
     * modification count, then the table id), peeked at 11 as the dispatcher does for a target at 11.
     */
    @Test
    public void testReturnsCorrectKeyspaceAtVersion11() throws IOException
    {
        InternodeCompatibilityMode before = DatabaseDescriptor.getInternodeCompatibilityMode();
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        try
        {
            Hint hint = createHint("key11");
            ByteBuffer buf;
            try (DataOutputBuffer dob = new DataOutputBuffer())
            {
                Hint.serializer.serialize(hint, dob, MessagingService.VERSION_3014);
                buf = dob.buffer();
            }
            assertEquals(KEYSPACE, Hint.serializer.getKeyspaceFromBuffer(buf, MessagingService.VERSION_3014));
        }
        finally
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(before);
        }
    }

    @Test
    public void testReturnsCorrectKeyspace() throws IOException
    {
        Hint hint = createHint("key1");
        ByteBuffer buf = serializeHint(hint);

        String keyspace = Hint.serializer.getKeyspaceFromBuffer(buf, MessagingService.current_version);

        assertEquals(KEYSPACE, keyspace);
    }

    @Test
    public void testMatchesFullDeserialization() throws IOException
    {
        Hint hint = createHint("key2");
        ByteBuffer buf = serializeHint(hint);

        String fromBuffer = Hint.serializer.getKeyspaceFromBuffer(buf, MessagingService.current_version);
        String fromHint = hint.mutation().getKeyspaceName();

        assertEquals(fromHint, fromBuffer);
    }
}
