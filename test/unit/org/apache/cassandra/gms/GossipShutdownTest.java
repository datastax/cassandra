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

package org.apache.cassandra.gms;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.Verb;
import org.assertj.core.api.Assertions;

public class GossipShutdownTest
{
    private static final int BEFORE_CHANGE = MessagingService.Version.VERSION_40.value;
    private static final int AFTER_CHANGE = MessagingService.Version.VERSION_50.value;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void mixedMode() throws IOException
    {
        Message<GossipShutdown> message = Message.out(Verb.GOSSIP_SHUTDOWN, new GossipShutdown(new EndpointState(HeartBeatState.empty())));

        Assertions.assertThat(serde(message, BEFORE_CHANGE)).isNull();
        Assertions.assertThat(serde(message, AFTER_CHANGE)).isInstanceOf(GossipShutdown.class);

        // got from 4.x peer
        Assertions.assertThat(serde(Message.out(Verb.GOSSIP_SHUTDOWN, NoPayload.noPayload), BEFORE_CHANGE)).isNull();
    }

    /**
     * HCD-621: the DS 4.0 versions (100..102) are numerically above {@link MessagingService#VERSION_50} but are spoken by
     * HCD 1.x nodes, which never write the {@link EndpointState} payload.
     */
    @Test
    public void mixedModeWithDataStaxVersions() throws IOException
    {
        Message<GossipShutdown> message = Message.out(Verb.GOSSIP_SHUTDOWN, new GossipShutdown(new EndpointState(HeartBeatState.empty())));

        for (int version : new int[]{ MessagingService.VERSION_DS_10, MessagingService.VERSION_DS_11, MessagingService.VERSION_DS_12 })
        {
            Assertions.assertThat(GossipShutdown.serializer.serializedSize(message.payload, version)).as("version %d", version).isZero();
            Assertions.assertThat(serde(message, version)).as("version %d", version).isNull();
            // got from an HCD 1.x peer: no payload at all
            Assertions.assertThat(serde(Message.out(Verb.GOSSIP_SHUTDOWN, NoPayload.noPayload), version)).as("version %d", version).isNull();
        }

        Assertions.assertThat(serde(message, MessagingService.VERSION_DS_20)).isInstanceOf(GossipShutdown.class);
    }

    @Test
    public void isVersion50OrLater()
    {
        for (MessagingService.Version version : MessagingService.Version.values())
        {
            boolean expected;
            switch (version)
            {
                case VERSION_50:
                case VERSION_DS_20:
                    expected = true;
                    break;
                default:
                    expected = false;
            }
            Assertions.assertThat(MessagingService.isVersion50OrLater(version.value)).as(version.name()).isEqualTo(expected);
        }
    }

    private Object serde(Message<?> message, int version) throws IOException
    {
        try (DataOutputBuffer out = DataOutputBuffer.scratchBuffer.get())
        {
            Message.serializer.serialize(message, out, version);
            return Message.serializer.deserialize(new DataInputBuffer(out.unsafeGetBufferAndFlip(), false), message.header, version).payload;
        }
    }
}