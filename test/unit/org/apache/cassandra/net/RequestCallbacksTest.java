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

package org.apache.cassandra.net;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.IVersionedAsymmetricSerializer;
import org.apache.cassandra.locator.InetAddressAndPort;

import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

public class RequestCallbacksTest
{
    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testInternalResponseNullSerializer() throws Exception
    {
        deprecatedResponsesShouldReturnNullSerializer(Verb.INTERNAL_RSP);
    }

    @Test
    public void testRequestResponseNullSerializer() throws Exception
    {
        deprecatedResponsesShouldReturnNullSerializer(Verb.REQUEST_RSP);
    }

    public void deprecatedResponsesShouldReturnNullSerializer(Verb verb) throws Exception
    {
        MessagingService messagingService = mock(MessagingService.class);
        RequestCallbacks requestCallbacks = new RequestCallbacks(messagingService);
        Message<?> msg = Message.remoteResponse(InetAddressAndPort.getByName("127.0.0.1"), verb, null);
        requestCallbacks.addWithExpiration(mock(RequestCallback.class), msg, msg.from());

        IVersionedAsymmetricSerializer<Object, Object> serializer = requestCallbacks.responseSerializer(msg.id(), msg.from());

        assertNull(serializer);
    }
}