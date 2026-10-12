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
import javax.annotation.Nullable;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;

public class GossipShutdown
{
    public static final Serializer serializer = new Serializer();

    public final EndpointState state;

    public GossipShutdown(EndpointState state)
    {
        this.state = state;
    }

    /**
     * The {@link EndpointState} payload exists only on 5.0-family messaging versions. Older peers, including HCD 1.x nodes
     * that negotiate the DS 4.0 versions ({@link MessagingService#VERSION_DS_10} .. {@link MessagingService#VERSION_DS_12}),
     * neither send nor expect it.
     */
    public static final class Serializer implements IVersionedSerializer<Object>
    {

        @Override
        public void serialize(Object t, DataOutputPlus out, int version) throws IOException
        {
            if (!MessagingService.isVersion50OrLater(version)) return;
            GossipShutdown shutdown = (GossipShutdown) t;
            EndpointState.serializer.serialize(shutdown.state, out, version);
        }

        @Nullable
        @Override
        public Object deserialize(DataInputPlus in, int version) throws IOException
        {
            if (!MessagingService.isVersion50OrLater(version)) return null;
            return new GossipShutdown(EndpointState.serializer.deserialize(in, version));
        }

        @Override
        public long serializedSize(Object t, int version)
        {
            if (!MessagingService.isVersion50OrLater(version)) return 0;
            GossipShutdown shutdown = (GossipShutdown) t;
            return EndpointState.serializer.serializedSize(shutdown.state, version);
        }
    }
}
