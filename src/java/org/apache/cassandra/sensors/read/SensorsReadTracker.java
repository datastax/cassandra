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

package org.apache.cassandra.sensors.read;

import java.nio.ByteBuffer;
import java.util.Map;

import com.google.common.util.concurrent.AtomicDouble;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.QueryInfoTracker;

public class SensorsReadTracker implements QueryInfoTracker.ReadTracker
{

    private final AtomicDouble quaryReadBytes = new AtomicDouble();

    @Override
    public void onDone()
    {
    }

    @Override
    public void onError(Throwable exception)
    {
    }

    @Override
    public void onReplicaPlan(ReplicaPlan.ForRead<?> replicaPlan)
    {
    }

    @Override
    public void onPartition(DecoratedKey partitionKey)
    {
    }

    @Override
    public void onRow(Row row)
    {
    }

    @Override
    public void onResponse(Message<ReadResponse> message)
    {
        Map<String, byte[]> customParams = message.header.customParams();
        if (customParams == null)
            return;

        byte[] bytes = customParams.get("READ_BYTES");
        if (bytes == null)
            return;

        ByteBuffer readBytesBuffer = ByteBuffer.allocate(Double.BYTES);
        readBytesBuffer.put(bytes);
        readBytesBuffer.flip();
        double messageReadBytes = readBytesBuffer.getDouble();
        this.quaryReadBytes.getAndAdd(messageReadBytes);
    }

    public ByteBuffer getQueryReadBytesAsByteBuffer() {
        ByteBuffer readBytesBuffer = ByteBuffer.allocate(Double.BYTES);
        readBytesBuffer.putDouble(this.quaryReadBytes.get());
        return readBytesBuffer;
    }
}
