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

package org.apache.cassandra.transport.messages;

import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.MonotonicClockTranslation;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

public class RequestMessageTest
{
    @Test
    public void testCreateTimeNanosInitialized()
    {
        long now = MonotonicClock.approxTime.now();
        TestRequestMessage message = new TestRequestMessage();
        assertThat(message.getCreationTimeNanos()).isGreaterThanOrEqualTo(now);
    }

    @Test
    public void testMaybeOverrideCreationTimeNanos()
    {
        MonotonicClockTranslation timeSnapshot = MonotonicClock.approxTime.translate();
        long overrideEpochMillis = 1234567890L;
        long overrideTimeNanos = timeSnapshot.fromMillisSinceEpoch(overrideEpochMillis);
        Map<String, ByteBuffer> customPayload = Collections.singletonMap("REQUEST_CREATE_MILLIS", ByteBufferUtil.bytes(overrideEpochMillis));
        TestRequestMessage message = new TestRequestMessage();

        message.setCustomPayload(customPayload);
        assertThat(message.getCreationTimeNanos()).isNotEqualTo(overrideTimeNanos);
        message.maybeOverrideCreationTimeNanos(timeSnapshot);
        assertThat(message.getCreationTimeNanos()).isEqualTo(overrideTimeNanos);
    }

    @Test
    public void testMaybeOverrideCreationTimeNanosWithoutSideEffects()
    {
        Map<String, ByteBuffer> customPayload = Collections.singletonMap("REQUEST_CREATE_MILLIS", ByteBufferUtil.bytes("string"));
        TestRequestMessage message = new TestRequestMessage();

        long initialCreationTimeNanos = message.getCreationTimeNanos();
        message.setCustomPayload(customPayload);
        message.maybeOverrideCreationTimeNanos(MonotonicClock.approxTime.translate());
        assertThat(message.getCreationTimeNanos()).isEqualTo(initialCreationTimeNanos);
    }

    private static class TestRequestMessage extends Message.Request
    {
        public TestRequestMessage()
        {
            super(Type.QUERY); // Using QUERY as a representative type for any request message
        }

        @Override
        protected CompletableFuture<Response> maybeExecuteAsync(QueryState queryState, long queryStartNanoTime, boolean traceRequest)
        {
            return null;
        }
    }
}
