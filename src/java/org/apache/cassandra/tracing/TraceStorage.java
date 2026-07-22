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

package org.apache.cassandra.tracing;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.cassandra.service.ClientState;

/**
 * Pluggable interface for different ways to store trace data. By default, this will store in system_traces,
 * but there may be other options or configurations in different situations(such as exporting to OpenTelemetry).
 */
public interface TraceStorage
{

    Future<Void> startSession(TraceState state,
                              InetAddress client,
                              String request,
                              Map<String, String> parameters,
                              long startedAt);

    Future<Void> stopSession(TraceState state);

    Future<Void> recordEvent(TraceState state, String threadName, String message);

    // Called for non-local traces (traces that are not initiated by local node == coordinator).
    Future<Void> recordNonLocalEvent(ClientState clientState,
                                     ByteBuffer sessionId,
                                     String message,
                                     int elapsedTime,
                                     String threadName,
                                     int ttl);
}
