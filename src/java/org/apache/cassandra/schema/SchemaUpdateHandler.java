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

import java.time.Duration;

public interface SchemaUpdateHandler
{
    /**
     * Starts actively synchronizing schema with the rest of the cluster. It is called in the very beginning of the
     * node startup. It is not expected to block - to await for the startup completion we have another method
     * {@link #waitUntilReady(Duration)}.
     */
    void start();

    /**
     * Waits until the schema update handler is ready and returns the result. If the method returns {@code false} it
     * means that readiness could not be achieved within the specified period of time. The method can be used just to
     * check if schema is ready by passing {@link Duration#ZERO} as the timeout - in such case it returns immediately.
     *
     * @param timeout the maximum time to wait for schema readiness
     * @return whether readiness is achieved
     */
    boolean waitUntilReady(Duration timeout);
}
