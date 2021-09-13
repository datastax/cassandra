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

/**
 * Schema update handler is responsible for maintaining the current schema and synchronizing it with other nodes in
 * the cluster, which means pushing and pulling changes, as well as tracking the current version in the cluster.
 * <p/>
 * The changes made in schema are applied to {@link SchemaManager}.
 * <p/>
 * The interface has been extracted to abstract out that functionality, allow for various implementations like Gossip
 * based (the default), ETCD, offline, etc., and make it easier for mocking in unit tests.
 */
public interface SchemaUpdateHandler
{
    /**
     * Starts actively synchronizing schema with other nodes
     */
    void start();

    /**
     * Waits until the schema is ready for the specified amount of time and return the result. If the method returns
     * {@code false} it means that schema readiness could not be achieved within the specified period of time. The
     * method can be used just to check if schema is ready by passing {@link Duration#ZERO} as the timeout.
     *
     * @param timeout the maximum time to wait for schema readiness
     * @return whether readiness is achieved
     */
    boolean waitUntilReady(Duration timeout);

    /**
     * Returns the current schema, the newest known version
     *
     * @return the current schema
     */
    SchemaManager schema();
}
