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

import java.util.Collection;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.locator.InetAddressAndPort;

/**
 * If schema update handler needs to process native schema messages exchanged via Gossip, it should implement this interface.
 */
public interface GossipAwareSchemaUpdateHandler extends SchemaUpdateHandler
{
    /**
     * Called when schema push message is received. It basically does the same thing as
     * {@link #apply(SchemaTransformation, boolean)} but it accepts the transformation in legacy format
     * - a collection of mutations to be applied on schema keyspace. It lets passing pre-update handler whose
     * semantics is the same as in case of {@link #apply(SchemaTransformation, boolean)}.
     *
     * @param pushRequestFrom the endpoint from which the schema transformation was received
     * @param schemaMutations schema transformation
     * @return the result of changes applied to the runtime schema
     */
    SchemaTransformation.SchemaTransformationResult applyReceivedSchemaMutations(InetAddressAndPort pushRequestFrom, Collection<Mutation> schemaMutations);

    /**
     * Called when schema pull message is received. It converts the runtime schema into a collection of mutations
     * (a legacy schema format).
     *
     * @param pullRequestFrom the endpoint from which the schema pull request was received
     * @return the runtime schema as a collection of mutations
     */
    Collection<Mutation> prepareRequestedSchemaMutations(InetAddressAndPort pullRequestFrom);

    /**
     * Clears the local schema and pull schema from other nodes.
     * <p>
     * This method is kind of broken/dangerous because clearing the local schema is not safe at all. First,
     * this method is presumably meant to be called when a node is online (otherwise, just hard-removing the system
     * schema tables is probably easier/safer) but, even if we try to pull from another node right away, there will
     * be a window during which the node has no schema and queries will likely fail while that is.
     * But more importantly, this drops all the TableMetadataRef from SchemaManager, but existing instances of
     * ColumnFamilyStore (and other consumers) will still refer to them. So even after the schema is restored from
     * the schema PULL, those ColumnFamilyStore instance will refer to the old refs that will not get updated and
     * that could lead to silent unexpected behavior while the node is not restarted.
     */
    @Deprecated
    // TODO remove or refactor this method as it is dangerous
    SchemaTransformation.SchemaTransformationResult clearUnsafe();
}
