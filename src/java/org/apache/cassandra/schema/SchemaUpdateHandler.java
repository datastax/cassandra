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
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import javax.annotation.Nonnull;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.SchemaTransformation.SchemaTransformationResult;

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
     * Initialize the schema from some storage. This is called in the beginning before we actually start accessing
     * schema. It should return the applied diff on the schema which is actually the diff between the empty schema
     * and the loaded one.
     * <p>
     * This method is not expected to call pre-update and post-update callbacks
     *
     * @return the difference in schema made by this method
     */
    SchemaTransformationResult initializeSchemaFromDisk();

    /**
     * Starts actively synchronizing schema with the rest of the cluster. It is called in the very beginning of the
     * node startup. It is not expected to block - to await for the startup completion we have another method
     * {@link #waitUntilReady(Duration)}.
     */
    void start();

    /**
     * Waits until the schema update handler is ready and return the result. If the method returns {@code false} it
     * means that readiness could not be achieved within the specified period of time. The method can be used just to
     * check if schema is ready by passing {@link Duration#ZERO} as the timeout - in such case it returns immediately.
     *
     * @param timeout the maximum time to wait for schema readiness
     * @return whether readiness is achieved
     */
    boolean waitUntilReady(Duration timeout);

    /**
     * Returns the runtime schema, the newest known version on which the update handler operates.
     *
     * @return the current schema
     */
    @Nonnull
    Schema schema();

    /**
     * Applies the provided transformation to the current schema. It persists the changes in the underlying storage
     * and updates the runtime schema, so that the subsequent calls to {@link #schema()} returns the updated schema.
     * <p>
     * Allows to pass an additional callback which is triggered once we know the changes to be made but before updating
     * the runtime schema. Whether the callback is called before or after persisting schema to the underlying storage
     * is unspecified.
     *
     * @param transformation the transformation to apply to the current schema
     * @param locally        whether the changes should be immediately synced with the cluster
     */
    SchemaTransformationResult apply(SchemaTransformation transformation, boolean locally);

    /**
     * Reloads the schema from the underlying storage.
     * <p>
     * The method is similar to {@link #apply(SchemaTransformation, boolean)}, where the transformation is
     * made from the runtime schema to the schema loaded from the underlying storage. The method synchronizes the change
     * with the cluster and similarly to {@link #apply(SchemaTransformation, boolean)} lets passing pre-update
     * callback with the same semantics.
     *
     * @return the difference between the runtime schema and the schema loaded from the underlying storage
     * TODO maybe instead of this method, it would be better to have a method which just returns the schema from the underlying storage, then the called could manually invoke #apply and we would not have any redundancy here
     */
    SchemaTransformationResult reloadSchemaFromDisk();

    /**
     * If schema tracker needs to process native schema messages exchanged via Gossip, it should implement this interface.
     */
    interface GossipAware extends SchemaUpdateHandler
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
        SchemaTransformationResult applyReceivedSchemaMutations(InetAddressAndPort pushRequestFrom, Collection<Mutation> schemaMutations);

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
        SchemaTransformationResult clearUnsafe();
    }
}
