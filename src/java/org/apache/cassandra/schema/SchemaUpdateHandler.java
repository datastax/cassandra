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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;

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
    Keyspaces.KeyspacesDiff initializeSchemaFromDisk();

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
    @Nonnull
    Schema schema();

    /**
     * Apply the provided transformation to the current schema.
     * @param transformation           the transformation to apply to the current schema.
     * @param locally                  whether the updated version should be announced and changes pushed to other nodes
     */
    SchemaTransformationResult apply(SchemaTransformation transformation, boolean locally);

    /*
     * Reload schema from local disk. Useful if a user made changes to schema tables by hand, or has suspicion that
     * in-memory representation got out of sync somehow with what's on disk.
     */
    SchemaTransformationResult reloadSchemaFromDisk();

    /**
     * If schema tracker needs to process native schema messages exchanged via Gossip, it should implement this
     * interface.
     */
    interface GossipAware extends SchemaUpdateHandler
    {
        /**
         * Called when schema push message is received.
         * @return
         */
        SchemaTransformationResult applyReceivedSchemaMutations(InetAddressAndPort pushReqeustFrom, Collection<Mutation> schemaMutations);

        /**
         * Called when schema pull messsage is received.
         */
        Collection<Mutation> prepareRequestedSchemaMutations(InetAddressAndPort pullRequestFrom);

        /**
         * Clears the local schema and pull schema from other nodes.
         *
         * This method is kind of broken/dangerous because clearing the local schema is not safe at all. First,
         * this method is presumably meant to be called when a node is online (otherwise, just hard-removing the system
         * schema tables is probably easier/safer) but, even if we try to pull from another node right away, there will
         * be a window during which the node has no schema and queries will likely fail while that is.
         * But more importantly, this drops all the TableMetadataRef from SchemaManager, but existing instances of
         * ColumnFamilyStore (and other consumers) will still refer to them. So even after the schema is restored from
         * the schema PULL, those ColumnFamilyStore instance will refer to the old refs that will not get updated and
         * that could lead to silent unexpected behavior while the node is not restarted.
         *
         * TODO remove or refactor this method as it is dangerous
         * @return
         */
        @Deprecated
        CompletableFuture<SchemaTransformationResult> clearUnsafe();
    }

    default Optional<GossipAware> asGossipAwareTracker()
    {
        return this instanceof SchemaUpdateHandler.GossipAware
               ? Optional.of((SchemaUpdateHandler.GossipAware) this)
               : Optional.empty();
    }

    default SchemaUpdateHandler.GossipAware asGossipAwareTrackerOrThrow(String msg)
    {
        String format = "The current schema tracker (%s) does not implement GossipAware. %s";
        return asGossipAwareTracker().orElseThrow(() -> new UnsupportedOperationException(String.format(format, this.getClass().getName(), StringUtils.trimToEmpty(msg))));
    }
}
