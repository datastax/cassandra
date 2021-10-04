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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.schema.SchemaTransformation.SchemaTransformationResult;
import org.apache.cassandra.utils.ByteArrayUtil;

public class OfflineSchemaUpdateHandler implements SchemaUpdateHandler
{
    private final static Logger logger = LoggerFactory.getLogger(OfflineSchemaUpdateHandler.class);

    private volatile Schema schema;

    private final Executor executor;
    private final Consumer<SchemaTransformationResult> preUpdateCallback;
    private final Consumer<SchemaTransformationResult> postUpdateCallback;

    public OfflineSchemaUpdateHandler(Executor executor, Consumer<SchemaTransformationResult> preUpdateCallback, Consumer<SchemaTransformationResult> postUpdateCallback)
    {
        this.schema = new Schema(Keyspaces.none(), SchemaConstants.emptyVersion);
        this.executor = executor;
        this.preUpdateCallback = preUpdateCallback;
        this.postUpdateCallback = postUpdateCallback;
    }

    @Override
    public void start()
    {
        // no-op
    }

    @Override
    public @Nonnull
    Schema schema()
    {
        return schema;
    }

    @Override
    public boolean waitUntilReady(Duration timeout)
    {
        return true;
    }

    @Override
    public CompletableFuture<SchemaTransformationResult> apply(SchemaTransformation transformation, boolean locally)
    {
        return CompletableFuture.supplyAsync(() -> applyInternal(transformation, locally), executor);
    }

    private SchemaTransformationResult applyInternal(SchemaTransformation transformation, boolean locally)
    {
        Schema before = schema();
        Keyspaces afterKeyspaces = transformation.apply(before.getKeyspaces());
        Keyspaces.KeyspacesDiff diff = Keyspaces.diff(before.getKeyspaces(), afterKeyspaces);

        if (diff.isEmpty())
            return new SchemaTransformationResult(before, before, diff);

        Schema after = new Schema(afterKeyspaces, UUID.nameUUIDFromBytes(ByteArrayUtil.bytes(schema.getKeyspaces().hashCode())));
        SchemaTransformationResult update = new SchemaTransformationResult(before, after, diff);

        preUpdateCallback.accept(update);
        updateSchema(update);
        postUpdateCallback.accept(update);

        return update;
    }

    /**
     * Load schema definitions from disk.
     *
     * @return
     */
    @Override
    public CompletableFuture<SchemaTransformationResult> initializeSchemaFromDisk()
    {
        return CompletableFuture.supplyAsync(() -> {
            Schema before = schema();
            Keyspaces keyspaces = SchemaKeyspace.fetchNonSystemKeyspaces();
            UUID version = UUID.nameUUIDFromBytes(ByteArrayUtil.bytes(keyspaces.hashCode()));
            Schema after = new Schema(keyspaces, version);
            setSchema(after);
            return new SchemaTransformationResult(before, after, Keyspaces.diff(before.getKeyspaces(), after.getKeyspaces()));
        }, executor);
    }

    /*
     * Reload schema from local disk. Useful if a user made changes to schema tables by hand, or has suspicion that
     * in-memory representation got out of sync somehow with what's on disk.
     */
    @Override
    public CompletableFuture<SchemaTransformationResult> reloadSchemaFromDisk()
    {
        return CompletableFuture.supplyAsync(() -> {
            Keyspaces after = SchemaKeyspace.fetchNonSystemKeyspaces();
            return applyInternal(existing -> after, false);
        }, executor);
    }

    private void updateSchema(SchemaTransformationResult update)
    {
        assert schema == update.before;

        if (update.diff.isEmpty())
            return;

        setSchema(update.after);
    }

    private void setSchema(Schema schema)
    {
        this.schema = schema;
        SchemaDiagnostics.versionUpdated(schema());
    }
}
