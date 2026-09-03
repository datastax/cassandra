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

package org.apache.cassandra.db;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class CassandraKeyspaceWriteHandler implements KeyspaceWriteHandler
{
    private final Keyspace keyspace;

    public CassandraKeyspaceWriteHandler(Keyspace keyspace)
    {
        this.keyspace = keyspace;
    }

    @Override
    public WriteContext beginWrite(Mutation mutation, WriteOptions writeOptions) throws RequestExecutionException
    {
        OpOrder.Group group = null;
        Map<Index, Index.PreparedWrite> prepared = null;
        try
        {
            group = Keyspace.writeOrder.start();

            // Let the indexes see the write BEFORE anything is persisted (Index#prepareWrite). This must
            // precede the commit log append: it is the ordering that lets an index treat a replayed
            // mutation as one it has already been told about.
            if (writeOptions.updateIndexes)
                prepared = prepareIndexWrites(mutation, writeOptions);

            // write the mutation to the commitlog and memtables
            CommitLogPosition position = null;
            if (writeOptions.shouldWriteCommitLog(mutation.getKeyspaceName()))
            {
                position = addToCommitLog(mutation);
            }
            return new CassandraWriteContext(group, position, writeOptions, mutation.origin(), prepared);
        }
        catch (Throwable t)
        {
            // The write is failing before anything was persisted: whatever the indexes prepared for it
            // is undone. (A failure inside prepareIndexWrites itself has already aborted and emptied it.)
            if (prepared != null && !prepared.isEmpty())
                SecondaryIndexManager.abortPreparedWrites(prepared);
            if (group != null)
            {
                group.close();
            }
            throw t;
        }
    }

    /**
     * Offers each partition update to its table's indexes, mirroring the per-update loop of
     * {@code Keyspace.applyInternal}: an update whose table has been dropped is skipped there, so it is skipped
     * here too. Costs a volatile read per update for a table whose indexes do not prepare writes, which is
     * the common case; the handle map is only created when an index actually returns a handle.
     *
     * @return the handles keyed by index, for the mutation's {@link CassandraWriteContext}; null if none
     */
    private Map<Index, Index.PreparedWrite> prepareIndexWrites(Mutation mutation, WriteOptions writeOptions)
    {
        Map<Index, Index.PreparedWrite> prepared = null;
        for (PartitionUpdate update : mutation.getPartitionUpdates())
        {
            ColumnFamilyStore cfs = keyspace.getIfExists(update.metadata().id);
            if (cfs == null || !cfs.indexManager.preparesWrites())
                continue;
            prepared = cfs.indexManager.prepareWrite(update, writeOptions, mutation.origin(), prepared);
        }
        return prepared;
    }

    private CommitLogPosition addToCommitLog(Mutation mutation)
    {
        // Usually one of these will be true, so first check if that's the case.
        boolean allSkipCommitlog = true;
        boolean noneSkipCommitlog = true;
        for (PartitionUpdate update : mutation.getPartitionUpdates())
        {
            if (update.metadata().params.memtable.factory().writesShouldSkipCommitLog())
                noneSkipCommitlog = false;
            else
                allSkipCommitlog = false;
        }

        if (!noneSkipCommitlog)
        {
            if (allSkipCommitlog)
                return null;
            else
            {
                Set<TableId> ids = new HashSet<>();
                for (PartitionUpdate update : mutation.getPartitionUpdates())
                {
                    if (update.metadata().params.memtable.factory().writesShouldSkipCommitLog())
                        ids.add(update.metadata().id);
                }
                mutation = mutation.without(ids);
            }
        }
        // Note: It may be a good idea to precalculate none/all for the set of all tables in the keyspace,
        // or memoize the mutation.getTableIds()->ids map (needs invalidation on schema version change).

        Tracing.trace("Appending to commitlog");
        return CommitLog.instance.add(mutation);
    }

    private WriteContext createEmptyContext()
    {
        OpOrder.Group group = null;
        try
        {
            group = Keyspace.writeOrder.start();
            return new CassandraWriteContext(group, null);
        }
        catch (Throwable t)
        {
            if (group != null)
            {
                group.close();
            }
            throw t;
        }
    }

    @Override
    public WriteContext createContextForIndexing()
    {
        return createEmptyContext();
    }

    @Override
    public WriteContext createContextForRead()
    {
        return createEmptyContext();
    }
}
