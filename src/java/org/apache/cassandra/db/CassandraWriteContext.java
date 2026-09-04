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

import java.util.Map;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class CassandraWriteContext implements WriteContext
{
    private final OpOrder.Group opGroup;
    private final CommitLogPosition position;
    private final WriteOptions writeOptions;
    private final WriteOrigin origin;
    /**
     * The handles the indexes returned from {@link Index#prepareWrite} for this mutation, keyed by index
     * (identity), or null when none did. A mutation holds at most one update per table, so one handle per
     * index is enough. Only the mutation thread touches it; a handle leaves it in {@link #takePreparedWrite}
     * or is aborted in {@link #close()}, never both.
     */
    private final @Nullable Map<Index, Index.PreparedWrite> preparedWrites;

    public CassandraWriteContext(OpOrder.Group opGroup, CommitLogPosition position)
    {
        this(opGroup, position, null, null, null);
    }

    public CassandraWriteContext(OpOrder.Group opGroup,
                                 CommitLogPosition position,
                                 WriteOptions writeOptions,
                                 WriteOrigin origin)
    {
        this(opGroup, position, writeOptions, origin, null);
    }

    public CassandraWriteContext(OpOrder.Group opGroup,
                                 CommitLogPosition position,
                                 WriteOptions writeOptions,
                                 WriteOrigin origin,
                                 @Nullable Map<Index, Index.PreparedWrite> preparedWrites)
    {
        Preconditions.checkArgument(opGroup != null);
        this.opGroup = opGroup;
        this.position = position;
        this.writeOptions = writeOptions;
        this.origin = origin;
        this.preparedWrites = preparedWrites;
    }

    public static CassandraWriteContext fromContext(WriteContext context)
    {
        Preconditions.checkArgument(context instanceof CassandraWriteContext);
        return (CassandraWriteContext) context;
    }

    public OpOrder.Group getGroup()
    {
        return opGroup;
    }

    public CommitLogPosition getPosition()
    {
        return position;
    }

    /**
     * The options the enclosing mutation is being applied with, or null when this context was not opened for
     * a mutation (index build, compaction, cleanup, and the read path — see
     * {@link KeyspaceWriteHandler#createContextForIndexing()} and
     * {@link KeyspaceWriteHandler#createContextForRead()}).
     * <p>
     * Secondary indexes receive this context in {@code Index.Group#indexerFor} and can use it to tell an
     * ordinary write from a hint replay, a read repair, a batchlog replay or a commit log replay, which
     * {@code IndexTransaction.Type} alone does not distinguish.
     */
    public WriteOptions getWriteOptions()
    {
        return writeOptions;
    }

    /**
     * Where the enclosing mutation came from, or null when this context was not opened for a mutation.
     * <p>
     * Null, {@link WriteOrigin#LOCAL} and {@link WriteOrigin#UNKNOWN} are three different answers. Null
     * means "there is no mutation here at all" -- an index build, a compaction, a cleanup, or the read
     * path. {@code LOCAL} means "there is a mutation and it provably did not arrive over the wire", which
     * is a real, common origin: a write this node coordinated, a commit log replay, a paxos commit applied
     * where it was proposed. {@code UNKNOWN} means the origin could not be determined (snitch missing or
     * failing, local datacenter unresolved) -- the write may have come from anywhere.
     */
    public WriteOrigin getOrigin()
    {
        return origin;
    }

    /**
     * Hands {@code index} the handle it returned from {@link Index#prepareWrite} for the enclosing mutation --
     * captured by the keyspace write handler BEFORE the commit log append -- and forgets it: a second call
     * returns null, and a handle taken here is not {@linkplain Index#abortWrite aborted} when the context is
     * closed. Meant to be called from the index's own {@code indexerFor}, on the mutation thread.
     *
     * @return the handle, or null if the index returned none, was not asked (indexing skipped for this write,
     * index not writable, context not opened for a mutation), or already took it
     */
    @Override
    public @Nullable Index.PreparedWrite takePreparedWrite(Index index)
    {
        return preparedWrites == null ? null : preparedWrites.remove(index);
    }

    /**
     * Ends the write: whatever handles the indexes did not {@linkplain #takePreparedWrite take back} -- the
     * write failed after they were prepared, the table was dropped, the index was not asked for an indexer or
     * did not take it -- are {@linkplain Index#abortWrite aborted}, then the write order group is closed.
     */
    @Override
    public void close()
    {
        try
        {
            if (preparedWrites != null && !preparedWrites.isEmpty())
            {
                SecondaryIndexManager.abortPreparedWrites(preparedWrites);
                preparedWrites.clear();
            }
        }
        finally
        {
            opGroup.close();
        }
    }
}
