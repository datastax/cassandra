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

package org.apache.cassandra.db.lifecycle;

import java.util.Collection;
import java.util.Set;

import com.google.common.collect.Iterables;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Transactional;

public interface ILifecycleTransaction extends Transactional, LifecycleNewTracker
{
    void checkpoint();

    /**
     * Rolls back what was staged since the last {@link #checkpoint()}, i.e. the {@link #update}s and
     * {@link #obsolete}s not yet made visible: the staged readers' references are released and the staged
     * obsoletions are forgotten. Nothing that was already checkpointed is touched, so the live set stays as it was
     * after the last checkpoint.
     * <p>
     * This is meant for an attempt that failed between staging and checkpointing, like a periodic early open (see
     * {@code SSTableRewriter}). Afterwards the caller may continue using the transaction: stage further changes
     * with new reader instances (the instances that were staged may not be provided again, as the transaction still
     * remembers their identities), checkpoint, commit or abort. A reader whose {@link #update} threw was not staged,
     * so it is not released here.
     * <p>
     * Transactions that defer their checkpoint to an enclosing operation, like {@link PartialLifecycleTransaction} and
     * the shared transaction of anticompaction, implement this as a no-op, like {@link #checkpoint()}.
     * <p>
     * The default implementation is such a no-op, which keeps implementations written before this method existed
     * compiling. It is only correct for transactions that stage nothing between checkpoints, or defer their checkpoint
     * to an enclosing operation that owns the staged state. A wrapper that forwards {@link #update} and
     * {@link #obsolete} to a delegate must forward this method too (see {@link WrappedLifecycleTransaction}). A
     * transaction that stages readers must override it to release them, otherwise a failed early open leaks their
     * references.
     *
     * @return the given accumulator, with any failure to release a reader merged into it
     */
    default Throwable abortCheckpoint(Throwable accumulate)
    {
        return accumulate;
    }

    /**
     * Stages a new version of a reader, see {@link LifecycleTransaction#update(SSTableReader, boolean)}. If this
     * throws, nothing has been staged and the reader's reference is still owned by the caller.
     */
    void update(SSTableReader reader, boolean original);
    void update(Collection<SSTableReader> readers, boolean original);
    SSTableReader current(SSTableReader reader);
    void obsolete(SSTableReader reader);
    void obsoleteOriginals();
    Set<SSTableReader> originals();
    boolean isObsolete(SSTableReader reader);
    boolean isOffline();
    TimeUUID opId();

    /// Op identifier as a string to use in debug prints. Usually just the opId, with added part information for partial
    /// transactions.
    default String opIdString()
    {
        return opId().toString();
    }

    void cancel(SSTableReader removedSSTable);

    default void abort()
    {
        Throwables.maybeFail(abort(null));
    }

    default void commit()
    {
        Throwables.maybeFail(commit(null));
    }

    default SSTableReader onlyOne()
    {
        final Set<SSTableReader> originals = originals();
        assert originals.size() == 1;
        return Iterables.getFirst(originals, null);
    }
}
