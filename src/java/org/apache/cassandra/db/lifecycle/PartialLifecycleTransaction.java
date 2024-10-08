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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;

@NotThreadSafe
public class PartialLifecycleTransaction implements ILifecycleTransaction
{

    final CompositeLifecycleTransaction delegate;
    final AtomicBoolean committedOrAborted = new AtomicBoolean(false);

    public PartialLifecycleTransaction(CompositeLifecycleTransaction delegate)
    {
        this.delegate = delegate;
    }

    public void checkpoint()
    {
        // don't do anything, composite will checkpoint at end
    }

    private RuntimeException earlyOpenUnsupported()
    {
        throw new UnsupportedOperationException("PartialLifecycleTransaction does not support early opening of SSTables");
    }

    public void update(SSTableReader reader, boolean original)
    {
        earlyOpenUnsupported();
    }

    public void update(Collection<SSTableReader> readers, boolean original)
    {
        delegate.update(readers, original);
    }

    public SSTableReader current(SSTableReader reader)
    {
        return delegate.current(reader);
    }

    public void obsolete(SSTableReader reader)
    {
        earlyOpenUnsupported();
    }

    public void obsoleteOriginals()
    {
        delegate.requestObsoleteOriginals();
    }

    public Set<SSTableReader> originals()
    {
        return delegate.originals();
    }

    public boolean isObsolete(SSTableReader reader)
    {
        throw earlyOpenUnsupported();
    }

    private boolean markCommittedOrAborted()
    {
        return committedOrAborted.compareAndSet(false, true);
    }

    public Throwable commit(Throwable accumulate)
    {
        if (markCommittedOrAborted())
            delegate.commitPart();
        return accumulate;
    }

    public Throwable abort(Throwable accumulate)
    {
        if (markCommittedOrAborted())
            delegate.abortPart();
        return accumulate;
    }

    private void throwIfAborted()
    {
        // maybe throw if the composite transaction is already aborted?
        if (delegate.wasAborted())
            throw new IllegalStateException("Transaction aborted");
    }

    public void prepareToCommit()
    {
        throwIfAborted();
        // nothing else to do, the composite transaction will perform the preparation when all parts are done
    }

    public void close()
    {
        if (markCommittedOrAborted())   // close should abort if not committed
            delegate.abortPart();
    }

    public void trackNew(SSTable table)
    {
        throwIfAborted();
        delegate.trackNew(table);
    }

    public void untrackNew(SSTable table)
    {
        delegate.untrackNew(table);
    }

    public OperationType opType()
    {
        return delegate.opType();
    }

    public boolean isOffline()
    {
        return delegate.isOffline();
    }

    @Override
    public UUID opId()
    {
        return delegate.opId();
    }

    @Override
    public void cancel(SSTableReader removedSSTable)
    {
        delegate.cancel(removedSSTable);
    }
}
