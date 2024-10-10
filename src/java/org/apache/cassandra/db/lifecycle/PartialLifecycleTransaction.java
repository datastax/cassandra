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

    final CompositeLifecycleTransaction composite;
    final ILifecycleTransaction mainTransaction;
    final AtomicBoolean committedOrAborted = new AtomicBoolean(false);

    public PartialLifecycleTransaction(CompositeLifecycleTransaction composite)
    {
        this.composite = composite;
        this.mainTransaction = composite.mainTransaction;
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
        if (original)
            throw earlyOpenUnsupported();

        mainTransaction.update(reader, original);
    }

    public void update(Collection<SSTableReader> readers, boolean original)
    {
        mainTransaction.update(readers, original);
    }

    public SSTableReader current(SSTableReader reader)
    {
        return mainTransaction.current(reader);
    }

    public void obsolete(SSTableReader reader)
    {
        earlyOpenUnsupported();
    }

    public void obsoleteOriginals()
    {
        composite.requestObsoleteOriginals();
    }

    public Set<SSTableReader> originals()
    {
        return mainTransaction.originals();
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
            composite.commitPart();
        return accumulate;
    }

    public Throwable abort(Throwable accumulate)
    {
        if (markCommittedOrAborted())
            composite.abortPart();
        return accumulate;
    }

    private void throwIfAborted()
    {
        // maybe throw if the composite transaction is already aborted?
        if (composite.wasAborted())
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
            composite.abortPart();
    }

    public void trackNew(SSTable table)
    {
        throwIfAborted();
        mainTransaction.trackNew(table);
    }

    public void untrackNew(SSTable table)
    {
        mainTransaction.untrackNew(table);
    }

    public OperationType opType()
    {
        return mainTransaction.opType();
    }

    public boolean isOffline()
    {
        return mainTransaction.isOffline();
    }

    @Override
    public UUID opId()
    {
        return mainTransaction.opId();
    }

    @Override
    public void cancel(SSTableReader removedSSTable)
    {
        mainTransaction.cancel(removedSSTable);
    }
}
