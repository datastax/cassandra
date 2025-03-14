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
package org.apache.cassandra.db.compaction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Preconditions;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.io.FSDiskFullWriteError;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.WrappedRunnable;

import static com.google.common.base.Throwables.propagate;


public abstract class AbstractCompactionTask extends WrappedRunnable
{
    // See CNDB-10549
    static final boolean SKIP_REPAIR_STATE_CHECKING =
        CassandraRelevantProperties.COMPACTION_SKIP_REPAIR_STATE_CHECKING.getBoolean();
    static final boolean SKIP_COMPACTING_STATE_CHECKING =
    CassandraRelevantProperties.COMPACTION_SKIP_COMPACTING_STATE_CHECKING.getBoolean();

    protected final CompactionRealm realm;
    protected ILifecycleTransaction transaction;
    protected boolean isUserDefined;
    protected OperationType compactionType;
    protected TableOperationObserver opObserver;
    protected final List<CompactionObserver> compObservers;

    /**
     * @param realm
     * @param transaction the modifying managing the status of the sstables we're replacing
     */
    protected AbstractCompactionTask(CompactionRealm realm, ILifecycleTransaction transaction)
    {
        this.realm = realm;
        this.transaction = transaction;
        this.isUserDefined = false;
        this.compactionType = OperationType.COMPACTION;
        this.opObserver = TableOperationObserver.NOOP;
        this.compObservers = new ArrayList<>();

        try
        {
            if (!SKIP_COMPACTING_STATE_CHECKING && !transaction.isOffline())
            {
                // enforce contract that caller should mark sstables compacting
                var compacting = realm.getCompactingSSTables();
                for (SSTableReader sstable : transaction.originals())
                    assert compacting.contains(sstable) : sstable.getFilename() + " is not correctly marked compacting";
            }

            validateSSTables(transaction.originals());
        }
        catch (Throwable err)
        {
            propagate(cleanup(err));
        }
    }

    /**
     * Confirm that we're not attempting to compact repaired/unrepaired/pending repair sstables together
     */
    private void validateSSTables(Set<SSTableReader> sstables)
    {
        if (SKIP_REPAIR_STATE_CHECKING)
            return;

        // do not allow sstables in different repair states to be compacted together
        if (!sstables.isEmpty())
        {
            Iterator<SSTableReader> iter = sstables.iterator();
            SSTableReader first = iter.next();
            boolean isRepaired = first.isRepaired();
            UUID pendingRepair = first.getPendingRepair();
            while (iter.hasNext())
            {
                SSTableReader next = iter.next();
                Preconditions.checkArgument(isRepaired == next.isRepaired(),
                                            "Cannot compact repaired and unrepaired sstables");

                if (pendingRepair == null)
                {
                    Preconditions.checkArgument(!next.isPendingRepair(),
                                                "Cannot compact pending repair and non-pending repair sstables");
                }
                else
                {
                    Preconditions.checkArgument(next.isPendingRepair(),
                                                "Cannot compact pending repair and non-pending repair sstables");
                    Preconditions.checkArgument(pendingRepair.equals(next.getPendingRepair()),
                                                "Cannot compact sstables from different pending repairs");
                }
            }
        }
    }

    /**
     * Executes the task after setting a new observer, normally the observer is the
     * compaction manager metrics.
     */
    public void execute(TableOperationObserver observer)
    {
        setOpObserver(observer).execute();
    }

    /** Executes the task */
    public void execute()
    {
        Throwable t = null;
        try
        {
            executeInternal();
        }
        catch (FSDiskFullWriteError e)
        {
            RuntimeException cause = new RuntimeException("Converted from FSDiskFullWriteError: " + e.getMessage());
            cause.setStackTrace(e.getStackTrace());
            t = cause;
            throw new RuntimeException("Throwing new Runtime to bypass exception handler when disk is full", cause);
        }
        catch (Throwable t1)
        {
            t = t1;
            throw t1;
        }
        finally
        {
            Throwables.maybeFail(cleanup(t));
        }
    }

    public Throwable rejected(Throwable t)
    {
        return cleanup(t);
    }

    protected Throwable cleanup(Throwable err)
    {
        final Throwable originalError = err;
        for (CompactionObserver compObserver : compObservers)
            err = Throwables.perform(err, () -> compObserver.onCompleted(transaction.opId(), originalError));

        return Throwables.perform(err, () -> transaction.close());
    }

    protected void executeInternal()
    {
        run();
    }

    // TODO Eventually these three setters should be passed in to the constructor.

    public AbstractCompactionTask setUserDefined(boolean isUserDefined)
    {
        this.isUserDefined = isUserDefined;
        return this;
    }

    /**
     * @return The type of compaction this task is performing. Used by CNDB.
     */
    public OperationType getCompactionType()
    {
        return compactionType;
    }

    public AbstractCompactionTask setCompactionType(OperationType compactionType)
    {
        this.compactionType = compactionType;
        return this;
    }

    /**
     * Override the NO OP observer, this is normally overridden by the compaction metrics.
     */
    public AbstractCompactionTask setOpObserver(TableOperationObserver opObserver)
    {
        this.opObserver = opObserver;
        return this;
    }

    public void addObserver(CompactionObserver compObserver)
    {
        compObservers.add(compObserver);
    }

    /**
     * Returns the space overhead of this compaction. This can be used to limit running compactions to they fit under
     * a given space budget. Only implemented for the types of tasks used by the unified compaction strategy and used
     * by CNDB.
     */
    public abstract long getSpaceOverhead();

    /**
     * @return The compaction observers for this task. Used by CNDB.
     */
    public List<CompactionObserver> getCompObservers()
    {
        return compObservers;
    }

    /**
     * Return the transaction that this task is working on. Used by CNDB as well as tests.
     */
    public ILifecycleTransaction getTransaction()
    {
        return transaction;
    }

    public String toString()
    {
        return "CompactionTask(" + transaction + ")";
    }
}
