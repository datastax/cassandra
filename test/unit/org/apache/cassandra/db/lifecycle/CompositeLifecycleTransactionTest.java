/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db.lifecycle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import com.google.common.util.concurrent.Runnables;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.monitoring.runtime.instrumentation.common.collect.Sets;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Transactional.AbstractTransactional.State;

import static com.google.monitoring.runtime.instrumentation.common.collect.ImmutableSet.copyOf;

public class CompositeLifecycleTransactionTest
{
    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
        MockSchema.cleanup();
    }

    @Test
    public void testUpdates()
    {
        int count = 3;
        ColumnFamilyStore cfs = MockSchema.newCFS();
        Tracker tracker = Tracker.newDummyTracker(cfs.metadata);
        SSTableReader[] readers = readersArray(0, 3, cfs);
        SSTableReader[] readers2 = readersArray(0, 5, cfs);
        SSTableReader[] readers3 = readersArray(5, 5 + count, cfs);
        tracker.addInitialSSTables(copyOf(readers));
        LifecycleTransaction txn = tracker.tryModify(copyOf(readers), OperationType.UNKNOWN);

        CompositeLifecycleTransaction composite = new CompositeLifecycleTransaction(txn);
        var partials = IntStream.range(0, count).mapToObj(i -> new PartialLifecycleTransaction(composite)).toArray(PartialLifecycleTransaction[]::new);
        composite.completeInitialization();

        partials[0].update(readers2[3], false);

        testBadUpdate(partials[2], readers2[0], false);  // same reader && instances
        testBadUpdate(partials[2], readers2[1], true);  // early open unsupported

        Assert.assertEquals(3, tracker.getView().compacting.size());
        partials[0].checkpoint();
        for (int i = 0 ; i < count ; i++)
            partials[i].update(readers3[i], false);

        for (var partial : partials)
        {
            Assert.assertEquals(State.IN_PROGRESS, txn.state());
            partial.checkpoint();
            partial.obsoleteOriginals();
            partial.commit();
        }

        Assert.assertEquals(State.COMMITTED, txn.state());
        Assert.assertEquals(0, tracker.getView().compacting.size());
        var result = new HashSet<SSTableReader>();
        result.addAll(Arrays.asList(readers3));
        result.add(readers2[3]);
        Assert.assertEquals(result, tracker.getView().sstables);

        testThrows(() -> txn.abort());
        testThrows(() -> txn.prepareToCommit());
        testThrows(() -> txn.commit());

        testThrows(() -> partials[1].abort());
        testThrows(() -> partials[2].prepareToCommit());
        testThrows(() -> partials[0].commit());
    }

    @Test
    public void testCommit()
    {
        testPartialTransactions(300, true, none(), none());
    }

    @Test
    public void testCommitPreserveOriginals()
    {
        testPartialTransactions(300, false, none(), none());
    }

    @Test
    public void testAbort()
    {
        testPartialTransactions(300, true, arr(33), none());
    }

    @Test
    public void testAbortAll()
    {
        int count = 300;
        testPartialTransactions(count, true, all(count), none());
    }

    @Test
    public void testOnlyClose()
    {
        testPartialTransactions(300, true, none(), arr(55));
    }

    @Test
    public void testOnlyCloseAll()
    {
        int count = 300;
        testPartialTransactions(count, true, none(), all(count));
    }

    @Test
    public void testAbortAndOnlyClose()
    {
        testPartialTransactions(300, true, arr(89), arr(98));
    }

    public void testPartialTransactions(int count, boolean obsoleteOriginals, int[] indexesToAbort, int[] indexesToOnlyClose)
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        Tracker tracker = Tracker.newDummyTracker(cfs.metadata);
        SSTableReader[] inputs = readersArray(0, count, cfs);
        SSTableReader[] outputs = readersArray(count, 2*count, cfs);
        tracker.addInitialSSTables(copyOf(inputs));
        LifecycleTransaction txn = tracker.tryModify(copyOf(inputs), OperationType.UNKNOWN);

        CompositeLifecycleTransaction composite = new CompositeLifecycleTransaction(txn);
        // register partial transactions before we launch committing threads
        var partials = new PartialLifecycleTransaction[count];
        for (int i = 0; i < count; ++i)
            partials[i] = new PartialLifecycleTransaction(composite);
        composite.completeInitialization();

        var futures = new ArrayList<CompletableFuture<Void>>();
        for (int i = 0; i < count; ++i)
        {
            boolean abort = in(indexesToAbort, i);
            boolean onlyClose = in(indexesToOnlyClose, i);
            PartialLifecycleTransaction partial = partials[i];
            SSTableReader output = outputs[i];
            Runnable r = onlyClose ? Runnables.doNothing()
                                   : () ->
                                     {
                                         partial.update(output, false);
                                         partial.checkpoint();
                                         if (obsoleteOriginals)
                                             partial.obsoleteOriginals();
                                         if (abort)
                                             partial.abort();
                                         else
                                         {
                                             partial.prepareToCommit();
                                             partial.commit();
                                         }

                                         testThrows(() -> partial.abort());
                                         testThrows(() -> partial.prepareToCommit());
                                         testThrows(() -> partial.commit());
                                     };
            futures.add(CompletableFuture.runAsync(r)
                                         .whenComplete((v, t) -> partial.close()));
        }

        if (indexesToAbort.length == 0 && indexesToOnlyClose.length == 0)
        {
            CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();

            Assert.assertEquals(State.COMMITTED, txn.state());
            Assert.assertEquals(0, tracker.getView().compacting.size());
            Assert.assertEquals(obsoleteOriginals ? copyOf(outputs)
                                                  : Sets.union(copyOf(inputs), copyOf(outputs)),
                                tracker.getView().sstables);
        }
        else
        {
            try
            {
                CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
                // Abort may have happened last; this is okay.
            }
            catch(Throwable t)
            {
                if (!Throwables.isCausedBy(t, PartialLifecycleTransaction.AbortedException.class))
                    throw t;
                // expected path where other tasks are aborted by an exception
                System.out.println("Got expected exception: " + t);
            }
            Assert.assertEquals(State.ABORTED, txn.state());
            Assert.assertEquals(0, tracker.getView().compacting.size());
            Assert.assertEquals(copyOf(inputs), tracker.getView().sstables);
        }
    }

    private static void testThrows(Runnable r)
    {
        boolean failed = false;
        try
        {
            r.run();
        }
        catch (Throwable t)
        {
            failed = true;
        }
        Assert.assertTrue(failed);
    }

    private static void testBadUpdate(ILifecycleTransaction txn, SSTableReader update, boolean original)
    {
        boolean failed = false;
        try
        {
            txn.update(update, original);
        }
        catch (Throwable t)
        {
            failed = true;
        }
        Assert.assertTrue(failed);
    }


    private static SSTableReader[] readersArray(int lb, int ub, ColumnFamilyStore cfs)
    {
        List<SSTableReader> readers = new ArrayList<>();
        for (int i = lb ; i < ub ; i++)
            readers.add(MockSchema.sstable(i, i, true, cfs));
        return readers.toArray(SSTableReader[]::new);
    }

    private int[] arr(int... values)
    {
        return values;
    }

    private int[] none()
    {
        return new int[0];
    }

    private boolean in(int[] arr, int value)
    {
        for (int i : arr)
            if (i == value)
                return true;
        return false;
    }

    private int[] all(int count)
    {
        int[] result = new int[count];
        for (int i = 0 ; i < count ; i++)
            result[i] = i;
        return result;
    }
}
