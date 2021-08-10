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

import java.io.IOError;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.concurrent.WrappedExecutorPlus;
import org.apache.cassandra.utils.concurrent.Promise;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.BackgroundCompactionRunner.RequestResult;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.Tracker;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.assertj.core.util.Lists;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BackgroundCompactionRunnerTest
{
    private final WrappedExecutorPlus compactionExecutor = Mockito.mock(WrappedExecutorPlus.class);
    private final ScheduledExecutorPlus checkExecutor = Mockito.mock(ScheduledExecutorPlus.class);
    private final ActiveOperations activeOperations = Mockito.mock(ActiveOperations.class);
    private final ColumnFamilyStore cfs = Mockito.mock(ColumnFamilyStore.class);
    private final CompactionStrategy compactionStrategy = Mockito.mock(CompactionStrategy.class);

    private BackgroundCompactionRunner runner;
    public int pendingTaskCount;
    private List<AbstractCompactionTask> compactionTasks;
    private ArgumentCaptor<Runnable> capturedCompactionRunnables, capturedCheckRunnables;

    private static boolean savedAutomaticSSTableUpgrade;
    private static int savedMaxConcurrentAuotUpgradeTasks;

    @BeforeClass
    public static void initClass()
    {
        DatabaseDescriptor.daemonInitialization();
        savedAutomaticSSTableUpgrade = DatabaseDescriptor.automaticSSTableUpgrade();
        savedMaxConcurrentAuotUpgradeTasks = DatabaseDescriptor.maxConcurrentAutoUpgradeTasks();
    }

    @AfterClass
    public static void tearDownClass()
    {
        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(savedAutomaticSSTableUpgrade);
        DatabaseDescriptor.setMaxConcurrentAutoUpgradeTasks(savedMaxConcurrentAuotUpgradeTasks);
    }

    @Before
    public void initTest()
    {
        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(true);
        DatabaseDescriptor.setMaxConcurrentAutoUpgradeTasks(2);

        pendingTaskCount = 0;
        runner = new BackgroundCompactionRunner(compactionExecutor, checkExecutor, activeOperations);
        compactionTasks = new ArrayList<>();
        capturedCompactionRunnables = ArgumentCaptor.forClass(Runnable.class);
        capturedCheckRunnables = ArgumentCaptor.forClass(Runnable.class);

        assertThat(runner.getOngoingCompactionsCount()).isZero();
        assertThat(runner.getOngoingUpgradesCount()).isZero();

        when(compactionExecutor.getMaximumPoolSize()).thenReturn(2);
        when(cfs.isAutoCompactionDisabled()).thenReturn(false);
        when(cfs.isValid()).thenReturn(true);
        when(checkExecutor.getPendingTaskCount()).thenAnswer(i-> pendingTaskCount);
        when(cfs.getCompactionStrategy()).thenReturn(compactionStrategy);
        when(compactionStrategy.getNextBackgroundTasks(ArgumentMatchers.anyLong())).thenReturn(compactionTasks);
        doNothing().when(checkExecutor).execute(capturedCheckRunnables.capture());
        doNothing().when(compactionExecutor).execute(capturedCompactionRunnables.capture());
    }

    @After
    public void tearDownTest()
    {
        reset(compactionExecutor, checkExecutor, activeOperations, cfs, compactionStrategy);
    }


    // when cfs is invalid we should immediately return ABORTED
    @Test
    public void invalidCFS() throws Exception
    {
        when(cfs.isAutoCompactionDisabled()).thenReturn(false);
        when(cfs.isValid()).thenReturn(false);

        Promise<RequestResult> result = runner.markForCompactionCheck(cfs);

        assertThat(result).isDone();
        assertThat(result.get()).isEqualTo(RequestResult.ABORTED);
        assertThat(runner.getMarkedCFSs()).isEmpty();
        verify(checkExecutor, never()).execute(notNull());
    }


    // when automatic compactions are disabled for cfs, we should immediately return ABORTED
    @Test
    public void automaticCompactionsDisabled() throws Exception
    {
        when(cfs.isAutoCompactionDisabled()).thenReturn(true);
        when(cfs.isValid()).thenReturn(true);

        Promise<RequestResult> result = runner.markForCompactionCheck(cfs);

        assertThat(result).isDone();
        assertThat(result.get()).isEqualTo(RequestResult.ABORTED);
        assertThat(runner.getMarkedCFSs()).isEmpty();
        verify(checkExecutor, never()).execute(notNull());
    }


    // we should mark cfs for compaction and schedule a check
    @Test
    public void markCFSForCompactionAndScheduleCheck() throws Exception
    {
        Promise<RequestResult> result = runner.markForCompactionCheck(cfs);

        assertThat(result).isNotDone();

        verify(checkExecutor).execute(notNull());
        assertThat(runner.getMarkedCFSs()).contains(cfs);
    }


    // when cfs is invalid we should immediately return ABORTED
    @Test
    public void invalidCFSs() throws Exception
    {
        when(cfs.isAutoCompactionDisabled()).thenReturn(false);
        when(cfs.isValid()).thenReturn(false);

        runner.markForCompactionCheck(ImmutableSet.of(cfs));

        assertThat(runner.getMarkedCFSs()).isEmpty();
        verify(checkExecutor, never()).execute(notNull());
    }


    // when automatic compactions are disabled for cfs, we should immediately return ABORTED
    @Test
    public void automaticCompactionsDisabledForCFSs() throws Exception
    {
        when(cfs.isAutoCompactionDisabled()).thenReturn(true);
        when(cfs.isValid()).thenReturn(true);

        runner.markForCompactionCheck(ImmutableSet.of(cfs));

        assertThat(runner.getMarkedCFSs()).isEmpty();
        verify(checkExecutor, never()).execute(notNull());
    }


    // we should mark cfs for compaction and schedule a check
    @Test
    public void markCFSsForCompactionAndScheduleCheck() throws Exception
    {
        runner.markForCompactionCheck(ImmutableSet.of(cfs));

        verify(checkExecutor).execute(notNull());
        assertThat(runner.getMarkedCFSs()).contains(cfs);
    }


    // we should mark cfs for compaction but not schedule new check if there is one already scheduled
    @Test
    public void markCFSForCompactionAndNotScheduleCheck() throws Exception
    {
        pendingTaskCount = 100;
        Promise<RequestResult> result = runner.markForCompactionCheck(cfs);

        assertThat(result).isNotDone();

        verify(checkExecutor, never()).execute(notNull());
        assertThat(runner.getMarkedCFSs()).contains(cfs);
    }


    // we should immeditatlly return ABORTED if the executor is shutdown
    @Test
    public void immediatelyReturnIfExecutorIsDown() throws Exception
    {
        when(checkExecutor.isShutdown()).thenReturn(true);
        doThrow(new RejectedExecutionException("rejected")).when(checkExecutor).execute(ArgumentMatchers.notNull());

        Promise<RequestResult> result = runner.markForCompactionCheck(cfs);

        assertThat(result).isDone();
        assertThat(result.get()).isEqualTo(RequestResult.ABORTED);

        verify(checkExecutor).execute(notNull());
    }


    // shutdown should shut down the check executor and should not shut down the compaction executor
    @Test
    public void shutdown() throws Exception
    {
        Promise<RequestResult> result = runner.markForCompactionCheck(cfs);

        assertThat(result.isDone()).isFalse();

        runner.shutdown();

        assertThat(result).isDone();
        assertThat(result.get()).isEqualTo(RequestResult.ABORTED);

        verify(checkExecutor).shutdown();
        verify(compactionExecutor, never()).shutdown();
    }


    // a check should make a task finish with NOT_NEEDED if there are no compaction tasks and upgrades are disabled
    @Test
    public void finishWithNotNeededWhenNoCompactionTasksAndUpgradesDisabled() throws Exception
    {
        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(false);

        when(cfs.getCandidatesForUpgrade()).thenReturn(ImmutableList.of(mock(SSTableReader.class)));

        Promise<RequestResult> result = runner.markForCompactionCheck(cfs);
        verifyCFSWasMarkedForCompaction();
        capturedCheckRunnables.getValue().run();

        assertThat(result).isDone();
        assertThat(result.get()).isEqualTo(RequestResult.NOT_NEEDED);
        verify(checkExecutor, never()).execute(notNull());
        assertThat(runner.getMarkedCFSs()).isEmpty();
    }

    // a check should make a task finish with NOT_NEEDED if there are no compaction tasks and no upgrade tasks
    @Test
    public void finishWithNotNeededWhenNoCompactionTasksAndNoUpgradeTasks() throws Exception
    {
        when(cfs.getCandidatesForUpgrade()).thenReturn(Lists.emptyList());

        Promise<RequestResult> result = runner.markForCompactionCheck(cfs);
        verifyCFSWasMarkedForCompaction();
        capturedCheckRunnables.getValue().run();

        assertThat(result).isDone();
        assertThat(result.get()).isEqualTo(RequestResult.NOT_NEEDED);
        verify(checkExecutor, never()).execute(notNull());
        assertThat(runner.getMarkedCFSs()).isEmpty();
    }


    // a check should start a compaction task if there is some
    @Test
    public void startCompactionTask() throws Exception
    {
        // although it is possible to run upgrade tasks, we make sure that compaction tasks are selected
        Promise<RequestResult> result = markCFSAndRunCheck();

        // check the task was scheduled on compaction executor
        verifyTaskScheduled(compactionExecutor);
        verifyState(1, 0);
        assertThat(result).isNotDone();

        // ... we immediatelly marked that CFS for compaction again
        verifyCFSWasMarkedForCompaction();

        // now we will execute the task
        capturedCompactionRunnables.getValue().run();

        // so we expect that:
        assertThat(result).isDone();
        assertThat(result.get()).isEqualTo(RequestResult.COMPLETED);
        verifyState(0, 0);

        // another check should be schedued upon task completion
        verifyCFSWasMarkedForCompaction();

        // make sure we haven't even attempted to check for upgrade tasks (because there were compaction tasks to run)
        verify(cfs, Mockito.never()).getCandidatesForUpgrade();
    }


    // a check should start an upgrade task if there is some and there is no compaction task
    @Test
    public void startUpgradeTask() throws Exception
    {
        AbstractCompactionTask compactionTask = mock(AbstractCompactionTask.class);
        // although it is possible to run upgrade tasks, we make sure that compaction tasks are selected
        DatabaseDescriptor.setAutomaticSSTableUpgradeEnabled(true);
        SSTableReader sstable = mock(SSTableReader.class);
        Tracker tracker = mock(Tracker.class);
        LifecycleTransaction txn = mock(LifecycleTransaction.class);
        when(cfs.getCandidatesForUpgrade()).thenReturn(Collections.singletonList(sstable));
        when(cfs.getTracker()).thenReturn(tracker);
        when(tracker.tryModify(sstable, OperationType.UPGRADE_SSTABLES)).thenReturn(txn);
        when(compactionStrategy.createCompactionTask(same(txn), anyLong(), anyLong())).thenReturn(compactionTask);

        Promise<RequestResult> result = runner.markForCompactionCheck(cfs);
        verifyCFSWasMarkedForCompaction();

        capturedCheckRunnables.getValue().run();

        // make sure we did check for the upgrade candidates
        verify(cfs).getCandidatesForUpgrade();

        // check the task was scheduled on compaction executor
        verifyTaskScheduled(compactionExecutor);
        verifyState(1, 1);
        assertThat(result).isNotDone();

        // ... we immediatelly marked that CFS for compaction again
        verifyCFSWasMarkedForCompaction();

        // now we will execute the task
        capturedCompactionRunnables.getValue().run();

        // so we expect that:
        assertThat(result).isDone();
        assertThat(result.get()).isEqualTo(RequestResult.COMPLETED);
        verifyState(0, 0);

        // another check should be schedued upon task completion
        verifyCFSWasMarkedForCompaction();
    }


    // we should run multiple compactions for a CFS in parallel if possible
    @Test
    public void startMultipleCompactionTasksInParallel() throws Exception
    {
        // first task
        Promise<RequestResult> result1 = markCFSAndRunCheck();
        verifyTaskScheduled(compactionExecutor);
        verifyState(1, 0);
        verifyCFSWasMarkedForCompaction();

        // second task
        Promise<RequestResult> result2 = markCFSAndRunCheck();
        verifyTaskScheduled(compactionExecutor);
        verifyState(2, 0);
        verifyCFSWasMarkedForCompaction();

        assertThat(result2).isNotSameAs(result1);

        // now we will execute the first task
        assertThat(result1).isNotDone();
        capturedCompactionRunnables.getAllValues().get(0).run();
        assertThat(result1).isDone();
        assertThat(result1.get()).isEqualTo(RequestResult.COMPLETED);

        // so we expect that:
        verifyState(1, 0);

        // execute the second task
        assertThat(result2).isNotDone();
        capturedCompactionRunnables.getAllValues().get(1).run();
        assertThat(result2).isDone();
        assertThat(result2.get()).isEqualTo(RequestResult.COMPLETED);

        // so we expect that:
        verifyState(0, 0);

        verifyState(0, 0);
    }


    // postpone execution if the thread pool is busy
    @Test
    public void postponeCompactionTasksIfPoolIsBusy() throws Exception
    {
        // first task
        Promise<RequestResult> result1 = markCFSAndRunCheck();
        verifyTaskScheduled(compactionExecutor);
        verifyState(1, 0);
        verifyCFSWasMarkedForCompaction();

        // second task
        Promise<RequestResult> result2 = markCFSAndRunCheck();
        verifyTaskScheduled(compactionExecutor);
        verifyState(2, 0);
        verifyCFSWasMarkedForCompaction();

        // third task, but now the task should not be scheduled for execution because of the pool size (2)
        clearInvocations(compactionStrategy);
        Promise<RequestResult> result3 = markCFSAndRunCheck();
        verifyState(2, 0);
        // we should not execute a new task, actually not even attempt to get a new compaction task
        verify(compactionStrategy, never()).getNextBackgroundTasks(anyInt());
        verify(compactionExecutor, never()).execute(notNull());
        // we should also not schedule a new check or remove mark for the CFS
        verify(checkExecutor, never()).execute(notNull());
        assertThat(runner.getMarkedCFSs()).contains(cfs);

        assertThat(result3).isNotSameAs(result1);
        assertThat(result3).isNotSameAs(result2);

        // now we will execute the task 1
        assertThat(result1).isNotDone();
        capturedCompactionRunnables.getAllValues().get(0).run();
        // so we expect that:
        assertThat(result1).isDone();
        assertThat(result1.get()).isEqualTo(RequestResult.COMPLETED);
        verifyState(1, 0);

        // execute the check, so that the thrid task is submitted
        clearInvocations(checkExecutor);
        capturedCheckRunnables.getValue().run();
        verifyTaskScheduled(compactionExecutor);
        verifyState(2, 0);
        verifyCFSWasMarkedForCompaction();

        // execute the rest of the tasks
        assertThat(result2).isNotDone();
        capturedCompactionRunnables.getAllValues().get(1).run();
        assertThat(result2).isDone();
        assertThat(result2.get()).isEqualTo(RequestResult.COMPLETED);

        assertThat(result3).isNotDone();
        capturedCompactionRunnables.getAllValues().get(2).run();
        assertThat(result3).isDone();
        assertThat(result3.get()).isEqualTo(RequestResult.COMPLETED);

        verifyState(0, 0);
    }


    // returned future should not support complete or cancel
    @Test
    public void futureRequestResultNotSupportForTermination()
    {
        Promise<RequestResult> result = markCFSAndRunCheck();

        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> result.setSuccess(RequestResult.COMPLETED));
        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> result.setFailure(new RuntimeException()));
        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> result.cancel(false));

        runner.shutdown();
    }


    // handling submission failure
    @Test
    public void handleTaskSubmissionFailure() throws Exception
    {
        doThrow(new RejectedExecutionException()).when(compactionExecutor).execute(notNull());

        Promise<RequestResult> result = markCFSAndRunCheck();
        clearInvocations(checkExecutor);

        // so we expect that:
        assertThat(result).isDone();
        assertThat(result.get()).isEqualTo(RequestResult.COMPLETED);
        verifyState(0, 0);

        verify(checkExecutor, never()).execute(notNull());
    }


    // handling task failure
    @Test
    public void handleTaskFailure() throws Exception
    {
        Promise<RequestResult> result = markCFSAndRunCheck();
        clearInvocations(checkExecutor);

        doThrow(new IOError(new RuntimeException())).when(compactionTasks.get(0)).execute(activeOperations);
        capturedCompactionRunnables.getValue().run();

        // so we expect that:
        assertThatThrownBy(() -> result.get()).isInstanceOf(ExecutionException.class);
        verifyState(0, 0);

        // another check should be schedued upon task completion
        verifyCFSWasMarkedForCompaction();
    }


    private void verifyTaskScheduled(Executor executor)
    {
        verify(executor).execute(notNull());
        clearInvocations(executor);
    }

    private void verifyState(int ongoingCompactions, int ongoingUpgrades)
    {
        assertThat(runner.getOngoingCompactionsCount()).isEqualTo(ongoingCompactions);
        assertThat(runner.getOngoingUpgradesCount()).isEqualTo(ongoingUpgrades);
    }

    private void verifyCFSWasMarkedForCompaction()
    {
        verifyTaskScheduled(checkExecutor);
        assertThat(runner.getMarkedCFSs()).contains(cfs);
    }

    private Promise<RequestResult> markCFSAndRunCheck()
    {
        AbstractCompactionTask compactionTask = mock(AbstractCompactionTask.class);

        compactionTasks.clear();
        compactionTasks.add(compactionTask);

        Promise<RequestResult> result = runner.markForCompactionCheck(cfs);
        verifyCFSWasMarkedForCompaction();

        capturedCheckRunnables.getValue().run();
        return result;
    }
}