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

package org.apache.cassandra.concurrent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.ThreadsFactory;
import org.jctools.queues.MpscUnboundedArrayQueue;

/**
 * This class is responsible for keeping tabs on threads that register as a {@link MonitorableThread}.
 * <p/>
 * When a {@link MonitorableThread} parks itself this thread wakes it up when ready to execute work, according to the
 * {@link MonitorableThread#shouldUnpark(long)} method. This allows threads to (for example) use a non-blocking queue 
 * without constantly spinning.
 * <p/>
 * It's up to the {@link MonitorableThread#shouldUnpark(long )} implementation to track its own state.
 */
public class ParkedThreadsMonitor
{
    public static final boolean IS_ENABLED = Boolean.parseBoolean(System.getProperty("dse.thread_monitor_enabled", "true"));
    private static final Logger LOGGER = LoggerFactory.getLogger(ParkedThreadsMonitor.class);

    public static final Supplier<Optional<ParkedThreadsMonitor>> instance = Suppliers.memoize(() ->
    {
        if (IS_ENABLED)
            return Optional.of(new ParkedThreadsMonitor());
        else
        {
            LOGGER.warn("ParkedThreadsMonitor is DISABLED.");
            return Optional.empty();
        }
    });

    /*
     * NOTE: parkNanos seems to operate as follows (on my machine, expect differences):
     *  - 1 to 80ns   -> p50% is 400-500ns
     *  - 80 to 100ns -> some instability, flip between 500ns and 52us
     *  - > 100ns     -> p50% looks like 52us + parkTime
     */
    private static final long SLEEP_INTERVAL_NS = Long.getLong("dse.thread_monitor_sleep_nanos", 50000);
    private static final boolean AUTO_CALIBRATE =
      Boolean.parseBoolean(System.getProperty("dse.thread_monitor_auto_calibrate", "true")) &&
        SLEEP_INTERVAL_NS > 0;

    private static final Sleeper SLEEPER =
        AUTO_CALIBRATE ? new CalibratingSleeper(SLEEP_INTERVAL_NS) : new Sleeper();

    private final MpscUnboundedArrayQueue<Runnable> commands = new MpscUnboundedArrayQueue<>(128);
    private final ArrayList<MonitorableThread> monitoredThreads =
        new ArrayList<>(Runtime.getRuntime().availableProcessors() * 2);
    private final ArrayList<Runnable> loopActions = new ArrayList<>(4);
    private final Thread watcherThread;
        
    private volatile boolean shutdown;
    
    /**
     * The epoch monotonically tracks how many times the monitor has run: each run is a new epoch, and at each run
     * this will be passed to the {@link MonitorableThread} unpark methods, so they can use it to distinguish between
     * calls happening for the same (or different) epoch.
     */
    private long epoch;

    @VisibleForTesting
    ParkedThreadsMonitor(long startEpoch)
    {
        epoch = startEpoch;
        shutdown = false;
        watcherThread = ThreadsFactory.newDaemonThread(this::run, "ParkedThreadsMonitor");
        watcherThread.setPriority(Thread.MAX_PRIORITY);
        watcherThread.start();
    }
    
    private ParkedThreadsMonitor()
    {
        this(0);
    }

    private void run()
    {
        final ArrayList<Runnable> loopActions = this.loopActions;
        final ArrayList<MonitorableThread> monitoredThreads = this.monitoredThreads;
        final MpscUnboundedArrayQueue<Runnable> commands = this.commands;
        while (!shutdown)
        {
            try
            {
                runCommands(commands);
                executeLoopActions(loopActions);
                monitorThreads(monitoredThreads);
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                LOGGER.error("ParkedThreadsMonitor exception: ", t);
            }
            finally
            {
                epoch++;
            }
            SLEEPER.sleep();
        }
        unparkOnShutdown(monitoredThreads);
    }

    private void monitorThreads(ArrayList<MonitorableThread> monitoredThreads)
    {
        if (monitoredThreads.isEmpty())
            return;
        
        // To avoid biasing the work threads do based on their order of unparking, we start iterating from an offset
        // based on the epoch, so it's cheap and always changing.
        int offset = ((int) epoch & Integer.MAX_VALUE) % monitoredThreads.size();
        for (int i = 0; i < monitoredThreads.size(); i++)
        {
            MonitorableThread thread = monitoredThreads.get((i + offset) % monitoredThreads.size());
            try
            {
                if (thread.shouldUnpark(epoch))
                {
                    // TODO: add a counter we can track for unpark rate
                    thread.unpark(epoch);
                }
            }
            catch (Throwable t)
            {
                LOGGER.error("Exception unparking a monitored thread", t);
            }
        }
    }

    private void executeLoopActions(ArrayList<Runnable> loopActions)
    {
        for (int i = 0; i < loopActions.size(); i++)
        {
            Runnable action = loopActions.get(i);
            try
            {
                action.run();
            }
            catch (Throwable t)
            {
                LOGGER.error("Exception running an action", t);
            }
        }
    }

    private void runCommands(MpscUnboundedArrayQueue<Runnable> commands)
    {
        // queue is almost always empty, so pre check makes sense
        if (!commands.isEmpty())
        {
            commands.drain(Runnable::run);
        }
    }

    private void unparkOnShutdown(ArrayList<MonitorableThread> monitoredThreads)
    {
        for (MonitorableThread thread : monitoredThreads)
            thread.unpark(epoch);
    }

    /**
     * Adds a collection of threads to monitor
     */
    public void addThreadsToMonitor(Collection<MonitorableThread> threads)
    {
        threads.forEach(this::addThreadToMonitor);
    }

    /**
     * Adds a thread to monitor
     */
    public void addThreadToMonitor(MonitorableThread thread)
    {
        commands.offer(() -> monitoredThreads.add(thread));
    }

    /**
     * Removes a thread from monitoring
     */
    public void removeThreadToMonitor(MonitorableThread thread)
    {
        commands.offer(() -> monitoredThreads.remove(thread));
    }

    /**
     * Removes a collection of threads from monitoring
     */
    public void removeThreadsToMonitor(Collection<MonitorableThread> threads)
    {
        threads.forEach(this::removeThreadToMonitor);
    }

    /**
     * Runs the specified action in each loop. Mainly used to avoid many threads doing the same work
     * over and over.
     */
    public void addAction(Runnable action)
    {
        commands.offer(() -> loopActions.add(action));
    }

    public void shutdown()
    {
        shutdown = true;
    }

    public boolean awaitTermination(long timeout, TimeUnit timeUnit) throws InterruptedException
    {
        shutdown();
        watcherThread.join(timeUnit.toMillis(timeout));
        return !watcherThread.isAlive();
    }

    /**
     * Interface for threads wishing to be watched by the {@link ParkedThreadsMonitor} in order to be unparked.
     * <p/>
     * When a Thread has no work to do it should park itself.
     */
    public static interface MonitorableThread
    {
        /**
         * What a MonitorableThread should use to track it's current state
         */
        enum ThreadState
        {
            PARKED, // no work to do
            UNPARKING, // unparked but not yet working
            WORKING
        }

        /**
         * Will unpark a {@link ThreadState#PARKED} thread.
         * <br/>
         * Called by {@link ParkedThreadsMonitor} AFTER {@link #shouldUnpark(long )} returns true.
         * 
         * @@param epoch A monotonic counter increased every time the {@link ParkedThreadsMonitor} runs, 
         * see {@link ParkedThreadsMonitor#epoch}.
         */
        void unpark(long epoch);

        /**
         * Called continuously by the {@link ParkedThreadsMonitor} to decide if unpark() should be called on a
         * thread.
         * <br/>
         * Should return true IFF the thread is parked and there (potentially) is work to be done when the thread is
         * unparked.
         * 
         * @param epoch A monotonic counter increased every time the {@link ParkedThreadsMonitor} runs, 
         * see {@link ParkedThreadsMonitor#epoch}.
         */
        boolean shouldUnpark(long epoch);
    }

    static class Sleeper
    {
        void sleep()
        {
            if (SLEEP_INTERVAL_NS > 0)
                LockSupport.parkNanos(SLEEP_INTERVAL_NS);
        }
    }

    @VisibleForTesting
    public static class CalibratingSleeper extends Sleeper
    {
        final long targetNs;
        long calibratedSleepNs;
        long expSmoothedSleepTimeNs;
        int comparisonDelay;

        @VisibleForTesting
        public CalibratingSleeper(long targetNs)
        {
            this.targetNs = targetNs;
            this.calibratedSleepNs = targetNs;
            this.expSmoothedSleepTimeNs = 0;
        }

        @VisibleForTesting
        public void sleep()
        {
            long start = nanoTime();
            park();
            long sleptNs = nanoTime() - start;
            if (expSmoothedSleepTimeNs == 0)
                expSmoothedSleepTimeNs = sleptNs;
            else
                expSmoothedSleepTimeNs = (long) (0.001 * sleptNs + 0.999 * expSmoothedSleepTimeNs);

            // calibrate sleep time if we miss target by more than 10%, wait for at least 100 observations
            if (comparisonDelay < 100)
            {
                comparisonDelay++;
            }
            else if (expSmoothedSleepTimeNs > targetNs * 1.1)
            {
                calibratedSleepNs = (long) (calibratedSleepNs * 0.9) + 1;
                expSmoothedSleepTimeNs = 0;
                comparisonDelay = 0;
            }
            else if (expSmoothedSleepTimeNs < targetNs * 0.9)
            {
                calibratedSleepNs = (long) (calibratedSleepNs * 1.1);
                expSmoothedSleepTimeNs = 0;
                comparisonDelay = 0;
            }
        }

        @VisibleForTesting
        void park()
        {
            LockSupport.parkNanos(calibratedSleepNs);
        }

        @VisibleForTesting
        long nanoTime()
        {
            return MonotonicClock.preciseTime.now();
        }
    }
}
