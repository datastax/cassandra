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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.ThreadPoolMetrics;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Future;

import static java.util.stream.Collectors.toMap;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_STAGE_EXECUTOR_FACTORY_PROPERTY;
import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_TASK_EXECUTION_CALLBACK_CLASS;

public enum Stage
{
    READ              (false, "ReadStage",             "request",  DatabaseDescriptor::getConcurrentReaders,        DatabaseDescriptor::setConcurrentReaders,        maybeCustomStageExecutor(Stage::multiThreadedLowSignalStage)),
    MUTATION          (true,  "MutationStage",         "request",  DatabaseDescriptor::getConcurrentWriters,        DatabaseDescriptor::setConcurrentWriters,        maybeCustomStageExecutor(Stage::multiThreadedLowSignalStage)),
    COORDINATE_READ   (false, "CoordReadStage",        "request",  DatabaseDescriptor::getConcurrentCoordinatorReaders,        DatabaseDescriptor::setConcurrentCoordinatorReaders,        maybeCustomStageExecutor(Stage::multiThreadedLowSignalStage)),
    COORDINATE_MUTATION(true, "CoordMutationStage",    "request",  DatabaseDescriptor::getConcurrentCoordinatorWriters,        DatabaseDescriptor::setConcurrentCoordinatorWriters,        maybeCustomStageExecutor(Stage::multiThreadedLowSignalStage)),
    COUNTER_MUTATION  (true,  "CounterMutationStage",  "request",  DatabaseDescriptor::getConcurrentCounterWriters, DatabaseDescriptor::setConcurrentCounterWriters, maybeCustomStageExecutor(Stage::multiThreadedLowSignalStage)),
    VIEW_MUTATION     (true,  "ViewMutationStage",     "request",  DatabaseDescriptor::getConcurrentViewWriters,    DatabaseDescriptor::setConcurrentViewWriters,    maybeCustomStageExecutor(Stage::multiThreadedLowSignalStage)),
    GOSSIP            (true,  "GossipStage",           "internal", () -> 1,                                         null,                                            Stage::singleThreadedStage),
    REQUEST_RESPONSE  (false, "RequestResponseStage",  "request",  FBUtilities::getAvailableProcessors,             null,                                            maybeCustomStageExecutor(Stage::multiThreadedLowSignalStage)),
    ANTI_ENTROPY      (false, "AntiEntropyStage",      "internal", () -> 1,                                         null,                                            Stage::singleThreadedStage),
    MIGRATION         (false, "MigrationStage",        "internal", () -> 1,                                         null,                                            Stage::migrationStage),
    MISC              (false, "MiscStage",             "internal", () -> 1,                                         null,                                            Stage::singleThreadedStage),
    TRACING           (false, "TracingStage",          "internal", () -> 1,                                         null,                                            Stage::tracingStage),
    INTERNAL_RESPONSE (false, "InternalResponseStage", "internal", FBUtilities::getAvailableProcessors,             null,                                            Stage::multiThreadedStage),
    IMMEDIATE         (false, "ImmediateStage",        "internal", () -> 0,                                         null,                                            Stage::immediateExecutor),
    PAXOS_REPAIR      (false, "PaxosRepairStage",      "internal", FBUtilities::getAvailableProcessors,             null,                                            Stage::multiThreadedStage),
    IO                (false, "Internal IO Stage",     "internal", FBUtilities::getAvailableProcessors,             null,                                            Stage::multiThreadedStage),
    NATIVE_TRANSPORT_REQUESTS  (false, "Native-Transport-Requests","transport", DatabaseDescriptor::getNativeTransportMaxThreads, DatabaseDescriptor::setNativeTransportMaxThreads, maybeCustomStageExecutor(Stage::multiThreadedLowSignalStage));

    public final String jmxName;
    private final Supplier<ExecutorPlus> executorSupplier;
    private volatile ExecutorPlus executor;
    /** Set true if this executor should be gracefully shutdown before stopping
     * the commitlog allocator. Tasks on executors that issue mutations may
     * block indefinitely waiting for a new commitlog segment, preventing a
     * clean drain/shutdown.
     */
    public final boolean shutdownBeforeCommitlog;

    Stage(boolean shutdownBeforeCommitlog, String jmxName, String jmxType, IntSupplier numThreads, LocalAwareExecutorPlus.MaximumPoolSizeListener onSetMaximumPoolSize, ExecutorServiceInitialiser executorSupplier)
    {
        this.shutdownBeforeCommitlog = shutdownBeforeCommitlog;
        this.jmxName = jmxName;
        this.executorSupplier = () -> executorSupplier.init(jmxName, jmxType, numThreads.getAsInt(), onSetMaximumPoolSize);
    }

    private static String normalizeName(String stageName)
    {
        // Handle discrepancy between JMX names and actual pool names
        String upperStageName = stageName.toUpperCase();
        if (upperStageName.endsWith("STAGE"))
        {
            upperStageName = upperStageName.substring(0, stageName.length() - 5);
        }
        return upperStageName;
    }

    private static final Map<String,Stage> nameMap = Arrays.stream(values())
                                                           .collect(toMap(s -> Stage.normalizeName(s.jmxName),
                                                                          s -> s));

    /**
     * Set of Stage names that may be used with {@link #fromPoolName(String)}.
     * Used by CNDB.
     */
    public static Set<String> poolNames()
    {
        return nameMap.keySet();
    }

    public static Stage fromPoolName(String stageName)
    {
        String upperStageName = normalizeName(stageName);

        Stage result = nameMap.get(upperStageName);
        if (result != null)
            return result;

        try
        {
            return valueOf(upperStageName);
        }
        catch (IllegalArgumentException e)
        {
            switch(upperStageName) // Handle discrepancy between configuration file and stage names
            {
                case "CONCURRENT_READS":
                    return READ;
                case "CONCURRENT_WRITERS":
                    return MUTATION;
                case "CONCURRENT_COUNTER_WRITES":
                    return COUNTER_MUTATION;
                case "CONCURRENT_MATERIALIZED_VIEW_WRITES":
                    return VIEW_MUTATION;
                default:
                    throw new IllegalStateException("Must be one of " + Arrays.stream(values())
                                                                              .map(Enum::toString)
                                                                              .collect(Collectors.joining(",")));
            }
        }
    }

    public static Optional<Stage> fromStatement(CQLStatement statement)
    {
        if (CassandraRelevantProperties.NATIVE_TRANSPORT_ASYNC_READ_WRITE_ENABLED.getBoolean())
        {
            if (statement instanceof SelectStatement)
            {
                return Optional.of(Stage.COORDINATE_READ);
            }
            else if (statement instanceof ModificationStatement || statement instanceof BatchStatement)
            {
                return Optional.of(Stage.COORDINATE_MUTATION);
            }
        }
        return Optional.empty();
    }

    // Convenience functions to execute on this stage
    public void execute(Runnable command)
    {
        if (!CUSTOM_TASK_EXECUTION_CALLBACK_CLASS.isPresent())
        {
            executor.execute(command);
            return;
        }
        long enqueueStartTime = Clock.Global.nanoTime();
        executor().execute(withTimeMeasurement(command, enqueueStartTime));
    }

    public void execute(Runnable command, ExecutorLocals locals)
    {
        if (!CUSTOM_TASK_EXECUTION_CALLBACK_CLASS.isPresent())
        {
            executor.execute(locals, command);
            return;
        }
        long enqueueStartTime = Clock.Global.nanoTime();
        executor().execute(locals, withTimeMeasurement(command, enqueueStartTime));
    }

    public void maybeExecuteImmediately(Runnable command)
    {
        if (!CUSTOM_TASK_EXECUTION_CALLBACK_CLASS.isPresent())
        {
            executor.execute(command);
            return;
        }
        long enqueueStartTime = Clock.Global.nanoTime();
        executor().maybeExecuteImmediately(withTimeMeasurement(command, enqueueStartTime));
    }

    public <T> Future<T> submit(Callable<T> task)
    {
        long enqueueStartTime = Clock.Global.nanoTime();
        return executor().submit(() -> {
            try
            {
                if (!CUSTOM_TASK_EXECUTION_CALLBACK_CLASS.isPresent())
                    return task.call();
                return withTimeMeasurement(task, enqueueStartTime).call();
            }
            catch (Exception e)
            {
                throw Throwables.unchecked(e);
            }
        });
    }

    public Future<?> submit(Runnable task)
    {
        if (!CUSTOM_TASK_EXECUTION_CALLBACK_CLASS.isPresent())
           return executor().submit(task, executor());
        long enqueueStartTime = Clock.Global.nanoTime();
        return executor().submit(withTimeMeasurement(task, enqueueStartTime), executor());
    }

    public <T> Future<T> submit(Runnable task, T result)
    {

        if (!CUSTOM_TASK_EXECUTION_CALLBACK_CLASS.isPresent())
        {
            return executor().submit(() -> {
                task.run();
                return result;
            });
        }
        long enqueueStartTime = Clock.Global.nanoTime();
        return executor().submit(() -> {
            withTimeMeasurement(task, enqueueStartTime).run();
            return result;
        });
    }

    public ExecutorPlus executor()
    {
        if (executor == null)
        {
            synchronized (this)
            {
                if (executor == null)
                {
                    executor = executorSupplier.get();
                }
            }
        }
        return executor;
    }

    @VisibleForTesting
    public void unsafeSetExecutor(ExecutorPlus executor)
    {
        this.executor = executor;
    }

    private static List<ExecutorPlus> executors()
    {
        return Stream.of(Stage.values())
                     .map(Stage::executor)
                     .collect(Collectors.toList());
    }

    private static List<ExecutorPlus> mutatingExecutors()
    {
        return Stream.of(Stage.values())
                     .filter(stage -> stage.shutdownBeforeCommitlog)
                     .map(Stage::executor)
                     .collect(Collectors.toList());
    }

    /**
     * This method shuts down all registered stages.
     */
    public static void shutdownNow()
    {
        ExecutorUtils.shutdownNow(executors());
    }

    public static void shutdownAndAwaitMutatingExecutors(boolean interrupt, long timeout, TimeUnit units) throws InterruptedException, TimeoutException
    {
        List<ExecutorPlus> executors = mutatingExecutors();
        ExecutorUtils.shutdown(interrupt, executors);
        ExecutorUtils.awaitTermination(timeout, units, executors);
    }

    public static boolean areMutationExecutorsTerminated()
    {
        return mutatingExecutors().stream().allMatch(ExecutorPlus::isTerminated);
    }

    @VisibleForTesting
    public static void shutdownAndWait(long timeout, TimeUnit units) throws InterruptedException, TimeoutException
    {
        List<ExecutorPlus> executors = executors();
        ExecutorUtils.shutdownNow(executors);
        ExecutorUtils.awaitTermination(timeout, units, executors);
    }

    private static ExecutorPlus tracingStage(String jmxName, String jmxType, int numThreads, LocalAwareExecutorPlus.MaximumPoolSizeListener onSetMaximumPoolSize)
    {
        return executorFactory()
                .withJmx(jmxType)
                .configureSequential(jmxName)
                .withQueueLimit(1000)
                .withRejectedExecutionHandler((r, executor) -> MessagingService.instance().metrics.recordSelfDroppedMessage(Verb._TRACE)).build();
    }

    private static ExecutorPlus migrationStage(String jmxName, String jmxType, int numThreads, LocalAwareExecutorPlus.MaximumPoolSizeListener onSetMaximumPoolSize)
    {
        return executorFactory()
               .localAware()
               .withJmx(jmxType)
               .sequential(jmxName);
    }

    private static ExecutorServiceInitialiser maybeCustomStageExecutor(ExecutorServiceInitialiser fallbackInitializer)
    {
        return CUSTOM_STAGE_EXECUTOR_FACTORY_PROPERTY.isPresent() ? Stage::customExecutor : fallbackInitializer;
    }

    private static LocalAwareExecutorPlus singleThreadedStage(String jmxName, String jmxType, int numThreads, LocalAwareExecutorPlus.MaximumPoolSizeListener onSetMaximumPoolSize)
    {
        return executorFactory()
                .localAware()
                .withJmx(jmxType)
                .sequential(jmxName);
    }

    static LocalAwareExecutorPlus multiThreadedStage(String jmxName, String jmxType, int numThreads, LocalAwareExecutorPlus.MaximumPoolSizeListener onSetMaximumPoolSize)
    {
        return executorFactory()
                .localAware()
                .withJmx(jmxType)
                .pooled(jmxName, numThreads);
    }

    static LocalAwareExecutorPlus multiThreadedLowSignalStage(String jmxName, String jmxType, int numThreads, LocalAwareExecutorPlus.MaximumPoolSizeListener onSetMaximumPoolSize)
    {
        return executorFactory()
                .localAware()
                .withJmx(jmxType)
                .shared(jmxName, numThreads, onSetMaximumPoolSize);
    }

    static LocalAwareExecutorPlus immediateExecutor(String jmxName, String jmxType, int numThreads, LocalAwareExecutorPlus.MaximumPoolSizeListener onSetMaximumPoolSize)
    {
        return ImmediateExecutor.INSTANCE;
    }

    static LocalAwareExecutorPlus customExecutor(String jmxName, String jmxType, int numThreads, LocalAwareExecutorPlus.MaximumPoolSizeListener onSetMaximumPoolSize)
    {
        StageExecutorFactory customStageExecutorFactory = FBUtilities.construct(CUSTOM_STAGE_EXECUTOR_FACTORY_PROPERTY.getString(), "Custom stage executor factory");
        return customStageExecutorFactory.init(jmxName, jmxType, numThreads, onSetMaximumPoolSize);
    }

    public int getPendingTaskCount()
    {
        return executor().getPendingTaskCount();
    }

    public int getActiveTaskCount()
    {
        return executor().getActiveTaskCount();
    }

    public long getCompletedTaskCount()
    {
        return executor().getCompletedTaskCount();
    }

    /**
     * return additional, executor-related ThreadPoolMetrics for the executor
     * @param metricsExtractor function to extract metrics from the executor
     * @return ThreadPoolMetrics or null if the extractor was not able to extract metrics
     */
    public @Nullable ThreadPoolMetrics getExecutorMetrics(Function<Executor, ThreadPoolMetrics> metricsExtractor)
    {
        return metricsExtractor.apply(executor());
    }

    public boolean isShutdown()
    {
        return executor().isShutdown();
    }

    public boolean isTerminated()
    {
        return executor().isTerminated();
    }

    @FunctionalInterface
    public interface ExecutorServiceInitialiser
    {
        public ExecutorPlus init(String jmxName, String jmxType, int numThreads, LocalAwareExecutorPlus.MaximumPoolSizeListener onSetMaximumPoolSize);
    }

    /**
     * Returns core thread pool size
     */
    public int getCorePoolSize()
    {
        return executor().getCorePoolSize();
    }

    /**
     * Allows user to resize core thread pool size
     */
    public void setCorePoolSize(int newCorePoolSize)
    {
        executor().setCorePoolSize(newCorePoolSize);
    }

    /**
     * Returns maximum pool size of thread pool.
     */
    public int getMaximumPoolSize()
    {
        return executor().getMaximumPoolSize();
    }

    /**
     * Allows user to resize maximum size of the thread pool.
     */
    public void setMaximumPoolSize(int newMaximumPoolSize)
    {
        executor().setMaximumPoolSize(newMaximumPoolSize);
    }

    private Runnable withTimeMeasurement(Runnable command, long queueStartTime)
    {
        return () -> {
            long executionStartTime = Clock.Global.nanoTime();
            try
            {
                TaskExecutionCallback.instance.onDequeue(this, executionStartTime - queueStartTime);
                command.run();
            }
            finally
            {
                TaskExecutionCallback.instance.onCompleted(this, Clock.Global.nanoTime() - executionStartTime);
            }
        };
    }

    private <T> Callable<T> withTimeMeasurement(Callable<T> command, long queueStartTime)
    {
        return () -> {
            long executionStartTime = Clock.Global.nanoTime();
            try
            {
                TaskExecutionCallback.instance.onDequeue(this, executionStartTime - queueStartTime);
                return command.call();
            }
            finally
            {
                TaskExecutionCallback.instance.onCompleted(this, Clock.Global.nanoTime() - executionStartTime);
            }
        };
    }
}
