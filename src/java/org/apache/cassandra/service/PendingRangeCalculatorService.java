/**
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

package org.apache.cassandra.service;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.Throwables;

import static java.util.Objects.requireNonNull;

public class PendingRangeCalculatorService
{
    public static final PendingRangeCalculatorService instance = new PendingRangeCalculatorService();

    private static final Logger logger = LoggerFactory.getLogger(PendingRangeCalculatorService.class);

    private final ThreadPoolExecutor executor;

    private final Schema schema;

    private final Set<String> keyspacesWithPendingRanges = new CopyOnWriteArraySet<>();

    private final Function<String, TokenMetadata> tokenMetadataProvider;

    private final AtomicInteger updateJobs = new AtomicInteger(0);

    public PendingRangeCalculatorService()
    {
        this("PendingRangeCalculator", Schema.instance);
    }

    @VisibleForTesting
    public PendingRangeCalculatorService(String executorName, Schema schema)
    {
        // the executor will only run a single range calculation at a time while keeping at most one task queued in order
        // to trigger an update only after the most recent state change and not for each update individually
        //noinspection Convert2MethodRef
        this(new JMXEnabledThreadPoolExecutor(1, Integer.MAX_VALUE, TimeUnit.SECONDS,
                                              new LinkedBlockingQueue<>(1), new NamedThreadFactory(executorName), "internal"),
             keyspaceName -> StorageService.instance.getTokenMetadataForKeyspace(keyspaceName),
             schema);
    }

    @VisibleForTesting
    PendingRangeCalculatorService(ThreadPoolExecutor executor, Function<String, TokenMetadata> tokenMetadataProvider, Schema schema)
    {
        this.executor = requireNonNull(executor);
        this.tokenMetadataProvider = requireNonNull(tokenMetadataProvider);
        this.executor.setRejectedExecutionHandler((r, e) ->
                                                  {
                                                      PendingRangeCalculatorServiceDiagnostics.taskRejected(this, updateJobs);
                                                      finishUpdate();
                                                  });
        this.schema = requireNonNull(schema);
    }

    private class PendingRangeTask implements Runnable
    {
        public void run()
        {
            try
            {
                PendingRangeCalculatorServiceDiagnostics.taskStarted(PendingRangeCalculatorService.this, updateJobs);

                // repeat until all keyspaced are consumed
                while (!keyspacesWithPendingRanges.isEmpty())
                {
                    long start = System.currentTimeMillis();

                    int updated = 0;
                    int total = 0;
                    try
                    {
                        Set<String> keyspaces = new HashSet<>(keyspacesWithPendingRanges);
                        total = keyspaces.size();
                        keyspacesWithPendingRanges.removeAll(keyspaces); // only remove those which were consumed

                        Iterator<String> it = keyspaces.iterator();
                        while (it.hasNext())
                        {
                            String keyspaceName = it.next();
                            try
                            {
                                calculatePendingRanges(keyspaceName);
                                it.remove();
                                updated++;
                            }
                            catch (RuntimeException | Error ex)
                            {
                                logger.error("Error calculating pending ranges for keyspace {}", keyspaceName, ex);
                            }
                        }
                    }
                    finally
                    {
                        if (logger.isTraceEnabled())
                            logger.trace("Finished PendingRangeTask for {} keyspaces out of {} in {}ms", updated, total, System.currentTimeMillis() - start);
                    }
                }
            }
            finally
            {
                PendingRangeCalculatorServiceDiagnostics.taskFinished(PendingRangeCalculatorService.this, updateJobs);
                PendingRangeCalculatorService.this.finishUpdate();
            }
        }
    }

    private void finishUpdate()
    {
        int jobs = updateJobs.decrementAndGet();
        PendingRangeCalculatorServiceDiagnostics.taskCountChanged(this, jobs);
    }

    public void update()
    {
        update(t -> true);
    }

    public void update(Predicate<String> filter)
    {
        int jobs = updateJobs.incrementAndGet();
        PendingRangeCalculatorServiceDiagnostics.taskCountChanged(this, jobs);

        Collection<String> keyspaces = schema.getNonLocalStrategyKeyspaces().filter(ksm -> filter.test(ksm.name)).names();
        keyspacesWithPendingRanges.addAll(keyspaces);
        executor.execute(new PendingRangeTask());
    }

    public void blockUntilFinished()
    {
        while (updateJobs.get() > 0)
        {
            try
            {
                Thread.sleep(100);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public void calculatePendingRanges(String keyspaceName)
    {
        calculatePendingRanges(Keyspace.open(keyspaceName).getReplicationStrategy(), keyspaceName);
    }

    public void calculatePendingRanges(AbstractReplicationStrategy strategy, String keyspaceName)
    {
        tokenMetadataProvider.apply(keyspaceName).calculatePendingRanges(strategy, keyspaceName);
    }

    @VisibleForTesting
    public void shutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        ExecutorUtils.shutdownNowAndWait(timeout, unit, executor);
    }
}
