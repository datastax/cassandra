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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.SequentialExecutorPlus;
import org.apache.cassandra.concurrent.SequentialExecutorPlus.AtLeastOnceTrigger;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.TokenMetadataProvider;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.utils.ExecutorUtils;

import static java.util.Objects.requireNonNull;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

public class PendingRangeCalculatorService
{
    public static final PendingRangeCalculatorService instance = new PendingRangeCalculatorService();

    private static final Logger logger = LoggerFactory.getLogger(PendingRangeCalculatorService.class);

    // the executor will only run a single range calculation at a time while keeping at most one task queued in order
    // to trigger an update only after the most recent state change and not for each update individually
    private final SequentialExecutorPlus executor;

    private final Schema schema;

    private final AtLeastOnceTrigger update;

    private final Set<String> keyspacesWithPendingRanges = new CopyOnWriteArraySet<>();

    private final TokenMetadataProvider tokenMetadataProvider;

    private void doUpdate()
    {
        // repeat until all keyspaced are consumed
        while (!keyspacesWithPendingRanges.isEmpty())
        {
            long start = currentTimeMillis();

            int updated = 0;
            int total = 0;
            PendingRangeCalculatorServiceDiagnostics.taskStarted(1);
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
                PendingRangeCalculatorServiceDiagnostics.taskFinished();
                if (logger.isTraceEnabled())
                    logger.trace("Finished PendingRangeTask for {} keyspaces out of {} in {}ms", updated, total, currentTimeMillis() - start);
            }
        }
    }

    public PendingRangeCalculatorService()
    {
        this("PendingRangeCalculator", Schema.instance);
    }

    public PendingRangeCalculatorService(String executorName, Schema schema)
    {
        this(executorFactory().withJmxInternal()
                              .configureSequential(executorName)
                              .withRejectedExecutionHandler((r, e) -> {})  // silently handle rejected tasks, this::update takes care of bookkeeping
                              .build(),
             TokenMetadataProvider.instance,
             schema);
    }

    public PendingRangeCalculatorService(SequentialExecutorPlus executor, TokenMetadataProvider tokenMetadataProvider, Schema schema)
    {
        this.executor = requireNonNull(executor);
        this.tokenMetadataProvider = requireNonNull(tokenMetadataProvider);
        this.schema = requireNonNull(schema);
        this.update = executor.atLeastOnceTrigger(this::doUpdate);
    }

    public void update()
    {
        update(keyspaceName -> true);
    }

    public void update(Predicate<String> keyspaceNamePredicate)
    {
        Collection<String> affectedKeyspaces = schema.distributedKeyspaces().names().stream().filter(keyspaceNamePredicate).collect(Collectors.toList());
        keyspacesWithPendingRanges.addAll(affectedKeyspaces);
        boolean success = update.trigger();
        if (!success) PendingRangeCalculatorServiceDiagnostics.taskRejected(1);
        else PendingRangeCalculatorServiceDiagnostics.taskCountChanged(1);
    }

    public void blockUntilFinished()
    {
        update.sync();
    }

    public void executeWhenFinished(Runnable runnable)
    {
        update.runAfter(runnable);
    }

    @VisibleForTesting
    protected void calculatePendingRanges(String keyspaceName)
    {
        Keyspace keyspace = Keyspace.open(keyspaceName);
        AbstractReplicationStrategy strategy = keyspace.getReplicationStrategy();
        calculatePendingRanges(strategy, keyspaceName);
    }

    @VisibleForTesting
    public void calculatePendingRanges(AbstractReplicationStrategy strategy, String keyspaceName)
    {
        tokenMetadataProvider.getTokenMetadataForKeyspace(keyspaceName).calculatePendingRanges(strategy, keyspaceName);
    }

    @VisibleForTesting
    public void shutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        ExecutorUtils.shutdownNowAndWait(timeout, unit, executor);
    }

}
