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

package org.apache.cassandra.db.monitoring;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.MinMaxPriorityQueue;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.jctools.queues.MpscArrayQueue;

import static org.apache.cassandra.db.monitoring.SaiSlowLog.SLOW_SAI_QUERY_COMPARATOR;

/**
 * A thread-safe class for recording the N slowest SAI queries
 * <p>
 * Allows an asynchronous, lock-free recording of new queries and
 * blocking retrieval and resize.
 * <p>
 * Internally it is backed by a {@link MinMaxPriorityQueue} ordered by
 * {@link SaiSlowLog#SLOW_SAI_QUERY_COMPARATOR} that is configured to keep
 * the N slowest queries and a buffer {@link MpscArrayQueue} for fast and
 * lock-free recording of new slow queries.
 * <p>
 * The priority queue is not synchronized. Instead, it is processed only in
 * a single thread. The thread is responsible for periodically polling the buffer
 * to update the priority queue and for executing additional operations
 * like retrieval of the slow queries and resize of the priority queue
 */
@ThreadSafe
public class SaiSlowestQueriesQueue
{
    private static final int RECORDED_QUERIES_BUFFER_CAPACITY = CassandraRelevantProperties.SLOW_SAI_QUERY_BUFFER_SIZE.getInt();
    private static final int RECORDED_QUERIES_PROCESSING_DELAY_MS = CassandraRelevantProperties.SLOW_SAI_QUERY_BUFFER_PROCESSING_DELAY_MS.getInt();
    private static final int RECORDED_QUERIES_OPERATION_HARD_DEADLINE_MINUTES = 10;

    private final MpscArrayQueue<SaiSlowLog.SlowSaiQuery> recordedQueriesBuffer;
    private Queue<SaiSlowLog.SlowSaiQuery> slowestQueries;
    private final ScheduledExecutorService queueProcessor;

    public SaiSlowestQueriesQueue(int maxSize)
    {
        this(maxSize, RECORDED_QUERIES_PROCESSING_DELAY_MS);
    }

    SaiSlowestQueriesQueue(int maxSize, int consumptionDelayMilliseconds)
    {
        recordedQueriesBuffer = new MpscArrayQueue<>(RECORDED_QUERIES_BUFFER_CAPACITY);
        slowestQueries = MinMaxPriorityQueue.orderedBy(SLOW_SAI_QUERY_COMPARATOR)
                                            .maximumSize(maxSize)
                                            .create();
        queueProcessor = Executors.newSingleThreadScheduledExecutor();
        queueProcessor.scheduleWithFixedDelay(this::processNewlyAddedSlowQueries, consumptionDelayMilliseconds, consumptionDelayMilliseconds, TimeUnit.MILLISECONDS);
    }

    /**
     * Record a new slow sai query. The query will not be instantly available for retrieval.
     */
    public void addAsync(SaiSlowLog.SlowSaiQuery slowSaiQuery)
    {
        // if the buffer is full - tough luck; we loose some slowSaiQueries
        // if that's a problem we can tweak recorded queries-related system properties
        recordedQueriesBuffer.offer(slowSaiQuery);
    }

    /**
     * Resize the priority queue. It will now hold up to newMaxSize slow queries
     * @param newMaxSize the new max size of the priority queue
     */
    void resize(int newMaxSize)
    {
        Future<?> resizeFuture = queueProcessor.submit(() -> doResize(newMaxSize));
        try
        {
            resizeFuture.get(RECORDED_QUERIES_OPERATION_HARD_DEADLINE_MINUTES, TimeUnit.MINUTES);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Extracts all slow queries from the priority queue and returns them in a List.
     * List order is unspecified
     *
     * @return list of the slowest sai queries
     */
    List<SaiSlowLog.SlowSaiQuery> getAndReset()
    {
        Future<List<SaiSlowLog.SlowSaiQuery>> slowQueries = queueProcessor.submit(this::doGetAndReset);
        try
        {
            return slowQueries.get(RECORDED_QUERIES_OPERATION_HARD_DEADLINE_MINUTES, TimeUnit.MINUTES);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e)
        {
            throw new RuntimeException(e);
        }
    }

    private void processNewlyAddedSlowQueries()
    {
        for (SaiSlowLog.SlowSaiQuery slowQuery = recordedQueriesBuffer.poll();
             slowQuery != null;
             slowQuery = recordedQueriesBuffer.poll())
        {
            slowestQueries.add(slowQuery);
        }
    }

    private List<SaiSlowLog.SlowSaiQuery> doGetAndReset()
    {
        ArrayList<SaiSlowLog.SlowSaiQuery> queriesList = new ArrayList<>(slowestQueries.size());
        queriesList.addAll(slowestQueries);
        slowestQueries.clear();
        return queriesList;
    }

    private void doResize(int newMaxSize)
    {
        Queue<SaiSlowLog.SlowSaiQuery> oldQueries = slowestQueries;
        slowestQueries = MinMaxPriorityQueue.orderedBy(SLOW_SAI_QUERY_COMPARATOR)
                                            .maximumSize(newMaxSize)
                                            .create(oldQueries);
    }

    @VisibleForTesting
    boolean bufferIsEmpty() throws ExecutionException, InterruptedException
    {
        // isEmpty is valid only in consumer thread
        return queueProcessor.submit(recordedQueriesBuffer::isEmpty).get();
    }
}
