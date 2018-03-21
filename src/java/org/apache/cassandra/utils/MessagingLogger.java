/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.utils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.EventLoop;
import org.apache.cassandra.utils.NoSpamLogger;

public class MessagingLogger
{
    private static final Logger logger = LoggerFactory.getLogger(MessagingLogger.class);

    private static ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
//    private final EventLoop eventLoop;

    public AtomicInteger currentBlocked = new AtomicInteger();
    public AtomicInteger totalQueued = new AtomicInteger();
    public AtomicInteger totalFlushed = new AtomicInteger();
    public AtomicInteger totalChannels = new AtomicInteger();
    public AtomicInteger totalFlusherRuns = new AtomicInteger();
    public AtomicInteger totalCompletedRuns = new AtomicInteger();
    public AtomicInteger totalFlusherStarts = new AtomicInteger();
    public AtomicInteger totalAllowed = new AtomicInteger();
    public AtomicInteger totalReschedules = new AtomicInteger();
    public AtomicInteger totalLoopBreaks = new AtomicInteger();
    public AtomicInteger totalCompletedFlushes = new AtomicInteger();
    public AtomicInteger totalDequeued = new AtomicInteger();
    public AtomicBoolean rescheduledLast = new AtomicBoolean(false);
    public AtomicInteger inflightRequests = new AtomicInteger();
    public AtomicInteger completedRequests = new AtomicInteger();

    static
    {
        service.scheduleAtFixedRate(() -> logTotals(), 5, 5, TimeUnit.SECONDS);

    }

    private static Map<EventLoop, FlushStatistics> flushStatisticsMap = new ConcurrentHashMap<>();

    public static FlushStatistics getFlushStatistics(EventLoop eventLoop)
    {
        if (!flushStatisticsMap.containsKey(eventLoop))
        {
            flushStatisticsMap.put(eventLoop, new FlushStatistics());
        }
        return flushStatisticsMap.get(eventLoop);
    }

    private static void logTotals()
    {
        StringBuilder builder = new StringBuilder();

        builder.append("\n");

        for (EventLoop eventLoop : flushStatisticsMap.keySet())
        {
            FlushStatistics stats = flushStatisticsMap.get(eventLoop);

            builder.append(String.format("eventLoop = %d, inflight = %d, completed = %d, queued = %d, dequeued = %d, allowed = %d, runs = %d, completed = %d, reschedules = %d, rescheduledLast = %s\n",
                                         eventLoop.hashCode(), stats.inflightRequests.get(), stats.completedRequests.get(), stats.totalQueued.get(), stats.totalDequeued.get(),
                                         stats.totalAllowed.get(), stats.totalFlusherRuns.get(), stats.totalCompletedRuns.get(), stats.totalReschedules.get(),
                                         stats.rescheduledLast.get() ? "true" : "false"
                                         ));

        }

        logger.info(builder.toString());
    }

    public static class FlushStatistics
    {
        public AtomicInteger totalQueued = new AtomicInteger();
        public AtomicInteger totalFlushed = new AtomicInteger();
        public AtomicInteger totalChannels = new AtomicInteger();
        public AtomicInteger totalFlusherRuns = new AtomicInteger();
        public AtomicInteger totalCompletedRuns = new AtomicInteger();
        public AtomicInteger totalFlusherStarts = new AtomicInteger();
        public AtomicInteger totalAllowed = new AtomicInteger();
        public AtomicInteger totalReschedules = new AtomicInteger();
        public AtomicInteger totalLoopBreaks = new AtomicInteger();
        public AtomicInteger totalCompletedFlushes = new AtomicInteger();
        public AtomicInteger totalDequeued = new AtomicInteger();
        public AtomicBoolean rescheduledLast = new AtomicBoolean(false);
        public AtomicInteger inflightRequests = new AtomicInteger();
        public AtomicInteger completedRequests = new AtomicInteger();
    }
}
