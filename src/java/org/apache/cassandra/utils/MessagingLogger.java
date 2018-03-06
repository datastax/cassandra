/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.utils;

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

    private ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
    private final EventLoop eventLoop;

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

    public MessagingLogger(EventLoop eventLoop)
    {
        this.eventLoop = eventLoop;
        service.scheduleAtFixedRate(() -> logTotals(), 5, 5, TimeUnit.SECONDS);
    }

    private void logTotals()
    {
        logger.info(String.format("eventLoop = %s, requests inflight = %d, requests completed = %d, queued = %d, dequeued = %d, allowed = %d, runs = %d, completed = %d, reschedules = %d, rescheduledLast = %s",
                                  eventLoop, inflightRequests.get(), completedRequests.get(), totalQueued.get(), totalDequeued.get(),
                                  totalAllowed.get(), totalFlusherRuns.get(), totalCompletedRuns.get(), totalReschedules.get(), rescheduledLast.get() ? "true" : "false"));
    }
}
