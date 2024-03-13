/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.db.utils.leaks.detection;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.utils.JVMStabilityInspector;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The implementation of {@link LeaksDetector}.
 * <p/>
 * It tracks instances of its resource type according to the parameters specified by the current {@link LeaksDetectionParams}.
 */
class LeaksDetectorImpl<T> implements LeaksDetector<T>
{
    private static final Logger logger = LoggerFactory.getLogger(LeaksDetectorImpl.class);
    private final static Charset CHARSET = StandardCharsets.UTF_8;

    // Use this string to report a missing stack trace that has already been evicted form the stacks cache
    private final static String STACK_TRACE_NO_LONGER_AVAILABLE = "Stack trace no longer available, increase stacks cache size for this resource using nodetool leaksdetection";

    // Exclude the low level classes defined by LeaksDetectorImpl from the stack traces since they are not useful for debugging, they just waste space
    private final static List<String> STACK_TRACE_EXCLUSIONS = Stream.concat(Stream.of(LeaksDetectorImpl.class.getDeclaredClasses()),
                                                                             Stream.of(LeaksDetectorImpl.class))
                                                                     .map(c -> c.getName())
                                                                     .collect(Collectors.toList());
    /**
     * The resource being tracked.
     */
    private final LeaksResource resource;

    /**
     * A map that keeps a strong reference to all the phantom references. For a phantom reference to be enqueued, it must
     * still be strongly reachable when the referent is finalized.
     */
    private final ConcurrentHashMap<LeaksTracker, LeakEntry> allTrackedResources = new ConcurrentHashMap<>();

    /**
     * The reference queue where the phantom references will be enqueued.
     */
    private final ReferenceQueue<? super T> refQueue;

    /**
     * The reporter is a helper class that logs a leak, it is abstracted for unit test mocks.
     */
    private final LeakReporter leakReporter;

    /**
     * Some parameters that control the operation of this leak detector, these can be changed by nodetool.
     */
    private volatile LeaksDetectionParams params;

    /**
     * A cache of call stack traces, used to avoid duplicating identical stacks and to put an upper bound on the memory
     * kept for stack traces, not so much for avoiding the computation of a stack trace, which must be done anyway.
     * <p/>
     * If nodetool changes the parameters, we may need to create a new cache and copy the contents of the old one.
     */
    private volatile Cache<Long, String> stacksCache;

    LeaksDetectorImpl(LeaksResource resource, LeaksDetectionParams params)
    {
        this(resource, params, new LeakReporter());
    }

    @VisibleForTesting
    LeaksDetectorImpl(LeaksResource resource, double samplingProbability, int numAccessRecords, LeakReporter leakReporter)
    {
        this(resource, LeaksDetectionParams.create(samplingProbability, numAccessRecords), leakReporter);
    }

    private LeaksDetectorImpl(LeaksResource resource, LeaksDetectionParams params, LeakReporter leakReporter)
    {
        this.resource = resource;
        this.params = params;
        this.refQueue  = new ReferenceQueue<>();
        this.leakReporter = leakReporter;
        this.stacksCache = createStacksCache(params.max_stacks_cache_size_mb, null);

        ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(this::checkForLeaks, 1, 1, TimeUnit.MINUTES);
    }

    private static Cache<Long, String> createStacksCache(int maxStacksCacheSizeMb, @Nullable Cache<Long, String> existing)
    {
        Cache<Long, String> ret = Caffeine.newBuilder()
                                                  .maximumWeight(maxStacksCacheSizeMb * 1024 * 1024)
                                                  .executor(Runnable::run)
                                                  .weigher((key, val) -> Long.BYTES + ((String) val).getBytes().length)
                                                  .build();
        if (existing != null)
            ret.putAll(existing.asMap());

        return ret;
    }

    @Override
    @Nullable
    public LeaksTracker<T> trackForDebug(T resForTracking)
    {
        return trackForDebug(resForTracking, null, null);
    }

    @Override
    @Nullable
    public <C> LeaksTracker<T> trackForDebug(T resForTracking,  @Nullable Cleaner<C> cleaner, @Nullable C resForCleaning)
    {
        if (ThreadLocalRandom.current().nextDouble() > params.sampling_probability)
            return null;

        return new ReportingTracker(resForTracking, cleaner, resForCleaning);
    }

    @Override
    public <C> LeaksTracker<T> trackForCleaning(T resForTracking,  Cleaner<C> cleaner, C resForCleaning)
    {
        if (cleaner == null)
            throw new IllegalArgumentException("Cleaner cannot be null, did you mean to call trackForDebug()?");

        return new CleaningTracker(resForTracking, cleaner, resForCleaning);
    }

    @Override
    public LeaksResource getResource()
    {
        return resource;
    }

    @Override
    public LeaksDetectionParams getParams()
    {
        return params;
    }

    @Override
    public synchronized LeaksDetectionParams setParams(LeaksDetectionParams params)
    {
        LeaksDetectionParams ret = this.params;
        this.params = params;

        if (ret.max_stacks_cache_size_mb != params.max_stacks_cache_size_mb)
            this.stacksCache.policy().eviction().get().setMaximum(params.max_stacks_cache_size_mb * 1024 * 1024);

        return ret;
    }

    /**
     * This is used by unit tests for checking that {@link this#setParams(LeaksDetectionParams)}.
     *
     * @return the stacks trace cache size
     */
    @VisibleForTesting
    long getStacksCacheSize()
    {
        return this.stacksCache.policy().eviction().get().getMaximum();
    }

    /**
     * Detect and report any leaks by scanning the ref queue. If a resource tracker is found and
     * this tracker was not closed, then this is a leak.
     */
    @Override
    @VisibleForTesting
    public void checkForLeaks()
    {
        CleaningTracker ref;
        while ((ref = (CleaningTracker) refQueue.poll()) != null)
        {
            // clear the reference, this is not automatically done by the GC, see
            // https://docs.oracle.com/javase/7/docs/api/java/lang/ref/PhantomReference.html
            ref.clear();

            if (!ref.isClosed())
            { // leak detected
                ref.onResourceLeaked();
                ref.close();
            }
        }
    }

    /**
     * A class for reporting leaks, the default implementation simply logs to file but for tests we
     * need to be able to mock this.
     */
    static class LeakReporter
    {
        void reportLeakedResource(LeaksResource resource, String details)
        {
            // com.datastax.bdp.test.ng.LeakDetectorInLogs intercepts this log message in DSE unit tests and only
            // logs the description, that's why we need to use String.format()
            logger.info(String.format("Debugging details for leaked resource %s:\n%s", resource, details));
        }
    }

    /**
     * Updates {@link  CleaningTracker#closed} atomically.
     */
    private static final AtomicIntegerFieldUpdater<LeaksDetectorImpl.CleaningTracker> closeUpdater =
            AtomicIntegerFieldUpdater.newUpdater(LeaksDetectorImpl.CleaningTracker.class, "closed");

    /**
     * Updates {@link  ReportingTracker#accessRecordsHead} atomically.
     */
    private static final AtomicReferenceFieldUpdater<LeaksDetectorImpl.ReportingTracker, Record> accessRecordHeadUpdater =
            AtomicReferenceFieldUpdater.newUpdater(LeaksDetectorImpl.ReportingTracker.class, Record.class, "accessRecordsHead");

    private final Record BOTTOM_ACCESS_RECORD = new Record(Hashing.md5().hashString("", CHARSET).asLong());

    /**
     * An implementation of {@link LeaksTracker} that performs some cleaning when the resource is leaked
     * if a cleaner is available.
     */
    private class CleaningTracker<C> extends PhantomReference<T> implements LeaksTracker<T>
    {
        final int trackedHash;

        @Nullable final Cleaner<C> cleaner;
        @Nullable final C resForCleaning;

        volatile int closed;

        CleaningTracker(T resForTracking,  @Nullable Cleaner<C> cleaner, @Nullable C resForCleaning)
        {
            super(resForTracking, refQueue);
            assert resForTracking != null : "Resource cannot be null";

            // Store the hash of the tracked object to later assert it in the close(...) method.
            // It's important that we not store a reference to the referent as this would disallow it from
            // be collected via the PhantomReference.
            this.trackedHash = System.identityHashCode(resForTracking);
            this.cleaner = cleaner;
            this.resForCleaning = resForCleaning;
            this.closed = 0;

            allTrackedResources.put(this, LeakEntry.INSTANCE);
        }

        @Override
        public void record()
        {
            // no op
        }

        @Override
        public void onResourceLeaked()
        {
            String autoCleaningResult;
            if (cleaner != null)
            {
                assert resForCleaning != null : "Expected non null resource for cleaning";
                try
                {
                    cleaner.clean(resForCleaning);
                    autoCleaningResult = "succeeded";
                }
                catch (Throwable t)
                {
                    JVMStabilityInspector.inspectThrowable(t);
                    autoCleaningResult = String.format("failed with exception %s/%s", t.getClass().getName(), t.getMessage());
                }
            }
            else
            {
                autoCleaningResult = "not attempted, no cleaner";
            }

            if (resource.level == LeakLevel.FIRST_LEVEL)
            {
                // com.datastax.bdp.test.ng.LeakDetectorInLogs intercepts this log message in DSE unit tests and only
                // logs the description, that's why we need to use String.format()
                logger.error(String.format("LEAK: %s was not released before it was garbage-collected. " +
                                           "Please report this even if auto-cleaning succeeded. Auto cleaning result: %s.",
                                            resource, autoCleaningResult));
            }
            else
            {
                // com.datastax.bdp.test.ng.LeakDetectorInLogs intercepts this log message in DSE unit tests and only
                // logs the description, that's why we need to use String.format()
                logger.warn(String.format("LEAK: %s was not released before it was garbage-collected. This resource " +
                                          "is a debugging aid, no negative consequences follow from this leak. " +
                                          "However, please report this nonetheless even if auto-cleaning succeeded. " +
                                          "Auto cleaning result: %s.",
                                          resource, autoCleaningResult));
            }
        }

        @Override
        public boolean close(T trackedObject)
        {
            // Ensure that the object that was tracked is the same as the one that was passed to close(...).
            assert trackedHash == System.identityHashCode(trackedObject);

            // We need to actually do the null check of the trackedObject after we close the leak because otherwise
            // we may get false-positives. This can happen as the JIT / GC may be able to figure out that we do not
            // need the trackedObject anymore and so already enqueue it for
            // collection before we actually get a chance to close the enclosing ResourceLeak.
            boolean ret = close() && trackedObject != null;
            if (!ret)
                logger.error("Close was called too late, there was a false positive LEAK for {}", trackedObject);
            return ret;
        }

        private boolean close()
        {
            if (closeUpdater.compareAndSet(this, 0, 1))
            {
                allTrackedResources.remove(this);
                return true;
            }

            return false;
        }

        @Override
        public boolean isClosed()
        {
            return closed == 1;
        }
    }

    /**
     * An implementation of {@link LeaksTracker} that in addition to perform cleaning, if a cleaner is available, it
     * also reports a creation stack trace and multiple stack traces created each time {@link this#record()} is called.
     */
    private final class ReportingTracker<C> extends CleaningTracker<C>
    {
        final long creationStackDigest;
        final String creationThreadName;
        final String resourceDescription;
        volatile Record accessRecordsHead;

        ReportingTracker(T resForTracking,  @Nullable Cleaner<C> cleaner, @Nullable C resForCleaning)
        {
            super(resForTracking, cleaner, resForCleaning);

            this.creationStackDigest = createStackTrace();
            this.creationThreadName = Thread.currentThread().getName();
            this.resourceDescription = resForTracking.toString();
            this.accessRecordsHead = params.num_access_records > 0 ? BOTTOM_ACCESS_RECORD : null;
        }

        @Override
        public void record()
        {
            if (accessRecordsHead == null || isClosed())
                return;

            record0();
        }

        @Override
        public void onResourceLeaked()
        {
            super.onResourceLeaked();
            leakReporter.reportLeakedResource(resource, getStacksTraces());
        }

        /**
         * Record the current stack trace for a resource instance. Each instance has a queue of records, where a record
         * contains the digest corresponding to a stack trace.
         * <p/>
         * If the queue has fewer than {@link LeaksDetectionParams#num_access_records} items, then the current stack trace is
         * added as a new record, assuming no race with another thread.
         * <p/>
         * In case of a race only one thread will win and get to add a record with the current stack trace.
         * <p/>
         * If the queue already has {@link LeaksDetectionParams#num_access_records}, then this method works by exponentially
         * backing off as more records are present. Each invocation has a 1 / 2^n chance of replacing the head of the
         * queue with itself.
         * <p/>
         * This has a number of convenient properties:
         *
         * <ol>
         * <li>  The last access will always recorded unless there is a race.
         * <li>  Any access before {@link LeaksDetectionParams#num_access_records} will also always be recorded.
         * <li>  It is possible to retain more records than the target, based upon the probability distribution.
         * <li>  It is easy to keep a precise record of the number of elements in the stack, since each element has to know how tall the queue is.
         * </ol>
         */
        private void record0()
        {
            Record oldHead = accessRecordHeadUpdater.get(this);
            Record prevHead = oldHead;

            int numElements = oldHead.pos + 1;
            if (numElements >= params.num_access_records)
            {
                final int backOffFactor = Math.min(numElements - params.num_access_records, 30);
                if (ThreadLocalRandom.current().nextInt(1 << backOffFactor) != 0)
                {
                    prevHead = oldHead.next;
                }
            }

            Record newHead = new Record(prevHead, createStackTrace());
            accessRecordHeadUpdater.compareAndSet(this, oldHead, newHead); // if we fail to update the stack trace due to a race, so be it
        }

        /**
         * Create a call stack trace and check if an identical stack trace exists in the cache by relying on an MD5 digest.
         * If a call stack trace with the same MD5 exists, use it. If not, insert the new one into the cache. Then return
         * the MD5 digest.
         * <p/>
         * Note that we could return a digest whose stack trace gets evicted by the cache, in which case we will
         * lose the stack trace. We're happy to accept this risk, the solution is to tell operators to make the cache
         * larger.
         * <p/>
         * An error is logged in the extremely unlikely event of an MD5 collision.
         * <p/>
         * @return the MD5 digest for the stack trace that was created
         */
        private Long createStackTrace()
        {
            String stackTrace = getStackTrace(params.max_stack_depth);

            Hasher hasher = Hashing.md5().newHasher();
            hasher.putString(stackTrace, CHARSET); // hash the string because off-heap BBs get copied into on-heap arrays
            Long hashCode = hasher.hash().asLong();

            String ret = stacksCache.get(hashCode, key -> stackTrace);

            if (!ret.equals(stackTrace))
                logger.warn("MD5 collision for stack traces with MD5 {}, stack traces with this MD5 may be inaccurate", getDigestStr(hashCode));

            return hashCode;
        }

        private String getStackTrace(int maxStackDepth)
        {
            StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
            StringBuilder buf = new StringBuilder(2048);
            int numTraces = 0;

            // skip the first one since that's the call to Thread.currentThread().getStackTrace()
            for (int i = 1; i < stackTraceElements.length; i++)
            {
                StackTraceElement element = stackTraceElements[i];

                if (STACK_TRACE_EXCLUSIONS.contains(element.getClassName()))
                    continue;

                numTraces++;
                if (numTraces >= maxStackDepth)
                    break;

                buf.append('\t').append("at ");
                buf.append(element.toString());
                buf.append('\n');
            }

            return buf.toString();
        }

        private String getDigestStr(Long digest)
        {
            return String.format("%02x", digest);
        }

        String getStacksTraces()
        {
            StringBuilder buf = new StringBuilder(4096);

            if (accessRecordsHead != null)
            {
                buf.append("Recent access records:\n");

                Record record = accessRecordsHead;
                Stack<Record> records = new Stack<>();

                while(record != null && record.pos > 0)
                {
                    records.push(record);
                    record = record.next;
                }

                while(!records.isEmpty())
                {
                    record = records.pop();
                    buf.append("#").append(record.pos).append(" (").append(getDigestStr(record.digest)).append(") ").append(":\n");
                    buf.append(getStackTrace(record.digest));
                }
            }

            buf.append("< ")
               .append(resourceDescription)
               .append(" > \n");

            buf.append("Created by thread ")
               .append(creationThreadName)
               .append(" with stack trace (")
               .append(getDigestStr(creationStackDigest))
               .append(") ")
               .append(":\n");
            buf.append(getStackTrace(creationStackDigest));

            return buf.toString();
        }

        /**
         * Get the stack trace for the MD5 digest if available, otherwise return {@link this#STACK_TRACE_NO_LONGER_AVAILABLE}.
         */
        private String getStackTrace(long digest)
        {
            String stackTrace = stacksCache.getIfPresent(digest);
            if (stackTrace != null)
                return stackTrace;

            return STACK_TRACE_NO_LONGER_AVAILABLE;
        }

        @Override
        public String toString()
        {
            return String.format("Resource id %d, num stacks: %d, closed: %s",
                                 trackedHash,
                                 accessRecordsHead == null ? 0: accessRecordsHead.pos + 1,
                                 isClosed());
        }
    }

    /**
     * A class that carries a stack trace hash code for a resource.
     */
    private final static class Record
    {
        private final long digest;
        @Nullable private final Record next;
        private final int pos;

        public Record(long digest)
        {
           this(null, digest);
        }

        Record(Record next, long digest)
        {
            this.digest = digest;
            this.next = next;
            this.pos = next == null ? 0 : next.pos + 1;
        }

        @Override
        public String toString()
        {
           return String.format("%s@pos %d", digest, pos);
        }
    }

    private static final class LeakEntry
    {
        private static final LeakEntry INSTANCE = new LeakEntry();
        private static final int HASH = System.identityHashCode(INSTANCE);

        private LeakEntry()
        {
        }

        @Override
        public int hashCode() {
            return HASH;
        }

        @Override
        public boolean equals(Object obj) {
            return obj == this;
        }
    }
}
