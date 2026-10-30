/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.utils.concurrent;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.UnaccessibleFieldException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import jdk.internal.ref.Cleaner;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.io.util.SafeMemory;
import org.apache.cassandra.utils.ExecutorUtils;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Shared;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import static java.util.Collections.emptyList;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.SimulatorSafe.UNSAFE;
import static org.apache.cassandra.config.CassandraRelevantProperties.DEBUG_REF_COUNT_COPY_SAMPLE_INTERVAL;
import static org.apache.cassandra.config.CassandraRelevantProperties.DEBUG_REF_COUNT_PRIMARY_SAMPLE_INTERVAL;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_DEBUG_REF_COUNT;
import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;
import static org.apache.cassandra.utils.Throwables.maybeFail;
import static org.apache.cassandra.utils.Throwables.merge;

/**
 * An object that needs ref counting does the two following:
 *   - defines a Tidy object that will cleanup once it's gone,
 *     (this must retain no references to the object we're tracking (only its resources and how to clean up))
 * Then, one of two options:
 * 1) Construct a Ref directly pointing to it, and always use this Ref; or
 * 2)
 *   - implements RefCounted
 *   - encapsulates a Ref, we'll call selfRef, to which it proxies all calls to RefCounted behaviours
 *   - users must ensure no references to the selfRef leak, or are retained outside of a method scope.
 *     (to ensure the selfRef is collected with the object, so that leaks may be detected and corrected)
 * <p>
 * This class' functionality is achieved by what may look at first glance like a complex web of references,
 * but boils down to:
 * <p>
 * {@code
 * Target --> selfRef --> [Ref.State] <--> Ref.GlobalState --> Tidy
 *                                             ^
 *                                             |
 * Ref ----------------------------------------
 *                                             |
 * Global -------------------------------------
 * }
 * So that, if Target is collected, Impl is collected and, hence, so is selfRef.
 * <p>
 * Once ref or selfRef are collected, the paired Ref.State's release method is called, which if it had
 * not already been called will update Ref.GlobalState and log an error.
 * <p>
 * Once the Ref.GlobalState has been completely released, the Tidy method is called and it removes the global reference
 * to itself so it may also be collected.
 */
public final class Ref<T> implements RefCounted<T>
{
    static final Logger logger = LoggerFactory.getLogger(Ref.class);
    public static final boolean DEBUG_ENABLED = TEST_DEBUG_REF_COUNT.getBoolean();
    /**
     * Without {@link #DEBUG_ENABLED}, one reference in this many, picked at random, gets a {@link Debug} record;
     * 0 means none. {@code PRIMARY_SAMPLE_INTERVAL} applies to the references {@link #Ref(Object, Tidy)} makes, and
     * {@code COPY_SAMPLE_INTERVAL} to their copies, made by {@link #ref()} and {@link #tryRef()}. Both are 0 with
     * {@link #DEBUG_ENABLED}, which records every reference. Sampling never starts the {@link Visitor} or the
     * {@link StrongLeakDetector}.
     */
    static final int PRIMARY_SAMPLE_INTERVAL = sampleInterval(DEBUG_REF_COUNT_PRIMARY_SAMPLE_INTERVAL);
    static final int COPY_SAMPLE_INTERVAL = sampleInterval(DEBUG_REF_COUNT_COPY_SAMPLE_INTERVAL);
    private static final String PRIMARY_NO_DEBUG_HINT = noDebugHint(true, PRIMARY_SAMPLE_INTERVAL);
    private static final String NO_DEBUG_HINT = noDebugHint(false, COPY_SAMPLE_INTERVAL);
    static OnLeak ON_LEAK;

    @Shared(scope = SIMULATION)
    public interface OnLeak
    {
        void onLeak(Object state);
    }

    final State state;
    final T referent;

    public Ref(T referent, Tidy tidy)
    {
        this.state = new PrimaryState(new GlobalState(tidy), this, referenceQueue);
        this.referent = referent;
    }

    Ref(T referent, GlobalState state)
    {
        this.state = new State(state, this, referenceQueue);
        this.referent = referent;
    }

    /**
     * Must be called exactly once, when the logical operation for which this Ref was created has terminated.
     * Failure to abide by this contract will result in an error (eventually) being reported, assuming a
     * hard reference to the resource it managed is not leaked.
     */
    public void release()
    {
        state.release(false);
        // We require a reachability fence here to prevent the JIT from clearing the Ref reference from the stack
        // during the state.release() call. If that happens, the ref may become phantom reachable, and the GC may clear
        // the state as a phantom reference to the ref and then enqueue the state on the phantom queue. This allows
        // the Reference-Reaper to race with our state.release call above, and the reference-reaper may be able to
        // update the released flag before the non-leak release path. In this case, we report a spurious leak and a
        // spurious bad release.
        Reference.reachabilityFence(this);
    }

    public Throwable ensureReleased(Throwable accumulate)
    {
        return state.ensureReleased(accumulate);
    }

    public void ensureReleased()
    {
        maybeFail(state.ensureReleased(null));
    }

    public void close()
    {
        ensureReleased();
    }

    public T get()
    {
        state.assertNotReleased();
        return referent;
    }

    public boolean refers(T object)
    {
        return referent == object;
    }

    public Ref<T> tryRef()
    {
        return state.globalState.ref() ? new Ref<>(referent, state.globalState) : null;
    }

    public Ref<T> ref()
    {
        Ref<T> ref = tryRef();
        // TODO: print the last release as well as the release here
        if (ref == null)
            state.assertNotReleased();
        return ref;
    }

    public String printDebugInfo()
    {
        if (state.debug != null)
        {
            state.debug.log(state.toString());
            return "Memory was freed by " + state.debug.deallocateThread();
        }
        return "Memory was freed";
    }

    /**
     * A convenience method for reporting:
     * @return the number of currently extant references globally, including the shared reference
     */
    public int globalCount()
    {
        return state.globalState.count();
    }

    // similar to Ref.GlobalState, but tracks only the management of each unique ref created to the managed object
    // ensures it is only released once, and that it is always released
    static class State extends PhantomReference<Ref>
    {
        final Debug debug;
        final GlobalState globalState;
        private volatile int released;

        private static final AtomicIntegerFieldUpdater<State> releasedUpdater = AtomicIntegerFieldUpdater.newUpdater(State.class, "released");

        State(final GlobalState globalState, Ref reference, ReferenceQueue<? super Ref> q)
        {
            this(globalState, reference, q, COPY_SAMPLE_INTERVAL);
        }

        State(final GlobalState globalState, Ref reference, ReferenceQueue<? super Ref> q, int sampleInterval)
        {
            super(reference, q);
            this.debug = DEBUG_ENABLED || sampled(sampleInterval) ? new Debug() : null;
            this.globalState = globalState;
            globalState.register(this);
        }

        String noDebugHint()
        {
            return NO_DEBUG_HINT;
        }

        void assertNotReleased()
        {
            if (debug != null && released == 1)
                debug.log(toString());
            assert released == 0;
        }

        Throwable ensureReleased(Throwable accumulate)
        {
            if (releasedUpdater.getAndSet(this, 1) == 0)
            {
                accumulate = globalState.release(this, accumulate);
                if (debug != null)
                    debug.deallocate();
            }
            return accumulate;
        }

        void release(boolean leak)
        {
            if (!releasedUpdater.compareAndSet(this, 0, 1))
            {
                if (!leak)
                {
                    String id = this.toString();
                    logger.error("BAD RELEASE: attempted to release a reference ({}) that has already been released", id);
                    if (debug != null)
                        debug.log(id);
                    throw new IllegalStateException("Attempted to release a reference that has already been released");
                }
                return;
            }
            Throwable fail = globalState.release(this, null);
            if (leak)
            {
                String id = this.toString();
                logger.error("LEAK DETECTED: a reference ({}) to {} was not released before the reference was garbage collected{}", id, globalState, debug == null ? noDebugHint() : "");
                if (debug != null)
                    debug.log(id);
                OnLeak onLeak = ON_LEAK;
                if (onLeak != null)
                    onLeak.onLeak(this);
            }
            else if (debug != null)
            {
                debug.deallocate();
            }
            if (fail != null)
                logger.error("Error when closing {}", globalState, fail);
        }

        @Override
        public String toString()
        {
            return globalState.toString();
        }
    }

    /**
     * The state of a reference made by {@link Ref#Ref(Object, Tidy)}, sampled at {@link #PRIMARY_SAMPLE_INTERVAL}.
     * It is a subclass rather than a flag so that a leak report can tell a leaked reference from a leaked copy, and
     * name the interval that applied to it, without making every {@link State} bigger.
     */
    static final class PrimaryState extends State
    {
        PrimaryState(GlobalState globalState, Ref reference, ReferenceQueue<? super Ref> q)
        {
            super(globalState, reference, q, PRIMARY_SAMPLE_INTERVAL);
        }

        @Override
        String noDebugHint()
        {
            return PRIMARY_NO_DEBUG_HINT;
        }
    }

    static final class Debug
    {
        // Named, so that when jdk.JavaExceptionThrow or an exception-tracking agent is on, the traces sampling
        // captures in production can be identified and filtered; jdk.ExceptionStatistics only counts them.
        private static final class Trace extends Throwable
        {
            private static final long serialVersionUID = 1L;
        }

        final String allocateThread;
        String deallocateThread;
        // A Throwable keeps the VM's compact backtrace and builds the frames only if the record is logged. Unlike
        // a StackTraceElement[], it keeps the class of every frame, and so that class's loader, reachable for as
        // long as the record lives, and a frame whose method was redefined after the capture (by Byteman or another
        // retransforming agent) prints without its file and line.
        final Throwable allocateTrace;
        Throwable deallocateTrace;
        private boolean allocateLogged, deallocateLogged;
        // the id the allocate trace was logged with, for a deallocate trace that comes after it
        private String loggedId;
        Debug()
        {
            Thread thread = Thread.currentThread();
            allocateThread = thread.toString();
            allocateTrace = new Trace();
        }
        synchronized void deallocate()
        {
            Thread thread = Thread.currentThread();
            deallocateThread = thread.toString();
            deallocateTrace = new Trace();
            // The allocate trace was logged before the release, as by the losing release of a concurrent double
            // release or by a use racing with the release, and nothing may log this record again.
            if (allocateLogged)
                logDeallocate(loggedId);
        }
        synchronized String deallocateThread()
        {
            return deallocateThread;
        }
        /**
         * Logs the traces of this record. Only call this once the reference is released or being released: once
         * the allocate trace is logged, {@link #deallocate()} logs the deallocate trace, so a later legitimate
         * release would log its trace at ERROR.
         */
        synchronized void log(String id)
        {
            // Each trace at most once, as with assertions off a released reference that is still used would log on
            // every use. The deallocate trace is logged once it exists, even if the allocate trace was logged before
            // it did: then deallocate() logs it.
            if (!allocateLogged)
            {
                allocateLogged = true;
                loggedId = id;
                logger.error("Allocate trace {}:\n{}", id, print(allocateThread, allocateTrace));
            }
            if (deallocateThread != null)
                logDeallocate(id);
        }
        private void logDeallocate(String id)
        {
            if (!deallocateLogged)
            {
                deallocateLogged = true;
                logger.error("Deallocate trace {}:\n{}", id, print(deallocateThread, deallocateTrace));
            }
        }
        String print(String thread, Throwable trace)
        {
            StringBuilder sb = new StringBuilder();
            sb.append(thread);
            sb.append("\n");
            for (StackTraceElement element : trace.getStackTrace())
            {
                sb.append("\tat ");
                sb.append(element );
                sb.append("\n");
            }
            return sb.toString();
        }
    }

    // the object that manages the actual cleaning up; this does not reference the target object
    // so that we can detect when references are lost to the resource itself, and still cleanup afterwards
    // the Tidy object MUST NOT contain any references to the object we are managing
    static final class GlobalState
    {
        // we need to retain a reference to each of the PhantomReference instances
        // we are using to track individual refs
        private final Collection<State> locallyExtant = new ConcurrentLinkedDeque<>();
        // the number of live refs
        private final AtomicInteger counts = new AtomicInteger();
        // the object to call to cleanup when our refs are all finished with
        private final Tidy tidy;

        GlobalState(Tidy tidy)
        {
            this.tidy = tidy;
            globallyExtant.add(this);
        }

        void register(Ref.State ref)
        {
            locallyExtant.add(ref);
        }

        // increment ref count if not already tidied, and return success/failure
        boolean ref()
        {
            while (true)
            {
                int cur = counts.get();
                if (cur < 0)
                    return false;
                if (counts.compareAndSet(cur, cur + 1))
                    return true;
            }
        }

        // release a single reference, and cleanup if no more are extant
        Throwable release(Ref.State ref, Throwable accumulate)
        {
            locallyExtant.remove(ref);
            if (-1 == counts.decrementAndGet())
            {
                globallyExtant.remove(this);
                try
                {
                    if (tidy != null)
                        tidy.tidy();
                }
                catch (Throwable t)
                {
                    accumulate = merge(accumulate, t);
                }
            }
            return accumulate;
        }

        int count()
        {
            return 1 + counts.get();
        }

        public String toString()
        {
            if (tidy != null)
                return tidy.getClass() + "@" + System.identityHashCode(tidy) + ":" + tidy.name();
            return "@" + System.identityHashCode(this);
        }
    }

    private static final Class<?>[] concurrentIterableClasses = new Class<?>[]
    {
        ConcurrentLinkedQueue.class,
        ConcurrentLinkedDeque.class,
        ConcurrentSkipListSet.class,
        CopyOnWriteArrayList.class,
        CopyOnWriteArraySet.class,
        DelayQueue.class,
        NonBlockingHashMap.class,
    };
    static final Set<Class<?>> concurrentIterables = Collections.newSetFromMap(new IdentityHashMap<>());
    private static final Set<GlobalState> globallyExtant = Collections.newSetFromMap(new ConcurrentHashMap<>());
    static final ReferenceQueue<Object> referenceQueue = new ReferenceQueue<>();
    private static final Shutdownable EXEC = executorFactory().infiniteLoop("Reference-Reaper", Ref::reapOneReference, UNSAFE);
    static final ScheduledExecutorService STRONG_LEAK_DETECTOR = !DEBUG_ENABLED ? null : executorFactory().scheduled("Strong-Reference-Leak-Detector");
    static
    {
        if (DEBUG_ENABLED)
        {
            STRONG_LEAK_DETECTOR.scheduleAtFixedRate(new Visitor(), 1, 15, TimeUnit.MINUTES);
            STRONG_LEAK_DETECTOR.scheduleAtFixedRate(new StrongLeakDetector(), 2, 15, TimeUnit.MINUTES);
        }
        concurrentIterables.addAll(Arrays.asList(concurrentIterableClasses));
    }

    /**
     * Reads a sample interval: {@link CassandraRelevantProperties#DEBUG_REF_COUNT_PRIMARY_SAMPLE_INTERVAL} or
     * {@link CassandraRelevantProperties#DEBUG_REF_COUNT_COPY_SAMPLE_INTERVAL}. Returns 0 with
     * {@link #DEBUG_ENABLED}, which ignores the intervals. An invalid value logs one warning and turns that sampling
     * off, rather than failing startup.
     */
    @VisibleForTesting
    static int sampleInterval(CassandraRelevantProperties property)
    {
        if (DEBUG_ENABLED)
            return 0;
        try
        {
            int interval = property.getInt();
            if (interval >= 0)
                return interval;
        }
        catch (ConfigurationException e)
        {
            // reported below
        }
        logger.warn("Invalid value '{}' for {}, expected an integer >= 0; this Ref debug sampling is off",
                    property.getString(), property.getKey());
        return 0;
    }

    /**
     * Whether a new reference gets a {@link Debug} record when only sampling is on: true for one reference in
     * {@code interval}, at random, and never for 0.
     */
    @VisibleForTesting
    static boolean sampled(int interval)
    {
        return interval > 0 && ThreadLocalRandom.current().nextInt(interval) == 0;
    }

    /**
     * What a leak report without a {@link Debug} record appends, to say why it has no allocation stack: for a
     * reference made by {@link #Ref(Object, Tidy)} if {@code primary}, else for a copy.
     */
    @VisibleForTesting
    static String noDebugHint(boolean primary, int sampleInterval)
    {
        String what = primary ? "references" : "copied references";
        String option = "-D" + (primary ? DEBUG_REF_COUNT_PRIMARY_SAMPLE_INTERVAL : DEBUG_REF_COUNT_COPY_SAMPLE_INTERVAL).getKey();
        return sampleInterval > 0
               ? "; no allocation stack, as only 1 in " + sampleInterval + ' ' + what + " is sampled (changing " + option + " needs a restart)"
               : "; no allocation stack, as sampling of " + what + " is off (restart with " + option + "=N to sample 1 in N)";
    }

    private static void reapOneReference() throws InterruptedException
    {
        Object obj = referenceQueue.remove(100);
        if (obj instanceof Ref.State)
        {
            ((Ref.State) obj).release(true);
        }
    }

    static final Deque<InProgressVisit> inProgressVisitPool = new ArrayDeque<>();

    @SuppressWarnings({ "rawtypes", "unchecked" })
    static InProgressVisit newInProgressVisit(Object o, List<Field> fields, Field field, String name)
    {
        Preconditions.checkNotNull(o);
        InProgressVisit ipv = inProgressVisitPool.pollLast();
        if (ipv == null)
            ipv = new InProgressVisit();

        ipv.o = o;
        if (o instanceof Object[])
            ipv.collectionIterator = Arrays.asList((Object[])o).iterator();
        else if (o instanceof ConcurrentMap)
        {
            ipv.isMapIterator = true;
            ipv.collectionIterator = ((Map)o).entrySet().iterator();
        }
        else if (concurrentIterables.contains(o.getClass()) | o instanceof BlockingQueue)
            ipv.collectionIterator = ((Iterable)o).iterator();

        ipv.fields = fields;
        ipv.field = field;
        ipv.name = name;
        return ipv;
    }

    static void returnInProgressVisit(InProgressVisit ipv)
    {
        if (inProgressVisitPool.size() > 1024)
            return;
        ipv.name = null;
        ipv.fields = null;
        ipv.o = null;
        ipv.fieldIndex = 0;
        ipv.field = null;
        ipv.collectionIterator = null;
        ipv.mapEntryValue = null;
        ipv.isMapIterator = false;
        inProgressVisitPool.offer(ipv);
    }

    /*
     * Stack state for walking an object graph.
     * Field index is the index of the current field being fetched.
     */
    @SuppressWarnings({ "rawtypes"})
    static class InProgressVisit
    {
        String name;
        List<Field> fields;
        Object o;
        int fieldIndex = 0;
        Field field;

        //Need to know if Map.Entry should be returned or traversed as an object
        boolean isMapIterator;
        //If o is a ConcurrentMap, BlockingQueue, or Object[], this is populated with an iterator over the contents
        Iterator<Object> collectionIterator;
        //If o is a ConcurrentMap the entry set contains keys and values. The key is returned as the first child
        //And the associated value is stashed here and returned next
        Object mapEntryValue;

        private Field nextField()
        {
            if (fields.isEmpty())
                return null;

            if (fieldIndex >= fields.size())
                return null;

            Field retval = fields.get(fieldIndex);
            fieldIndex++;
            return retval;
        }

        Pair<Object, Field> nextChild() throws IllegalAccessException
        {
            //If the last child returned was a key from a map, the value from that entry is stashed
            //so it can be returned next
            if (mapEntryValue != null)
            {
                Pair<Object, Field> retval = Pair.create(mapEntryValue, field);
                mapEntryValue = null;
                return retval;
            }

            //If o is a ConcurrentMap, BlockingQueue, or Object[], then an iterator will be stored to return the elements
            if (collectionIterator != null)
            {
                if (!collectionIterator.hasNext())
                    return null;
                Object nextItem = null;
                //Find the next non-null element to traverse since returning null will cause the visitor to stop
                while (collectionIterator.hasNext() && (nextItem = collectionIterator.next()) == null){}
                if (nextItem != null)
                {
                    if (isMapIterator & nextItem instanceof Map.Entry)
                    {
                        Map.Entry entry = (Map.Entry)nextItem;
                        mapEntryValue = entry.getValue();
                        return Pair.create(entry.getKey(), field);
                    }
                    return Pair.create(nextItem, field);
                }
                else
                {
                    return null;
                }
            }

            //Basic traversal of an object by its member fields
            //Don't return null values as that indicates no more objects
            while (true)
            {
                Field nextField = nextField();
                if (nextField == null)
                    return null;

                //A weak reference isn't strongly reachable
                //subclasses of WeakReference contain strong references in their fields, so those need to be traversed
                //The weak reference fields are in the common Reference class base so filter those out
                if (o instanceof WeakReference & nextField.getDeclaringClass() == Reference.class)
                    continue;

                Object nextObject = getFieldValue(o, nextField);
                if (nextObject != null)
                    return Pair.create(getFieldValue(o, nextField), nextField);
            }
        }

        @Override
        public String toString()
        {
            return field == null ? name : field + "-" + o.getClass().getName();
        }
    }

    static class Visitor implements Runnable
    {
        final Deque<InProgressVisit> path = new ArrayDeque<>();
        final Set<Object> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        @VisibleForTesting
        int lastVisitedCount;
        @VisibleForTesting
        long iterations = 0;
        GlobalState visiting;
        Set<GlobalState> haveLoops;

        public void run()
        {
            try
            {
                for (GlobalState globalState : globallyExtant)
                {
                    if (globalState.tidy == null)
                        continue;

                    // do a graph exploration of the GlobalState, since it should be shallow; if it references itself, we have a problem
                    path.clear();
                    visited.clear();
                    lastVisitedCount = 0;
                    iterations = 0;
                    visited.add(globalState);
                    visiting = globalState;
                    traverse(globalState.tidy);
                }
            }
            catch (Throwable t)
            {
                t.printStackTrace();
            }
            finally
            {
                lastVisitedCount = visited.size();
                path.clear();
                visited.clear();
            }
        }

        /*
         * Searches for an indirect strong reference between rootObject and visiting.
         */
        void traverse(final RefCounted.Tidy rootObject)
        {
            path.offer(newInProgressVisit(rootObject, getFields(rootObject.getClass()), null, rootObject.name()));

            InProgressVisit inProgress = null;
            while (inProgress != null || !path.isEmpty())
            {
                //If necessary fetch the next object to start tracing
                if (inProgress == null)
                    inProgress = path.pollLast();

                try
                {
                    Pair<Object, Field> p = inProgress.nextChild();
                    Object child = null;
                    Field field = null;

                    if (p != null)
                    {
                        iterations++;
                        child = p.left;
                        field = p.right;
                    }

                    if (child != null && visited.add(child))
                    {
                        path.offer(inProgress);
                        inProgress = newInProgressVisit(child, getFields(child.getClass()), field, null);
                    }
                    else if (visiting == child)
                    {
                        if (haveLoops != null)
                            haveLoops.add(visiting);
                        NoSpamLogger.log(logger,
                                NoSpamLogger.Level.ERROR,
                                rootObject.getClass().getName(),
                                1,
                                TimeUnit.SECONDS,
                                "Strong self-ref loop detected {}",
                                path);
                    }
                    else if (child == null)
                    {
                        returnInProgressVisit(inProgress);
                        inProgress = null;
                    }
                }
                catch (IllegalAccessException e)
                {
                    NoSpamLogger.log(logger, NoSpamLogger.Level.ERROR, 5, TimeUnit.MINUTES, "Could not fully check for self-referential leaks", e);
                }
            }
        }
    }

    static final Map<Class<?>, List<Field>> fieldMap = new HashMap<>();
    static List<Field> getFields(Class<?> clazz)
    {
        if (clazz == null || clazz == PhantomReference.class || clazz == Class.class || java.lang.reflect.Member.class.isAssignableFrom(clazz))
            return emptyList();
        List<Field> fields = fieldMap.get(clazz);
        if (fields != null)
            return fields;
        fieldMap.put(clazz, fields = new ArrayList<>());
        for (Field field : clazz.getDeclaredFields())
        {
            if (field.getType().isPrimitive() || Modifier.isStatic(field.getModifiers()))
                continue;
            fields.add(field);
        }
        fields.addAll(getFields(clazz.getSuperclass()));
        return fields;
    }

    /**
     * The unsafe instance used to access object protected by the Module System
     */
    private static final Unsafe unsafe = loadUnsafe();

    private static Unsafe loadUnsafe()
    {
        try
        {
            return Unsafe.getUnsafe();
        }
        catch (final Exception ex)
        {
            try
            {
                Field field = Unsafe.class.getDeclaredField("theUnsafe");
                field.setAccessible(true);
                return (Unsafe) field.get(null);
            }
            catch (Exception e)
            {
                return null;
            }
        }
    }

    public static Object getFieldValue(Object object, Field field)
    {
        try
        {
            // This call will unfortunately emit a warning for some scenario (which was a weird decision from the JVM designer)
            if (field.trySetAccessible())
            {
                // The field is accessible lets use reflection.
                return field.get(object);
            }

            // The access to the field is being restricted by the module system. Let's try to go around it through Unsafe.
            if (unsafe == null)
                throw new UnaccessibleFieldException("The value of the '" + field.getName() + "' field from " + object.getClass().getName()
                                                     + " cannot be retrieved as the field cannot be made accessible and Unsafe is unavailable");

            long offset = unsafe.objectFieldOffset(field);

            boolean isFinal = Modifier.isFinal(field.getModifiers());
            boolean isVolatile = Modifier.isVolatile(field.getModifiers());

            return isFinal || isVolatile ? unsafe.getObjectVolatile(object, offset) : unsafe.getObject(object, offset);

        }
        catch (Throwable e)
        {
            throw new UnaccessibleFieldException("The value of the '" + field.getName() + "' field from " + object.getClass().getName() + " cannot be retrieved", e);
        }
    }

    public static class IdentityCollection
    {
        final Set<Tidy> candidates;
        public IdentityCollection(Set<Tidy> candidates)
        {
            this.candidates = candidates;
        }

        public void add(Ref<?> ref)
        {
            candidates.remove(ref.state.globalState.tidy);
        }
        public void add(SelfRefCounted<?> ref)
        {
            add(ref.selfRef());
        }
        public void add(SharedCloseable ref)
        {
            if (ref instanceof SharedCloseableImpl)
                add((SharedCloseableImpl)ref);
        }
        public void add(SharedCloseableImpl ref)
        {
            add(ref.ref);
        }
        public void add(Memory memory)
        {
            if (memory instanceof SafeMemory)
                ((SafeMemory) memory).addTo(this);
        }
    }

    private static class StrongLeakDetector implements Runnable
    {
        Set<Tidy> candidates = new HashSet<>();

        public void run()
        {
            final Set<Tidy> candidates = Collections.newSetFromMap(new IdentityHashMap<>());
            for (GlobalState state : globallyExtant)
            {
                if (state.tidy != null)
                    candidates.add(state.tidy);
            }
            removeExpected(candidates);
            this.candidates.retainAll(candidates);
            if (!this.candidates.isEmpty())
            {
                List<String> names = new ArrayList<>(this.candidates.size());
                for (Tidy tidy : this.candidates)
                    names.add(tidy.name());
                logger.error("Strong reference leak candidates detected: {}", names);
            }
            this.candidates = candidates;
        }

        private void removeExpected(Set<Tidy> candidates)
        {
            final Ref.IdentityCollection expected = new Ref.IdentityCollection(candidates);
            for (Keyspace ks : Keyspace.all())
            {
                for (ColumnFamilyStore cfs : ks.getColumnFamilyStores())
                {
                    View view = cfs.getTracker().getView();
                    for (SSTableReader reader : view.allKnownSSTables())
                        reader.addTo(expected);
                }
            }
        }
    }

    public static void setOnLeak(OnLeak onLeak)
    {
        ON_LEAK = onLeak;
    }

    @VisibleForTesting
    public static void shutdownReferenceReaper(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        ExecutorUtils.shutdownNowAndWait(timeout, unit, EXEC, STRONG_LEAK_DETECTOR);
    }

    /**
     * A version of {@link Ref} for objects that implement {@link DirectBuffer}.
     */
    public static final class DirectBufferRef<T extends DirectBuffer> implements RefCounted<T>, DirectBuffer
    {
        private final Ref<T> wrappedRef;
        
        public DirectBufferRef(T referent, Tidy tidy)
        {
            wrappedRef = new Ref<>(referent, tidy);
        }

        @Override
        public long address()
        {
            return wrappedRef.referent != null ? wrappedRef.referent.address() : 0;
        }

        @Override
        public Object attachment()
        {
            return wrappedRef.referent != null ? wrappedRef.referent.attachment() : null;
        }

        @Override
        public Cleaner cleaner()
        {
            return wrappedRef.referent != null ? wrappedRef.referent.cleaner() : null;
        }

        @Override
        public Ref<T> tryRef()
        {
            return wrappedRef.tryRef();
        }

        @Override
        public Ref<T> ref()
        {
            return wrappedRef.ref();
        }

        public void release()
        {
            wrappedRef.release();
        }

        public T get()
        {
            return wrappedRef.get();
        }
    }
}
