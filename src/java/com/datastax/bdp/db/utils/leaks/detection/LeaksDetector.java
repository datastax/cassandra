/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.db.utils.leaks.detection;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.utils.UnsafeMemoryAccess;

/**
 * An interface for detecting leaks of resources that should get explicitly closed or released.
 * <p/>
 * Resources that should be tracked could be ref counted objects that need to perform some cleanup when the ref
 * count is zero. For example {@link java.nio.ByteBuffer} instances that must be returned to a
 * {@link org.apache.cassandra.utils.memory.buffers.BufferPool}, or objects using memory allocated
 * with {@link UnsafeMemoryAccess#allocate(long)}.
 * <p/>
 * There should exist a detector per resource type and each detector will have dedicated parameters as described in
 * {@link LeaksDetectionParams}. These parameters can be changed by calling {@link this#setParams(LeaksDetectionParams)}.
 * <p/>
 * Refer to {@link LeaksDetectorImpl} for implementation details.
 *
 * @param <T> the type of the resource that needs tracking
 */
public interface LeaksDetector<T>
{
    /**
     * A cleaner is invoked when the instance that was being tracked has been leaked.
     * <p/>
     * A resource of type T may own resources that need cleaning of type C. T and C must be different because
     * there can't be any strong reference to T. For example, a cache chunk may need to return buffers, in this case T would
     * be the chunk and C either the byte buffer or an address belonging to the buffer pool. A resource may also
     * have several types of resources that need cleaning and not just one. Some resources may be tracked for debugging only,
     * in which case they have nothing that needs cleaning, in this case they simply won't need a cleaner.
     *
     * @param <C> the resource to be cleaned, this is not the same as the resource being tracked because there
     *            can't be any strong references to the resource being tracked as explained in more details above.
     */
    interface Cleaner<C>
    {
        void clean(C resForCleaning);
    }

    /**
     * Track the given resource instance for debugging only. Tracking only happens with the sampling probability that was
     * last set with the {@link this#setParams(LeaksDetectionParams)} If the resource is leaked, some debug information
     * such as the creation call-stack trace will be logged.
     *
     * @param resForTracking the resource to track
     *
     * @return a valid handle that should be closed before the resource goes out of scope, or null if this instance won't be tracked.
     */
    @Nullable
    LeaksTracker<T> trackForDebug(T resForTracking);

    /**
     *  Track the given resource instance for debug and cleaning. Tracking only happens with the sampling probability that was
     *  last set with the {@link this#setParams(LeaksDetectionParams)} If the resource is leaked, some debug information
     *  such as the creation call-stack trace will be logged and the cleaner will be run if it is not null. It will
     *  receive the resources passed into this method.
     * <p/>
     * <b>It is essential that the cleaner or the resource for cleaning do not strongly reference the resource for tracking.</b>
     *
     * @param resForTracking the resource to track
     * @param cleaner an optional cleaner to be run when the resource is detected as leaked, it may be null
     * @param resForCleaning the resource to be passed to the cleaner when it is run
     *
     * @return a valid handle that should be closed before the resource goes out of scope, or null if this instance won't be tracked.
     */
    @Nullable
    <C> LeaksTracker<T> trackForDebug(T resForTracking, @Nullable Cleaner<C> cleaner, @Nullable C resForCleaning);

    /**
     * Track the given resource instance for cleaning only. Tracking happens all the time regardless of the sampling
     * probability. If the resource is leaked, the cleaner will be run. The cleaner cannot be null. The cleaner will
     * receive the resources passed into this method.
     * <p/>
     * <b>It is essential that the cleaner or the resource for cleaning do not strongly reference the resource for tracking.</b>
     *
     * @param resForTracking the resource to track
     * @param cleaner code that will be run if the resource is leaked
     * @param resForCleaning the resource to be passed to the cleaner when it is run
     *
     * @return a valid handle that should be closed before the resource goes out of scope,
     */
    <C> LeaksTracker<T> trackForCleaning(T resForTracking, Cleaner<C> cleaner, C resForCleaning);

    /**
     * Check for leaked resources, this is called periodically and so there is normally
     * no need to call this method except for tests.
     */
    @VisibleForTesting
    void checkForLeaks();

    /**
     * @return the type of resource being tracked for leaks
     */
    LeaksResource getResource();

    /**
     * @return the current parameters
     */
    LeaksDetectionParams getParams();

    /**
     * Change the parameters such as the sampling probability.
     *
     * @param params - the new parameters
     *
     * @return the old parameters
     */
    LeaksDetectionParams setParams(LeaksDetectionParams params);
}
