/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.db.utils.leaks.detection;

/**
 * A {@link LeaksTracker} is returned to the caller for each resource that is tracked by calling {@link LeaksDetector#trackForDebug(Object)}.
 * <p/>
 * Clients can optionally call {@link this#record()} to add information on how the resource is accessed in order
 * to help with debugging.
 * <p/>
 * Clients *must* call {@link this#close(Object)} before the resource is garbage collected. To ensure the resource
 * is not collected first, therefore causing false positives, it must be passed-in as an argument.
 */
public interface LeaksTracker<T>  {
    /**
     * Records the caller's current stack trace so that we can tell where the leaked
     * resource was recently accessed.
     */
    void record();

    /**
     * Method called when the resource has been reported as leaked by the garbage collector.
     */
    void onResourceLeaked();

    /**
     * Close the leak so that we do not warn about leaked resources.
     * After this method is called a leak associated with this ResourceLeakTracker should not be reported.
     *
     * @param trackedObject - the object being tracked. We need to receive this again to make sure the JIT / GC
     *                        doesn't decide that the resource is already unreachable, e.g. if close is called
     *                        asynchronously, therefore causing false positives, see
     *                        https://github.com/netty/netty/pull/6087
     *
     * @return {@code true} if called first time, {@code false} if called already
     */
    boolean close(T trackedObject);

    /**
     * Returns true if the resource tracker was already closed.
     *
     * @return true if {@link this#close(Object)} was called
     */
    boolean isClosed();
}
