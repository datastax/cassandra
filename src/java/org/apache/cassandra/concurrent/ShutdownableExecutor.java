/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.concurrent;

import java.util.concurrent.Executor;

/**
 * An executor you can shut down!
 */
public interface ShutdownableExecutor extends Executor
{

    /**
     * Stop the threads started for this executor(if any), returns when the threads spawned for this executor have all
     * been stopped. Wait as long as it takes.
     * <p>
     * Note that tasks sent via {@link #execute(Runnable)} may be stuck in queue indefinitely.
     */
    void shutdown() throws InterruptedException;
}
