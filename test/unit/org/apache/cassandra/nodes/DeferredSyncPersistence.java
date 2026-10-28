/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.nodes;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import com.google.common.util.concurrent.Uninterruptibles;

/**
 * Reproduces, without a cluster, the shape of the start deadlock of an import: the loads run while {@link Nodes} is
 * being initialised and a flush of the system tables cannot end before that initialisation did (the flush writer
 * needs {@code Nodes.local()}). A flush requested on the thread that runs the loads fails at once, instead of
 * hanging (the loads do not call {@link #syncLocal()} or {@link #syncPeers()}: those overrides fail the test if a
 * load ever does); the flush of the import completion waits until {@link #loadsReturned()} is called, as a real one
 * would wait for the initialisation, and records whether the import was already certified (marker written) once the
 * flush ended.
 */
class DeferredSyncPersistence extends CC4UpgradeNodesPersistence
{
    private final Thread loadingThread = Thread.currentThread();
    private final CountDownLatch loadsReturned = new CountDownLatch(1);
    private final BooleanSupplier certified;
    /** One entry per flush of the completion: the flushed tables and whether the marker existed when it ended. */
    final List<String> flushes = new CopyOnWriteArrayList<>();

    /**
     * @param certified whether the import is certified, i.e. the marker exists
     */
    DeferredSyncPersistence(BooleanSupplier certified)
    {
        this.certified = certified;
    }

    /** Stands for the end of the initialisation of {@link Nodes}: the flushes may end from now on. */
    void loadsReturned()
    {
        loadsReturned.countDown();
    }

    @Override
    public void syncLocal()
    {
        failOnTheLoadingThread("local");
        super.syncLocal();
    }

    @Override
    public void syncPeers()
    {
        failOnTheLoadingThread("peers");
        super.syncPeers();
    }

    @Override
    protected void flushImported(boolean local, boolean peers)
    {
        String tables = (local ? "local" : "") + (local && peers ? "+" : "") + (peers ? "peers" : "");
        failOnTheLoadingThread(tables);
        if (!Uninterruptibles.awaitUninterruptibly(loadsReturned, 1, TimeUnit.MINUTES))
            throw new AssertionError("the loads never returned");
        super.flushImported(local, peers);
        flushes.add(tables + (certified.getAsBoolean() ? " flushed after the certification" : " flushed before the certification"));
    }

    private void failOnTheLoadingThread(String tables)
    {
        if (Thread.currentThread() == loadingThread)
            throw new AssertionError("blocking flush of " + tables + " requested by the thread that loads the node metadata");
    }
}
