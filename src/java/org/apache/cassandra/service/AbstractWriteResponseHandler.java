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
package org.apache.cassandra.service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.ConsistencyLevel;

import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.ReplicaPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.sensors.RequestSensors;
import org.apache.cassandra.sensors.RequestTracker;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.locator.Replicas.countInOurDc;

public abstract class AbstractWriteResponseHandler<T> implements RequestCallback<T>
{
    protected static final Logger logger = LoggerFactory.getLogger(AbstractWriteResponseHandler.class);

    //Count down until all responses and expirations have occured before deciding whether the ideal CL was reached.
    private AtomicInteger responsesAndExpirations;
    private final SimpleCondition condition = new SimpleCondition();
    protected final ReplicaPlan.ForTokenWrite replicaPlan;

    protected final Runnable callback;
    protected final WriteType writeType;
    private static final AtomicIntegerFieldUpdater<AbstractWriteResponseHandler> failuresUpdater
    = AtomicIntegerFieldUpdater.newUpdater(AbstractWriteResponseHandler.class, "failures");
    protected volatile int failures = 0;
    protected final Map<InetAddressAndPort, RequestFailureReason> failureReasonByEndpoint;
    private final long queryStartNanoTime;

    private final RequestSensors requestSensors;

    /**
      * Delegate to another WriteResponseHandler or possibly this one to track if the ideal consistency level was reached.
      * Will be set to null if ideal CL was not configured
      * Will be set to an AWRH delegate if ideal CL was configured
      * Will be same as "this" if this AWRH is the ideal consistency level
      */
    private AbstractWriteResponseHandler idealCLDelegate;

    /**
     * We don't want to increment the writeFailedIdealCL if we didn't achieve the original requested CL
     */
    private boolean requestedCLAchieved = false;

    /**
     * @param callback           A callback to be called when the write is successful.
     * @param queryStartNanoTime
     */
    protected AbstractWriteResponseHandler(ReplicaPlan.ForTokenWrite replicaPlan,
                                           Runnable callback,
                                           WriteType writeType,
                                           long queryStartNanoTime)
    {
        this.replicaPlan = replicaPlan;
        this.callback = callback;
        this.writeType = writeType;
        this.failureReasonByEndpoint = new ConcurrentHashMap<>();
        this.queryStartNanoTime = queryStartNanoTime;
        this.requestSensors = RequestTracker.instance.get();
    }

    public void get() throws WriteTimeoutException, WriteFailureException
    {
        boolean success = await();

        if (!success)
        {
            int blockedFor = blockFor();
            int acks = ackCount();
            // It's pretty unlikely, but we can race between exiting await above and here, so
            // that we could now have enough acks. In that case, we "lie" on the acks count to
            // avoid sending confusing info to the user (see CASSANDRA-6491).
            if (acks >= blockedFor)
                acks = blockedFor - 1;
            throw new WriteTimeoutException(writeType, replicaPlan.consistencyLevel(), acks, blockedFor);
        }

        if (blockFor() + failures > candidateReplicaCount())
        {
            throw new WriteFailureException(replicaPlan.consistencyLevel(), ackCount(), blockFor(), writeType, failureReasonByEndpoint);
        }
    }

    public boolean await()
    {
        long timeoutNanos = currentTimeoutNanos();

        try
        {
            return condition.await(timeoutNanos, NANOSECONDS);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError(ex);
        }
    }

    public long currentTimeoutNanos()
    {
        long requestTimeout = writeType == WriteType.COUNTER
                              ? DatabaseDescriptor.getCounterWriteRpcTimeout(NANOSECONDS)
                              : DatabaseDescriptor.getWriteRpcTimeout(NANOSECONDS);
        return requestTimeout - (System.nanoTime() - queryStartNanoTime);
    }

    public ReplicaPlan.ForTokenWrite replicaPlan()
    {
        return replicaPlan;
    }

    public WriteType writeType()
    {
        return writeType;
    }

    public long queryStartNanoTime()
    {
        return queryStartNanoTime;
    }

    /**
     * Set a delegate ideal CL write response handler. Note that this could be the same as this
     * if the ideal CL and requested CL are the same.
     */
    public void setIdealCLResponseHandler(AbstractWriteResponseHandler handler)
    {
        this.idealCLDelegate = handler;
        idealCLDelegate.responsesAndExpirations = new AtomicInteger(replicaPlan.contacts().size());
    }

    /**
     * This logs the response but doesn't do any further processing related to this write response handler
     * on whether the CL was achieved. Only call this after the subclass has completed all it's processing
     * since the subclass instance may be queried to find out if the CL was achieved.
     */
    protected final void logResponseToIdealCLDelegate(Message<T> m)
    {
        //Tracking ideal CL was not configured
        if (idealCLDelegate == null)
        {
            return;
        }

        if (idealCLDelegate == this)
        {
            //Processing of the message was already done since this is the handler for the
            //ideal consistency level. Just decrement the counter.
            decrementResponseOrExpired();
        }
        else
        {
            //Let the delegate do full processing, this will loop back into the branch above
            //with idealCLDelegate == this, because the ideal write handler idealCLDelegate will always
            //be set to this in the delegate.
            idealCLDelegate.onResponse(m);
        }
    }

    public final void expired()
    {
        //Tracking ideal CL was not configured
        if (idealCLDelegate == null)
        {
            return;
        }

        //The requested CL matched ideal CL so reuse this object
        if (idealCLDelegate == this)
        {
            decrementResponseOrExpired();
        }
        else
        {
            //Have the delegate track the expired response
            idealCLDelegate.decrementResponseOrExpired();
        }
    }

    /**
     * @return the minimum number of endpoints that must respond.
     */
    public int blockFor()
    {
        // During bootstrap, we have to include the pending endpoints or we may fail the consistency level
        // guarantees (see #833)
        return replicaPlan.blockFor();
    }

    /**
     * TODO: this method is brittle for its purpose of deciding when we should fail a query;
     *       this needs to be aware of which nodes are live/down
     * @return the total number of endpoints the request can send to.
     */
    public int candidateReplicaCount()
    {
        if (replicaPlan.consistencyLevel().isDatacenterLocal())
            return countInOurDc(replicaPlan.liveAndDown()).allReplicas();

        return replicaPlan.liveAndDown().size();
    }

    public ConsistencyLevel consistencyLevel()
    {
        return replicaPlan.consistencyLevel();
    }

    /**
     * @return true if the message counts towards the blockFor() threshold
     */
    public boolean waitingFor(InetAddressAndPort from)
    {
        return true;
    }

    /**
     * @return number of responses received
     */
    public abstract int ackCount();

    /**
     * null message means "response from local write"
     */
    public abstract void onResponse(Message<T> msg);

    public void signal()
    {
        //The ideal CL should only count as a strike if the requested CL was achieved.
        //If the requested CL is not achieved it's fine for the ideal CL to also not be achieved.
        if (idealCLDelegate != null)
        {
            idealCLDelegate.requestedCLAchieved = true;
        }

        condition.signalAll();
        if (callback != null)
            callback.run();
    }

    /**
     * @return true if condition is signaled either for success or failure
     */
    @VisibleForTesting
    public boolean isCompleted()
    {
        return condition.isSignaled();
    }

    /**
     * @return true if condition is signaled for failure
     */
    public boolean isCompletedExceptionally()
    {
        return isCompleted() && blockFor() + failures > candidateReplicaCount();
    }

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
    {
        logger.trace("Got failure from {}", from);

        int n = waitingFor(from)
                ? failuresUpdater.incrementAndGet(this)
                : failures;

        failureReasonByEndpoint.put(from, failureReason);

        if (blockFor() + n > candidateReplicaCount())
            signal();
    }

    @Override
    public boolean invokeOnFailure()
    {
        return true;
    }

    @Override
    public RequestSensors getRequestSensors()
    {
        return requestSensors;
    }

    /**
     * Decrement the counter for all responses/expirations and if the counter
     * hits 0 check to see if the ideal consistency level (this write response handler)
     * was reached using the signal.
     */
    private final void decrementResponseOrExpired()
    {
        int decrementedValue = responsesAndExpirations.decrementAndGet();
        if (decrementedValue == 0)
        {
            // The condition being signaled is a valid proxy for the CL being achieved
            // Only mark it as failed if the requested CL was achieved.
            if (!condition.isSignaled() && requestedCLAchieved)
            {
                replicaPlan.keyspace().metric.writeFailedIdealCL.inc();
            }
            else
            {
                replicaPlan.keyspace().metric.idealCLWriteLatency.addNano(System.nanoTime() - queryStartNanoTime);
            }
        }
    }

    /**
     * Cheap Quorum backup.  If we failed to reach quorum with our initial (full) nodes, reach out to other nodes.
     */
    public void maybeTryAdditionalReplicas(IMutation mutation, StorageProxy.WritePerformer writePerformer, String localDC)
    {
        EndpointsForToken uncontacted = replicaPlan.liveUncontacted();
        if (uncontacted.isEmpty())
            return;

        long timeout = Long.MAX_VALUE;
        List<ColumnFamilyStore> cfs = mutation.getTableIds().stream()
                                              .map(Schema.instance::getColumnFamilyStoreInstance)
                                              .collect(Collectors.toList());
        for (ColumnFamilyStore cf : cfs)
            timeout = Math.min(timeout, cf.additionalWriteLatencyNanos);

        // no latency information, or we're overloaded
        if (timeout > mutation.getTimeout(NANOSECONDS))
            return;

        try
        {
            if (!condition.await(timeout, NANOSECONDS))
            {
                for (ColumnFamilyStore cf : cfs)
                    cf.metric.additionalWrites.inc();

                writePerformer.apply(mutation, replicaPlan.withContact(uncontacted),
                                     (AbstractWriteResponseHandler<IMutation>) this,
                                     localDC);
            }
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError(ex);
        }
    }
}
