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
package org.apache.cassandra.gms;

import java.util.function.Predicate;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_FAILURE_DETECTOR_PROPERTY;

/**
 * An interface that provides an application with the ability
 * to query liveness information of a node in the cluster. It
 * also exposes methods which help an application register callbacks
 * for notifications of liveness information of nodes.
 */

public interface IFailureDetector
{
    IFailureDetector instance = CUSTOM_FAILURE_DETECTOR_PROPERTY.isPresent()
                                ? FBUtilities.construct(CUSTOM_FAILURE_DETECTOR_PROPERTY.getString(),
                                                        "Failure Detector")
                                : new FailureDetector();

    public static final Predicate<InetAddressAndPort> isEndpointAlive = instance::isAlive;
    public static final Predicate<Replica> isReplicaAlive = r -> isEndpointAlive.test(r.endpoint());

    /**
     * Failure Detector's knowledge of whether a node is up or
     * down.
     *
     * @param ep endpoint in question.
     * @return true if UP and false if DOWN.
     */
    public boolean isAlive(InetAddressAndPort ep);

    /**
     * This method is invoked by any entity wanting to interrogate the status of an endpoint.
     * In our case it would be the Gossiper. The Failure Detector will then calculate Phi and
     * deem an endpoint as suspicious or alive as explained in the Hayashibara paper.
     *
     * param ep endpoint for which we interpret the inter arrival times.
     */
    public void interpret(InetAddressAndPort ep);

    /**
     * This method is invoked by the receiver of the heartbeat. In our case it would be
     * the Gossiper. Gossiper inform the Failure Detector on receipt of a heartbeat. The
     * FailureDetector will then sample the arrival time as explained in the paper.
     *
     * param ep endpoint being reported.
     */
    public void report(InetAddressAndPort ep);

    /**
     * remove endpoint from failure detector
     */
    public void remove(InetAddressAndPort ep);

    /**
     * force conviction of endpoint in the failure detector
     */
    public void forceConviction(InetAddressAndPort ep);

    /**
     * Register interest for Failure Detector events.
     *
     * @param listener implementation of an application provided IFailureDetectionEventListener
     */
    public void registerFailureDetectionEventListener(IFailureDetectionEventListener listener);

    /**
     * Un-register interest for Failure Detector events.
     *
     * @param listener implementation of an application provided IFailureDetectionEventListener
     */
    public void unregisterFailureDetectionEventListener(IFailureDetectionEventListener listener);
}
