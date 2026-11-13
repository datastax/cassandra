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

package org.apache.cassandra.net;

/**
 * Exception thrown when the messaging service encounters an endpoint that cannot be resolved or is not known
 * to the cluster. This typically occurs during network operations when attempting to communicate with a node
 * that is not recognized by the current node's topology information.
 *
 * <p>This exception is thrown when the
 * {@link org.apache.cassandra.locator.IEndpointSnitch#getPreferredAddress(org.apache.cassandra.locator.InetAddressAndPort)}
 * method encounters an endpoint that cannot be resolved to a preferred address.</p>
 *
 * <p>Common scenarios where this exception may be thrown include:</p>
 *
 * <ul>
 *   <li>During speculative read retries when the target replica endpoint is not known to the messaging service</li>
 *   <li>When attempting to establish connections to endpoints that are not part of the known cluster topology</li>
 * </ul>
 *
 * <p>The exception is used internally by the messaging system to handle cases where endpoint resolution fails,
 * allowing the system to gracefully handle unknown endpoints rather than causing fatal errors. For example,
 * during speculative read operations, this exception is caught and logged as a debug message, allowing the
 * operation to continue without the failed speculative retry.
 *
 * @see org.apache.cassandra.locator.IEndpointSnitch#getPreferredAddress(org.apache.cassandra.locator.InetAddressAndPort)
 * @see org.apache.cassandra.service.reads.AbstractReadExecutor#maybeTryAdditionalReplicas()
 */
public class UnknownEndpointException extends RuntimeException
{
    /**
     * Constructs a new UnknownEndpointException with the specified detail message.
     *
     * @param message the detail message explaining why the endpoint is unknown or cannot be resolved.
     *                This message should provide context about which endpoint failed and the circumstances
     *                of the failure (e.g., "Failed to resolve preferred address for endpoint 192.168.1.100:9042")
     */
    public UnknownEndpointException(String message)
    {
        super(message);
    }
}
