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

package org.apache.cassandra.sensors;

import java.util.Optional;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.REQUEST_SENSORS_FACTORY;

/**
 * Provides a customizable factory to create a {@link RequestSensors} to track sensors per user request.
 */

public interface RequestSensorsFactory
{
    RequestSensorsFactory instance = REQUEST_SENSORS_FACTORY.getString() == null ?
                                   new RequestSensorsFactory() {} :
                                   FBUtilities.construct(CassandraRelevantProperties.REQUEST_SENSORS_FACTORY.getString(), "requests sensors factory");

    /**
     * Creates a {@link RequestSensors} for the given keyspace. Implementation of this methods should be very efficient because this method is potentially on each vern handler serving a user request.
     *
     * @param keyspace the keyspace of the request
     * @return a {@link RequestSensors} instance or an empty optional if no sensors should be tracked for the given keyspace
     */
    default Optional<RequestSensors> create(String keyspace)
    {
        return Optional.empty();
    }
}
