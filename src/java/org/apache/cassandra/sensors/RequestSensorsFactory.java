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

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.REQUEST_SENSORS_FACTORY;

/**
 * Provides a customizable factory to create a {@link RequestSensors} to track sensors per user request.
 * Configured via the {@link CassandraRelevantProperties#REQUEST_SENSORS_FACTORY} system property, if unset a {@link NoOpRequestSensors} will be used.
 * To activate tracking of request sensors, use {@link ActiveRequestSensorsFactory}.
 */
public interface RequestSensorsFactory
{
    RequestSensorsFactory instance = REQUEST_SENSORS_FACTORY.getString() == null ?
                                   new RequestSensorsFactory() {} :
                                   FBUtilities.construct(CassandraRelevantProperties.REQUEST_SENSORS_FACTORY.getString(), "requests sensors factory");

    /**
     * Creates a {@link RequestSensors} for the given keyspace. Implementations should be very efficient because this method is potentially invoked on each verb handler serving a user request.
     *
     * @param keyspace the keyspace of the request
     * @return a {@link RequestSensors} instance. The default implementation returns a singleton no-op instance.
     */
    default RequestSensors create(String keyspace)
    {
        return NoOpRequestSensors.instance;
    }
}
