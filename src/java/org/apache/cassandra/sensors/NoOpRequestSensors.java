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
import java.util.Set;

import com.google.common.collect.ImmutableSet;

/**
 * No-op implementation of {@link RequestSensors}. This is used when sensors are disabled.
 */
public class NoOpRequestSensors implements RequestSensors
{
    public static final NoOpRequestSensors instance = new NoOpRequestSensors();

    @Override
    public void registerSensor(Context context, Type type)
    {

    }

    @Override
    public Optional<Sensor> getSensor(Context context, Type type)
    {
        return Optional.empty();
    }

    @Override
    public Set<Sensor> getSensors(Type type)
    {
        return ImmutableSet.of();
    }

    @Override
    public void incrementSensor(Context context, Type type, double value)
    {

    }

    @Override
    public void syncAllSensors()
    {

    }
}
