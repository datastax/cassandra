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

/**
 * Listener that gets notified by the {@link SensorsRegistry}
 * about the creation and removal of {@link Sensor}s.
 */
public interface SensorsRegistryListener
{
    /**
     * React to the creation of a new sensor
     * @param sensor the sensor just created
     */
    void onSensorCreated(Sensor sensor);

    /**
     * React to the removal of a sensor
     * @param sensor the sensor just removed
     */
    void onSensorRemoved(Sensor sensor);
}
