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
package org.apache.cassandra.metrics;

import java.lang.reflect.Field;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for working with ClientRequestsMetrics.
 */
public class ClientRequestsMetricsUtil
{
    private static final Logger logger = LoggerFactory.getLogger(ClientRequestsMetricsUtil.class);

    /**
     * Clears all metrics in the provided ClientRequestsMetrics instance by replacing
     * the metric objects with new instances.
     *
     * @param metrics The ClientRequestsMetrics instance to clear
     */
    @VisibleForTesting
    public static void clearMetrics(ClientRequestsMetrics metrics)
    {
        if (metrics == null)
            return;

        try
        {
            // Get the namePrefix from one of the existing metrics
            String namePrefix = getNamePrefix(metrics.readMetrics);

            // Release all existing metrics to unregister them
            metrics.release();

            // Replace all metrics with new instances using reflection
            replaceField(metrics, "readMetrics", new ClientRequestMetrics("Read", namePrefix));
            replaceField(metrics, "aggregationMetrics", new ClientRangeRequestMetrics("Aggregation", namePrefix));
            replaceField(metrics, "rangeMetrics", new ClientRangeRequestMetrics("RangeSlice", namePrefix));
            replaceField(metrics, "writeMetrics", new ClientWriteRequestMetrics("Write", namePrefix));
            replaceField(metrics, "casWriteMetrics", new CASClientWriteRequestMetrics("CASWrite", namePrefix));
            replaceField(metrics, "casReadMetrics", new CASClientRequestMetrics("CASRead", namePrefix));
            replaceField(metrics, "viewWriteMetrics", new ViewWriteMetrics("ViewWrite", namePrefix));

            // Replace the consistency level maps
            replaceConsistencyLevelMaps(metrics, namePrefix);

            logger.debug("All metrics in ClientRequestsMetrics have been cleared by replacing with new instances");
        }
        catch (Exception e)
        {
            logger.error("Failed to clear metrics: {}", e.getMessage(), e);
        }
    }

    /**
     * Gets the namePrefix from a ClientRequestMetrics instance
     */
    private static String getNamePrefix(ClientRequestMetrics metrics) throws Exception
    {
        Field field = ClientRequestMetrics.class.getDeclaredField("namePrefix");
        field.setAccessible(true);
        return (String) field.get(metrics);
    }

    /**
     * Replaces a field in the target object with a new value using reflection
     */
    private static void replaceField(Object target, String fieldName, Object newValue) throws Exception
    {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, newValue);
    }

    /**
     * Replaces the consistency level maps with new instances
     */
    @SuppressWarnings("unchecked")
    private static void replaceConsistencyLevelMaps(ClientRequestsMetrics metrics, String namePrefix) throws Exception
    {
        // Get the existing maps
        Field readMapField = ClientRequestsMetrics.class.getDeclaredField("readMetricsMap");
        Field writeMapField = ClientRequestsMetrics.class.getDeclaredField("writeMetricsMap");
        readMapField.setAccessible(true);
        writeMapField.setAccessible(true);

        Map<ConsistencyLevel, ClientRequestMetrics> readMap =
            (Map<ConsistencyLevel, ClientRequestMetrics>) readMapField.get(metrics);
        Map<ConsistencyLevel, ClientWriteRequestMetrics> writeMap =
            (Map<ConsistencyLevel, ClientWriteRequestMetrics>) writeMapField.get(metrics);

        // Clear the existing maps
        readMap.clear();
        writeMap.clear();

        // Populate with new instances
        for (ConsistencyLevel level : ConsistencyLevel.values())
        {
            readMap.put(level, new ClientRequestMetrics("Read-" + level.name(), namePrefix));
            writeMap.put(level, new ClientWriteRequestMetrics("Write-" + level.name(), namePrefix));
        }
    }
}
