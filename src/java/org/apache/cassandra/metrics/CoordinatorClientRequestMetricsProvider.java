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


/**
 * Provides access to the {@link CoordinatorClientRequestMetrics} instance used by this node
 * and provides per-tenant metrics in CNDB.
 */
public interface CoordinatorClientRequestMetricsProvider
{
    String CUSTOM_CLIENT_REQUEST_METRICS_PROVIDER_PROPERTY = "cassandra.custom_client_request_metrics_provider_class";
    CoordinatorClientRequestMetricsProvider instance = System.getProperty(CUSTOM_CLIENT_REQUEST_METRICS_PROVIDER_PROPERTY) == null ?
                                                       new DefaultCoordinatorMetricsProvider() :
                                                       make(System.getProperty(CUSTOM_CLIENT_REQUEST_METRICS_PROVIDER_PROPERTY));

    CoordinatorClientRequestMetrics metrics(String keyspace);

    static CoordinatorClientRequestMetricsProvider make(String customImpl)
    {
        try
        {
            return (CoordinatorClientRequestMetricsProvider) Class.forName(customImpl).newInstance();
        }
        catch (Throwable ex)
        {
            throw new IllegalStateException("Unknown client request metrics provider: " + customImpl);
        }
    }

    class DefaultCoordinatorMetricsProvider implements CoordinatorClientRequestMetricsProvider
    {
        private final CoordinatorClientRequestMetrics metrics = new CoordinatorClientRequestMetrics();

        @Override
        public CoordinatorClientRequestMetrics metrics(String keyspace)
        {
            return metrics;
        }
    }
}
