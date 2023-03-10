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

import java.util.Collections;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ClientMetricsTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.toolInitialization();
    }

    @Test
    public void testUpdateIfInitialized()
    {
        assertThat(ClientMetrics.instance.getPausedConnections()).isEmpty();

        // Check that updating ClientMetrics without initialization will throw a NPE
        assertThatThrownBy(ClientMetrics.instance::unpauseConnection).isInstanceOf(NullPointerException.class);

        // Check that updating CLientMetrics through the updateIfInitialized method is a no-op
        ClientMetrics.updateIfInitialized(ClientMetrics::unpauseConnection);

        // Now initialize the metrics and check updates through the updateIfInitialized have the desired effect
        ClientMetrics.instance.init(Collections.emptyList());

        ClientMetrics.updateIfInitialized(ClientMetrics::pauseConnection);
        assertThat(ClientMetrics.instance.getPausedConnections()).isPresent().hasValue(1);

        ClientMetrics.updateIfInitialized(ClientMetrics::unpauseConnection);
        assertThat(ClientMetrics.instance.getPausedConnections()).isPresent().hasValue(0);
    }
}