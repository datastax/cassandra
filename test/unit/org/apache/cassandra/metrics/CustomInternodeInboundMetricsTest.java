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

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.InboundMessageHandlers;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_INTERNODE_INBOUND_METRICS_PROVIDER_PROPERTY;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test for custom internode inbound metrics provider functionality.
 *
 */
public class CustomInternodeInboundMetricsTest
{
    @After
    public void teardown()
    {
        System.clearProperty(CUSTOM_INTERNODE_INBOUND_METRICS_PROVIDER_PROPERTY.getKey());
    }

    @Test
    public void testInvalidClassNameThrowsException() throws Exception
    {
        CUSTOM_INTERNODE_INBOUND_METRICS_PROVIDER_PROPERTY.setString("org.apache.cassandra.metrics.NonExistentClass");

        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.1");
        InboundMessageHandlers handlers = null;

        assertThatThrownBy(() -> InternodeInboundMetrics.create(peer, handlers))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("NonExistentClass");
    }

    @Test
    public void testCustomProviderWithoutProperConstructorThrowsException() throws Exception
    {
        // When custom provider doesn't have required constructor (InetAddressAndPort, InboundMessageHandlers),
        // should throw RuntimeException with NoSuchMethodException as cause
        CUSTOM_INTERNODE_INBOUND_METRICS_PROVIDER_PROPERTY.setString(InvalidCustomMetrics.class.getName());

        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.1");
        InboundMessageHandlers handlers = null;

        assertThatThrownBy(() -> InternodeInboundMetrics.create(peer, handlers))
            .isInstanceOf(RuntimeException.class)
            .hasCauseInstanceOf(NoSuchMethodException.class);
    }

    @Test
    public void testNonPublicConstructorThrowsException() throws Exception
    {
        // When custom provider has private constructor, should throw RuntimeException with IllegalAccessException
        CUSTOM_INTERNODE_INBOUND_METRICS_PROVIDER_PROPERTY.setString(PrivateConstructorMetrics.class.getName());

        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.1");
        InboundMessageHandlers handlers = null;

        assertThatThrownBy(() -> InternodeInboundMetrics.create(peer, handlers))
            .isInstanceOf(RuntimeException.class)
            .hasCauseInstanceOf(IllegalAccessException.class);
    }

    @Test
    public void testAbstractClassThrowsException() throws Exception
    {
        // When custom provider is abstract, should throw RuntimeException with InstantiationException
        CUSTOM_INTERNODE_INBOUND_METRICS_PROVIDER_PROPERTY.setString(AbstractCustomMetrics.class.getName());

        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.1");
        InboundMessageHandlers handlers = null;

        assertThatThrownBy(() -> InternodeInboundMetrics.create(peer, handlers))
            .isInstanceOf(RuntimeException.class)
            .hasCauseInstanceOf(InstantiationException.class);
    }

    /**
     * Invalid implementation missing required constructor signature
     */
    public static class InvalidCustomMetrics extends InternodeInboundMetrics
    {
        // Missing required constructor (InetAddressAndPort, InboundMessageHandlers)
        public InvalidCustomMetrics(String wrongParam)
        {
            super(null, null);
        }
    }

    /**
     * Invalid implementation with private constructor
     */
    public static class PrivateConstructorMetrics extends InternodeInboundMetrics
    {
        private PrivateConstructorMetrics(InetAddressAndPort peer, InboundMessageHandlers handlers)
        {
            super(peer, handlers);
        }
    }

    /**
     * Invalid abstract implementation
     */
    public static abstract class AbstractCustomMetrics extends InternodeInboundMetrics
    {
        public AbstractCustomMetrics(InetAddressAndPort peer, InboundMessageHandlers handlers)
        {
            super(peer, handlers);
        }
    }
}
