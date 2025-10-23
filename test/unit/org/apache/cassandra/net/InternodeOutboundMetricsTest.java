/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.net;

import com.google.common.net.InetAddresses;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.InternodeOutboundMetrics;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_INTERNODE_OUTBOUND_METRICS_PROVIDER_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;

/**
 * Tests for the InternodeOutboundMetrics factory pattern which allows custom metrics implementations
 * to be configured via the cassandra.custom_internode_outbound_metrics_provider_class property.
 */
public class InternodeOutboundMetricsTest
{
    private static final InetAddressAndPort TEST_ADDR = InetAddressAndPort.getByAddressOverrideDefaults(InetAddresses.forString("127.0.0.2"), 1234);
    private String originalProviderValue = null;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
    }

    @After
    public void tearDown()
    {
        // Restore original property value
        if (originalProviderValue != null)
            CUSTOM_INTERNODE_OUTBOUND_METRICS_PROVIDER_PROPERTY.setString(originalProviderValue);
        else
            System.clearProperty(CUSTOM_INTERNODE_OUTBOUND_METRICS_PROVIDER_PROPERTY.getKey());
    }

    /**
     * Helper method to test the factory method with a given provider class.
     * Saves the original property value, sets the new one, and returns the created metrics.
     */
    private InternodeOutboundMetrics createMetricsWithProvider(String providerClassName)
    {
        originalProviderValue = CUSTOM_INTERNODE_OUTBOUND_METRICS_PROVIDER_PROPERTY.getString();

        if (providerClassName != null)
            CUSTOM_INTERNODE_OUTBOUND_METRICS_PROVIDER_PROPERTY.setString(providerClassName);
        else
            System.clearProperty(CUSTOM_INTERNODE_OUTBOUND_METRICS_PROVIDER_PROPERTY.getKey());

        OutboundConnections connections = OutboundConnections.unsafeCreate(new OutboundConnectionSettings(TEST_ADDR));
        try
        {
            return InternodeOutboundMetrics.create(TEST_ADDR, connections);
        }
        finally
        {
            connections.close(false);
        }
    }

    @Test
    public void testDefaultMetricsProvider()
    {
        InternodeOutboundMetrics metrics = createMetricsWithProvider(null);

        // Verify we get the default implementation
        assertThat(metrics).isNotNull();
        assertThat(metrics.getClass()).isEqualTo(InternodeOutboundMetrics.class);

        // Verify the metrics object is functional - check that gauges are registered
        assertNotNull("largeMessagePendingTasks gauge should be registered", metrics.largeMessagePendingTasks);
        assertNotNull("smallMessagePendingTasks gauge should be registered", metrics.smallMessagePendingTasks);
        assertNotNull("urgentMessagePendingTasks gauge should be registered", metrics.urgentMessagePendingTasks);
    }

    @Test
    public void testCustomMetricsProvider()
    {
        InternodeOutboundMetrics metrics = createMetricsWithProvider(TestInternodeOutboundMetrics.class.getName());

        // Verify we get the custom implementation
        assertThat(metrics).isInstanceOf(TestInternodeOutboundMetrics.class);

        // Verify the custom metrics inherit all functionality from the base class
        assertNotNull("Custom implementation should have largeMessagePendingTasks", metrics.largeMessagePendingTasks);
        assertNotNull("Custom implementation should have smallMessagePendingTasks", metrics.smallMessagePendingTasks);
    }

    @Test(expected = RuntimeException.class)
    public void testInvalidCustomMetricsProvider_ClassNotFound()
    {
        createMetricsWithProvider("org.apache.cassandra.metrics.NonExistentClass");
    }

    @Test
    public void testInvalidCustomMetricsProvider_NoConstructor()
    {
        try
        {
            createMetricsWithProvider(InvalidConstructorMetrics.class.getName());
            assertThat(false).as("Expected RuntimeException for missing constructor").isTrue();
        }
        catch (RuntimeException e)
        {
            // Verify the cause is NoSuchMethodException
            assertThat(e.getCause())
                    .as("Exception should be caused by NoSuchMethodException")
                    .isInstanceOf(NoSuchMethodException.class);
        }
    }

    @Test
    public void testInvalidCustomMetricsProvider_ConstructorThrows()
    {
        try
        {
            createMetricsWithProvider(ThrowingConstructorMetrics.class.getName());
            assertThat(false).as("Expected RuntimeException from constructor failure").isTrue();
        }
        catch (RuntimeException e)
        {
            // Verify the cause is InvocationTargetException wrapping our thrown RuntimeException
            assertThat(e.getCause())
                    .as("Exception should be caused by InvocationTargetException")
                    .isInstanceOf(java.lang.reflect.InvocationTargetException.class);

            assertThat(e.getCause().getCause())
                    .as("InvocationTargetException should wrap the constructor's RuntimeException")
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("Constructor failed");
        }
    }

    /**
     * Test implementation of InternodeOutboundMetrics
     */
    public static class TestInternodeOutboundMetrics extends InternodeOutboundMetrics
    {
        public TestInternodeOutboundMetrics(InetAddressAndPort ip, OutboundConnections messagingPool)
        {
            super(ip, messagingPool);
        }
    }

    /**
     * Test implementation with no matching constructor (only has private no-arg constructor)
     */
    public static class InvalidConstructorMetrics extends InternodeOutboundMetrics
    {
        // Wrong constructor signature - no constructor matching (InetAddressAndPort, OutboundConnections)
        private InvalidConstructorMetrics()
        {
            super(null, null);
        }
    }

    /**
     * Test implementation that throws in constructor
     */
    public static class ThrowingConstructorMetrics extends InternodeOutboundMetrics
    {
        public ThrowingConstructorMetrics(InetAddressAndPort ip, OutboundConnections messagingPool)
        {
            super(ip, messagingPool);
            throw new RuntimeException("Constructor failed");
        }
    }
}
