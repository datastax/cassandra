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

package org.apache.cassandra.gms;

import java.util.EnumMap;
import java.util.Map;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessagingService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for HCD-241: Fix gossip state deserialization edge cases.
 * 
 * This test verifies that the EndpointState serializer correctly handles malformed or edge-case
 * values when deserializing gossip state from pre-4.0 nodes. Specifically, it tests the scenario
 * where address/port values may not contain the expected delimiter (colon), which would previously
 * cause an ArrayIndexOutOfBoundsException.
 * 
 * The bug occurred in the filterOutgoingState method when converting 4.0+ format states
 * (e.g., INTERNAL_ADDRESS_AND_PORT: "10.0.0.1:7000") to pre-4.0 format states
 * (e.g., INTERNAL_IP: "10.0.0.1", STORAGE_PORT: "7000"). If the value didn't contain
 * a colon separator, the code would attempt to access array index [1] after splitting,
 * causing an exception.
 * 
 * This test ensures that:
 * 1. Values without delimiters are handled gracefully
 * 2. The system falls back to using the entire value when no delimiter is present
 * 3. No ArrayIndexOutOfBoundsException is thrown during deserialization
 */
public class EndpointStateDeserializationEdgeCasesTest
{
    // DSE 6.x legacy ApplicationState ordinals used for backward compatibility
    private static final ApplicationState DSE__STATUS = ApplicationState.values()[0];
    private static final ApplicationState DSE__INTERNAL_IP = ApplicationState.values()[7];
    private static final ApplicationState DSE__NATIVE_TRANSPORT_PORT = ApplicationState.values()[15];
    private static final ApplicationState DSE__STORAGE_PORT = ApplicationState.values()[17];

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    /**
     * Test Case 1: INTERNAL_ADDRESS_AND_PORT without port delimiter
     * 
     * Scenario: A gossip message contains an INTERNAL_ADDRESS_AND_PORT value that is just
     * an IP address without a port (e.g., "10.0.0.1" instead of "10.0.0.1:7000").
     * 
     * Expected behavior:
     * - WITHOUT fix: ArrayIndexOutOfBoundsException when accessing split(":")[1]
     * - WITH fix: The entire value is used as STORAGE_PORT, no exception thrown
     * 
     * This can occur in edge cases where:
     * - Configuration errors result in malformed gossip data
     * - Network issues corrupt the gossip message
     * - Legacy nodes send incomplete state information
     */
    @Test
    public void testInternalAddressAndPortWithoutPortDelimiter()
    {
        // Create a VersionedValue with just an IP address (no port)
        VersionedValue valueWithoutPort = VersionedValue.unsafeMakeVersionedValue("10.0.0.1", 1);
        Map.Entry<ApplicationState, VersionedValue> entry = 
            Map.entry(ApplicationState.INTERNAL_ADDRESS_AND_PORT, valueWithoutPort);

        // Filter for pre-4.0 messaging version (VERSION_30)
        Set<Map.Entry<ApplicationState, VersionedValue>> filtered = 
            EndpointStateSerializer.filterOutgoingStates(Set.of(entry), MessagingService.VERSION_30);

        // Convert to map for easier assertion
        Map<ApplicationState, VersionedValue> filteredMap = new EnumMap<>(ApplicationState.class);
        for (Map.Entry<ApplicationState, VersionedValue> e : filtered)
            filteredMap.put(e.getKey(), e.getValue());

        // Verify the behavior:
        // - Should not throw ArrayIndexOutOfBoundsException
        // - Should map the entire value to STORAGE_PORT (since no colon delimiter exists)
        // - Should NOT create an INTERNAL_IP entry (since split only produces one element)
        assertNotNull("Filtered map should not be null", filteredMap);
        assertTrue("Should contain STORAGE_PORT mapping", 
                   filteredMap.containsKey(DSE__STORAGE_PORT));
        assertEquals("STORAGE_PORT should contain the full IP address", 
                     "10.0.0.1", 
                     filteredMap.get(DSE__STORAGE_PORT).value);
        
        // When there's no colon, we only get one element from split, so only STORAGE_PORT is set
        assertEquals("Should only have one entry when no delimiter present", 
                     1, 
                     filteredMap.size());
    }

    /**
     * Test Case 2: INTERNAL_ADDRESS_AND_PORT with proper port delimiter
     * 
     * Scenario: Normal case where INTERNAL_ADDRESS_AND_PORT contains both IP and port
     * separated by a colon (e.g., "10.0.0.1:7000").
     * 
     * Expected behavior:
     * - Should split correctly into INTERNAL_IP and STORAGE_PORT
     * - This verifies the fix doesn't break the normal case
     */
    @Test
    public void testInternalAddressAndPortWithPortDelimiter()
    {
        // Create a VersionedValue with IP and port
        VersionedValue valueWithPort = VersionedValue.unsafeMakeVersionedValue("10.0.0.1:7000", 1);
        Map.Entry<ApplicationState, VersionedValue> entry = 
            Map.entry(ApplicationState.INTERNAL_ADDRESS_AND_PORT, valueWithPort);

        // Filter for pre-4.0 messaging version
        Set<Map.Entry<ApplicationState, VersionedValue>> filtered = 
            EndpointStateSerializer.filterOutgoingStates(Set.of(entry), MessagingService.VERSION_30);

        Map<ApplicationState, VersionedValue> filteredMap = new EnumMap<>(ApplicationState.class);
        for (Map.Entry<ApplicationState, VersionedValue> e : filtered)
            filteredMap.put(e.getKey(), e.getValue());

        // Verify normal behavior is preserved
        assertEquals("Should have two entries when delimiter present", 
                     2, 
                     filteredMap.size());
        assertEquals("INTERNAL_IP should contain IP address", 
                     "10.0.0.1", 
                     filteredMap.get(DSE__INTERNAL_IP).value);
        assertEquals("STORAGE_PORT should contain port", 
                     "7000", 
                     filteredMap.get(DSE__STORAGE_PORT).value);
    }

    /**
     * Test Case 3: NATIVE_ADDRESS_AND_PORT without port delimiter
     * 
     * Scenario: A gossip message contains a NATIVE_ADDRESS_AND_PORT value without a port
     * (e.g., "127.0.0.1" instead of "127.0.0.1:9042").
     * 
     * Expected behavior:
     * - WITHOUT fix: ArrayIndexOutOfBoundsException when accessing split(":")[1]
     * - WITH fix: The entire value is used as NATIVE_TRANSPORT_PORT
     */
    @Test
    public void testNativeAddressAndPortWithoutPortDelimiter()
    {
        VersionedValue valueWithoutPort = VersionedValue.unsafeMakeVersionedValue("127.0.0.1", 1);
        Map.Entry<ApplicationState, VersionedValue> entry = 
            Map.entry(ApplicationState.NATIVE_ADDRESS_AND_PORT, valueWithoutPort);

        Set<Map.Entry<ApplicationState, VersionedValue>> filtered = 
            EndpointStateSerializer.filterOutgoingStates(Set.of(entry), MessagingService.VERSION_30);

        Map<ApplicationState, VersionedValue> filteredMap = new EnumMap<>(ApplicationState.class);
        for (Map.Entry<ApplicationState, VersionedValue> e : filtered)
            filteredMap.put(e.getKey(), e.getValue());

        assertNotNull("Filtered map should not be null", filteredMap);
        assertTrue("Should contain NATIVE_TRANSPORT_PORT mapping", 
                   filteredMap.containsKey(DSE__NATIVE_TRANSPORT_PORT));
        assertEquals("NATIVE_TRANSPORT_PORT should contain the full IP address", 
                     "127.0.0.1", 
                     filteredMap.get(DSE__NATIVE_TRANSPORT_PORT).value);
        assertEquals("Should only have one entry when no delimiter present", 
                     1, 
                     filteredMap.size());
    }

    /**
     * Test Case 4: NATIVE_ADDRESS_AND_PORT with proper port delimiter
     * 
     * Scenario: Normal case where NATIVE_ADDRESS_AND_PORT contains both IP and port.
     * 
     * Expected behavior:
     * - Should extract only the port for NATIVE_TRANSPORT_PORT
     * - This verifies the fix doesn't break the normal case
     */
    @Test
    public void testNativeAddressAndPortWithPortDelimiter()
    {
        VersionedValue valueWithPort = VersionedValue.unsafeMakeVersionedValue("127.0.0.1:9042", 1);
        Map.Entry<ApplicationState, VersionedValue> entry = 
            Map.entry(ApplicationState.NATIVE_ADDRESS_AND_PORT, valueWithPort);

        Set<Map.Entry<ApplicationState, VersionedValue>> filtered = 
            EndpointStateSerializer.filterOutgoingStates(Set.of(entry), MessagingService.VERSION_30);

        Map<ApplicationState, VersionedValue> filteredMap = new EnumMap<>(ApplicationState.class);
        for (Map.Entry<ApplicationState, VersionedValue> e : filtered)
            filteredMap.put(e.getKey(), e.getValue());

        assertEquals("Should have one entry", 
                     1, 
                     filteredMap.size());
        assertEquals("NATIVE_TRANSPORT_PORT should contain only the port", 
                     "9042", 
                     filteredMap.get(DSE__NATIVE_TRANSPORT_PORT).value);
    }

    /**
     * Test Case 5: STATUS_WITH_PORT without delimiters
     * 
     * Scenario: A gossip message contains a STATUS_WITH_PORT value without the expected
     * delimiters (e.g., "NORMAL" instead of "NORMAL,address:port").
     * 
     * Expected behavior:
     * - WITHOUT fix: ArrayIndexOutOfBoundsException when accessing split("[:,]")[0]
     * - WITH fix: The entire value is used as STATUS
     */
    @Test
    public void testStatusWithPortWithoutDelimiters()
    {
        VersionedValue valueWithoutDelimiters = VersionedValue.unsafeMakeVersionedValue("NORMAL", 1);
        Map.Entry<ApplicationState, VersionedValue> entry = 
            Map.entry(ApplicationState.STATUS_WITH_PORT, valueWithoutDelimiters);

        Set<Map.Entry<ApplicationState, VersionedValue>> filtered = 
            EndpointStateSerializer.filterOutgoingStates(Set.of(entry), MessagingService.VERSION_30);

        Map<ApplicationState, VersionedValue> filteredMap = new EnumMap<>(ApplicationState.class);
        for (Map.Entry<ApplicationState, VersionedValue> e : filtered)
            filteredMap.put(e.getKey(), e.getValue());

        assertNotNull("Filtered map should not be null", filteredMap);
        assertTrue("Should contain STATUS mapping", 
                   filteredMap.containsKey(DSE__STATUS));
        assertEquals("STATUS should contain the full value", 
                     "NORMAL", 
                     filteredMap.get(DSE__STATUS).value);
        assertEquals("Should only have one entry", 
                     1, 
                     filteredMap.size());
    }

    /**
     * Test Case 6: STATUS_WITH_PORT with proper delimiters
     * 
     * Scenario: Normal case where STATUS_WITH_PORT contains status with additional data
     * separated by colon or comma (e.g., "NORMAL,address:port").
     * 
     * Expected behavior:
     * - Should extract only the first part (status) for STATUS
     * - This verifies the fix doesn't break the normal case
     */
    @Test
    public void testStatusWithPortWithDelimiters()
    {
        VersionedValue valueWithDelimiters = 
            VersionedValue.unsafeMakeVersionedValue("NORMAL,127.0.0.1:7000", 1);
        Map.Entry<ApplicationState, VersionedValue> entry = 
            Map.entry(ApplicationState.STATUS_WITH_PORT, valueWithDelimiters);

        Set<Map.Entry<ApplicationState, VersionedValue>> filtered = 
            EndpointStateSerializer.filterOutgoingStates(Set.of(entry), MessagingService.VERSION_30);

        Map<ApplicationState, VersionedValue> filteredMap = new EnumMap<>(ApplicationState.class);
        for (Map.Entry<ApplicationState, VersionedValue> e : filtered)
            filteredMap.put(e.getKey(), e.getValue());

        assertEquals("Should have one entry", 
                     1, 
                     filteredMap.size());
        assertEquals("STATUS should contain only the first part", 
                     "NORMAL", 
                     filteredMap.get(DSE__STATUS).value);
    }
}
