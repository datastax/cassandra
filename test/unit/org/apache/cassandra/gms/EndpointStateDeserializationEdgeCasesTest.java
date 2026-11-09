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
import static org.junit.Assert.assertFalse;

/**
 * Address, port and status values written to a peer identified as a DSE 6 node at a pre-4.0 version
 * ({@code DseGossipStates.toDse}, which replaced the HCD-241 translation) when they do not have
 * the expected shape. None of them may fail the serialization, and none may put a non-numeric value into a
 * DSE port state: DSE parses its port states with {@code Integer.parseInt} and drops the whole gossip message
 * when that fails. The translation, which does not read the mode, is called on its own, without the mode.
 */
public class EndpointStateDeserializationEdgeCasesTest
{
    // DSE 6.x ApplicationState ordinals
    private static final ApplicationState DSE__STATUS = ApplicationState.values()[0];
    private static final ApplicationState DSE__INTERNAL_IP = ApplicationState.values()[7];
    private static final ApplicationState DSE__NATIVE_TRANSPORT_ADDRESS = ApplicationState.values()[8];
    private static final ApplicationState DSE__NATIVE_TRANSPORT_PORT = ApplicationState.values()[15];
    private static final ApplicationState DSE__STORAGE_PORT = ApplicationState.values()[17];

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static Map<ApplicationState, VersionedValue> toPre40(ApplicationState state, String value)
    {
        Set<Map.Entry<ApplicationState, VersionedValue>> filtered =
            EndpointStateSerializer.filterOutgoingStates(Set.of(Map.entry(state, VersionedValue.unsafeMakeVersionedValue(value, 1))),
                                                         MessagingService.VERSION_30, true);
        Map<ApplicationState, VersionedValue> filteredMap = new EnumMap<>(ApplicationState.class);
        for (Map.Entry<ApplicationState, VersionedValue> e : filtered)
            filteredMap.put(e.getKey(), e.getValue());
        return filteredMap;
    }

    /** An INTERNAL_ADDRESS_AND_PORT without a port becomes INTERNAL_IP alone, never a STORAGE_PORT holding the address. */
    @Test
    public void testInternalAddressAndPortWithoutPortDelimiter()
    {
        Map<ApplicationState, VersionedValue> filteredMap = toPre40(ApplicationState.INTERNAL_ADDRESS_AND_PORT, "10.0.0.1");
        assertEquals(1, filteredMap.size());
        assertEquals("10.0.0.1", filteredMap.get(DSE__INTERNAL_IP).value);
        assertFalse(filteredMap.containsKey(DSE__STORAGE_PORT));
    }

    /** The normal case: INTERNAL_ADDRESS_AND_PORT splits into INTERNAL_IP and STORAGE_PORT. */
    @Test
    public void testInternalAddressAndPortWithPortDelimiter()
    {
        Map<ApplicationState, VersionedValue> filteredMap = toPre40(ApplicationState.INTERNAL_ADDRESS_AND_PORT, "10.0.0.1:7000");
        assertEquals(2, filteredMap.size());
        assertEquals("10.0.0.1", filteredMap.get(DSE__INTERNAL_IP).value);
        assertEquals("7000", filteredMap.get(DSE__STORAGE_PORT).value);
    }

    /** An IPv6 INTERNAL_ADDRESS_AND_PORT splits at the port, not at the first colon of the address. */
    @Test
    public void testInternalAddressAndPortIpv6()
    {
        Map<ApplicationState, VersionedValue> filteredMap = toPre40(ApplicationState.INTERNAL_ADDRESS_AND_PORT, "[2001:db8:0:0:0:0:0:7]:7000");
        assertEquals(2, filteredMap.size());
        assertEquals("2001:db8:0:0:0:0:0:7", filteredMap.get(DSE__INTERNAL_IP).value);
        assertEquals("7000", filteredMap.get(DSE__STORAGE_PORT).value);
    }

    /** A value that is not an IP address is not sent at all rather than sent into the wrong state. */
    @Test
    public void testInternalAddressAndPortNotAnAddress()
    {
        assertEquals(0, toPre40(ApplicationState.INTERNAL_ADDRESS_AND_PORT, "not-an-address:7000").size());
    }

    /** A NATIVE_ADDRESS_AND_PORT without a port becomes the native transport address alone. */
    @Test
    public void testNativeAddressAndPortWithoutPortDelimiter()
    {
        Map<ApplicationState, VersionedValue> filteredMap = toPre40(ApplicationState.NATIVE_ADDRESS_AND_PORT, "127.0.0.1");
        assertEquals(1, filteredMap.size());
        assertEquals("127.0.0.1", filteredMap.get(DSE__NATIVE_TRANSPORT_ADDRESS).value);
        assertFalse(filteredMap.containsKey(DSE__NATIVE_TRANSPORT_PORT));
    }

    /** The normal case: NATIVE_ADDRESS_AND_PORT becomes DSE's native transport address and port. */
    @Test
    public void testNativeAddressAndPortWithPortDelimiter()
    {
        Map<ApplicationState, VersionedValue> filteredMap = toPre40(ApplicationState.NATIVE_ADDRESS_AND_PORT, "127.0.0.1:9042");
        assertEquals(2, filteredMap.size());
        assertEquals("127.0.0.1", filteredMap.get(DSE__NATIVE_TRANSPORT_ADDRESS).value);
        assertEquals("9042", filteredMap.get(DSE__NATIVE_TRANSPORT_PORT).value);
    }

    /** A STATUS_WITH_PORT without delimiters is sent as STATUS unchanged. */
    @Test
    public void testStatusWithPortWithoutDelimiters()
    {
        Map<ApplicationState, VersionedValue> filteredMap = toPre40(ApplicationState.STATUS_WITH_PORT, "NORMAL");
        assertEquals(1, filteredMap.size());
        assertEquals("NORMAL", filteredMap.get(DSE__STATUS).value);
    }

    /** A STATUS_WITH_PORT keeps its pieces; an address with a port among them loses the port, as in the pre-4.0 STATUS. */
    @Test
    public void testStatusWithPortWithDelimiters()
    {
        assertEquals("NORMAL,127.0.0.1", toPre40(ApplicationState.STATUS_WITH_PORT, "NORMAL,127.0.0.1:7000").get(DSE__STATUS).value);
        assertEquals("BOOT_REPLACE,2001:db8:0:0:0:0:0:7", toPre40(ApplicationState.STATUS_WITH_PORT, "BOOT_REPLACE,[2001:db8::7]:7000").get(DSE__STATUS).value);
        assertEquals("LEFT,-660323125693613959,1790256234000", toPre40(ApplicationState.STATUS_WITH_PORT, "LEFT,-660323125693613959,1790256234000").get(DSE__STATUS).value);
    }
}
