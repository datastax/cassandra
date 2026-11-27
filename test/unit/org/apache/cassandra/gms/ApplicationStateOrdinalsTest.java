/*
 * Copyright IBM Corp.
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

package org.apache.cassandra.gms;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Gossip states are serialized by ordinal: an ordinal must never change, and the padding states are used by position
 * by nodes of other versions. New states replace a padding state in place.
 */
public class ApplicationStateOrdinalsTest
{
    @Test
    public void testOrdinals()
    {
        Map<String, Integer> expected = new LinkedHashMap<>();
        expected.put("STATUS", 0);
        expected.put("LOAD", 1);
        expected.put("SCHEMA", 2);
        expected.put("DC", 3);
        expected.put("RACK", 4);
        expected.put("RELEASE_VERSION", 5);
        expected.put("REMOVAL_COORDINATOR", 6);
        expected.put("INTERNAL_IP", 7);
        expected.put("RPC_ADDRESS", 8);
        expected.put("X_11_PADDING", 9);
        expected.put("SEVERITY", 10);
        expected.put("NET_VERSION", 11);
        expected.put("HOST_ID", 12);
        expected.put("TOKENS", 13);
        expected.put("RPC_READY", 14);
        expected.put("INTERNAL_ADDRESS_AND_PORT", 15);
        expected.put("NATIVE_ADDRESS_AND_PORT", 16);
        expected.put("STATUS_WITH_PORT", 17);
        expected.put("SSTABLE_VERSIONS", 18);
        expected.put("DISK_USAGE", 19);
        expected.put("INDEX_STATUS", 20);
        expected.put("X1", 21);
        expected.put("X2", 22);
        expected.put("X3", 23);
        expected.put("X4", 24);
        expected.put("X5", 25);
        expected.put("X6", 26);
        expected.put("X7", 27);
        expected.put("X8", 28);
        expected.put("X9", 29);
        expected.put("SHRINK_TOKENS_SUPPORTED", 30);
        Map<String, Integer> actual = new LinkedHashMap<>();
        for (ApplicationState state : ApplicationState.values())
            actual.put(state.name(), state.ordinal());
        assertThat(actual).containsExactlyEntriesOf(expected);
    }
}
