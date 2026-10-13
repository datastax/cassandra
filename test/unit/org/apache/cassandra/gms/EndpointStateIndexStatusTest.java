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

import java.io.IOException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexStatusFormat;
import org.apache.cassandra.index.IndexStatusManager;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.CassandraVersion;
import org.json.simple.JSONValue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

/**
 * HCD-623: HCD 1.2 nodes (messaging version up to {@link MessagingService#VERSION_DS_12}) can only parse the legacy
 * index status format, so the compact one must be rewritten when gossiping with them.
 */
public class EndpointStateIndexStatusTest
{
    private static final Map<String, Index.Status> STATUSES = Map.of("ks1.idx1", Index.Status.BUILD_SUCCEEDED,
                                                                     "ks1.idx2", Index.Status.FULL_REBUILD_STARTED,
                                                                     "ks2.idx3", Index.Status.BUILD_FAILED);

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testLegacyConversionOfCompactFormat() throws Exception
    {
        String compact = IndexStatusFormat.toSerializedFormat(STATUSES);
        String legacy = IndexStatusFormat.toLegacySerializedFormat(compact);

        assertEquals(STATUSES, parseAsHcd12(legacy));
        assertEquals(STATUSES, IndexStatusFormat.fromSerializedFormat(legacy));
    }

    @Test
    public void testLegacyConversionKeepsLegacyAndUnparseableValues()
    {
        String legacy = IndexStatusFormat.toLegacySerializedFormat(STATUSES);
        assertSame(legacy, IndexStatusFormat.toLegacySerializedFormat(legacy));

        String empty = IndexStatusFormat.toSerializedFormat(Map.of());
        assertSame(empty, IndexStatusFormat.toLegacySerializedFormat(empty));

        String garbage = "not json";
        assertSame(garbage, IndexStatusFormat.toLegacySerializedFormat(garbage));

        String malformed = "{\"ks1\": {\"idx1\": 3}";
        assertSame(malformed, IndexStatusFormat.toLegacySerializedFormat(malformed));

        // a code this version does not know: passed through, the peer will fail to parse it in any format
        String unknownCode = "{\"ks1\": {\"idx1\": 99}}";
        assertSame(unknownCode, IndexStatusFormat.toLegacySerializedFormat(unknownCode));
    }

    @Test
    public void testFormatChosenByMinVersion()
    {
        String legacy = IndexStatusFormat.toLegacySerializedFormat(STATUSES);
        String compact = IndexStatusFormat.toSerializedFormat(STATUSES);

        assertEquals(legacy, IndexStatusFormat.toSerializedFormat(null, STATUSES));
        assertEquals(legacy, IndexStatusFormat.toSerializedFormat(new CassandraVersion("5.0.2"), STATUSES));
        assertEquals(compact, IndexStatusFormat.toSerializedFormat(new CassandraVersion("5.0.3"), STATUSES));
        assertEquals(compact, IndexStatusFormat.toSerializedFormat(new CassandraVersion("5.0.7.0"), STATUSES));
        // HCD 1.2 gets the legacy format from the gossip serializer, whatever the minimum version
        assertEquals(compact, IndexStatusFormat.toSerializedFormat(new CassandraVersion("4.0.11.0"), STATUSES));

        boolean forced = DatabaseDescriptor.getForceOptimizedIndexStatusFormat();
        try
        {
            DatabaseDescriptor.setForceOptimizedIndexStatusFormat(true);
            assertEquals(compact, IndexStatusFormat.toSerializedFormat(null, STATUSES));
            assertEquals(compact, IndexStatusFormat.toSerializedFormat(new CassandraVersion("5.0.2"), STATUSES));
        }
        finally
        {
            DatabaseDescriptor.setForceOptimizedIndexStatusFormat(forced);
        }
    }

    @Test
    public void testLegacyValueRelayedBackIsParsed() throws Exception
    {
        // an HCD 1.2 node relays our legacy rewrite of a peer's status to another HCD 2.0 node
        InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.42");
        String legacy = IndexStatusFormat.toLegacySerializedFormat(IndexStatusFormat.toSerializedFormat(STATUSES));
        IndexStatusManager.instance.receivePeerIndexStatus(peer, VersionedValue.unsafeMakeVersionedValue(legacy, 1));

        for (Map.Entry<String, Index.Status> e : STATUSES.entrySet())
        {
            String[] keyspaceAndIndex = e.getKey().split("\\.");
            assertEquals(e.getValue(), IndexStatusManager.instance.getIndexStatus(peer, keyspaceAndIndex[0], keyspaceAndIndex[1]));
        }
    }

    @Test
    public void testIndexStatusRewrittenForHcd12Peers() throws Exception
    {
        String compact = IndexStatusFormat.toSerializedFormat(STATUSES);

        // HCD 1.2 (up to VERSION_DS_12), and Apache Cassandra 5.0 which parses both formats
        for (int version : new int[]{ MessagingService.VERSION_40, MessagingService.VERSION_50,
                                      MessagingService.VERSION_DS_10, MessagingService.VERSION_DS_11,
                                      MessagingService.VERSION_DS_12 })
        {
            VersionedValue received = roundTrip(compact, version).getApplicationState(ApplicationState.INDEX_STATUS);
            assertEquals(STATUSES, parseAsHcd12(received.value));
            assertEquals(42, received.version);
        }
    }

    @Test
    public void testIndexStatusUnchangedForHcd20Peers() throws Exception
    {
        String compact = IndexStatusFormat.toSerializedFormat(STATUSES);

        for (int version : new int[]{ MessagingService.VERSION_DS_20, MessagingService.VERSION_DSE_68 })
        {
            EndpointState received = roundTrip(compact, version);
            VersionedValue status = received.getApplicationState(ApplicationState.INDEX_STATUS);
            assertEquals(compact, status.value);
            assertEquals(42, status.version);
            assertEquals("other", received.getApplicationState(ApplicationState.HOST_ID).value);
        }
    }

    @Test
    public void testIndexStatusDroppedForPre40Peers() throws Exception
    {
        String compact = IndexStatusFormat.toSerializedFormat(STATUSES);
        assertNull(roundTrip(compact, MessagingService.VERSION_30).getApplicationState(ApplicationState.INDEX_STATUS));
    }

    @Test
    public void testOtherStatesUnchangedForHcd12Peers() throws Exception
    {
        String compact = IndexStatusFormat.toSerializedFormat(STATUSES);

        EndpointState received = roundTrip(compact, MessagingService.VERSION_DS_12);
        assertEquals(2, received.states().size());
        assertEquals("other", received.getApplicationState(ApplicationState.HOST_ID).value);
        assertEquals(7, received.getApplicationState(ApplicationState.HOST_ID).version);
    }

    private static EndpointState roundTrip(String indexStatus, int version) throws IOException
    {
        Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
        states.put(ApplicationState.INDEX_STATUS, VersionedValue.unsafeMakeVersionedValue(indexStatus, 42));
        states.put(ApplicationState.HOST_ID, VersionedValue.unsafeMakeVersionedValue("other", 7));
        EndpointState state = new EndpointState(new HeartBeatState(1, 2), states);

        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            EndpointState.serializer.serialize(state, out, version);
            assertEquals(EndpointState.serializer.serializedSize(state, version), out.getLength());
            try (DataInputBuffer in = new DataInputBuffer(out.getData()))
            {
                return EndpointState.serializer.deserialize(in, version);
            }
        }
    }

    /**
     * Parses the index status like HCD 1.2 does in {@code SecondaryIndexManager.receivePeerIndexStatus}.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Index.Status> parseAsHcd12(String value) throws Exception
    {
        Map<String, String> peerStatus = (Map<String, String>) JSONValue.parseWithException(value);
        Map<String, Index.Status> indexStatus = new HashMap<>();
        for (Map.Entry<String, String> e : peerStatus.entrySet())
            indexStatus.put(e.getKey(), Index.Status.valueOf(e.getValue()));
        return indexStatus;
    }
}
