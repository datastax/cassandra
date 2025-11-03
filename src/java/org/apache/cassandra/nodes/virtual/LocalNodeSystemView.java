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
package org.apache.cassandra.nodes.virtual;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.nodes.ILocalInfo;
import org.apache.cassandra.nodes.Nodes;
import org.apache.cassandra.nodes.TruncationRecord;
import org.apache.cassandra.utils.FBUtilities;

public final class LocalNodeSystemView extends NodeSystemView
{
    private static final Logger logger = LoggerFactory.getLogger(LocalNodeSystemView.class);
    public static final String KEY = "local";

    public LocalNodeSystemView()
    {
        super(NodesSystemViews.virtualFromLegacy(NodesSystemViews.LocalMetadata, NodeConstants.LOCAL_VIEW_NAME));
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet dataset = new SimpleDataSet(metadata());

        // Have to duplicate the current LocalInfo object as it may change while we're constructing the row,
        // so null-values could sneak in and cause NPEs during serialization.
        ILocalInfo l = Nodes.local().get().duplicate();

        dataset = dataset.row(KEY)
                         //+ "bootstrapped text,"
                         .column("bootstrapped", safeToString(l.getBootstrapState()))
                         //+ "broadcast_address inet,"
                         .column("broadcast_address",
                                 l.getBroadcastAddressAndPort() == null ? null
                                                                        : l.getBroadcastAddressAndPort().getAddress())
                         //+ "broadcast_port int,"
                         .column("broadcast_port",
                                 l.getBroadcastAddressAndPort() == null ? null
                                                                        : l.getBroadcastAddressAndPort().getPort())
                         //+ "cluster_name text,"
                         .column("cluster_name", l.getClusterName())
                         //+ "cql_version text,"
                         .column("cql_version", safeToString(l.getCqlVersion()))
                         //+ "data_center text,"
                         .column("data_center", safeToString(l.getDataCenter()))
                         //+ "gossip_generation int,"
                         .column("gossip_generation", Gossiper.instance.getCurrentGenerationNumber(FBUtilities.getBroadcastAddressAndPort()))
                         //+ "listen_address inet,"
                         .column("listen_address",
                                 l.getListenAddressAndPort() == null ? null
                                                                     : l.getListenAddressAndPort().getAddress())
                         //+ "listen_address int,"
                         .column("listen_port",
                                 l.getListenAddressAndPort() == null ? null
                                                                     : l.getListenAddressAndPort().getPort())
                         //+ "rpc_address inet,"
                         .column("rpc_address",
                                 l.getNativeTransportAddressAndPort() == null ? null
                                                                              : l.getNativeTransportAddressAndPort().getAddress())
                         //+ "rpc_port int,"
                         .column("rpc_port",
                                 l.getNativeTransportAddressAndPort() == null ? null
                                                                              : l.getNativeTransportAddressAndPort().getPort())
                         //+ "native_protocol_version text,"
                         .column("native_protocol_version", l.getNativeProtocolVersion())
                         //+ "partitioner text,"
                         .column("partitioner", l.getPartitioner())
                         //+ "truncated_at map<uuid, blob>,"
                         .column("truncated_at", truncationRecords(l));

        return completeRow(dataset, l);
    }

    public Map<UUID, ByteBuffer> truncationRecords(ILocalInfo l)
    {
        Map<UUID, TruncationRecord> truncationRecords = l.getTruncationRecords();
        if (truncationRecords.isEmpty())
            return null;
        return truncationRecords.entrySet()
                                .stream()
                                .collect(Collectors.toMap(e -> e.getKey(),
                                                          e -> truncationAsMapEntry(e.getValue()),
                                                          (a, b) -> a,
                                                          this::newMap));
    }

    public <K, V> Map<K, V> newMap()
    {
        return new HashMap<>(16);
    }

    private static ByteBuffer truncationAsMapEntry(TruncationRecord truncationRecord)
    {
        try (DataOutputBuffer out = DataOutputBuffer.scratchBuffer.get())
        {
            CommitLogPosition.serializer.serialize(truncationRecord.position, out);
            out.writeLong(truncationRecord.truncatedAt);
            return out.asNewBuffer();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}

