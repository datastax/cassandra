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

import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.nodes.IPeerInfo;
import org.apache.cassandra.nodes.Nodes;

/**
 * A system view of the legacy system.peers table
 */
public class LegacyPeersSystemView extends NodeSystemView
{
    public LegacyPeersSystemView()
    {
        super(NodesSystemViews.virtualFromLegacy(NodesSystemViews.LegacyPeersMetadata, NodeConstants.LEGACY_PEERS_VIEW_NAME));
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet dataset = new SimpleDataSet(metadata());

        Nodes.peers()
             .get()
             .forEach(p -> {
                 // Have to duplicate the current PeerInfo object as it may change while we're constructing the row,
                 // so null-values could sneak in and cause NPEs during serialization.
                 IPeerInfo peerInfo = p.duplicate();

                 dataset.row(peerInfo.getPeerAddressAndPort().getAddress())
                        //+ "preferred_ip inet,"
                        .column("preferred_ip",
                                peerInfo.getPreferredAddressAndPort() == null ? null
                                                         : peerInfo.getPreferredAddressAndPort().getAddress())
                        //+ "rpc_address inet,"
                        .column("rpc_address",
                                peerInfo.getNativeTransportAddressAndPort() == null ? null
                                                                             : peerInfo.getNativeTransportAddressAndPort().getAddress());
                 completeRow(dataset, peerInfo);
             });

        return dataset;
    }
}

