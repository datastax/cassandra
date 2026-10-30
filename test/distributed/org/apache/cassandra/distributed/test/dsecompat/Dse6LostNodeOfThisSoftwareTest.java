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

package org.apache.cassandra.distributed.test.dsecompat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.net.MessagingService;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_CURRENT_VERSION;
import static org.apache.cassandra.config.CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION;
import static org.apache.cassandra.distributed.test.dsecompat.Dse6GuardsTest.SUBNET;
import static org.apache.cassandra.distributed.test.dsecompat.Dse6GuardsTest.formerDseNodes;
import static org.apache.cassandra.distributed.test.dsecompat.Dse6LostDseNodeTest.DSE_NET_VERSION;
import static org.apache.cassandra.distributed.test.dsecompat.Dse6LostDseNodeTest.LOST_DSE_NODE;
import static org.apache.cassandra.distributed.test.dsecompat.Dse6LostDseNodeTest.assassinateALostNode;
import static org.apache.cassandra.distributed.test.dsecompat.Dse6LostDseNodeTest.removeLostNodes;

/**
 * The removal of a node of this software lost for good during the upgrade, by nodes of this software in {@code DSE_6}
 * mode and without the mode, as {@link Dse6LostDseNodeTest} removes a lost DSE 6 node (with its helpers): removenode
 * once no DSE 6 node is alive (its data streamed between nodes of this software), assassinate while one is. The lost
 * node is a gossip state injected on node 1 as a ring member that is down, gossiping 4.0 as a node of this software in
 * {@code DSE_6} mode does, or this software's own version as one without the mode does. A class of its own, so that one
 * test JVM starts fewer clusters.
 */
public class Dse6LostNodeOfThisSoftwareTest extends TestBaseImpl
{
    /** A node of this software lost for good, injected on node 1 as {@link Dse6LostDseNodeTest} injects a DSE 6 node. */
    private static final String LOST_NODE = "127.0.10.97:7012";
    private static final String SUBJECT = "the node /" + LOST_NODE + " of this software";

    /** As in {@link Dse6GuardsTest}: the trie version of the mode is a JVM-wide property, and the SAI pin is taken as set. */
    private WithProperties trieIndexFormatVersion;
    private WithProperties saiVersionPin;

    @Before
    public void setSystemProperties()
    {
        trieIndexFormatVersion = new WithProperties().clear(TRIE_INDEX_FORMAT_VERSION);
        saiVersionPin = new WithProperties().set(SAI_CURRENT_VERSION, "aa");
    }

    @After
    public void restoreSystemProperties()
    {
        saiVersionPin.close();
        trieIndexFormatVersion.close();
    }

    /**
     * A node of this software lost for good is removed by {@code nodetool removenode} in {@code DSE_6} mode once no DSE
     * 6 node is alive: its ranges are streamed between the nodes of this software.
     */
    @Test
    public void aLostNodeOfThisSoftwareIsRemovedOnceNoDseNodeIsAlive() throws Throwable
    {
        try (Cluster cluster = formerDseNodes(2, builder -> {}))
        {
            removeLostNodes(cluster, new String[]{ LOST_NODE }, new int[]{ MessagingService.VERSION_40 }, new String[]{ SUBJECT });
        }
    }

    /**
     * The same without the mode, where a lost DSE 6 node keeps the upgrade in progress: the lost node of this software is
     * removed by {@code nodetool removenode}, the lost DSE 6 node then too, and every range is streamed between the nodes
     * of this software.
     */
    @Test
    public void aLostNodeOfThisSoftwareIsRemovedWithoutTheMode() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2).withSubnet(SUBNET).withDynamicPortAllocation(true)
                                           .withConfig(c -> c.with(Feature.NETWORK, Feature.GOSSIP)).start()))
        {
            removeLostNodes(cluster,
                            new String[]{ LOST_NODE, LOST_DSE_NODE },
                            new int[]{ MessagingService.VERSION_DS_20, DSE_NET_VERSION },
                            new String[]{ SUBJECT, "the DSE 6 node /" + LOST_DSE_NODE });
        }
    }

    /**
     * A node of this software lost for good while a DSE 6 node is alive: removenode, which would stream with the live
     * DSE 6 node, is refused with the way out, and assassinate, which streams nothing, removes the lost node.
     */
    @Test
    public void aLostNodeOfThisSoftwareIsAssassinatedWhileADseNodeIsAlive() throws Throwable
    {
        try (Cluster cluster = formerDseNodes(2, builder -> {}))
        {
            assassinateALostNode(cluster.get(1), LOST_NODE, MessagingService.VERSION_40, SUBJECT);
        }
    }
}
