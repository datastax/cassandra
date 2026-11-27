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
package org.apache.cassandra.net;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.cassandra.auth.CassandraAuthorizer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.InternodeOutboundMetrics;
import org.apache.cassandra.metrics.MessagingMetrics;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.service.Dse6AccessControlCheck;
import org.apache.cassandra.service.Dse6NonCoreTraces;
import org.apache.cassandra.service.Dse6SaiVersionPin;
import org.apache.cassandra.service.DsePeersRecord;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MBeanWrapper;

public class MessagingServiceMBeanImpl implements MessagingServiceMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.net:type=MessagingService";

    // we use CHM deliberately instead of NBHM, as both are non-blocking for readers (which this map mostly is used for)
    // and CHM permits prompter GC
    public final ConcurrentMap<InetAddressAndPort, OutboundConnections> channelManagers = new ConcurrentHashMap<>();
    public final ConcurrentMap<InetAddressAndPort, InboundMessageHandlers> messageHandlers = new ConcurrentHashMap<>();

    public final EndpointMessagingVersions versions;
    public final MessagingMetrics metrics;

    public MessagingServiceMBeanImpl(boolean testOnly, EndpointMessagingVersions versions, MessagingMetrics metrics)
    {
        this.versions = versions;
        this.metrics = metrics;
        if (!testOnly)
        {
            MBeanWrapper.instance.registerMBean(this, MBEAN_NAME);
            metrics.scheduleLogging();
        }
    }

    @Override
    public Map<String, Integer> getLargeMessagePendingTasks()
    {
        Map<String, Integer> pendingTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            pendingTasks.put(entry.getKey().toString(false), entry.getValue().large.pendingCount());
        return pendingTasks;
    }

    @Override
    public Map<String, Long> getLargeMessageCompletedTasks()
    {
        Map<String, Long> completedTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            completedTasks.put(entry.getKey().toString(false), entry.getValue().large.sentCount());
        return completedTasks;
    }

    @Override
    public Map<String, Long> getLargeMessageDroppedTasks()
    {
        Map<String, Long> droppedTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            droppedTasks.put(entry.getKey().toString(false), entry.getValue().large.dropped());
        return droppedTasks;
    }

    @Override
    public Map<String, Integer> getSmallMessagePendingTasks()
    {
        Map<String, Integer> pendingTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            pendingTasks.put(entry.getKey().toString(false), entry.getValue().small.pendingCount());
        return pendingTasks;
    }

    @Override
    public Map<String, Long> getSmallMessageCompletedTasks()
    {
        Map<String, Long> completedTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            completedTasks.put(entry.getKey().toString(false), entry.getValue().small.sentCount());
        return completedTasks;
    }

    @Override
    public Map<String, Long> getSmallMessageDroppedTasks()
    {
        Map<String, Long> droppedTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            droppedTasks.put(entry.getKey().toString(false), entry.getValue().small.dropped());
        return droppedTasks;
    }

    @Override
    public Map<String, Integer> getGossipMessagePendingTasks()
    {
        Map<String, Integer> pendingTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            pendingTasks.put(entry.getKey().toString(false), entry.getValue().urgent.pendingCount());
        return pendingTasks;
    }

    @Override
    public Map<String, Long> getGossipMessageCompletedTasks()
    {
        Map<String, Long> completedTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            completedTasks.put(entry.getKey().toString(false), entry.getValue().urgent.sentCount());
        return completedTasks;
    }

    @Override
    public Map<String, Long> getGossipMessageDroppedTasks()
    {
        Map<String, Long> droppedTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            droppedTasks.put(entry.getKey().toString(false), entry.getValue().urgent.dropped());
        return droppedTasks;
    }

    @Override
    public Map<String, Integer> getLargeMessagePendingTasksWithPort()
    {
        Map<String, Integer> pendingTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            pendingTasks.put(entry.getKey().toString(), entry.getValue().large.pendingCount());
        return pendingTasks;
    }

    @Override
    public Map<String, Long> getLargeMessageCompletedTasksWithPort()
    {
        Map<String, Long> completedTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            completedTasks.put(entry.getKey().toString(), entry.getValue().large.sentCount());
        return completedTasks;
    }

    @Override
    public Map<String, Long> getLargeMessageDroppedTasksWithPort()
    {
        Map<String, Long> droppedTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            droppedTasks.put(entry.getKey().toString(), entry.getValue().large.dropped());
        return droppedTasks;
    }

    @Override
    public Map<String, Integer> getSmallMessagePendingTasksWithPort()
    {
        Map<String, Integer> pendingTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            pendingTasks.put(entry.getKey().toString(), entry.getValue().small.pendingCount());
        return pendingTasks;
    }

    @Override
    public Map<String, Long> getSmallMessageCompletedTasksWithPort()
    {
        Map<String, Long> completedTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            completedTasks.put(entry.getKey().toString(), entry.getValue().small.sentCount());
        return completedTasks;
    }

    @Override
    public Map<String, Long> getSmallMessageDroppedTasksWithPort()
    {
        Map<String, Long> droppedTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            droppedTasks.put(entry.getKey().toString(), entry.getValue().small.dropped());
        return droppedTasks;
    }

    @Override
    public Map<String, Integer> getGossipMessagePendingTasksWithPort()
    {
        Map<String, Integer> pendingTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            pendingTasks.put(entry.getKey().toString(), entry.getValue().urgent.pendingCount());
        return pendingTasks;
    }

    @Override
    public Map<String, Long> getGossipMessageCompletedTasksWithPort()
    {
        Map<String, Long> completedTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            completedTasks.put(entry.getKey().toString(), entry.getValue().urgent.sentCount());
        return completedTasks;
    }

    @Override
    public Map<String, Long> getGossipMessageDroppedTasksWithPort()
    {
        Map<String, Long> droppedTasks = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
            droppedTasks.put(entry.getKey().toString(), entry.getValue().urgent.dropped());
        return droppedTasks;
    }

    @Override
    public Map<String, Integer> getDroppedMessages()
    {
        return metrics.getDroppedMessages();
    }
    
    @Override
    public Map<String, Long> getDroppedMutationsByTable()
    {
        return metrics.getDroppedMutationsByTable();
    }

    @Override
    public long getTotalTimeouts()
    {
        return InternodeOutboundMetrics.totalExpiredCallbacks.getCount();
    }

    // these are not messages that time out on sending, but callbacks that timedout without receiving a response
    @Override
    public Map<String, Long> getTimeoutsPerHost()
    {
        Map<String, Long> result = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
        {
            String ip = entry.getKey().toString(false);
            long recent = entry.getValue().expiredCallbacks();
            result.put(ip, recent);
        }
        return result;
    }

    // these are not messages that time out on sending, but callbacks that timedout without receiving a response
    @Override
    public Map<String, Long> getTimeoutsPerHostWithPort()
    {
        Map<String, Long> result = new HashMap<>(channelManagers.size());
        for (Map.Entry<InetAddressAndPort, OutboundConnections> entry : channelManagers.entrySet())
        {
            String ip = entry.getKey().toString();
            long recent = entry.getValue().expiredCallbacks();
            result.put(ip, recent);
        }
        return result;
    }

    @Override
    public Map<String, Double> getBackPressurePerHost()
    {
        throw new UnsupportedOperationException("This feature has been removed");
    }

    @Override
    public void setBackPressureEnabled(boolean enabled)
    {
        throw new UnsupportedOperationException("This feature has been removed");
    }

    @Override
    public boolean isBackPressureEnabled()
    {
        return false;
    }

    @Override
    public void reloadSslCertificates()
    {
        SSLFactory.forceCheckCertFiles();
    }

    @Override
    public int getVersion(String address) throws UnknownHostException
    {
        return versions.get(address);
    }

    @Override
    public String getInternodeCompatibilityMode()
    {
        return DatabaseDescriptor.getInternodeCompatibilityMode().name();
    }

    @Override
    public String getStorageCompatibilityMode()
    {
        return DatabaseDescriptor.getStorageCompatibilityMode().name();
    }

    @Override
    public String getSSTableFormatVersion()
    {
        SSTableFormat<?, ?> format = DatabaseDescriptor.getSelectedSSTableFormat();
        return format.name() + '-' + format.getLatestVersion().version;
    }

    @Override
    public Map<String, Integer> getEndpointMessagingVersions()
    {
        Map<String, Integer> result = new HashMap<>();
        for (Map.Entry<InetAddressAndPort, Integer> e : versions.snapshot().entrySet())
            result.put(e.getKey().getHostAddressAndPort(), e.getValue());
        return result;
    }

    @Override
    public Map<String, String> getDsePeers()
    {
        // outside a DSE compatibility mode no peer is a DSE node this node talks to
        if (!DseCompat.enabled())
            return Collections.emptyMap();
        Map<String, String> result = new HashMap<>();
        Map<InetAddressAndPort, String> identified = DsePeers.snapshot();
        InetAddressAndPort self = FBUtilities.getBroadcastAddressAndPort();
        for (Map.Entry<InetAddressAndPort, EndpointState> e : Gossiper.instance.endpointStateMap.entrySet())
        {
            // a DSE node that left or was removed stays in gossip for days: it is not a peer any more
            if (e.getKey().equals(self) || DsePeers.hasLeftTheRing(e.getValue()))
                continue;
            // DSE 6's own messaging version in DSE_6, 10 or 11 in DSE_5_1
            if (!DsePeers.gossipsDseVersion(e.getValue()))
                continue;
            String version = DseCompat.isDse51() ? e.getValue().getApplicationState(ApplicationState.NET_VERSION).value
                                                 : String.valueOf(DsePeers.gossipedDseVersion(e.getValue()));
            VersionedValue release = e.getValue().getApplicationState(ApplicationState.RELEASE_VERSION);
            StringBuilder description = new StringBuilder("release ").append(release == null ? "unknown" : release.value)
                                                                     .append(", NET_VERSION ").append(version);
            // DSE gossips its DSE_GOSSIP_STATE at the ordinal of this software's X_11_PADDING
            String dseVersion = DsePeers.gossipedDseRelease(e.getValue());
            if (dseVersion != null)
                description.append(", DSE ").append(dseVersion);
            result.put(e.getKey().getHostAddressAndPort(), description.append(", ").append(ordinals(identified.get(e.getKey()))).toString());
        }
        // the other identified peers: identified by a handshake, with no gossip state yet, a state without NET_VERSION
        // (after a restart) or a NET_VERSION of an earlier life (a node of this software rolled back to DSE)
        for (Map.Entry<InetAddressAndPort, String> e : identified.entrySet())
        {
            String key = e.getKey().getHostAddressAndPort();
            EndpointState state = Gossiper.instance.endpointStateMap.get(e.getKey());
            if (e.getKey().equals(self) || result.containsKey(key) || DsePeers.hasLeftTheRing(state))
                continue;
            VersionedValue netVersion = state == null ? null : state.getApplicationState(ApplicationState.NET_VERSION);
            result.put(key, "NET_VERSION " + (netVersion == null ? "unknown" : netVersion.value) + ", " + ordinals(e.getValue()));
        }
        return result;
    }

    /**
     * Which ordinals gossip with a peer uses, from what {@link DsePeers} identified it by (null when it did not): DSE's
     * for an identified DSE 6 peer; this software's for a DSE 5.1 peer, whose ordinals are a prefix of them.
     */
    private static String ordinals(String evidence)
    {
        if (DseCompat.isDse51())
            return evidence == null ? "gossip ordinals: this software's (not identified as DSE 5.1 yet)"
                                    : "gossip ordinals: this software's (identified as DSE 5.1 by " + evidence + ')';
        return evidence == null ? "gossip ordinals: this software's (not identified as DSE yet)" : "gossip ordinals: DSE's (" + evidence + ')';
    }

    @Override
    public Map<String, Long> getDseCompatRefusals()
    {
        return DseCompatRefusals.snapshot();
    }

    @Override
    public Map<String, List<String>> getDseAccessControlCheck()
    {
        Map<String, List<String>> result = new HashMap<>();
        Dse6AccessControlCheck.LastCheck check = Dse6AccessControlCheck.lastCheck();
        if (check != null)
            result.put(check.source, new ArrayList<>(check.findings));
        return result;
    }

    @Override
    public String getDseSaiVersionPin()
    {
        return DatabaseDescriptor.isDse6CompatibilityMode() ? Dse6SaiVersionPin.status() : null;
    }

    @Override
    public String getDseNonCoreTraces()
    {
        return DatabaseDescriptor.isDseCompatibilityMode() ? Dse6NonCoreTraces.status() : null;
    }

    @Override
    public List<String> getDsePeersOnRecord()
    {
        List<String> result = new ArrayList<>();
        for (InetAddressAndPort peer : DsePeersRecord.peers())
            result.add(peer.getHostAddressAndPort());
        // the lines of the persisted record that are not addresses with port refuse a start with NONE too
        result.addAll(DsePeersRecord.unparsedEntries());
        return result;
    }

    @Override
    public List<String> getDseLoneWriteGrants()
    {
        if (!(DatabaseDescriptor.getAuthorizer() instanceof CassandraAuthorizer))
            return Collections.emptyList();
        return CassandraAuthorizer.loneDseWriteGrants();
    }

    @Override
    public String getDseLoneWriteGrantsSource()
    {
        if (!(DatabaseDescriptor.getAuthorizer() instanceof CassandraAuthorizer))
            return null;
        return CassandraAuthorizer.loneDseWriteGrantsSource();
    }
}
