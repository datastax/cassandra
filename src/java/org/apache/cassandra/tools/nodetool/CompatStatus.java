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

package org.apache.cassandra.tools.nodetool;

import java.io.PrintStream;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Supplier;
import javax.management.AttributeNotFoundException;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.airline.Command;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.net.DseCompat;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

/**
 * Shows the compatibility modes the node runs with, the sstable format it writes, what it knows about its peers'
 * messaging versions, the DSE 6 peers it has on record, the SAI on-disk version it pins for a rollback to DSE and
 * the requests it refused because of the upgrade: the operator's view during an in-place upgrade from DataStax
 * Enterprise 6.
 */
@Command(name = "compatstatus", description = "Print the internode and storage compatibility modes, the sstable format written, the messaging version of every known peer, the DSE 6 peers on record, the SAI on-disk version pinned and the requests refused during the upgrade")
public class CompatStatus extends NodeTool.NodeToolCmd
{
    private static final String DSE_6 = InternodeCompatibilityMode.DSE_6.name();
    /** What is printed for an attribute the node does not have: a server older than this nodetool. */
    private static final String NOT_REPORTED = "unknown (this node does not report them)";

    @Override
    protected void execute(NodeProbe probe)
    {
        PrintStream out = probe.output().out;
        // the first attribute tells a server without any of them, which has no compatibility status to report
        String mode = ifReported(probe::getInternodeCompatibilityMode);
        if (mode == null)
        {
            out.printf("Internode compatibility mode : unknown (this node does not report its compatibility status)%n");
            return;
        }
        out.printf("Internode compatibility mode : %s%n", mode);
        out.printf("Storage compatibility mode   : %s%n", probe.getStorageCompatibilityMode());
        out.printf("SSTable format written       : %s%n", probe.getSSTableFormatVersion());

        // raw values: a peer recorded below 4.0 speaks the pre-4.0 dialect; a DSE node gossiped by an upgraded peer is
        // recorded at DSE's own gossip value (256 or more), a node this one cannot talk to
        Map<String, Integer> versions = new TreeMap<>(probe.getEndpointMessagingVersions());
        int dialectPeers = 0;
        int dseInGossipOnly = 0;
        for (int version : versions.values())
        {
            if (version < MessagingService.VERSION_40)
                dialectPeers++;
            else if (isDseGossipValue(version))
                dseInGossipOnly++;
        }
        out.printf("Peers with a known version   : %d (%d speaking the pre-4.0 dialect%s)%n", versions.size(), dialectPeers,
                   dseInGossipOnly == 0 ? "" : ", " + dseInGossipOnly + " DSE 6 node(s) known from gossip only");
        for (Map.Entry<String, Integer> e : versions.entrySet())
        {
            int version = e.getValue();
            String note = version < MessagingService.VERSION_40 ? "  (pre-4.0 dialect)"
                                                                : isDseGossipValue(version) ? "  (DSE 6 node known from gossip only)" : "";
            out.printf("  %-45s %d%s%n", e.getKey(), version, note);
        }

        // the record of DSE 6 peers exists in DSE_6 mode only; its entries that are not addresses with port are counted and listed too
        boolean dse6 = DSE_6.equals(mode);
        String notInTheMode = " (internode_compatibility_mode " + mode + ')';
        List<String> onRecord = dse6 ? ifReported(probe::getDsePeersOnRecord) : null;
        if (!dse6)
            out.printf("DSE 6 peers on record        : not maintained%s%n", notInTheMode);
        else if (onRecord == null)
            out.printf("DSE 6 peers on record        : %s%n", NOT_REPORTED);
        else
            out.printf("DSE 6 peers on record        : %d%s%n", onRecord.size(), onRecord.isEmpty() ? "" : " " + onRecord);

        // the SAI on-disk version a node rolled back to DSE reads, pinned by a startup check in DSE_6 mode
        if (dse6)
            printDseSaiVersionPin(probe, out);
        else
            out.printf("SAI version pinned           : not checked%s%n", notInTheMode);

        printDseCompatRefusals(probe, out);
    }

    private static void printDseCompatRefusals(NodeProbe probe, PrintStream out)
    {
        // the server never returns null: null is a server without the attribute
        Map<String, Long> refusals = ifReported(probe::getDseCompatRefusals);
        if (refusals == null)
        {
            out.printf("Requests refused (upgrade)   : %s%n", NOT_REPORTED);
            return;
        }
        long total = 0;
        for (long count : refusals.values())
            total += count;
        out.printf("Requests refused (upgrade)   : %d%n", total);
        for (Map.Entry<String, Long> e : refusals.entrySet())
            out.printf("  %-45s %d%n", e.getKey(), e.getValue());
    }

    /** DSE's own messaging version as it gossips it (256 or more): a DSE node known from gossip only. */
    private static boolean isDseGossipValue(int rawVersion)
    {
        return DseCompat.messagingVersionFromGossip(rawVersion) != rawVersion;
    }

    private static void printDseSaiVersionPin(NodeProbe probe, PrintStream out)
    {
        // the server returns null before the check: an Optional tells it from a server without the attribute
        Optional<String> pin = ifReported(() -> Optional.ofNullable(probe.getDseSaiVersionPin()));
        if (pin == null)
            out.printf("SAI version pinned           : unknown (this node does not report it)%n");
        else
            out.printf("SAI version pinned           : %s%n", pin.orElse("unknown (not checked yet)"));
    }

    /**
     * The value of an attribute of the node's MessagingService, or null when the node does not have it: a server older
     * than this nodetool. Any other failure is thrown.
     */
    @VisibleForTesting
    static <T> T ifReported(Supplier<T> attribute)
    {
        try
        {
            return attribute.get();
        }
        catch (UndeclaredThrowableException e)
        {
            if (!(e.getCause() instanceof AttributeNotFoundException))
                throw e;
            return null;
        }
    }
}
