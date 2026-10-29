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
import org.apache.cassandra.service.Dse6AccessControlCheck;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

/**
 * Shows the compatibility modes the node runs with, the sstable format it writes, what it knows about its peers'
 * messaging versions, the DSE peers it sees in gossip or has identified by a handshake, the DSE 6 peers it has on
 * record, the SAI on-disk version it pins for a rollback to DSE, the DSE access controls it does not enforce, the
 * DataStax Enterprise grants of UPDATE or TRUNCATE alone it drops and the requests it refused because of the upgrade:
 * the operator's view during an in-place upgrade from DataStax Enterprise 6.
 */
@Command(name = "compatstatus", description = "Print the internode and storage compatibility modes, the sstable format written, the messaging version of every known peer, the DSE peers seen in gossip or identified by a handshake, the DSE 6 peers on record, the SAI on-disk version pinned, the DSE access controls not enforced, the DSE grants of UPDATE or TRUNCATE alone that are dropped and the requests refused during the upgrade")
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

        // raw values: a peer speaking the pre-4.0 dialect is recorded at its version (11 for a DSE node in DSE_6 mode, by the
        // handshake and by gossip alike); only a node without the mode records DSE's own gossip value (256 or more) of a
        // DSE node gossiped by an upgraded peer, a node it cannot talk to
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
                                                                : isDseGossipValue(version) ? "  (DSE 6 node known from gossip only; reachable in DSE_6 mode only)" : "";
            out.printf("  %-45s %d%s%n", e.getKey(), version, note);
        }

        // the DSE peers, their record and the access-control check exist in DSE_6 mode only; the record's entries that
        // are not addresses with port are counted and listed too
        boolean dse6 = DSE_6.equals(mode);
        String notInTheMode = " (internode_compatibility_mode " + mode + ')';
        if (dse6)
            printDsePeers(probe, out);
        else
            out.printf("DSE peers                    : not tracked%s%n", notInTheMode);

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

        // what a DSE node restricts but this node does not enforce (the last check in DSE_6 mode), and how it was found
        if (dse6)
            printDseAccessControlCheck(probe, out);
        else
            out.printf("DSE access controls ignored  : not checked%s%n", notInTheMode);
        // what the access-control check (and every read of role_permissions) finds dropped, in every mode
        printDseLoneWriteGrants(probe, out);
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

    /** DSE's own messaging version as it gossips it (256 or more), which only a node without the mode records. */
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

    private static void printDseAccessControlCheck(NodeProbe probe, PrintStream out)
    {
        Map<String, List<String>> check = ifReported(probe::getDseAccessControlCheck);
        if (check == null)
        {
            out.printf("DSE access controls ignored  : %s%n", NOT_REPORTED);
            return;
        }
        List<String> unenforced = check.isEmpty() ? List.of() : check.values().iterator().next();
        String source = check.isEmpty() ? null : check.keySet().iterator().next();
        if (source == null)
            out.printf("DSE access controls ignored  : unknown (not checked yet)%n");
        else if (source.startsWith(Dse6AccessControlCheck.NOT_CHECKED))
            out.printf("DSE access controls ignored  : unknown (%s)%n", source);
        else if (source.startsWith(Dse6AccessControlCheck.LOCAL_SOURCE))
            // this node's replica only: the other replicas may hold more
            out.printf("DSE access controls ignored  : at least %d (found by %s)%n", unenforced.size(), source);
        else
            out.printf("DSE access controls ignored  : %d (found by %s)%n", unenforced.size(), source);
        for (String finding : unenforced)
            out.printf("  %s%n", finding);
    }

    @VisibleForTesting
    static void printDsePeers(NodeProbe probe, PrintStream out)
    {
        Map<String, String> reported = ifReported(probe::getDsePeers);
        if (reported == null)
        {
            out.printf("DSE peers                    : %s%n", NOT_REPORTED);
            return;
        }
        Map<String, String> dsePeers = new TreeMap<>(reported);
        out.printf("DSE peers                    : %d%n", dsePeers.size());
        for (Map.Entry<String, String> e : dsePeers.entrySet())
            out.printf("  %-45s %s%n", e.getKey(), e.getValue());
    }

    /**
     * DSE writes UPDATE + TRUNCATE for GRANT ALL and a creator's permissions (its GRANT MODIFY writes the name MODIFY),
     * read as MODIFY; one of them alone grants nothing here.
     */
    @VisibleForTesting
    static void printDseLoneWriteGrants(NodeProbe probe, PrintStream out)
    {
        String loneGrantsSource;
        try
        {
            loneGrantsSource = probe.getDseLoneWriteGrantsSource();
        }
        catch (UndeclaredThrowableException e)
        {
            // not ifReported: the attribute itself is null when another authorizer is configured
            if (!(e.getCause() instanceof AttributeNotFoundException))
                throw e;
            out.printf("DSE UPDATE/TRUNCATE alone    : %s%n", NOT_REPORTED);
            return;
        }
        if (loneGrantsSource == null)
        {
            out.printf("DSE UPDATE/TRUNCATE alone    : not tracked (the authorizer is not CassandraAuthorizer)%n");
            return;
        }
        List<String> loneGrants = probe.getDseLoneWriteGrants();
        out.printf("DSE UPDATE/TRUNCATE alone    : %d dropped, as last read on this node (%s)%n", loneGrants.size(), loneGrantsSource);
        for (String grant : loneGrants)
            out.printf("  %s%n", grant);
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
