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

package org.apache.cassandra.service;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.DseCompat;
import org.apache.cassandra.nodes.CC4UpgradeNodesPersistence;
import org.apache.cassandra.nodes.IPeerInfo;
import org.apache.cassandra.nodes.Nodes;
import org.apache.cassandra.utils.CassandraVersion;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_SAI_VERSION_PIN;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_CURRENT_VERSION;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_VERSION_SELECTOR_CLASS;

/**
 * In {@code internode_compatibility_mode: DSE_6}, refuses to start unless the SAI on-disk version this node writes
 * ({@code cassandra.sai.latest.version}) is pinned to the one the cluster's DSE release writes: {@code aa} for DSE 6.8,
 * {@code ca} for DSE 6.9 (DSE 6.9.26 writes {@code bb-1-bti-SAI+ca+*.db}). By default this software writes a newer
 * version, whose files DSE ignores: a node rolled back to DSE rebuilds the index of every sstable this node flushed or
 * compacted, whereas DSE 6.8.65 with the pin at {@code aa}, and DSE 6.9.26 with the pin at {@code ca}, were observed
 * to keep the index files this node wrote, without rebuilding them, after a rollback. The pin is required until the flag
 * drop, after which no node goes back to DSE.
 * <p>
 * The DSE release is the {@code 4.0.0.<build>} release ({@link DseCompat#isDse6ReleaseVersion}) stored for
 * this node, in the {@code nodes/local} file DSE wrote (it stays in place in this mode) or in {@code system.local}
 * (until the first start writes this software's release there), and for the saved peers in {@code system.peers}. A
 * build of three or four digits starting with {@code 68} is DSE 6.8 ({@code 4.0.0.6865} is 6.8.65, {@code 4.0.0.685}
 * would be 6.8.5, as {@code 4.0.0.678} is 6.7.8), one starting with {@code 69} is DSE 6.9; any other DSE 6 release is
 * of no known SAI version. The rules:
 * <ul>
 *     <li>a custom SAI version selector ({@code cassandra.sai.version.selector.class}) is refused: it, not the pinned
 *     property, would choose the version written;</li>
 *     <li>releases of more than one DSE version (6.8 and 6.9, or either and another DSE 6 release): refused, the
 *     upgrade requires one DSE release across the cluster;</li>
 *     <li>one DSE version: the property must be present (its default value does not count) and equal to the version
 *     of that release;</li>
 *     <li>no release of a known DSE version (no DSE release stored, as on a node that never ran DSE, or only
 *     releases of other DSE 6 versions): the property must be present and be {@code aa} or {@code ca}.</li>
 * </ul>
 * {@code cassandra.internode_compatibility_mode.ignore_sai_version_pin=true} turns a refusal into a warning, and is
 * reported at WARN on every start. With {@code NONE} nothing is read or checked.
 * <p>
 * The check runs among the startup checks, after {@code checkSystemKeyspaceState}, which initialises {@link Nodes}
 * (and imports DSE's node metadata on the first start).
 */
public final class Dse6SaiVersionPin
{
    private static final Logger logger = LoggerFactory.getLogger(Dse6SaiVersionPin.class);

    static final String REFUSED = "internode_compatibility_mode DSE_6 refused: ";
    private static final String WHERE = "conf/jvm-server.options (or JVM_EXTRA_OPTS)";
    private static final String WHY = "a node rolled back to DSE must read the SAI index files this node writes, and DSE ignores " +
                                      "the index files of a version it does not know and rebuilds those indexes at startup";
    /** The holders listed in a message; the others are counted. */
    @VisibleForTesting
    static final int LISTED_HOLDERS = 5;
    static final String THIS_NODE_IN_NODES_LOCAL = "this node in nodes/local";
    static final String THIS_NODE_IN_SYSTEM_LOCAL = "this node in system.local";

    /** The DSE versions that write SAI index files, with the SAI on-disk version each writes. */
    enum DseVersion
    {
        DSE_6_8("DSE 6.8", 8, "aa"),
        DSE_6_9("DSE 6.9", 9, "ca");

        final String displayName;
        /** the minor version of the DSE 6 release ({@link DseCompat#dse6MinorAndPatch}) */
        final int minor;
        final String saiVersion;

        DseVersion(String displayName, int minor, String saiVersion)
        {
            this.displayName = displayName;
            this.minor = minor;
            this.saiVersion = saiVersion;
        }
    }

    /** A DSE 6 release stored for this node or for a saved peer. */
    static final class StoredRelease
    {
        final String holder;
        final CassandraVersion release;
        /** stored for this node itself, not for a peer */
        final boolean thisNode;

        StoredRelease(String holder, CassandraVersion release, boolean thisNode)
        {
            this.holder = holder;
            this.release = release;
            this.thisNode = thisNode;
        }

        @Override
        public String toString()
        {
            return release + " of " + holder;
        }
    }

    /** The system properties the check reads, as they are when it runs. */
    @VisibleForTesting
    static final class Settings
    {
        /** the value of {@code cassandra.sai.latest.version}, or null when it is not set */
        final String pinned;
        /** the version written when it is not set */
        final String unpinnedDefault;
        /** {@code cassandra.sai.version.selector.class}, empty when not set */
        final String selectorClass;
        /** {@code cassandra.internode_compatibility_mode.ignore_sai_version_pin} */
        final boolean override;

        Settings(String pinned, String unpinnedDefault, String selectorClass, boolean override)
        {
            this.pinned = pinned;
            this.unpinnedDefault = unpinnedDefault;
            this.selectorClass = selectorClass;
            this.override = override;
        }

        static Settings fromSystemProperties()
        {
            return new Settings(SAI_CURRENT_VERSION.isPresent() ? SAI_CURRENT_VERSION.getString() : null,
                                SAI_CURRENT_VERSION.getDefaultValue(),
                                SAI_VERSION_SELECTOR_CLASS.getString(),
                                INTERNODE_COMPATIBILITY_MODE_IGNORE_SAI_VERSION_PIN.getBoolean());
        }

        /** What this node writes, as far as the properties tell. */
        String written()
        {
            if (!selectorClass.isEmpty())
                return "the version chosen by " + selectorClass;
            return "SAI version " + (pinned == null ? unpinnedDefault : pinned);
        }
    }

    /** What the last check in this process found, for {@code nodetool compatstatus}; null before it or with {@code NONE}. */
    private static volatile String status;

    private Dse6SaiVersionPin()
    {
    }

    /**
     * The pinned SAI on-disk version and what it was derived from, as the last check found it, or why it was not
     * enforced; null before the check and with {@code NONE}.
     */
    public static String status()
    {
        return status;
    }

    @VisibleForTesting
    public static void resetStatus()
    {
        status = null;
    }

    /**
     * The DSE version of a DSE 6 release, from its build number ({@link DseCompat#dse6MinorAndPatch}), or null
     * when the release is none of the versions that write SAI index files, or no DSE 6 release.
     */
    @VisibleForTesting
    @Nullable
    static DseVersion dseVersion(@Nullable CassandraVersion release)
    {
        int[] minorAndPatch = DseCompat.dse6MinorAndPatch(release);
        if (minorAndPatch == null)
            return null;
        for (DseVersion version : DseVersion.values())
        {
            if (version.minor == minorAndPatch[0])
                return version;
        }
        return null;
    }

    /**
     * The SAI on-disk version the DSE release {@code release} writes ({@code aa} for DSE 6.8, {@code ca} for DSE 6.9),
     * by the rules of {@link #dseVersion}; null when {@code release} is not a DSE 6 release of either version.
     */
    @Nullable
    public static String saiVersionOf(@Nullable CassandraVersion release)
    {
        DseVersion version = dseVersion(release);
        return version == null ? null : version.saiVersion;
    }

    /** Runs the check in {@code DSE_6}; does nothing with {@code NONE}. */
    public static void verifyAtStartup() throws StartupException
    {
        if (!DatabaseDescriptor.isDse6CompatibilityMode())
            return;
        Settings settings = Settings.fromSystemProperties();
        List<StoredRelease> releases;
        try
        {
            releases = storedReleases(CC4UpgradeNodesPersistence.localReleaseVersionInNodesDirectory(),
                                      Nodes.local().get().getReleaseVersion(),
                                      Nodes.peers().get().collect(Collectors.toList()));
        }
        catch (RuntimeException e)
        {
            String problem = "the DSE release stored for this node or its saved peers, from which the SAI on-disk version to pin " +
                             "is chosen, cannot be read (" + e + ')';
            if (ignored(problem, settings, e))
                return;
            throw new StartupException(StartupException.ERR_WRONG_DISK_STATE, REFUSED + problem + overrideHint(), e);
        }
        verify(releases, settings);
    }

    /**
     * The DSE 6 releases among the release of {@code nodes/local}, the one of {@code system.local} (when it differs)
     * and those of the saved peers, in that order; any of them may be null.
     */
    @VisibleForTesting
    static List<StoredRelease> storedReleases(CassandraVersion nodesLocal, CassandraVersion systemLocal, List<? extends IPeerInfo> peers)
    {
        List<StoredRelease> releases = new ArrayList<>();
        addIfDse6(releases, THIS_NODE_IN_NODES_LOCAL, nodesLocal, true);
        if (!Objects.equals(systemLocal, nodesLocal))
            addIfDse6(releases, THIS_NODE_IN_SYSTEM_LOCAL, systemLocal, true);
        for (IPeerInfo peer : peers)
        {
            InetAddressAndPort address = peer.getPeerAddressAndPort();
            addIfDse6(releases, "peer " + (address == null ? "(no address)" : address.getHostAddressAndPort()), peer.getReleaseVersion(), false);
        }
        return releases;
    }

    private static void addIfDse6(List<StoredRelease> releases, String holder, CassandraVersion release, boolean thisNode)
    {
        if (DseCompat.isDse6ReleaseVersion(release))
            releases.add(new StoredRelease(holder, release, thisNode));
    }

    /**
     * Checks {@code settings} against {@code releases}, the DSE 6 releases stored for this node and its saved peers:
     * logs and records the pinned version, or throws the refusal, or logs a warning when the override is set.
     */
    @VisibleForTesting
    static void verify(List<StoredRelease> releases, Settings settings) throws StartupException
    {
        String key = SAI_CURRENT_VERSION.getKey();
        String pinned = settings.pinned;

        if (!settings.selectorClass.isEmpty())
        {
            String problem = SAI_VERSION_SELECTOR_CLASS.getKey() + " is " + settings.selectorClass + ": that class, not " + key +
                             ", chooses the SAI on-disk version this node writes, so the version cannot be pinned";
            if (ignored(problem, settings))
                return;
            throw new StartupException(StartupException.ERR_WRONG_CONFIG,
                                       REFUSED + problem + ". Remove -D" + SAI_VERSION_SELECTOR_CLASS.getKey() + " from " + WHERE +
                                       " and restart; " + WHY + overrideHint());
        }

        Map<DseVersion, List<StoredRelease>> byVersion = new EnumMap<>(DseVersion.class);
        List<StoredRelease> otherDse6 = new ArrayList<>();
        for (StoredRelease stored : releases)
        {
            DseVersion version = dseVersion(stored.release);
            if (version == null)
                otherDse6.add(stored);
            else
                byVersion.computeIfAbsent(version, v -> new ArrayList<>()).add(stored);
        }

        if (byVersion.size() + (otherDse6.isEmpty() ? 0 : 1) > 1)
        {
            List<String> groups = new ArrayList<>();
            for (Map.Entry<DseVersion, List<StoredRelease>> e : byVersion.entrySet())
                groups.add(e.getKey().displayName + ": " + join(e.getValue()));
            if (!otherDse6.isEmpty())
                groups.add("other DSE 6 releases: " + join(otherDse6));
            String problem = "the DSE releases stored for this node and its saved peers are not all of one DSE version (" +
                             String.join("; ", groups) + "), so the SAI on-disk version to pin cannot be chosen";
            if (ignored(problem, settings))
                return;
            throw new StartupException(StartupException.ERR_WRONG_CONFIG,
                                       REFUSED + problem + ". The upgrade requires one DSE release across the cluster: complete " +
                                       "the DSE upgrade (or its rollback) on every node first, then set -D" + key + " to the " +
                                       "version of that release (aa for DSE 6.8, ca for DSE 6.9) in " + WHERE + "; " + WHY +
                                       ". A peer row that still holds the release of a node since upgraded, and down since, " +
                                       "can cause this: once the release of the cluster is checked, set the pin to it, start " +
                                       "once with -D" + INTERNODE_COMPATIBILITY_MODE_IGNORE_SAI_VERSION_PIN.getKey() + "=true " +
                                       "(the pin still sets the version written, it is only not checked) and remove that option " +
                                       "once the peer is back and its row is refreshed, so that later starts are checked again");
        }

        if (byVersion.size() == 1)
        {
            Map.Entry<DseVersion, List<StoredRelease>> only = byVersion.entrySet().iterator().next();
            DseVersion version = only.getKey();
            boolean thisNodeOnly = only.getValue().stream().allMatch(stored -> stored.thisNode);
            String derivation = version.displayName + " (" + join(only.getValue()) + ')';
            String expected = version.saiVersion;
            if (expected.equals(pinned))
            {
                pass(expected, "derived from " + derivation, settings);
                return;
            }
            String ran = (thisNodeOnly ? "this node ran " : "the cluster ran ") + derivation + ", which writes SAI version " + expected;
            String problem = pinned == null
                             ? "the SAI on-disk version is not pinned (" + key + " is not set, so this node writes version " +
                               settings.unpinnedDefault + ") but " + ran
                             : key + " is " + pinned + " but " + ran;
            if (ignored(problem, settings))
                return;
            throw new StartupException(StartupException.ERR_WRONG_CONFIG,
                                       REFUSED + problem + ". " + (pinned == null ? "Add" : "Set") + " -D" + key + '=' + expected +
                                       (pinned == null ? " to " : " in ") + WHERE + " and restart; " + WHY + overrideHint());
        }

        String undetermined = otherDse6.isEmpty()
                              ? "no DSE 6 release (4.0.0.<build>) is stored for this node or its saved peers"
                              : "the DSE 6 releases stored, " + join(otherDse6) + ", are not of DSE 6.8 or 6.9";
        if (DseVersion.DSE_6_8.saiVersion.equals(pinned) || DseVersion.DSE_6_9.saiVersion.equals(pinned))
        {
            pass(pinned, "set explicitly; the DSE release the cluster ran cannot be determined: " + undetermined, settings);
            return;
        }
        String problem = (pinned == null
                          ? "the SAI on-disk version is not pinned (" + key + " is not set, so this node writes version " + settings.unpinnedDefault + ')'
                          : key + " is " + pinned + " but only aa (DSE 6.8) or ca (DSE 6.9) can be pinned") +
                         " and the DSE release the cluster ran cannot be determined (" + undetermined + ')';
        if (ignored(problem, settings))
            return;
        throw new StartupException(StartupException.ERR_WRONG_CONFIG,
                                   REFUSED + problem + ". " + (pinned == null ? "Add" : "Set") + " -D" + key + "=aa for DSE 6.8 or -D" +
                                   key + "=ca for DSE 6.9" + (pinned == null ? " to " : " in ") + WHERE + ", matching the DSE " +
                                   "release of the cluster, and restart; " + WHY + overrideHint());
    }

    private static void pass(String pinned, String source, Settings settings)
    {
        logger.info("DSE 6 compatibility mode: SAI on-disk version pinned to {} ({}), {}", pinned, SAI_CURRENT_VERSION.getKey(), source);
        String recorded = pinned + ", " + source;
        // the override is reported on every start, also when it changes nothing
        if (settings.override)
        {
            logger.warn("{}=true is set: a missing or wrong SAI on-disk version pin would not refuse the start of this node",
                        INTERNODE_COMPATIBILITY_MODE_IGNORE_SAI_VERSION_PIN.getKey());
            recorded += "; " + INTERNODE_COMPATIBILITY_MODE_IGNORE_SAI_VERSION_PIN.getKey() + "=true is set";
        }
        status = recorded;
    }

    /**
     * True, after a warning, when the override is set: the start goes on although {@code problem} would refuse it.
     */
    private static boolean ignored(String problem, Settings settings)
    {
        return ignored(problem, settings, null);
    }

    /** As {@link #ignored(String, Settings)}, logging {@code cause} (when not null) with the warning. */
    private static boolean ignored(String problem, Settings settings, Throwable cause)
    {
        if (!settings.override)
            return false;
        logger.warn("The SAI on-disk version pin is not enforced ({}=true): {}; this node writes {}, and a node rolled " +
                    "back to DSE ignores the index files of a version it does not know and rebuilds those indexes at startup",
                    INTERNODE_COMPATIBILITY_MODE_IGNORE_SAI_VERSION_PIN.getKey(), problem, settings.written(), cause);
        status = "not enforced (" + INTERNODE_COMPATIBILITY_MODE_IGNORE_SAI_VERSION_PIN.getKey() + "=true), this node writes " +
                 settings.written() + ": " + problem;
        return true;
    }

    private static String overrideHint()
    {
        return "; -D" + INTERNODE_COMPATIBILITY_MODE_IGNORE_SAI_VERSION_PIN.getKey() + "=true starts the node despite this check " +
               "(with a warning), for a node that will not be rolled back to DSE";
    }

    /** The first {@link #LISTED_HOLDERS} releases, then how many more there are. */
    private static String join(List<StoredRelease> releases)
    {
        String listed = releases.stream().limit(LISTED_HOLDERS).map(StoredRelease::toString).collect(Collectors.joining(", "));
        int more = releases.size() - LISTED_HOLDERS;
        return more > 0 ? listed + " and " + more + " more" : listed;
    }
}
