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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.shared.WithProperties;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.isUpgraded;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.localPeers;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.networkTopology;
import static org.apache.cassandra.distributed.test.dsecompat.TwoDataCenters.remoteNodes;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The cluster-wide pass of the DSE access-control check ({@code Dse6AccessControlCheck.verifyAfterJoin}) on the
 * upgraded nodes of two data centers, in the layout of {@link TwoDataCenters}, with {@code system_auth} replicated as
 * DSE clusters of several data centers have it: by {@code NetworkTopologyStrategy}, or left on {@code SimpleStrategy}.
 * The pass reads {@code system_auth.role_permissions} at {@code auth_read_consistency_level}, {@code LOCAL_QUORUM} by
 * default.
 * <ul>
 * <li>With {@code NetworkTopologyStrategy} and replicas in both data centers, the read stays in the node's data center: it
 *     finds a row-level grant the node does not hold, held in its data center by HCD 1.2 nodes only (node 1) or by one node
 *     of each version (node 4), and still finds it when the node cannot reach the other data center at all, where a read
 *     at {@code QUORUM}, which succeeds otherwise, fails.</li>
 * <li>With {@code SimpleStrategy}, {@code LOCAL_QUORUM} takes a quorum of the whole replication factor among the replicas
 *     of the node's data center, which some ranges do not have: the read is unavailable from an upgraded node of either
 *     data center although every node is up, and after the wait of
 *     {@code cassandra.internode_compatibility_mode.dse_restrictions_check_wait_seconds} (short here) the start is
 *     refused, after two attempts or more; {@code QUORUM} as {@code auth_read_consistency_level} reads it, and
 *     {@code dse_restrictions_local_check_only} accepts the start on this node's replica alone, which then misses a grant
 *     this node does not replicate.</li>
 * </ul>
 * The pass is called explicitly, as {@code Dse6AccessControlMixedTest} does: the instances of this harness run without
 * gossip, and the startup runs only the first pass.
 */
public class MixedModeMultiDcAccessControlTest extends DseDialectTestBase
{
    private static final String RESOURCE = "rows/top_secret/data/ks1/table1";
    /**
     * The wait of the cluster-wide read in these tests is twice {@code Dse6AccessControlCheck.RETRY_INTERVAL_SECONDS}
     * (read from the instance, 5 s today) plus this margin, 12 s today. A failed attempt is retried, after that pause,
     * only when the pause ends within the wait; so the second attempt is made as long as the first fails within the
     * interval plus the margin (7 s). The unavailable {@code LOCAL_QUORUM} reads of the {@code SimpleStrategy} case fail
     * without waiting for a timeout (the coordinator throws once it plans a sub-range with too few live replicas in its
     * data center, and the replicas it may have asked first are up and answer at once): they are normally attempted three
     * times (at least two are asserted), about 10 s per node. The {@code QUORUM} read of node 1 cut off from
     * {@code datacenter2} waits for the range request timeout (10 s), more than the wait less the interval, so it is
     * attempted once; only a wait of more than 15 s (the timeout plus the interval) would retry it.
     */
    private static final int WAIT_MARGIN_SECONDS = 2;
    /** The refusal of an unavailable {@code LOCAL_QUORUM} read once a further attempt would start after the wait: the attempts, then the seconds. */
    private static final Pattern LOCAL_QUORUM_UNAVAILABLE =
        Pattern.compile(Pattern.quote("internode_compatibility_mode DSE_6 refused: unable to read system_auth.role_permissions at LOCAL_QUORUM " +
                                      "(Cannot achieve consistency level LOCAL_QUORUM) after ") + "(\\d+)" + Pattern.quote(" attempts in ") +
                        "(\\d+)" + Pattern.quote(" s, so the DSE access controls this software does not enforce cannot be checked across the " +
                                                  "cluster; make the replicas of system_auth reachable, or start with " +
                                                  "-Dcassandra.internode_compatibility_mode.dse_restrictions_local_check_only=true to rely on " +
                                                  "the check of this node's replica alone"));

    @Test
    public void networkTopologyStrategyReadsInTheLocalDataCenter() throws Throwable
    {
        TwoDataCenters.testCase(config -> {})
                      .setup(cluster -> cluster.schemaChange("ALTER KEYSPACE system_auth WITH replication = " + networkTopology(2, 2)))
                      .runAfterClusterUpgrade(cluster -> {
                          TwoDataCenters.assertMixed(cluster);
                          for (int node : TwoDataCenters.UPGRADED)
                          {
                              assertThat(clusterWidePass(node)).as("node %s", node).isNull();
                              assertThat(compatStatus(cluster, node)).contains("DSE access controls ignored  : 0 (found by a LOCAL_QUORUM read)");
                          }

                          // node 1: both replicas of datacenter1 are HCD 1.2 nodes; node 4: one HCD 1.2 node and one upgraded node
                          for (int node : new int[]{ 1, 4 })
                          {
                              String role = roleNotOn(cluster, node);
                              for (int upgraded : TwoDataCenters.UPGRADED)
                                  assertThat(clusterWidePass(upgraded)).as("node %s, role held by %s", upgraded, localPeers(node))
                                                                     .isEqualTo(rowLevelRefusal("LOCAL_QUORUM", role));
                              cluster.coordinator(2).execute("DELETE FROM system_auth.role_permissions WHERE role = ?", ALL, role);
                          }

                          // node 1 cut off from datacenter2: LOCAL_QUORUM does not need it, the grant is still read from datacenter1
                          String role = roleNotOn(cluster, 1);
                          String before = setAuthReadConsistencyLevel(1, "QUORUM");
                          try
                          {
                              assertThat(clusterWidePass(1)).as("node 1 at QUORUM, not cut off").isEqualTo(rowLevelRefusal("QUORUM", role));
                          }
                          finally
                          {
                              setAuthReadConsistencyLevel(1, before);
                          }
                          IMessageFilters.Filter cutOff = cluster.filters().allVerbs().from(1).to(remoteNodes(1).stream().mapToInt(Integer::intValue).toArray()).drop();
                          try (WithProperties wait = shortWait(shortWaitSeconds(retryIntervalSeconds(1))))
                          {
                              assertThat(clusterWidePass(1)).isEqualTo(rowLevelRefusal("LOCAL_QUORUM", role));
                              // QUORUM (three of the four replicas) needs datacenter2: the cut-off is real
                              String previous = setAuthReadConsistencyLevel(1, "QUORUM");
                              try
                              {
                                  assertThat(clusterWidePass(1)).startsWith("internode_compatibility_mode DSE_6 refused: unable to read system_auth.role_permissions at QUORUM (")
                                                                .contains("cannot be checked across the cluster");
                              }
                              finally
                              {
                                  setAuthReadConsistencyLevel(1, previous);
                              }
                          }
                          finally
                          {
                              cutOff.off();
                          }
                          cluster.coordinator(2).execute("DELETE FROM system_auth.role_permissions WHERE role = ?", ALL, role);
                          assertThat(clusterWidePass(1)).isNull();
                      })
                      .run();
    }

    @Test
    public void simpleStrategyIsUnavailableAtLocalQuorum() throws Throwable
    {
        TwoDataCenters.testCase(config -> {})
                      .setup(cluster -> cluster.schemaChange("ALTER KEYSPACE system_auth WITH replication = " +
                                                             "{'class': 'SimpleStrategy', 'replication_factor': 3}"))
                      .runAfterClusterUpgrade(cluster -> {
                          TwoDataCenters.assertMixed(cluster);
                          String role = roleNotOn(cluster, 1);
                          for (int node : new int[]{ 1, 4 })
                          {
                              // every node is up, but some ranges have fewer than two replicas in the node's data center
                              long interval = retryIntervalSeconds(node);
                              long waitSeconds = shortWaitSeconds(interval);
                              try (WithProperties wait = shortWait(waitSeconds))
                              {
                                  String refusal = clusterWidePass(node);
                                  assertThat(refusal).as("node %s", node).matches(LOCAL_QUORUM_UNAVAILABLE);
                                  Matcher matcher = LOCAL_QUORUM_UNAVAILABLE.matcher(refusal);
                                  matcher.matches(); // fills the groups; a mismatch was reported above
                                  int attempts = Integer.parseInt(matcher.group(1));
                                  long seconds = Long.parseLong(matcher.group(2));
                                  // an attempt is made only when the pauses before it end within the wait
                                  assertThat(attempts).as("node %s: attempts in %s", node, refusal)
                                                      .isBetween(2, (int) (waitSeconds / interval) + 1);
                                  // the seconds are the whole seconds from the first attempt to the refusal: a pause was taken
                                  // after every attempt but the last, and the read is refused only once a further attempt
                                  // would start after the wait, that is more than the wait less the interval from the first
                                  assertThat(seconds).as("node %s: seconds in %s", node, refusal)
                                                     .isGreaterThanOrEqualTo(Math.max((attempts - 1) * interval, waitSeconds - interval));
                              }

                              // QUORUM counts the replicas of both data centers
                              String previous = setAuthReadConsistencyLevel(node, "QUORUM");
                              try
                              {
                                  assertThat(clusterWidePass(node)).as("node %s at QUORUM", node).isEqualTo(rowLevelRefusal("QUORUM", role));
                              }
                              finally
                              {
                                  setAuthReadConsistencyLevel(node, previous);
                              }
                          }

                          // the local check alone accepts node 1, whose replica does not hold the grant
                          try (WithProperties localOnly = new WithProperties().set(CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_LOCAL_CHECK_ONLY, true))
                          {
                              assertThat(clusterWidePass(1)).isNull();
                          }
                          assertThat(compatStatus(cluster, 1)).contains("DSE access controls ignored  : at least 0 (found by this node's replica only: the cluster-wide " +
                                                                        "read failed (Cannot achieve consistency level LOCAL_QUORUM))");
                      })
                      .run();
    }

    /** {@code Dse6AccessControlCheck.RETRY_INTERVAL_SECONDS}, read through the class loader of an upgraded instance. */
    private static long retryIntervalSeconds(int node)
    {
        try
        {
            Field interval = Class.forName("org.apache.cassandra.service.Dse6AccessControlCheck", true, instanceClassLoader(node))
                                  .getDeclaredField("RETRY_INTERVAL_SECONDS");
            interval.setAccessible(true);
            long seconds = interval.getLong(null);
            assertThat(seconds).as("RETRY_INTERVAL_SECONDS on node %s", node).isPositive();
            return seconds;
        }
        catch (ReflectiveOperationException e)
        {
            throw new AssertionError(e);
        }
    }

    /** The short wait of the cluster-wide read for the given retry interval, see {@link #WAIT_MARGIN_SECONDS}. */
    private static long shortWaitSeconds(long retryIntervalSeconds)
    {
        return 2 * retryIntervalSeconds + WAIT_MARGIN_SECONDS;
    }

    /**
     * The wait of the cluster-wide read shortened to the given seconds, by a system property, so for every instance of
     * the JVM.
     */
    private static WithProperties shortWait(long waitSeconds)
    {
        return new WithProperties().set(CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_DSE_RESTRICTIONS_CHECK_WAIT_SECONDS, waitSeconds);
    }

    /**
     * Writes, at {@code ALL} through the HCD 1.2 node 2, a row-level grant for a role whose partition the given node does
     * not replicate, but the other nodes of its data center do (when {@code system_auth} has two replicas per data center),
     * or some of them (with {@code SimpleStrategy}); returns the role.
     */
    private static String roleNotOn(UpgradeableCluster cluster, int node)
    {
        Set<Integer> peers = localPeers(node);
        for (int i = 0; i < 64; i++)
        {
            String candidate = "analyst" + i;
            cluster.coordinator(2).execute("INSERT INTO system_auth.role_permissions (role, resource, permissions) VALUES (?, ?, {'SELECT'})",
                                           ALL, candidate, RESOURCE);
            String select = "SELECT role FROM system_auth.role_permissions WHERE role = '" + candidate + '\'';
            boolean onPeer = peers.stream().anyMatch(peer -> cluster.get(peer).executeInternal(select).length == 1);
            if (cluster.get(node).executeInternal(select).length == 0 && onPeer)
                return candidate;
            cluster.coordinator(2).execute("DELETE FROM system_auth.role_permissions WHERE role = ?", ALL, candidate);
        }
        throw new AssertionError("no role among 64 is held by the peers of node " + node + " in its data center and not by node " + node);
    }

    private static String rowLevelRefusal(String consistency, String role)
    {
        return "internode_compatibility_mode DSE_6 refused: DSE access controls that this software does not enforce would grant access if " +
               "ignored (found by a " + consistency + " read): row-level grants [role '" + role + "' on " + RESOURCE + ": [SELECT]]. Roll this " +
               "node back to DSE, remove them there (REVOKE the row-level grants) and upgrade it again; " +
               "or start with -Dcassandra.internode_compatibility_mode.ignore_dse_restrictions=true to drop them knowingly";
    }

    /**
     * Sets {@code auth_read_consistency_level} of an upgraded node, by reflection through its class loader, and returns the
     * level it replaces.
     */
    private static String setAuthReadConsistencyLevel(int node, String consistency)
    {
        assertThat(isUpgraded(node)).as("node %s runs this software", node).isTrue();
        ClassLoader classLoader = instanceClassLoader(node);
        try
        {
            Class<?> level = Class.forName("org.apache.cassandra.db.ConsistencyLevel", true, classLoader);
            Class<?> descriptor = Class.forName("org.apache.cassandra.config.DatabaseDescriptor", true, classLoader);
            String previous = String.valueOf(descriptor.getMethod("getAuthReadConsistencyLevel").invoke(null));
            descriptor.getMethod("setAuthReadConsistencyLevel", level)
                      .invoke(null, level.getMethod("valueOf", String.class).invoke(null, consistency));
            return previous;
        }
        catch (ReflectiveOperationException e)
        {
            throw new AssertionError(e);
        }
    }

    /**
     * Runs the cluster-wide pass inside an upgraded instance (by reflection: test classes are not shared with the instances
     * of this harness) and returns the refusal text, or null when the pass accepts the start.
     */
    private static String clusterWidePass(int node)
    {
        assertThat(isUpgraded(node)).as("node %s runs this software", node).isTrue();
        try
        {
            Class<?> check = Class.forName("org.apache.cassandra.service.Dse6AccessControlCheck", true, instanceClassLoader(node));
            check.getMethod("verifyAfterJoin").invoke(null);
            return null;
        }
        catch (InvocationTargetException e)
        {
            Throwable cause = e.getCause();
            if (cause.getClass().getName().equals("org.apache.cassandra.exceptions.StartupException"))
                return cause.getMessage();
            throw new AssertionError("the cluster-wide pass of node " + node + " failed", cause);
        }
        catch (ReflectiveOperationException e)
        {
            throw new AssertionError(e);
        }
    }
}
