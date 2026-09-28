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

package org.apache.cassandra.distributed.test.ring;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tools.TokenReductionPlannerTool;
import org.apache.cassandra.tools.TokenReductionRunner;

import static org.apache.cassandra.config.CassandraRelevantProperties.RING_DELAY;
import static org.apache.cassandra.distributed.test.ring.ShrinkTokensTest.ROWS;
import static org.apache.cassandra.distributed.test.ring.ShrinkTokensTest.TOKENS;
import static org.apache.cassandra.distributed.test.ring.ShrinkTokensTest.address;
import static org.apache.cassandra.distributed.test.ring.ShrinkTokensTest.assertDataPlacement;
import static org.apache.cassandra.distributed.test.ring.ShrinkTokensTest.cleanup;
import static org.apache.cassandra.distributed.test.ring.ShrinkTokensTest.createSchema;
import static org.apache.cassandra.distributed.test.ring.ShrinkTokensTest.tokens;
import static org.apache.cassandra.distributed.test.ring.ShrinkTokensTest.tokensSeenBy;
import static org.apache.cassandra.distributed.test.ring.ShrinkTokensTest.write;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The planner and the runner of {@code tools/bin} (tokenreductionplanner, tokenreduction-run) end to end, the runner
 * driving the cluster through the same operations it uses over JMX.
 */
public class ShrinkTokensRunnerTest extends TestBaseImpl
{
    @BeforeClass
    public static void setUpRingDelay()
    {
        RING_DELAY.setLong(5000);
    }

    /** The runner's operations on an in-JVM cluster. */
    private static final class InJvmCluster implements TokenReductionRunner.ClusterOperations
    {
        private final Map<String, IInvokableInstance> instances = new HashMap<>();
        private final IInvokableInstance coordinator;

        InJvmCluster(Cluster cluster)
        {
            for (IInvokableInstance instance : cluster)
                instances.put(address(instance), instance);
            coordinator = cluster.get(1);
        }

        public Set<String> endpoints()
        {
            return new HashSet<>(instances.keySet());
        }

        public Set<String> liveEndpoints()
        {
            return new HashSet<>(coordinator.callOnInstance(() -> StorageService.instance.getLiveNodesWithPort()));
        }

        public Set<String> endpointsInRangeMovement()
        {
            return new HashSet<>(coordinator.callOnInstance(() -> {
                List<String> moving = new ArrayList<>(StorageService.instance.getJoiningNodesWithPort());
                moving.addAll(StorageService.instance.getLeavingNodesWithPort());
                moving.addAll(StorageService.instance.getMovingNodesWithPort());
                return moving;
            }));
        }

        public Set<String> tokens(String observer, String endpoint)
        {
            return tokensSeenBy(instances.get(observer), instances.get(endpoint));
        }

        public String hostId(String endpoint)
        {
            return coordinator.callOnInstance(() -> {
                for (Map.Entry<String, String> entry : StorageService.instance.getHostIdToEndpointWithPort().entrySet())
                    if (entry.getValue().equals(endpoint))
                        return entry.getKey();
                return null;
            });
        }

        public Set<String> hostIdsWithPendingHints(String node)
        {
            return instances.get(node).callOnInstance(() -> {
                Set<String> hostIds = new HashSet<>();
                for (Map<String, String> hints : HintsService.instance.getPendingHints())
                    hostIds.add(hints.get("host_id"));
                return hostIds;
            });
        }

        public void shrink(String endpoint, List<String> keep) throws IOException
        {
            try
            {
                ShrinkTokensTest.shrink(instances.get(endpoint), keep);
            }
            catch (RuntimeException e)
            {
                throw new IOException(e.getMessage(), e);
            }
        }

        public void flushAndCleanup(String endpoint)
        {
            cleanup(instances.get(endpoint));
        }
    }

    @Test
    public void testPlanAndRun() throws Throwable
    {
        Path dir = Files.createTempDirectory("shrinkrunner");
        try (Cluster cluster = ShrinkTokensTest.builder(6).start())
        {
            createSchema(cluster);
            write(cluster, 0, ROWS);

            // the ring, as CSV
            List<String> ring = new ArrayList<>();
            for (IInvokableInstance instance : cluster)
                for (String token : tokens(instance))
                    ring.add(String.format("%s,datacenter0,rack0,%s", address(instance), token));
            Path ringFile = dir.resolve("ring.csv");
            Files.write(ringFile, ring, StandardCharsets.UTF_8);

            Path plan = dir.resolve("plan");
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            PrintStream print = new PrintStream(out, true, StandardCharsets.UTF_8.name());
            int exit = TokenReductionPlannerTool.run(new String[]{ "--ring", ringFile.toString(), "--replication", "datacenter0:3",
                                                                   "--target", "16", "--output", plan.toString() }, print, print);
            assertThat(exit).as(out.toString(StandardCharsets.UTF_8.name())).isZero();

            List<TokenReductionRunner.Step> steps = TokenReductionRunner.readPlan(plan);
            assertThat(steps).hasSize(4);
            TokenReductionRunner.Options options = new TokenReductionRunner.Options();
            options.pollMillis = 500;
            InJvmCluster operations = new InJvmCluster(cluster);

            // a run stopped after one step: the next run resumes
            List<TokenReductionRunner.Step> first = steps.subList(0, 1);
            assertThat(TokenReductionRunner.run(first, operations, options, print)).isEqualTo(1);
            assertThat(TokenReductionRunner.run(steps, operations, options, print)).isEqualTo(3);
            assertThat(TokenReductionRunner.run(steps, operations, options, print)).isZero();

            for (IInvokableInstance instance : cluster)
                assertThat(tokens(instance)).hasSize(TOKENS / 2);
            Set<Integer> keys = new HashSet<>();
            for (int i = 0; i < ROWS; i++)
                keys.add(i);
            // the runner ran flush and cleanup after every step
            assertDataPlacement(cluster, keys, true);
            Object[][] count = cluster.coordinator(1).execute(withKeyspace("SELECT count(*) FROM %s.tbl"), ConsistencyLevel.ALL);
            assertThat(count[0][0]).isEqualTo((long) ROWS);
        }
        finally
        {
            FileUtils.deleteRecursive(new File(dir));
        }
    }
}
