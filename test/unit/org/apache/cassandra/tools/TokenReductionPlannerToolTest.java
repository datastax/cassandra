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

package org.apache.cassandra.tools;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.tokenallocator.TokenReductionPlanner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TokenReductionPlannerToolTest
{
    /** Same layout as the output of nodetool ring, with vnodes, a node with an unknown load and two datacenters. */
    private static List<String> nodetoolRing(Map<String, List<String>> tokensByEndpoint, Map<String, String> dcByEndpoint)
    {
        List<String> lines = new ArrayList<>();
        lines.add("");
        for (String dc : new String[]{ "dc1", "dc2" })
        {
            lines.add("Datacenter: " + dc);
            lines.add("==========");
            lines.add("Address        Rack        Status State   Load            Owns                Token");
            lines.add("                                                                              9000000000000000000");
            tokensByEndpoint.forEach((endpoint, tokens) -> {
                if (!dcByEndpoint.get(endpoint).equals(dc))
                    return;
                String load = endpoint.endsWith(".3") ? "?" : "1.5 GiB";
                for (String token : tokens)
                    lines.add(String.format("%-14s rack1       Up     Normal  %-15s 75.00%%              %s", endpoint, load, token));
            });
            lines.add("");
        }
        lines.add("  Warning: \"nodetool ring\" is used to output all the tokens of a node.");
        lines.add("  To view status related info of a node use \"nodetool status\" instead.");
        return lines;
    }

    @Test
    public void testParseNodetoolRing()
    {
        Map<String, List<String>> tokens = new java.util.LinkedHashMap<>();
        tokens.put("127.0.0.1", Arrays.asList("-100", "100"));
        tokens.put("127.0.0.2", Arrays.asList("-200", "200"));
        tokens.put("127.0.0.3", Arrays.asList("-300", "300"));
        tokens.put("127.0.1.1", Arrays.asList("-400", "400"));
        Map<String, String> dcs = new java.util.HashMap<>();
        tokens.keySet().forEach(e -> dcs.put(e, e.startsWith("127.0.1.") ? "dc2" : "dc1"));

        TokenReductionPlannerTool.Ring ring = TokenReductionPlannerTool.parseRing(nodetoolRing(tokens, dcs), Murmur3Partitioner.instance);
        assertThat(ring.nodes).hasSize(4);
        for (TokenReductionPlanner.Node node : ring.nodes)
        {
            assertThat(node.datacenter).isEqualTo(dcs.get(node.endpoint));
            assertThat(node.rack).isEqualTo("rack1");
            assertThat(node.tokens.stream().map(Token::toString).collect(Collectors.toList())).isEqualTo(tokens.get(node.endpoint));
        }
        // one node has an unknown load, so the loads are not used
        assertThat(ring.loads).isEmpty();
    }

    @Test
    public void testParseNodetoolRingWithLoads()
    {
        Map<String, List<String>> tokens = new java.util.LinkedHashMap<>();
        tokens.put("127.0.0.1:7000", Arrays.asList("-100", "100"));
        tokens.put("127.0.0.2:7000", Arrays.asList("-200", "200"));
        Map<String, String> dcs = new java.util.HashMap<>();
        tokens.keySet().forEach(e -> dcs.put(e, "dc1"));
        TokenReductionPlannerTool.Ring ring = TokenReductionPlannerTool.parseRing(nodetoolRing(tokens, dcs), Murmur3Partitioner.instance);
        assertThat(ring.nodes).hasSize(2);
        assertThat(ring.loads).containsEntry("127.0.0.1:7000", (long) (1.5 * 1024 * 1024 * 1024));
    }

    @Test
    public void testParseCsv()
    {
        List<String> lines = Arrays.asList("# endpoint,datacenter,rack,token",
                                           "10.0.0.1,dc1,r1,-5",
                                           "10.0.0.1,dc1,r1,5",
                                           "10.0.0.2,dc1,r2,7");
        TokenReductionPlannerTool.Ring ring = TokenReductionPlannerTool.parseRing(lines, Murmur3Partitioner.instance);
        assertThat(ring.nodes).hasSize(2);
        assertThat(ring.nodes.get(0).tokens).hasSize(2);
        assertThat(ring.nodes.get(1).rack).isEqualTo("r2");

        assertThatThrownBy(() -> TokenReductionPlannerTool.parseRing(Arrays.asList("10.0.0.1,dc1,r1,1", "10.0.0.1,dc2,r1,2"), Murmur3Partitioner.instance))
        .hasMessageContaining("more than one datacenter or rack");
        assertThatThrownBy(() -> TokenReductionPlannerTool.parseRing(Arrays.asList("10.0.0.1,dc1,1"), Murmur3Partitioner.instance))
        .hasMessageContaining("expected endpoint,datacenter,rack,token");
    }

    @Test
    public void testParseReplication()
    {
        assertThat(TokenReductionPlannerTool.parseReplication("dc1:3, dc2:5")).containsEntry("dc1", 3).containsEntry("dc2", 5);
        assertThatThrownBy(() -> TokenReductionPlannerTool.parseReplication("dc1")).hasMessageContaining("expected dc:rf");
    }

    @Test
    public void testPlanFiles() throws Exception
    {
        Random random = new Random(3);
        List<String> csv = new ArrayList<>();
        Set<Token> used = new HashSet<>();
        for (int node = 1; node <= 5; node++)
        {
            int added = 0;
            while (added < 32)
            {
                Token token = Murmur3Partitioner.instance.getRandomToken(random);
                if (used.add(token))
                {
                    csv.add(String.format("10.0.0.%d,dc1,rack%d,%s", node, node % 3, token));
                    added++;
                }
            }
        }
        Path dir = Files.createTempDirectory("tokenreduction");
        Path ringFile = dir.resolve("ring.csv");
        Files.write(ringFile, csv, StandardCharsets.UTF_8);
        Path output = dir.resolve("plan");

        PrintStream stdout = System.out;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8.name()));
        try
        {
            TokenReductionPlannerTool.main(new String[]{ "--ring", ringFile.toString(), "--replication", "dc1:3", "--target", "4", "--output", output.toString() });
        }
        finally
        {
            System.setOut(stdout);
        }
        String report = captured.toString(StandardCharsets.UTF_8.name());
        assertThat(report).contains("rounds [16, 8, 4]").contains("Datacenter dc1: 5 nodes, RF 3").contains("final ownership");
        String planFile = new String(Files.readAllBytes(output.resolve("plan.txt")), StandardCharsets.UTF_8);
        assertThat(planFile).contains("final ownership");
        assertThat(report).startsWith(planFile);

        for (String round : new String[]{ "round-1-16", "round-2-8", "round-3-4" })
        {
            List<Path> files;
            try (Stream<Path> list = Files.list(output.resolve(round)))
            {
                files = list.sorted().collect(Collectors.toList());
            }
            assertThat(files).hasSize(5);
            int expected = Integer.parseInt(round.substring(round.lastIndexOf('-') + 1));
            for (Path file : files)
            {
                assertThat(file.getFileName().toString()).matches("\\d{3}-10\\.0\\.0\\.\\d\\.tokens");
                assertThat(Files.readAllLines(file, StandardCharsets.UTF_8)).hasSize(expected);
            }
        }
    }
}
