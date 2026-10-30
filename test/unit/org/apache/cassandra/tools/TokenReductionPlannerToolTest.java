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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TokenReductionPlannerToolTest
{
    private Path dir;

    @Before
    public void createDirectory() throws Exception
    {
        dir = Files.createTempDirectory("tokenreduction");
    }

    @After
    public void deleteDirectory()
    {
        FileUtils.deleteRecursive(new File(dir));
    }

    /** A node row of nodetool ring. */
    private static final class Row
    {
        final String dc, endpoint, rack, state, load;
        final List<String> tokens;

        Row(String dc, String endpoint, String rack, String state, String load, String... tokens)
        {
            this.dc = dc;
            this.endpoint = endpoint;
            this.rack = rack;
            this.state = state;
            this.load = load;
            this.tokens = Arrays.asList(tokens);
        }
    }

    /** Output of nodetool ring, formatted exactly as {@code nodetool.Ring#printDc} does. */
    private static List<String> nodetoolRing(Row... rows)
    {
        int maxAddressLength = Arrays.stream(rows).mapToInt(r -> r.endpoint.length()).max().getAsInt();
        String format = String.format("%%-%ds  %%-12s%%-7s%%-8s%%-16s%%-20s%%-44s", maxAddressLength);
        Map<String, List<Row>> byDc = new LinkedHashMap<>();
        for (Row row : rows)
            byDc.computeIfAbsent(row.dc, dc -> new ArrayList<>()).add(row);

        List<String> lines = new ArrayList<>();
        lines.add("");
        byDc.forEach((dc, dcRows) -> {
            lines.add("Datacenter: " + dc);
            lines.add("==========");
            lines.add(String.format(format, "Address", "Rack", "Status", "State", "Load", "Owns", "Token"));
            List<String> dcTokens = dcRows.stream().flatMap(r -> r.tokens.stream()).collect(Collectors.toList());
            lines.add(String.format(format, "", "", "", "", "", "", dcTokens.get(dcTokens.size() - 1)));
            for (Row row : dcRows)
                for (String token : row.tokens)
                    lines.add(String.format(format, row.endpoint, row.rack, "Up", row.state, row.load, "75.00%", token));
            lines.add("");
        });
        lines.add("  Warning: \"nodetool ring\" is used to output all the tokens of a node.");
        lines.add("  To view status related info of a node use \"nodetool status\" instead.");
        lines.add("");
        return lines;
    }

    @Test
    public void testParseNodetoolRing()
    {
        List<String> lines = nodetoolRing(new Row("dc1", "127.0.0.1", "rack1", "Normal", "1.5 GiB", "-100", "100"),
                                          new Row("dc1", "127.0.0.2", "us-east-1a-rack", "Normal", "2 GiB", "-200", "200"),
                                          new Row("dc1", "127.0.0.3", "rack1", "Normal", "?", "-300", "300"),
                                          new Row("dc2", "127.0.1.1", "r", "Normal", "10 bytes", "-400", "400"));
        TokenReductionPlannerTool.Ring ring = TokenReductionPlannerTool.parseRing(lines, Murmur3Partitioner.instance);
        assertThat(ring.nodes).hasSize(4);
        assertThat(ring.nodes.stream().map(n -> n.endpoint + '/' + n.datacenter + '/' + n.rack + '/' + n.tokens))
        .containsExactly("127.0.0.1/dc1/rack1/[-100, 100]",
                         "127.0.0.2/dc1/us-east-1a-rack/[-200, 200]",
                         "127.0.0.3/dc1/rack1/[-300, 300]",
                         "127.0.1.1/dc2/r/[-400, 400]");
        // one node has an unknown load, so the loads are not used
        assertThat(ring.loads).isEmpty();
        assertThat(ring.warnings).containsExactly("the load of 127.0.0.3 is unknown, the report does not show sizes");
    }

    @Test
    public void testParseNodetoolRingWithPortsAndLoads()
    {
        List<String> lines = nodetoolRing(new Row("dc1", "127.0.0.1:7000", "rack1", "Normal", "1.5 GiB", "-100", "100"),
                                          new Row("dc1", "[0:0:0:0:0:0:0:2]:7000", "rack2", "Normal", "512 KiB", "-200", "200"));
        TokenReductionPlannerTool.Ring ring = TokenReductionPlannerTool.parseRing(lines, Murmur3Partitioner.instance);
        assertThat(ring.nodes.stream().map(n -> n.endpoint)).containsExactly("127.0.0.1:7000", "[0:0:0:0:0:0:0:2]:7000");
        assertThat(ring.loads).containsEntry("127.0.0.1:7000", (long) (1.5 * 1024 * 1024 * 1024))
                              .containsEntry("[0:0:0:0:0:0:0:2]:7000", 512L * 1024);
        assertThat(ring.warnings).isEmpty();
    }

    @Test
    public void testParseNodetoolRingWithUnparsableLoad()
    {
        // e.g. a node formatting sizes with a comma as decimal separator
        List<String> lines = nodetoolRing(new Row("dc1", "127.0.0.1", "rack1", "Normal", "1,5 GiB", "-100", "100"),
                                          new Row("dc1", "127.0.0.2", "rack1", "Normal", "2 GiB", "-200", "200"));
        TokenReductionPlannerTool.Ring ring = TokenReductionPlannerTool.parseRing(lines, Murmur3Partitioner.instance);
        assertThat(ring.nodes).hasSize(2);
        assertThat(ring.loads).isEmpty();
        assertThat(ring.warnings).hasSize(1);
        assertThat(ring.warnings.get(0)).contains("1,5 GiB");
    }

    @Test
    public void testParseNodetoolRingRefusesNodesNotNormal()
    {
        for (String state : new String[]{ "Joining", "Leaving", "Moving", "Shrink" })
        {
            List<String> lines = nodetoolRing(new Row("dc1", "127.0.0.1", "rack1", "Normal", "1 GiB", "-100"),
                                              new Row("dc1", "127.0.0.2", "rack1", state, "1 GiB", "-200"));
            assertThatThrownBy(() -> TokenReductionPlannerTool.parseRing(lines, Murmur3Partitioner.instance))
            .hasMessageContaining("node 127.0.0.2 is " + state);
        }
        // a state longer than its column runs into the load: the row is refused, not skipped
        List<String> lines = nodetoolRing(new Row("dc1", "127.0.0.1", "rack1", "Normal", "1 GiB", "-100"),
                                          new Row("dc1", "127.0.0.2", "rack1", "Shrinking", "1 GiB", "-200"));
        assertThatThrownBy(() -> TokenReductionPlannerTool.parseRing(lines, Murmur3Partitioner.instance))
        .hasMessageContaining("unexpected nodetool ring line: 127.0.0.2");
    }

    @Test
    public void testParseNodetoolRingRefusesUnexpectedLines()
    {
        List<String> lines = new ArrayList<>(nodetoolRing(new Row("dc1", "127.0.0.1", "rack1", "Normal", "1 GiB", "-100")));
        lines.add(5, "127.0.0.9 something unexpected");
        assertThatThrownBy(() -> TokenReductionPlannerTool.parseRing(lines, Murmur3Partitioner.instance))
        .hasMessageContaining("unexpected nodetool ring line");
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
        assertThatThrownBy(() -> TokenReductionPlannerTool.parseRing(Arrays.asList("10.0.0.1,dc1,r1,1", "10.0.0.2,dc2,r1,1"), Murmur3Partitioner.instance))
        .hasMessageContaining("token 1 is listed for both 10.0.0.1 and 10.0.0.2");
        assertThatThrownBy(() -> TokenReductionPlannerTool.parseRing(Arrays.asList("10.0.0.1,dc1,r1,1", "10.0.0.1,dc1,r1,1"), Murmur3Partitioner.instance))
        .hasMessageContaining("token 1 is listed twice for 10.0.0.1");
    }

    @Test
    public void testParseReplication()
    {
        assertThat(TokenReductionPlannerTool.parseReplication("dc1:3, dc2:5")).containsEntry("dc1", 3).containsEntry("dc2", 5);
        assertThatThrownBy(() -> TokenReductionPlannerTool.parseReplication("dc1")).hasMessageContaining("expected dc:rf");
    }

    private static final class Result
    {
        final int exitCode;
        final String out;
        final String err;

        Result(int exitCode, String out, String err)
        {
            this.exitCode = exitCode;
            this.out = out;
            this.err = err;
        }
    }

    private static Result run(String... args) throws Exception
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        int exitCode = TokenReductionPlannerTool.run(args,
                                                     new PrintStream(out, true, StandardCharsets.UTF_8.name()),
                                                     new PrintStream(err, true, StandardCharsets.UTF_8.name()));
        return new Result(exitCode, out.toString(StandardCharsets.UTF_8.name()), err.toString(StandardCharsets.UTF_8.name()));
    }

    private Path randomRingCsv(int dc1Nodes, int dc2Nodes, int tokensPerNode) throws Exception
    {
        Random random = new Random(3);
        List<String> csv = new ArrayList<>();
        Set<Token> used = new HashSet<>();
        for (int node = 1; node <= dc1Nodes + dc2Nodes; node++)
        {
            String dc = node <= dc1Nodes ? "dc1" : "dc2";
            int added = 0;
            while (added < tokensPerNode)
            {
                Token token = Murmur3Partitioner.instance.getRandomToken(random);
                if (used.add(token))
                {
                    csv.add(String.format("10.0.0.%d,%s,rack%d,%s", node, dc, node % 3, token));
                    added++;
                }
            }
        }
        Path ringFile = dir.resolve("ring.csv");
        Files.write(ringFile, csv, StandardCharsets.UTF_8);
        return ringFile;
    }

    @Test
    public void testPlanFiles() throws Exception
    {
        Path ringFile = randomRingCsv(5, 3, 32);
        Path output = dir.resolve("plan");
        Result result = run("--ring", ringFile.toString(), "--replication", "dc1:3,dc2:0", "--target", "4", "--output", output.toString());
        assertThat(result.exitCode).as(result.err).isEqualTo(0);
        assertThat(result.out).contains("rounds [16, 8, 4]")
                              .contains("Datacenter dc1: 5 nodes, RF 3")
                              .contains("Datacenter dc2: 3 nodes, RF 0 (balanced as RF 1")
                              .contains("final ownership");
        String planFile = new String(Files.readAllBytes(output.resolve("plan.txt")), StandardCharsets.UTF_8);
        assertThat(planFile).contains("final ownership");
        assertThat(result.out).startsWith(planFile);

        for (String round : new String[]{ "round-1-16", "round-2-8", "round-3-4" })
        {
            int expected = Integer.parseInt(round.substring(round.lastIndexOf('-') + 1));
            for (String dc : new String[]{ "dc1", "dc2" })
            {
                List<Path> files;
                try (Stream<Path> list = Files.list(output.resolve(round).resolve(dc)))
                {
                    files = list.sorted().collect(Collectors.toList());
                }
                assertThat(files).hasSize(dc.equals("dc1") ? 5 : 3);
                for (int i = 0; i < files.size(); i++)
                {
                    assertThat(files.get(i).getFileName().toString()).matches(String.format("%04d-10\\.0\\.0\\.\\d\\.tokens", i + 1));
                    assertThat(Files.readAllLines(files.get(i), StandardCharsets.UTF_8)).hasSize(expected);
                }
            }
        }

        // the same output directory is refused, so that keep files of two plans are never mixed
        Result again = run("--ring", ringFile.toString(), "--replication", "dc1:3,dc2:0", "--target", "8", "--output", output.toString());
        assertThat(again.exitCode).isEqualTo(1);
        assertThat(again.err).contains("is not empty");
    }

    @Test
    public void testRoundWithoutStepsInADatacenter() throws Exception
    {
        List<String> csv = new ArrayList<>(Files.readAllLines(randomRingCsv(4, 0, 16), StandardCharsets.UTF_8));
        // a second datacenter that already has fewer tokens than the first rounds
        csv.add("10.0.1.1,dc2,r1,11");
        csv.add("10.0.1.1,dc2,r1,12");
        csv.add("10.0.1.2,dc2,r1,21");
        csv.add("10.0.1.2,dc2,r1,22");
        Path ringFile = dir.resolve("ring2.csv");
        Files.write(ringFile, csv, StandardCharsets.UTF_8);
        Result result = run("--ring", ringFile.toString(), "--replication", "dc1:2,dc2:1", "--target", "1", "--output", dir.resolve("plan2").toString());
        assertThat(result.exitCode).as(result.err).isEqualTo(0);
        assertThat(result.out).contains("no node of this datacenter has more tokens than the target")
                              .doesNotContain("round peak ownership 0.000");
    }

    @Test
    public void testUsageErrors() throws Exception
    {
        Path ringFile = randomRingCsv(3, 0, 4);
        assertThat(run("--ring", ringFile.toString()).err).contains("Missing required option");
        Result partitioner = run("--ring", ringFile.toString(), "--replication", "dc1:3", "--target", "2", "--output", dir.resolve("p").toString(),
                                 "--partitioner", "ByteOrderedPartitioner");
        assertThat(partitioner.exitCode).isEqualTo(1);
        assertThat(partitioner.err).contains("only Murmur3Partitioner and RandomPartitioner are supported");
        Result unknown = run("--ring", ringFile.toString(), "--replication", "dc1:3", "--target", "2", "--output", dir.resolve("p").toString(),
                             "--partitioner", "NoSuchPartitioner");
        assertThat(unknown.exitCode).isEqualTo(1);
        Result missingDc = run("--ring", ringFile.toString(), "--replication", "dc9:3", "--target", "2", "--output", dir.resolve("p").toString());
        assertThat(missingDc.exitCode).isEqualTo(1);
        assertThat(missingDc.err).contains("no replication factor for datacenter dc1");
        Path file = Files.write(dir.resolve("a-file"), Collections.singletonList("x"), StandardCharsets.UTF_8);
        Result notADirectory = run("--ring", ringFile.toString(), "--replication", "dc1:3", "--target", "2", "--output", file.toString());
        assertThat(notADirectory.exitCode).isEqualTo(1);
        assertThat(notADirectory.err).contains("is not a directory");
        Result missingRing = run("--ring", dir.resolve("missing").toString(), "--replication", "dc1:3", "--target", "2", "--output", dir.resolve("p").toString());
        assertThat(missingRing.exitCode).isEqualTo(1);
        assertThat(missingRing.err).contains("I/O error");
    }
}
