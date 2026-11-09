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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.dsecompat.DseFixtures.DseVersion;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A node upgraded in place finds a hints file DataStax Enterprise 6 wrote for a peer that is still DSE (the HCD 1.2
 * stand-in at messaging version 11; HCD 1.2 writes hints files in its own format, so the file is the 6.8.65 fixture
 * written for the peer's host id, with its hints created now so that they are live). In {@code DSE_6} mode the hints
 * dispatch start converts it into a hints file of this software and the hints reach the peer at 11, which applies
 * them: the data set of the fixtures README, which nothing else sends it. The DSE file and its companions are
 * deleted, and the converted file once delivered. The in-JVM nodes run without gossip, so the dispatch is started by
 * reflection ({@link #startHintsDispatch}) rather than by {@code StorageService.prepareToJoin}: the production call
 * site, after the schema load and the import of DSE's node metadata, is not exercised here.
 */
public class Dse6HintsConversionTest extends DseDialectTestBase
{
    private static final int UPGRADED = 1;
    private static final int DSE = 2;
    private static final String FIXTURE_TABLE = DseFixtures.FIXTURE_KEYSPACE + ".t";

    @Test
    public void dseHintsAreConvertedAndDeliveredToTheDsePeer() throws Throwable
    {
        AtomicReference<Path> dseFile = new AtomicReference<>();
        dse6TestCase(2, config -> config.set("hinted_handoff_enabled", true)
                                        .set("hints_flush_period_in_ms", 1000))
        .nodesToUpgrade(UPGRADED)
        .setup(cluster -> {
            // the fixture table with the id it had on the DSE node that wrote the hints, replicated on both nodes
            for (String statement : fixtureSchema(DseVersion.DSE_6_8_65, 2))
                cluster.schemaChange(statement);
        })
        .runBeforeNodeRestart((cluster, node) -> {
            UUID peer = cluster.get(DSE).config().hostId();
            Path hints = Path.of(cluster.get(node).config().getString("hints_directory"));
            long now = currentTimeMillis();
            try
            {
                Path file = DseFixtures.writeDseHintsFile(DseVersion.DSE_6_8_65, hints, peer, now, now);
                Files.write(companion(file, ".crc32"), "0".getBytes(StandardCharsets.UTF_8));
                Files.write(companion(file, "-Statistics.hints"), new byte[8]);
                dseFile.set(file);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        })
        .runAfterNodeUpgrade((cluster, node) -> {
            assertThat(compatStatus(cluster, node)).contains("Internode compatibility mode : DSE_6");
            assertThat(cluster.get(DSE).executeInternal("SELECT count(*) FROM " + FIXTURE_TABLE)[0][0]).isEqualTo(0L);
            // the catalog leaves the DSE file alone; the dispatch start converts it (the in-JVM nodes start it only on demand)
            assertThat(dseFile.get()).exists();
            startHintsDispatch(node);

            awaitAsserted(120, () -> assertThat(cluster.get(DSE).executeInternal("SELECT count(*) FROM " + FIXTURE_TABLE)[0][0])
                                     .isEqualTo((long) DseFixtures.COLLECTION_TABLE_COUNT_STAR));
            assertThat(cluster.get(DSE).executeInternal("SELECT v FROM " + FIXTURE_TABLE + " WHERE id = 25 AND ck = 'ck2'")[0][0]).isEqualTo("val50");
            assertThat(cluster.get(DSE).executeInternal("SELECT id, ck FROM " + FIXTURE_TABLE + " WHERE id = 1")).isEmpty();
            assertThat(dseFile.get()).doesNotExist();
            assertThat(companion(dseFile.get(), ".crc32")).doesNotExist();
            assertThat(companion(dseFile.get(), "-Statistics.hints")).doesNotExist();
            // DSE's nodesync_status is the one table of the fixture this software does not know
            int unknownTable = 0;
            int records = 0;
            try
            {
                for (byte[] record : DseFixtures.hintsRecords(DseVersion.DSE_6_8_65))
                {
                    records++;
                    if (DseFixtures.hintTableId(record).equals(DseFixtures.NODESYNC_STATUS_TABLE_ID))
                        unknownTable++;
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            int converted = records - unknownTable;
            assertThat(converted).isEqualTo(DseFixtures.FIXTURE_MUTATIONS_PER_RUN + 3); // the fixture table and three role_permissions hints
            assertThat(cluster.get(node).logs().grep("DSE 6 compatibility mode: converted the hints file " + dseFile.get().getFileName() +
                                                     " written by DataStax Enterprise 6 for host " + cluster.get(DSE).config().hostId() +
                                                     " \\(.*\\): " + converted + " hint\\(s\\) written to .*-2\\.hints, " + unknownTable +
                                                     " dropped \\(0 for a host not in the ring, " + unknownTable +
                                                     " for an unknown table, 0 for an unknown column, 0 expired\\); deleted it and its companion files")
                                               .getResult()).hasSize(1);
            assertThat(cluster.get(node).logs().grep("DSE 6 compatibility mode: 1 hints file\\(s\\) written by DataStax Enterprise 6 found in .*, " +
                                                     "1 converted and deleted, 0 gone before their conversion, 0 left in place after a failure; " + converted + " hint\\(s\\) converted")
                                               .getResult()).hasSize(1);
            // delivered and acknowledged: the converted file goes too
            awaitAsserted(60, () -> assertThat(hintsFiles(cluster, node)).isEmpty());
        })
        .run();
    }

    private static Path companion(Path dseFile, String suffix)
    {
        String name = dseFile.getFileName().toString();
        return dseFile.resolveSibling(name.substring(0, name.length() - ".hints".length()) + suffix);
    }

    private static Iterable<String> hintsFiles(UpgradeableCluster cluster, int node)
    {
        try (Stream<Path> files = Files.list(Path.of(cluster.get(node).config().getString("hints_directory"))))
        {
            return files.map(p -> p.getFileName().toString()).filter(n -> n.endsWith(".hints")).collect(Collectors.toList());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
