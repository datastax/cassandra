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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.dsecompat.DseFixtures.DseVersion;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.CacheService;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What a node upgraded in place leaves on disk after running in {@code DSE_6} mode, and therefore what a
 * rollback to DSE has to do. The HCD 1.2 nodes write the same files as DSE nodes (trie {@code bb}, numeric
 * generations); node 2 is stopped while node 1 is down for its upgrade, which makes the upgraded node write
 * hints, and the commit log segment of the DSE 6.8.65 fixtures is put in the commit log directory before the
 * restart, as a node that was not drained leaves it. After writes, a flush and a stop of the node (the harness stop
 * flushes every table and fsyncs hints and commit log, the state a drain followed by a stop leaves), every configured
 * directory is compared with an allowlist:
 * <ul>
 * <li>data directories: only {@code bb} sstable components with numeric generations, in every keyspace
 *     (system keyspaces included), and new ones written by this software; DSE reads them: keep; plus this
 *     software's paxos repair state {@code system/_paxos_repair_state/ballot.meta}: delete; a transaction log that a drain
 *     or a crash leaves, {@code bb_txn_<type>_<id>.log} with the remove records DSE 6 checks (the stop here leaves
 *     none): keep, DSE 6 completes it at its start ({@code DseTransactionLogs});</li>
 * <li>commit log: this software's {@code CommitLog-7-*.log} segments (the 4.0 layout of {@code HCD_1}), which
 *     DSE cannot read, and every DSE segment found at the start (one here), replayed (its rows are in the data)
 *     and renamed {@code *.log.replayed} so that it is not replayed again (a later deletion stays deleted after a
 *     restart, tested): delete them all after the drain; a pattern such as {@code CommitLog-*.log} does not match
 *     {@code *.log.replayed}. Deleting the DSE segments loses the writes DSE had not flushed to the tables this
 *     software does not know (in the fixture NodeSync's status and checkpoints, which NodeSync rebuilds; any other
 *     DSE-only table the same way, rebuilt by nothing); renaming them back for DSE to replay would bring back rows
 *     whose deletion was purged since. The {@code *.log.replayed} files are no segments to this software: the
 *     procedure that drops the mode must delete them too;</li>
 * <li>hints: only this software's {@code <host id>-<ts>-2.hints}, {@code .crc32} and {@code -2-Statistics.hints}
 *     (version 2 of {@code HCD_1}), which DSE does not read: delete. While this software runs it delivers them to
 *     a DSE peer that is up (at 11, each hint counted as delivered once DSE acknowledges it); the writes of the hints
 *     still pending at the rollback reach the peer only by a repair once every node runs the same product again.
 *     They include the hints converted from DSE's own hints files at the first start of this software
 *     ({@code DseHintsConverter}), whose DSE files are gone by then: drain the hints before a rollback. Here they
 *     stay pending: the peer stays down, and the in-JVM instances never start the hints dispatch (no gossip, see
 *     {@link #startHintsDispatch});</li>
 * <li>metadata: {@code nodes/local} and {@code nodes/peers} byte-for-byte as before the upgrade: keep; the import
 *     marker {@code nodes/.imported-by-hcd-<host id>}: delete. A later upgrade of the node already imports again
 *     when DSE rewrote {@code nodes/local} or {@code nodes/peers} after the rollback (the marker records their size
 *     and modification time); deleting it also covers a rewrite that keeps both (a restore that preserves the
 *     modification time, a coarse file-system clock). Its temporary file {@code nodes/.hcd-import-marker.tmp},
 *     left only by a crash while the marker is written: delete too; the upgrade marker {@code dse-upgrade-in-progress}:
 *     keep while the cluster is mixed (it is what refuses a start of this software without the mode);</li>
 * <li>saved caches: only cache files of this software, if any (none after a forced save here): delete.</li>
 * </ul>
 * A real first start also snapshots the local system keyspaces ({@code snapshots/upgrade-<old>-<new>-<ts>}, from
 * {@code SystemKeyspace.snapshotOnVersionChange}), which the in-JVM start skips; the snapshot is the restore point of
 * the system tables for a rollback: keep.
 * The content of the system tables and of {@code system_schema} written by this software cannot be checked by
 * a directory walk, and is not checked here.
 */
public class Dse6RollbackArtefactsTest extends DseDialectTestBase
{
    private static final String FIXTURE_KEYSPACE = DseFixtures.FIXTURE_KEYSPACE;

    /** {@code bb-<generation>-bti-<component>}, optionally below a keyspace/table directory. */
    private static final Pattern BB_COMPONENT = Pattern.compile("(?:.*/)?bb-([0-9]+)-bti-[A-Za-z+0-9_.-]+");
    private static final Pattern COMMIT_LOG_SEGMENT_V7 = Pattern.compile("CommitLog-7-[0-9]+\\.log");
    private static final Pattern HINTS_FILE_V2 = Pattern.compile("[0-9a-f-]{36}-[0-9]+-2(\\.hints|\\.crc32|-Statistics\\.hints)");
    private static final String PAXOS_REPAIR_STATE = "system/_paxos_repair_state/ballot.meta";
    private static final Pattern SAVED_CACHE = Pattern.compile(".*(KeyCache|RowCache|CounterCache).*\\.(db|crc)");

    @Test
    public void rollbackDeletesCommitLogHintsCachesAndTheImportMarkerOnly() throws Throwable
    {
        AtomicInteger highestGenerationBeforeUpgrade = new AtomicInteger(-1);
        Map<String, byte[]> nodeFilesBeforeUpgrade = new HashMap<>();
        String fixtureSegment = DseFixtures.commitLogSegmentGz(DseVersion.DSE_6_8_65).getFileName().toString().replace(".gz", "");

        dse6TestCase(2)
        .nodesToUpgrade(1)
        .setup(cluster -> {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".t (k int PRIMARY KEY, v text)");
            for (int i = 0; i < 20; i++)
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".t (k, v) VALUES (?, ?)", ConsistencyLevel.ALL, i, "v" + i);
            cluster.get(1).flush(KEYSPACE);
            cluster.get(2).flush(KEYSPACE);
            // the fixture table with the id it had on the DSE node that wrote the segment
            for (String statement : fixtureSchema(DseVersion.DSE_6_8_65, 2))
                cluster.schemaChange(statement);
            cluster.schemaChange("ALTER TABLE " + FIXTURE_KEYSPACE + ".t WITH gc_grace_seconds = 0");
            highestGenerationBeforeUpgrade.set(highestGeneration(dataFiles(cluster.get(1).config()), KEYSPACE));
        })
        .runBeforeNodeRestart((cluster, node) -> {
            // node 2 goes down only now, while node 1 is stopped: the HCD 1.2 stand-in must not write hints of its own
            // (named with its messaging version, 11) that a real DSE node would have written as -v1 files instead
            try
            {
                cluster.get(2).shutdown().get();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            // the harness has already configured the node for the DSE_6 mode
            IInstanceConfig config = cluster.get(node).config();
            Path nodes = Path.of(config.getString("metadata_directory")).resolve("nodes");
            for (String name : new String[]{ "local", "peers" })
                nodeFilesBeforeUpgrade.put(name, read(nodes.resolve(name)));
            try
            {
                DseFixtures.unpackCommitLogSegment(DseVersion.DSE_6_8_65, Path.of(config.getString("commitlog_directory")));
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        })
        .runAfterNodeUpgrade((cluster, node) -> {
            assertThat(compatStatus(cluster, node)).contains("Internode compatibility mode : DSE_6");
            // the DSE segment was replayed at the start: the data set of the fixtures README
            assertThat(cluster.get(node).executeInternal("SELECT count(*) FROM " + FIXTURE_KEYSPACE + ".t")[0][0])
            .isEqualTo((long) DseFixtures.COLLECTION_TABLE_COUNT_STAR);
            assertThat(cluster.get(node).executeInternal("SELECT v FROM " + FIXTURE_KEYSPACE + ".t WHERE id = 25 AND ck = 'ck2'")[0][0]).isEqualTo("val50");

            String downPeer = cluster.get(2).config().hostId().toString();
            // RF 2 and one replica down: every write at ONE leaves a hint for the down peer
            for (int i = 100; i < 120; i++)
                cluster.coordinator(node).execute("INSERT INTO " + KEYSPACE + ".t (k, v) VALUES (?, ?)", ConsistencyLevel.ONE, i, "v" + i);
            cluster.get(node).flush(KEYSPACE);
            invokable(cluster, node).runOnInstance(() -> {
                try
                {
                    CacheService.instance.saveCaches();
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            });

            // a deletion whose tombstone is purged (gc_grace_seconds 0, major compaction) must survive a restart: the DSE
            // segment is moved aside after its replay, not replayed again at every start
            cluster.get(node).executeInternal("DELETE FROM " + FIXTURE_KEYSPACE + ".t WHERE id = 25");
            cluster.get(node).flush(FIXTURE_KEYSPACE);
            sleepMillis(1100); // the tombstone's deletion time must be in the past for gc_grace_seconds 0 to purge it
            // twice: the replay flush wrote one sstable spanning the ranges of every data directory, which the first major
            // compaction only splits per directory; the second merges the replayed rows with the tombstone and purges both
            cluster.get(node).nodetoolResult("compact", FIXTURE_KEYSPACE, "t").asserts().success();
            cluster.get(node).nodetoolResult("compact", FIXTURE_KEYSPACE, "t").asserts().success();
            assertThat(invokable(cluster, node).callOnInstance(Dse6RollbackArtefactsTest::partition25OnDisk))
            .as("partition 25 and its tombstone purged by the compaction").isEmpty();

            // the harness stop flushes every table, fsyncs the hints and closes the commit log, as nodetool drain followed
            // by a stop does (a real drain cannot precede it here: the harness stop flushes the schema tables again)
            cluster.get(node).shutdown().get();

            IInstanceConfig config = cluster.get(node).config();

            List<String> data = dataFiles(config);
            assertThat(data).isNotEmpty();
            // the paxos repair state of this software (not written by the 4.0 line or DSE) is the one non-sstable file
            assertThat(data).contains(PAXOS_REPAIR_STATE);
            assertThat(data).allMatch(n -> n.equals(PAXOS_REPAIR_STATE) || BB_COMPONENT.matcher(n).matches(),
                                      "only trie bb components, numeric generations: " + data);
            assertThat(highestGeneration(data, KEYSPACE)).as("sstables written by this software")
                                                        .isGreaterThan(highestGenerationBeforeUpgrade.get());
            assertThat(data).anyMatch(n -> n.startsWith(FIXTURE_KEYSPACE + "/") && n.endsWith("-Data.db"));

            List<String> commitLog = walk(config.getString("commitlog_directory"));
            String movedAside = fixtureSegment + CommitLog.REPLAYED_DSE_SEGMENT_SUFFIX;
            assertThat(commitLog).contains(movedAside).doesNotContain(fixtureSegment);
            assertThat(commitLog).anyMatch(n -> COMMIT_LOG_SEGMENT_V7.matcher(n).matches());
            assertThat(commitLog).allMatch(n -> n.equals(movedAside) || COMMIT_LOG_SEGMENT_V7.matcher(n).matches(),
                                           "commit log version 7 and the replayed DSE segment: " + commitLog);

            List<String> hints = walk(config.getString("hints_directory"));
            assertThat(hints).anyMatch(n -> n.startsWith(downPeer) && n.endsWith(".hints"));
            assertThat(hints).allMatch(n -> HINTS_FILE_V2.matcher(n).matches(), "hints version 2: " + hints);

            Path metadataDir = Path.of(config.getString("metadata_directory"));
            assertThat(walk(metadataDir.toString())).containsExactlyInAnyOrder("dse-upgrade-in-progress",
                                                                               "nodes/local",
                                                                               "nodes/peers",
                                                                               "nodes/.imported-by-hcd-" + config.hostId());
            for (Map.Entry<String, byte[]> file : nodeFilesBeforeUpgrade.entrySet())
                assertThat(read(metadataDir.resolve("nodes").resolve(file.getKey()))).as("nodes/" + file.getKey() + " untouched")
                                                                                     .isEqualTo(file.getValue());

            // even after a forced save no cache file is written here (trie tables do not use the key cache, the row and
            // counter caches are empty); whatever a real node saves is a cache file of this software
            List<String> caches = walk(config.getString("saved_caches_directory"));
            assertThat(caches).allMatch(n -> SAVED_CACHE.matcher(n).matches(), "saved caches only: " + caches);

            assertThat(walk(config.getString("cdc_raw_directory"))).as("no CDC segments").isEmpty();

            // restart: the moved-aside segment is not replayed, the purged partition stays deleted
            cluster.get(node).startup();
            // partition 25 held two regular rows (ck1, ck2)
            assertThat(cluster.get(node).executeInternal("SELECT count(*) FROM " + FIXTURE_KEYSPACE + ".t")[0][0])
            .isEqualTo((long) DseFixtures.COLLECTION_TABLE_COUNT_STAR - 2);
            assertThat(cluster.get(node).executeInternal("SELECT * FROM " + FIXTURE_KEYSPACE + ".t WHERE id = 25")).isEmpty();
        })
        .run();
    }

    /**
     * Runs in the instance: what the live sstables of the fixture table still hold for partition 25 (data or
     * tombstone), empty when nothing.
     */
    private static String partition25OnDisk()
    {
        ColumnFamilyStore cfs = Keyspace.open(FIXTURE_KEYSPACE).getColumnFamilyStore("t");
        DecoratedKey key = cfs.decorateKey(Int32Type.instance.decompose(25));
        StringBuilder found = new StringBuilder();
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            try (ISSTableScanner scanner = sstable.getScanner())
            {
                while (scanner.hasNext())
                {
                    try (UnfilteredRowIterator partition = scanner.next())
                    {
                        if (!partition.partitionKey().equals(key))
                            continue;
                        found.append(sstable.getFilename()).append(": deletion ").append(partition.partitionLevelDeletion());
                        while (partition.hasNext())
                            found.append(", ").append(partition.next().toString(cfs.metadata()));
                        found.append("; ");
                    }
                }
            }
        }
        return found.toString();
    }

    private static IInvokableInstance invokable(UpgradeableCluster cluster, int node)
    {
        // the upgradeable wrapper delegates to an invokable instance of the version it runs
        return (IInvokableInstance) cluster.get(node);
    }

    /** Every file of the data directories, relative to its data directory ({@code <keyspace>/<table dir>/<file>}). */
    private static List<String> dataFiles(IInstanceConfig config)
    {
        List<String> files = new ArrayList<>();
        for (String dir : (String[]) config.get("data_file_directories"))
            files.addAll(walk(dir));
        return files;
    }

    private static int highestGeneration(List<String> dataFiles, String keyspace)
    {
        int highest = -1;
        for (String file : dataFiles)
        {
            Matcher m = BB_COMPONENT.matcher(file);
            if (file.startsWith(keyspace + "/") && file.endsWith("-Data.db") && m.matches())
                highest = Math.max(highest, Integer.parseInt(m.group(1)));
        }
        return highest;
    }

    private static byte[] read(Path path)
    {
        try
        {
            return Files.readAllBytes(path);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    /** Every regular file under the directory, relative to it and sorted; empty when the directory does not exist. */
    private static List<String> walk(String dir)
    {
        if (dir == null)
            return new ArrayList<>();
        Path root = Path.of(dir);
        if (!Files.isDirectory(root))
            return new ArrayList<>();
        try (Stream<Path> paths = Files.walk(root))
        {
            return paths.filter(Files::isRegularFile)
                        .map(p -> root.relativize(p).toString())
                        .sorted()
                        .collect(Collectors.toCollection(ArrayList::new));
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
