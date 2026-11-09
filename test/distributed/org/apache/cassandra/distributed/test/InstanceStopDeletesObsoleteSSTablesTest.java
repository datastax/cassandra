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

package org.apache.cassandra.distributed.test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The stop of an in-JVM instance lets the deletions of the sstables a compaction made obsolete run before
 * {@code SSTableReader.shutdownBlocking} clears the lookup those deletions need: no input of the compaction and no
 * transaction log is left in the table directory after the stop, and the compaction's output stays.
 */
public class InstanceStopDeletesObsoleteSSTablesTest extends TestBaseImpl
{
    /** The longest the task queued ahead of the deletions waits for the stop to shut the stages down. */
    private static final int BLOCKER_MAX_SECONDS = 60;

    @Test
    public void deletionsQueuedBeforeTheStopRunBeforeTheLookupIsCleared() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(1).withDataDirCount(1).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (pk int PRIMARY KEY, v int) WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'false'}"));
            IInvokableInstance node = cluster.get(1);
            for (int i = 0; i < 2; i++)
            {
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.t (pk, v) VALUES (?, ?)"), ConsistencyLevel.ONE, i, i);
                node.flush(KEYSPACE);
            }

            List<String> inputs = node.callOnInstance(() -> {
                List<String> files = new ArrayList<>();
                for (SSTableReader sstable : Keyspace.open(KEYSPACE).getColumnFamilyStore("t").getLiveSSTables())
                    files.add(sstable.getDataFile().absolutePath());
                return files;
            });
            assertEquals("sstables flushed before the compaction: " + inputs, 2, inputs.size());
            String tableDirectory = node.callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("t")
                                                                      .getDirectories().getDirectoryForNewSSTables().absolutePath());

            String output = node.callOnInstance(() -> {
                // keep the single thread of the executor busy until the stop has shut the stages down, so that the
                // deletions of the compaction's inputs are still queued behind this task at that point of the stop
                ScheduledExecutors.nonPeriodicTasks.execute(() -> {
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(BLOCKER_MAX_SECONDS);
                    while (!Stage.MUTATION.executor().isShutdown() && System.nanoTime() < deadline)
                        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
                });
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("t");
                cfs.forceMajorCompaction();
                assertEquals("sstables after the compaction", 1, cfs.getLiveSSTables().size());
                return cfs.getLiveSSTables().iterator().next().getDataFile().absolutePath();
            });
            for (String input : inputs)
                assertTrue(input + " was deleted before the stop, the busy executor did not hold its deletion back", Files.exists(Paths.get(input)));

            node.shutdown().get();

            for (String input : inputs)
                assertFalse(input + " is left on disk after the stop", Files.exists(Paths.get(input)));
            assertTrue("the output of the compaction " + output + " is gone after the stop", Files.exists(Paths.get(output)));
            try (Stream<Path> files = Files.list(Paths.get(tableDirectory)))
            {
                List<String> logs = files.map(p -> p.getFileName().toString()).filter(n -> n.contains("_txn_")).collect(Collectors.toList());
                assertTrue("transaction logs left in " + tableDirectory + " after the stop: " + logs, logs.isEmpty());
            }
        }
    }
}
