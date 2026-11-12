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

package org.apache.cassandra.cql3;

import java.util.function.Supplier;

import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.exceptions.QueryCancelledException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.utils.MonotonicClock.approxTime;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A read that skips through data covered by a deletion emits nothing while doing so, so it never reaches the
 * cancellation check that {@link ReadCommand#executeLocally} applies to the merged result. The merge has to check
 * for cancellation itself. Otherwise such a read runs on past its timeout, keeping its read OpOrder group open (and
 * memtable reclaim blocked) for as long as the scan takes.
 */
public class ShadowedReadTimeoutTest extends CQLTester
{
    private static final int SSTABLES = 10;
    private static final int ROWS_PER_SSTABLE = 50_000;
    private static final long TIMEOUT_MS = 10;

    @Test
    public void testSinglePartitionReadOfRangeDeletedRows() throws Throwable
    {
        ColumnFamilyStore cfs = populate();
        execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = 0 AND ck < ?", (long) SSTABLES + 1, SSTABLES * ROWS_PER_SSTABLE);
        assertCancelled(() -> Util.cmd(cfs, 0).build());
    }

    @Test
    public void testRangeReadOfRangeDeletedRows() throws Throwable
    {
        ColumnFamilyStore cfs = populate();
        execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = 0 AND ck < ?", (long) SSTABLES + 1, SSTABLES * ROWS_PER_SSTABLE);
        assertCancelled(() -> Util.cmd(cfs).build());
    }

    @Test
    public void testRangeReadOfDeletedPartition() throws Throwable
    {
        ColumnFamilyStore cfs = populate();
        execute("DELETE FROM %s USING TIMESTAMP ? WHERE pk = 0", (long) SSTABLES + 1);
        assertCancelled(() -> Util.cmd(cfs).build());
    }

    /**
     * Writes one partition whose rows are spread over several sstables.
     */
    private ColumnFamilyStore populate() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (int s = 0; s < SSTABLES; ++s)
        {
            UpdateBuilder builder = UpdateBuilder.create(cfs.metadata(), 0).withTimestamp(s + 1);
            for (int i = 0; i < ROWS_PER_SSTABLE; ++i)
                builder.newRow(s * ROWS_PER_SSTABLE + i).add("v", i);
            builder.applyUnsafe();
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        }
        return cfs;
    }

    private void assertCancelled(Supplier<ReadCommand> commands)
    {
        // Without a timeout, the read goes through all the rows and returns nothing.
        long start = System.nanoTime();
        assertTrue(Util.getAll(commands.get()).isEmpty());
        long fullScanMs = NANOSECONDS.toMillis(System.nanoTime() - start);
        assertTrue("Reading the deleted rows took only " + fullScanMs + " ms, too fast to tell if it is cancelled.\n" +
                   "Please increase this test's ROWS_PER_SSTABLE and/or SSTABLES.",
                   fullScanMs >= 5 * TIMEOUT_MS);

        // With a timeout, executed the way ReadCommandVerbHandler does it, the read must be cancelled.
        ReadCommand command = commands.get();
        command.setMonitoringTime(approxTime.now(), false, MILLISECONDS.toNanos(TIMEOUT_MS), MILLISECONDS.toNanos(TIMEOUT_MS));
        start = System.nanoTime();
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator iterator = command.executeLocally(controller))
        {
            command.createResponse(iterator, controller.getRepairedDataInfo());
            fail(String.format("Read with a %d ms timeout was not cancelled, it took %d ms (%d ms without a timeout)",
                               TIMEOUT_MS, NANOSECONDS.toMillis(System.nanoTime() - start), fullScanMs));
        }
        catch (QueryCancelledException e)
        {
            logger.info("Read with a {} ms timeout cancelled after {} ms ({} ms without a timeout)",
                        TIMEOUT_MS, NANOSECONDS.toMillis(System.nanoTime() - start), fullScanMs);
        }
        assertTrue(command.isAborted());
        assertFalse(command.complete());
    }
}
