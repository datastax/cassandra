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

package org.apache.cassandra.tools.dsepreflight;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogDescriptor;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.io.util.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The rule of the rollback check on the commit log ({@link Dse51RollbackInspection#countEntries}), on segments this
 * software writes: they hold an entry per mutation written, counted without being decoded, and none once the tables
 * are flushed and the segments recycled and the commit log shut down, as {@code nodetool drain} does. The commit log
 * of this software's version is the same in every mode, so the test runs with the defaults of the unit tests.
 */
public class Dse51RollbackCommitLogTest extends CQLTester
{
    /** The entries of every segment of the commit log directory, and the errors of their reads. */
    private static int entries(List<String> errors)
    {
        int entries = 0;
        File[] segments = DatabaseDescriptor.getCommitLogLocation().tryList(f -> CommitLogDescriptor.isValid(f.name()));
        assertThat(segments).isNotNull();
        for (File segment : segments)
            entries += Dse51RollbackInspection.countEntries(segment, errors);
        return entries;
    }

    @Test
    public void aDrainLeavesNoMutationInTheCommitLog() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        CompactionManager.instance.disableAutoCompaction();
        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, i);
        CommitLog.instance.sync(true);
        List<String> errors = new ArrayList<>();
        assertThat(entries(errors)).as("the entries before the drain").isGreaterThanOrEqualTo(10);
        assertThat(errors).isEmpty();

        // what the drain does to the commit log: every table flushed, the segments recycled, the commit log shut down
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        CommitLog.instance.forceRecycleAllSegments();
        CommitLog.instance.shutdownBlocking();
        try
        {
            assertThat(entries(errors)).as("the entries after the drain").isZero();
            assertThat(errors).isEmpty();
        }
        finally
        {
            CommitLog.instance.restartUnsafe();
        }
    }
}
