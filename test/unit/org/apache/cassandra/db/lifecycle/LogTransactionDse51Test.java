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

package org.apache.cassandra.db.lifecycle;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.schema.MockSchema;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The transaction log tests of {@link LogTransactionTest}, every one of them, with
 * {@code internode_compatibility_mode: DSE_5_1} and the {@code big} format the mode writes: the records then name the
 * sstables by their path without the trailing component separator, with the remove records' update time taken over
 * every file, in milliseconds ({@link DseTransactionLogs}), and every path of a transaction (tracking new and obsolete
 * sstables, untracking, commit, abort, the listing of temporary files, the clean-up at start) works on them as on the
 * records written without the mode.
 */
public class LogTransactionDse51Test extends LogTransactionTest
{
    private static final String KEYSPACE_DSE51 = "TransactionLogsDse51Test";

    private InternodeCompatibilityMode modeBefore;
    private SSTableFormat<?, ?> formatBefore;

    @Before
    public void dse51Mode()
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        formatBefore = DatabaseDescriptor.getSelectedSSTableFormat();
        DatabaseDescriptor.setSelectedSSTableFormat(DatabaseDescriptor.getSSTableFormats().get(BigFormat.NAME));
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        assertThat(DatabaseDescriptor.isDse51SSTableOutput()).isTrue();
        assertThat(BigFormat.isSelected()).isTrue();
    }

    @After
    public void restoreMode()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
        DatabaseDescriptor.setSelectedSSTableFormat(formatBefore);
    }

    /**
     * The logs of the tests run here are of the mode: each record names its sstable by the path without the trailing
     * component separator.
     */
    @Test
    public void theRecordsOfTheLogsNameTheSSTablesAsDse51Does() throws IOException
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE_DSE51);
        SSTableReader sstableOld = MockSchema.sstable(1, 128, cfs);
        SSTableReader sstableNew = MockSchema.sstable(2, 128, cfs);
        LogTransaction txn = new LogTransaction(OperationType.COMPACTION, LifecycleTransaction.newId());
        txn.trackNew(sstableNew);
        AbstractLogTransaction.ReaderTidier tidier = txn.obsoleted(sstableOld);
        List<LogRecord> records = Files.readAllLines(txn.logFiles().get(0).toPath()).stream()
                                       .map(LogRecord::make)
                                       .collect(Collectors.toList());
        assertThat(records).extracting(LogRecord::absolutePath)
                           .containsExactly(sstableNew.descriptor.baseFile().absolutePath(),
                                            sstableOld.descriptor.baseFile().absolutePath())
                           .noneMatch(path -> path.startsWith("file:") || path.endsWith("-"));
        assertThat(tidier.abort(null)).isNull();
        txn.abort();
    }
}
