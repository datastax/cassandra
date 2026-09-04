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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.CassandraWriteContext;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.WriteOptions;
import org.apache.cassandra.db.WriteOrigin;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.index.StubIndex;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.schema.IndexMetadata;

/**
 * A custom secondary index that logs what {@link WriteOrigin} and {@link WriteOptions} its Indexer is
 * handed for every apply, so a distributed test can assert what each replica actually saw.
 * <p>
 * This is the consumer the origin plumbing exists for: it observes the write path exactly where a real
 * index does -- {@code Index.Group#indexerFor}, via {@code SecondaryIndexManager#newUpdateTransaction} --
 * and reads the origin off the {@link WriteContext} it is already given. Nothing else is needed; there is
 * no new SPI to implement.
 * <p>
 * Indexing behaviour is inherited from {@link StubIndex} (record and do nothing).
 *
 * @see WriteOriginDistributedTest
 */
public class WriteOriginLoggingIndex extends StubIndex
{
    private static final Logger logger = LoggerFactory.getLogger(WriteOriginLoggingIndex.class);

    /** Every log line starts with this, so a test can grep for one index's applies. */
    public static final String MARKER = "WriteOriginLoggingIndex apply";

    private final ColumnFamilyStore baseCfs;

    public WriteOriginLoggingIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
    {
        super(baseCfs, metadata);
        this.baseCfs = baseCfs;
    }

    @Override
    public Indexer indexerFor(DecoratedKey key,
                              RegularAndStaticColumns columns,
                              long nowInSec,
                              WriteContext ctx,
                              IndexTransaction.Type transactionType,
                              Memtable memtable)
    {
        logger.info("{}: key={}, txType={}, {}", MARKER, keyAsString(key), transactionType, describe(ctx));
        return super.indexerFor(key, columns, nowInSec, ctx, transactionType, memtable);
    }

    private String keyAsString(DecoratedKey key)
    {
        try
        {
            return baseCfs.metadata().partitionKeyType.getString(key.getKey());
        }
        catch (Throwable t)
        {
            return key.toString();
        }
    }

    /**
     * Renders the two things the write context now carries. Both are null for a context opened outside a
     * mutation (index build, compaction, cleanup), which is a distinct state from
     * {@link WriteOrigin#LOCAL} -- "a mutation that did not arrive over the wire".
     */
    private static String describe(WriteContext ctx)
    {
        if (!(ctx instanceof CassandraWriteContext))
            return "options=none, origin=none (foreign write context)";

        CassandraWriteContext cassandraCtx = (CassandraWriteContext) ctx;
        WriteOrigin origin = cassandraCtx.getOrigin();
        if (origin == null)
            return "options=" + cassandraCtx.getWriteOptions() + ", origin=none (no mutation)";

        return "options=" + cassandraCtx.getWriteOptions()
               + ", coordinator=" + origin.coordinator()
               + ", coordinatorDc=" + origin.datacenter()
               + ", crossDc=" + origin.isCrossDatacenter()
               + ", direct=" + origin.isDirectFromRemoteDatacenter();
    }
}
