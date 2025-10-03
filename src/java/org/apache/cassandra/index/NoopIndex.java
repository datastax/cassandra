/*
 * Copyright DataStax, Inc.
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
package org.apache.cassandra.index;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.WriteContext;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;

/**
 * Dummy index used when an unknown index is found in system_schema.index and INDEX_UNKNOWN_IGNORE=true
 *
 * This is just a mock implementation to prevent problems with old schemas containing unknown indexes.
 * It ignores writes, declares that it supports no expressions, and throws an exception when asked to search.
 */
public final class NoopIndex implements Index, INotificationConsumer
{
    private static final String UNSUPPORTED_MESSAGE = "Custom index %s.%s of type '%s' on column %s is not supported anymore. "
                                                      + CassandraRelevantProperties.INDEX_UNKNOWN_IGNORE.getKey()
                                                      + " is enabled so using an noop index that will ignore writes and won't be queryable. "
                                                      + "Please drop this index and/or use a Storage Attached Index (SAI) instead.";
    @VisibleForTesting
    static final Callable<?> NO_OP_TASK = () -> null;

    private static final Logger logger = LoggerFactory.getLogger(NoopIndex.class);

    private final IndexMetadata config;
    private final ColumnMetadata column;

    public NoopIndex(ColumnFamilyStore baseCfs, IndexMetadata config)
    {
        this.config = config;
        column = TargetParser.parse(baseCfs.metadata(), config).left;
    }

    public String getUnsupportedMessage()
    {
        return String.format(UNSUPPORTED_MESSAGE, column.ksName, config.name, config.getIndexClassName(), column.name);
    }

    @SuppressWarnings("unused")
    public static Map<String, String> validateOptions(Map<String, String> options, TableMetadata metadata)
    {
        return Collections.emptyMap();
    }

    @Override
    public void register(IndexRegistry registry)
    {
        registry.registerIndex(this);
        logger.error(getUnsupportedMessage());
    }

    @Override
    public IndexMetadata getIndexMetadata()
    {
        return config;
    }

    @Override
    public Callable<?> getInitializationTask()
    {
        return null;
    }

    @Override
    public Callable<?> getMetadataReloadTask(IndexMetadata indexMetadata)
    {
        return null;
    }

    @Override
    public Callable<?> getBlockingFlushTask()
    {
        return NO_OP_TASK;
    }

    @Override
    public Callable<?> getInvalidateTask()
    {
        return NO_OP_TASK;
    }

    @Override
    public Callable<?> getTruncateTask(long truncatedAt)
    {
        return NO_OP_TASK;
    }

    @Override
    public boolean shouldBuildBlocking()
    {
        return true;
    }

    @Override
    public Optional<ColumnFamilyStore> getBackingTable()
    {
        return Optional.empty();
    }

    @Override
    public boolean dependsOn(ColumnMetadata column)
    {
        return this.column.name.equals(column.name);
    }

    @Override
    public boolean supportsExpression(ColumnMetadata column, Operator operator)
    {
        return false;
    }

    @Override
    public AbstractType<?> customExpressionValueType()
    {
        return null;
    }

    @Override
    public RowFilter getPostIndexQueryFilter(RowFilter filter)
    {
        return filter;
    }

    @Override
    public long getEstimatedResultRows()
    {
        return Long.MAX_VALUE;
    }

    @Override
    public void validate(PartitionUpdate update, ClientState state)
    {
    }

    @Override
    public Indexer indexerFor(DecoratedKey key,
                              RegularAndStaticColumns columns,
                              long nowInSec,
                              WriteContext ctx,
                              IndexTransaction.Type transactionType,
                              Memtable memtable)
    {
        return null;
    }

    @Override
    public Searcher searcherFor(ReadCommand command) throws InvalidRequestException
    {
        throw new UnsupportedOperationException(getUnsupportedMessage());
    }

    @Override
    public void handleNotification(INotification notification, Object sender)
    {
        // Nothing to handle here since we don't index anything
    }
}