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
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
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
import org.apache.cassandra.utils.Pair;

/**
 * Dummy index used when an unknown index is found in system_schema.index and INDEX_UNKNOWN_IGNORE=true, or while
 * the DSE 6 compatibility mode is on (an index type this software does not have, left by an in-place upgrade; the
 * DataStax Enterprise Search indexes are refused in that mode instead, see Dse6NonCoreTraces)
 *
 * This is just a mock implementation to prevent problems with old schemas containing unknown indexes.
 * It ignores writes, declares that it supports no expressions, and throws an exception when asked to search.
 */
public final class NoopIndex implements Index, INotificationConsumer
{
    private static final String UNSUPPORTED_MESSAGE = "Custom index %s.%s of type '%s' %s is not supported anymore. "
                                                      + CassandraRelevantProperties.INDEX_UNKNOWN_IGNORE.getKey()
                                                      + " is enabled so using an noop index that will ignore writes and won't be queryable. "
                                                      + "Please drop this index and/or use a Storage Attached Index (SAI) instead.";
    private static final String DSE6_UNSUPPORTED_MESSAGE = "Custom index %s.%s of type '%s' %s is not supported by this software. "
                                                           + "The DSE 6 compatibility mode (internode_compatibility_mode: DSE_6) is enabled "
                                                           + "so using a noop index that will ignore writes and won't be queryable. "
                                                           + "Please drop this index on DSE before the upgrade (a node in internode_compatibility_mode NONE refuses to load it) "
                                                           + "and/or use a Storage Attached Index (SAI) instead.";
    @VisibleForTesting
    static final Callable<?> NO_OP_TASK = () -> null;

    private static final Logger logger = LoggerFactory.getLogger(NoopIndex.class);

    private final IndexMetadata config;
    private final String keyspace;
    /** The indexed column, or null, in the DSE 6 compatibility mode only, for an index whose target is not a column here. */
    @Nullable
    private final ColumnMetadata column;
    /**
     * In the DSE 6 compatibility mode, when the target is not one column, the columns of the table among its
     * comma-separated parts, which the index depends on so that none of them is dropped while it is in the schema.
     */
    private final Set<ColumnIdentifier> listedColumns;
    /** Loaded because of the DSE 6 compatibility mode rather than {@link CassandraRelevantProperties#INDEX_UNKNOWN_IGNORE}. */
    private final boolean dse6Compatibility;

    public NoopIndex(ColumnFamilyStore baseCfs, IndexMetadata config)
    {
        this.config = config;
        // same precedence as SecondaryIndexManager#createInstance: the property first, then the DSE 6 mode
        dse6Compatibility = !CassandraRelevantProperties.INDEX_UNKNOWN_IGNORE.getBoolean() && DatabaseDescriptor.isDse6CompatibilityMode();
        if (DatabaseDescriptor.isDse6CompatibilityMode())
        {
            // the target may not be a column here, as for the solr_query pseudo-column of a DataStax Enterprise search index
            // (keyed on the mode alone, so that setting the ignore property as well does not make the load fail)
            String target = config.options.get(IndexTarget.TARGET_OPTION_NAME);
            Pair<ColumnMetadata, IndexTarget.Type> parsed = target == null ? null : TargetParser.parse(baseCfs.metadata(), target);
            column = parsed == null ? null : parsed.left;
            listedColumns = column == null && target != null ? listedColumns(baseCfs.metadata(), target) : Collections.emptySet();
        }
        else
        {
            column = TargetParser.parse(baseCfs.metadata(), config).left;
            listedColumns = Collections.emptySet();
        }
        keyspace = baseCfs.metadata().keyspace;
    }

    // the columns of the table named by the comma-separated parts of a target, each part as TargetParser reads a target
    // (a quoted column name holding a comma is split too, and then names no column: the index does not depend on it)
    private static Set<ColumnIdentifier> listedColumns(TableMetadata metadata, String target)
    {
        Set<ColumnIdentifier> columns = new HashSet<>();
        for (String part : target.split(","))
        {
            Pair<ColumnMetadata, IndexTarget.Type> parsed = TargetParser.parse(metadata, part.trim());
            if (parsed != null)
                columns.add(parsed.left.name);
        }
        return columns;
    }

    public String getUnsupportedMessage()
    {
        return String.format(dse6Compatibility ? DSE6_UNSUPPORTED_MESSAGE : UNSUPPORTED_MESSAGE, keyspace, config.name, config.getIndexClassName(), describedTarget());
    }

    // "on column c"; in the DSE 6 compatibility mode also "on target solr_query", for a target that is not one column
    // here, or "with no target"
    private String describedTarget()
    {
        if (column != null)
            return "on column " + column.name;
        String target = config.options.get(IndexTarget.TARGET_OPTION_NAME);
        return target != null ? "on target " + target : "with no target";
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
        // in the DSE 6 compatibility mode SecondaryIndexManager#createInstance has already logged a warning for it
        if (!dse6Compatibility)
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
        if (this.column != null)
            return this.column.name.equals(column.name);
        return listedColumns.contains(column.name);
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