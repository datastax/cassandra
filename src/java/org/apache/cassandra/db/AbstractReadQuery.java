/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db;

import java.util.Set;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.cql3.statements.SelectOptions;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.IndexHints;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.monitoring.MonitorableImpl;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Base class for {@code ReadQuery} implementations.
 */
abstract class AbstractReadQuery extends MonitorableImpl implements ReadQuery
{
    private final TableMetadata metadata;
    private final long nowInSec;

    private final ColumnFilter columnFilter;
    private final RowFilter rowFilter;
    private final DataLimits limits;

    protected AbstractReadQuery(TableMetadata metadata, long nowInSec, ColumnFilter columnFilter, RowFilter rowFilter, DataLimits limits)
    {
        this.metadata = metadata;
        this.nowInSec = nowInSec;
        this.columnFilter = columnFilter;
        this.rowFilter = rowFilter;
        this.limits = limits;
    }

    @Override
    public TableMetadata metadata()
    {
        return metadata;
    }

    // Monitorable interface
    public String name()
    {
        return toRedactedCQLString();
    }

    @Override
    public PartitionIterator executeInternal(ReadExecutionController controller)
    {
        return UnfilteredPartitionIterators.filter(executeLocally(controller), nowInSec());
    }

    @Override
    public DataLimits limits()
    {
        return limits;
    }

    @Override
    public long nowInSec()
    {
        return nowInSec;
    }

    @Override
    public RowFilter rowFilter()
    {
        return rowFilter;
    }

    @Override
    public ColumnFilter columnFilter()
    {
        return columnFilter;
    }

    /**
     * Recreates the CQL string corresponding to this query, representing any specific values with '?',
     * to prevent leaking sensitive data.
     * @see #toCQLString(boolean)
     */
    public String toRedactedCQLString()
    {
        return toCQLString(true);
    }

    /**
     * Recreates the CQL string corresponding to this query, printing specific values without any redaction.
     * This might leak sensitive data if the query string ends up in logs or any other unprotected place, so this only
     * should be used for debugging purposes or to present the query string to the same end user that created the query.
     * @see #toCQLString(boolean)
     */
    public String toUnredactedCQLString()
    {
        return toCQLString(false);
    }

    /**
     * Recreates the CQL string corresponding to this query.
     * </p>
     * If the {@code redact} parameter is set to {@code true}, the query string will be redacted, replacing any specific
     * column values with '?'. If set to {@code false}, the query string will not be redacted, and it might expose the
     * queried column values which might contain sensitive data. The latter will be problematic if the query string ends
     * up in logs or any other unprotected place. Therefore, non-redaction should only be used for debugging purposes or
     * to present the query string to the same end user that created the query.
     * <p>
     * Note that in general the returned string will not be exactly the original user string, first
     * because there isn't always a single syntax for a given query, but also because we don't have
     * all the information needed (we know the non-PK columns queried but not the PK ones as internally
     * we query them all). So this shouldn't be relied upon too strongly, but this should be good enough for
     * debugging purposes which is what this is for.
     *
     * @param redact whether to redact the queried column values.
     */
    @VisibleForTesting
    protected String toCQLString(boolean redact)
    {
        CqlBuilder builder = new CqlBuilder();
        builder.append("SELECT ").append(columnFilter().toCQLString(redact));
        builder.append(" FROM ").append(ColumnIdentifier.maybeQuote(metadata().keyspace))
               .append('.')
               .append(ColumnIdentifier.maybeQuote(metadata().name));

        appendCQLWhereClause(builder, redact);

        if (limits() != DataLimits.NONE)
            builder.append(' ').append(limits());

        // ALLOW FILTERING might not be strictly necessary
        builder.append(" ALLOW FILTERING");

        builder.appendOptions(b -> {
            IndexHints indexHints = rowFilter().indexHints;
            Set<String> included = IndexMetadata.toNames(indexHints.included);
            Set<String> excluded = IndexMetadata.toNames(indexHints.excluded);
            b.append(SelectOptions.INCLUDED_INDEXES, included)
             .append(SelectOptions.EXCLUDED_INDEXES, excluded)
             .append(SelectOptions.ANN_OPTIONS, rowFilter().annOptions().toCQLString());
        });

        return builder.toString();
    }

    protected abstract void appendCQLWhereClause(CqlBuilder builder, boolean redact);
}
