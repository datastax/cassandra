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
package org.apache.cassandra.service.pager;

import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.service.QueryState;

/**
 * A partition iterator that reads its rows from a provided {@link QueryPager}, consuming it until it's exhausted.
 */
abstract class PagedPartitionIterator implements PartitionIterator
{
    protected final QueryPager pager;
    protected final PageSize pageSize;
    protected PartitionIterator current;

    protected PagedPartitionIterator(QueryPager pager, PageSize pageSize)
    {
        this.pager = pager;
        this.pageSize = pageSize;
    }

    @Override
    public void close()
    {
        if (current != null)
        {
            current.close();
            current = null;
        }
    }

    @Override
    public boolean hasNext()
    {
        maybeFetch();
        return current != null && current.hasNext();
    }

    @Override
    public RowIterator next()
    {
        maybeFetch();
        return current.next();
    }

    private void maybeFetch()
    {
        if (current == null || !current.hasNext())
        {
            if (current != null)
            {
                current.close();
                current = null;
            }

            if (!pager.isExhausted())
                current = fetch();
        }
    }

    protected abstract PartitionIterator fetch();

    /**
     * {@link PagedPartitionIterator} that for local queries.
     */
    public static class Internal extends PagedPartitionIterator
    {
        private final ReadExecutionController controller;

        public Internal(QueryPager pager, PageSize pageSize, ReadExecutionController controller)
        {
            super(pager, pageSize);
            this.controller = controller;
        }

        @Override
        protected PartitionIterator fetch()
        {
            return pager.fetchPageInternal(pageSize, controller);
        }
    }

    /**
     * {@link PagedPartitionIterator} that for distributed queries.
     */
    public static class Distributed extends PagedPartitionIterator
    {
        private final ConsistencyLevel consistency;
        private final QueryState state;
        private final long queryStartNanoTime;

        public Distributed(QueryPager pager,
                           PageSize pageSize,
                           ConsistencyLevel consistency,
                           QueryState state,
                           long queryStartNanoTime)
        {
            super(pager, pageSize);
            this.consistency = consistency;
            this.state = state;
            this.queryStartNanoTime = queryStartNanoTime;
        }

        @Override
        protected PartitionIterator fetch()
        {
            return pager.fetchPage(pageSize, consistency, state, queryStartNanoTime);
        }
    }
}
