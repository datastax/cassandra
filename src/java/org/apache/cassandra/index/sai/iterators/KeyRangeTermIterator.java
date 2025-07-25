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
package org.apache.cassandra.index.sai.iterators;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.plan.QueryView;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;

/**
 * KeyRangeTermIterator wraps KeyRangeUnionIterator with code that tracks and releases the referenced indexes,
 * and adds timeout checkpoints around expensive operations.
 */
public class KeyRangeTermIterator extends KeyRangeIterator
{
    private static final Logger logger = LoggerFactory.getLogger(KeyRangeTermIterator.class);

    private final QueryContext context;

    private final KeyRangeIterator union;
    private final Set<SSTableIndex> referencedIndexes;

    private KeyRangeTermIterator(KeyRangeIterator union, Set<SSTableIndex> referencedIndexes, QueryContext queryContext)
    {
        super(union.getMinimum(), union.getMaximum(), union.getMaxKeys());

        this.union = union;
        this.referencedIndexes = referencedIndexes;
        this.context = queryContext;

        for (SSTableIndex index : referencedIndexes)
        {
            boolean success = index.reference();
            // Won't happen, because the indexes we get here must be already referenced by the query view
            assert success : "Failed to reference the index " + index;
        }
    }


    @SuppressWarnings("resource")
    public static KeyRangeTermIterator build(final Expression e, QueryView view, AbstractBounds<PartitionPosition> keyRange, QueryContext queryContext, boolean defer)
    {
        KeyRangeIterator rangeIterator = buildRangeIterator(e, view, keyRange, queryContext, defer);
        return new KeyRangeTermIterator(rangeIterator, view.sstableIndexes, queryContext);
    }

    private static KeyRangeIterator buildRangeIterator(final Expression e, QueryView view, AbstractBounds<PartitionPosition> keyRange, QueryContext queryContext, boolean defer)
    {
        final List<KeyRangeIterator> tokens = new ArrayList<>(1 + view.sstableIndexes.size());

        KeyRangeIterator memtableIterator = e.context.searchMemtable(queryContext, view.memtableIndexes, e, keyRange);
        if (memtableIterator != null)
            tokens.add(memtableIterator);

        for (final SSTableIndex index : view.sstableIndexes)
        {
            try
            {
                queryContext.checkpoint();
                queryContext.addSstablesHit(1);
                assert !index.isReleased();

                KeyRangeIterator keyIterator = index.search(e, keyRange, queryContext, defer);

                if (keyIterator == null || !keyIterator.hasNext())
                    continue;

                tokens.add(keyIterator);
            }
            catch (Throwable e1)
            {
                if (logger.isDebugEnabled() && !(e1 instanceof AbortedOperationException))
                    logger.debug(String.format("Failed search an index %s, skipping.", index.getSSTable()), e1);

                // Close the iterators that were successfully opened before the error
                FileUtils.closeQuietly(tokens);

                throw Throwables.cleaned(e1);
            }
        }

        return KeyRangeUnionIterator.build(tokens);
    }

    protected PrimaryKey computeNext()
    {
        try
        {
            return union.hasNext() ? union.next() : endOfData();
        }
        finally
        {
            context.checkpoint();
        }
    }

    protected void performSkipTo(PrimaryKey nextKey)
    {
        try
        {
            union.skipTo(nextKey);
        }
        finally
        {
            context.checkpoint();
        }
    }

    public void close()
    {
        FileUtils.closeQuietly(union);
        referencedIndexes.forEach(KeyRangeTermIterator::releaseQuietly);
    }

    private static void releaseQuietly(SSTableIndex index)
    {
        try
        {
            index.release();
        }
        catch (Throwable e)
        {
            logger.error(String.format("Failed to release index %s", index.getSSTable()), e);
        }
    }
}
