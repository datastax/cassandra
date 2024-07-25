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

package org.apache.cassandra.index.sai.view;

import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.IntervalTree;

public class RangeTermTree implements TermTree
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    protected final AbstractType<?> comparator;
    // Because each version can have different encodings, we group indexes by version.
    private final Map<Version, IntervalTree<Term, SSTableIndex, Interval<Term, SSTableIndex>>> rangeTrees;

    private RangeTermTree(Map<Version, IntervalTree<Term, SSTableIndex, Interval<Term, SSTableIndex>>> rangeTrees, AbstractType<?> comparator)
    {
        this.rangeTrees = rangeTrees;
        this.comparator = comparator;
    }

    public Set<SSTableIndex> search(Expression e)
    {
        Set<SSTableIndex> result = new HashSet<>();
        rangeTrees.forEach((version, rangeTree) -> {
            // Get the bounds given the version. Notice that we use the partially-encoded representation for bounds
            // because that is how we store them in the range tree. The comparator is used to compare the bounds to
            // each tree's min/max to see if the sstable index is in the query range.
            Term minTerm = e.lower == null ? rangeTree.min() : new Term(e.getPartiallyEncodedLowerBound(version), comparator, version);
            Term maxTerm = e.upper == null ? rangeTree.max() : new Term(e.getPartiallyEncodedUpperBound(version), comparator, version);
            result.addAll(rangeTree.search(Interval.create(minTerm, maxTerm, null)));
        });
        return result;
    }

    static class Builder extends TermTree.Builder
    {
        // Because different indexes can have different encodings, we must track the versions of the indexes
        final Map<Version, List<Interval<Term, SSTableIndex>>> intervalsByVersion = new HashMap<>();

        protected Builder(AbstractType<?> comparator)
        {
            super(comparator);
        }

        public void addIndex(SSTableIndex index)
        {
            Interval<Term, SSTableIndex> interval =
                    Interval.create(new Term(index.minTerm(), comparator, index.getVersion()),
                                    new Term(index.maxTerm(), comparator, index.getVersion()),
                                    index);

            if (logger.isTraceEnabled())
            {
                IndexContext context = index.getIndexContext();
                logger.trace(context.logMessage("Adding index for SSTable {} with minTerm={} and maxTerm={} and version={}..."),
                                                index.getSSTable().descriptor, 
                                                index.minTerm() != null ? comparator.compose(index.minTerm()) : null,
                                                index.maxTerm() != null ? comparator.compose(index.maxTerm()) : null,
                                                index.getVersion());
            }

            intervalsByVersion.compute(index.getVersion(), (__, list) ->
            {
                if (list == null)
                    list = new ArrayList<>();
                list.add(interval);
                return list;
            });
        }

        public TermTree build()
        {
            Map<Version, IntervalTree<Term, SSTableIndex, Interval<Term, SSTableIndex>>> trees = new HashMap<>();
            intervalsByVersion.forEach((version, intervals) -> trees.put(version, IntervalTree.build(intervals)));
            return new RangeTermTree(trees, comparator);
        }
    }

    /**
     * This is required since IntervalTree doesn't support custom Comparator
     * implementations and relied on items to be comparable which "raw" terms are not.
     */
    protected static class Term implements Comparable<Term>
    {
        private final ByteBuffer term;
        private final AbstractType<?> comparator;
        private final Version version;

        Term(ByteBuffer term, AbstractType<?> comparator, Version version)
        {
            this.term = term;
            this.comparator = comparator;
            this.version = version;
        }

        public int compareTo(Term o)
        {
            assert version == o.version : "Cannot compare terms from different versions, but found " + version + " and " + o.version;
            if (term == null && o.term == null)
                return 0;
            if (term == null)
                return -1;
            if (o.term == null)
                return 1;
            return TypeUtil.compare(term, o.term, comparator, version);
        }
    }
}
