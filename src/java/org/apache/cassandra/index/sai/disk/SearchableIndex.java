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

package org.apache.cassandra.index.sai.disk;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.v1.Segment;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.plan.Orderer;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithSortKey;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.CloseableIterator;

/**
 * This is used to abstract the index search between on-disk versions.
 * Callers to this interface should be unaware of the on-disk version for
 * the index.
 *
 * It is responsible for supplying metadata about the on-disk index. This is
 * used during query time to help coordinate queries and is also returned
 * by the virtual tables.
 */
public interface SearchableIndex extends Closeable
{
    long indexFileCacheSize();

    long getRowCount();

    long getApproximateTermCount();

    long minSSTableRowId();

    long maxSSTableRowId();

    ByteBuffer minTerm();

    ByteBuffer maxTerm();

    DecoratedKey minKey();

    DecoratedKey maxKey();

    KeyRangeIterator search(Expression expression,
                                   AbstractBounds<PartitionPosition> keyRange,
                                   QueryContext context,
                                   boolean defer) throws IOException;

    List<CloseableIterator<PrimaryKeyWithSortKey>> orderBy(Orderer orderer,
                                                                  Expression slice,
                                                                  AbstractBounds<PartitionPosition> keyRange,
                                                                  QueryContext context,
                                                                  int limit,
                                                                  long totalRows) throws IOException;

    List<CloseableIterator<PrimaryKeyWithSortKey>> orderResultsBy(QueryContext context,
                                                                         List<PrimaryKey> keys,
                                                                         Orderer orderer,
                                                                         int limit,
                                                                         long totalRows) throws IOException;

    List<Segment> getSegments();

    void populateSystemView(SimpleDataSet dataSet, SSTableReader sstable);

    long estimateMatchingRowsCount(Expression predicate, AbstractBounds<PartitionPosition> keyRange);
}
