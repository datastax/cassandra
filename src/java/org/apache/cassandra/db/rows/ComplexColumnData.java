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
package org.apache.cassandra.db.rows;

import java.util.Iterator;
import java.util.function.Function;

import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.BiLongAccumulator;
import org.apache.cassandra.utils.LongAccumulator;

/**
 * The data for a complex column, that is it's cells and potential complex
 * deletion time.
 */
public abstract class ComplexColumnData extends ColumnData implements Iterable<Cell<?>>
{
    ComplexColumnData(ColumnMetadata column)
    {
        super(column);
        assert column.isComplex();
    }

    // Used by CNDB
    public abstract boolean hasCells();

    public abstract int cellsCount();

    public abstract Cell<?> getCell(CellPath path);

    public abstract Cell<?> getCellByIndex(int idx);

    /**
     * The complex deletion time of the complex column.
     * <p>
     * The returned "complex deletion" is a deletion of all the cells of the column. For instance,
     * for a collection, this correspond to a full collection deletion.
     * Please note that this deletion says nothing about the individual cells of the complex column:
     * there can be no complex deletion but some of the individual cells can be deleted.
     *
     * @return the complex deletion time for the column this is the data of or {@code DeletionTime.LIVE}
     * if the column is not deleted.
     */
    public abstract DeletionTime complexDeletion();

    public abstract Iterator<Cell<?>> reverseIterator();

    public abstract ComplexColumnData transformAndFilter(Function<? super Cell<?>, ? extends Cell<?>> function);

    public abstract int liveDataSize(long nowInSec);

    public abstract long unsharedHeapSize();

    public abstract <V> ComplexColumnData transform(Function<? super Cell<?>, ? extends Cell<?>> function);


    public abstract long accumulate(LongAccumulator<Cell<?>> accumulator, long initialValue);

    public abstract <A> long accumulate(BiLongAccumulator<A, Cell<?>> accumulator, A arg, long initialValue);

    public abstract ComplexColumnData purge(DeletionPurger purger, long nowInSec);

    public abstract ComplexColumnData purgeDataOlderThan(long timestamp);
}
