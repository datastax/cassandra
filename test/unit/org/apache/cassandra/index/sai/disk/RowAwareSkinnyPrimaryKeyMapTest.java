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

package org.apache.cassandra.index.sai.disk;

import java.util.Set;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

/**
 * Skinny-table tests (no clustering columns) for
 * {@link PrimaryKeyMap} using the row-aware on-disk format.
 */
public class RowAwareSkinnyPrimaryKeyMapTest extends RowAwarePrimaryKeyMapTester
{
    private final IndexContext intContext = SAITester.createIndexContext("int_index", Int32Type.instance);
    private final IndexContext textContext = SAITester.createIndexContext("text_index", UTF8Type.instance);

    @Override
    protected Set<IndexContext> getIndexContexts()
    {
        return Set.of(intContext, textContext);
    }

    @Override
    protected void createTableSchema()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, int_value int, text_value text)");
        execute("CREATE CUSTOM INDEX int_index ON %s(int_value) USING 'StorageAttachedIndex'");
        execute("CREATE CUSTOM INDEX text_index ON %s(text_value) USING 'StorageAttachedIndex'");
    }

    @Override
    protected void insertTestData()
    {
        // Insert a few rows to have first/middle/last and token gaps
        execute("INSERT INTO %s (pk, int_value, text_value) VALUES (?, ?, ?)", 1, 10, "a");
        execute("INSERT INTO %s (pk, int_value, text_value) VALUES (?, ?, ?)", 1000, 20, "b");
        execute("INSERT INTO %s (pk, int_value, text_value) VALUES (?, ?, ?)", 2, 30, "c");
        execute("INSERT INTO %s (pk, int_value, text_value) VALUES (?, ?, ?)", 50000, 40, "d");
    }

    @Override
    protected MapWalker createMapWalker(PrimaryKeyMap map, PrimaryKeyMapFunction function)
    {
        return new SkinnyMapWalker(map, function);
    }

    private class SkinnyMapWalker extends MapWalker
    {
        private final long secondToken;

        SkinnyMapWalker(PrimaryKeyMap map, PrimaryKeyMapFunction rowIdFromPKMethod)
        {
            super(map, rowIdFromPKMethod);
            PrimaryKey secondPk = map.primaryKeyFromRowId(1);
            this.secondToken = secondPk.token().getLongValue();
        }

        PrimaryKey beforeFirst()
        {
            return pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(firstToken - 1));
        }

        PrimaryKey exactFirst()
        {
            return pkFactory.createTokenOnly(firstPk.token());
        }

        PrimaryKey betweenFirstAndSecond()
        {
            long midToken = firstToken + ((secondToken - firstToken) / 2);
            return pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(midToken));
        }

        PrimaryKey exactLast()
        {
            return pkFactory.createTokenOnly(lastPk.token());
        }

        PrimaryKey afterLast()
        {
            return pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(lastToken + 1));
        }

        @Override
        protected void testExactRowIdOrInvertedCeiling()
        {
            assertResult(beforeFirst(), -1, "before first expects the inverted first");
            assertResult(exactFirst(), 0, "exact first");
            assertResult(betweenFirstAndSecond(), -2, "between first and second expects the inverted second");
            assertResult(exactLast(), count - 1, "exact last");
            assertResult(afterLast(), Long.MIN_VALUE, "after last expects out of range");
        }

        @Override
        protected void testCeiling()
        {
            assertResult(beforeFirst(), 0, "before first expects the first");
            assertResult(exactFirst(), 0, "exact first");
            assertResult(betweenFirstAndSecond(), 1, "between first and second expects the second");
            assertResult(exactLast(), count - 1, "exact last");
            assertResult(afterLast(), -1, "after last expects out of range");
        }

        @Override
        protected void testFloor()
        {
            assertResult(beforeFirst(), -1, "before first expects out of range");
            assertResult(exactFirst(), 0, "exact first");
            assertResult(betweenFirstAndSecond(), 0, "between first and second expects the first");
            assertResult(exactLast(), count - 1, "exact last");
            assertResult(afterLast(), count - 1, "after last expects the last");
        }
    }
}
