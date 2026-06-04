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
 * Wide-table tests (with clustering columns) for
 * {@link PrimaryKeyMap} using the row-aware on-disk format.
 */
public class RowAwareWidePrimaryKeyMapTest extends RowAwarePrimaryKeyMapTester
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
        createTable("CREATE TABLE %s (pk int, ck int, int_value int, text_value text, PRIMARY KEY (pk, ck)) WITH CLUSTERING ORDER BY (ck ASC)");
        execute("CREATE CUSTOM INDEX int_index ON %s(int_value) USING 'StorageAttachedIndex'");
        execute("CREATE CUSTOM INDEX text_index ON %s(text_value) USING 'StorageAttachedIndex'");
    }

    @Override
    protected void insertTestData()
    {
        // Insert multiple rows per partition to exercise clustering ordering
        // Partition pk=1
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1, 1, 11, "a1");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1, 2, 12, "a2");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1, 3, 13, "a3");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1, 10, 110, "a10");

        // Partition pk=2
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 2, 1, 21, "b1");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 2, 5, 25, "b5");

        // Partition pk=1000
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1000, 1, 1001, "c1");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1000, 2, 1002, "c2");
    }

    @Override
    protected MapWalker createMapWalker(PrimaryKeyMap map, PrimaryKeyMapFunction function)
    {
        return new WideMapWalker(map, function);
    }

    private class WideMapWalker extends MapWalker
    {
        private final PrimaryKey pk11;
        private final PrimaryKey pk12;
        private final PrimaryKey pk13;
        private final PrimaryKey pk110;
        private long id11 = -1;
        private long id12 = -1;
        private long id13 = -1;
        private long id110 = -1;

        WideMapWalker(PrimaryKeyMap map, PrimaryKeyMapFunction rowIdFromPKMethod)
        {
            super(map, rowIdFromPKMethod);
            // Pre-compute row IDs for clustering tests
            this.pk11 = buildPk(partitioner, 1, 1);
            this.pk12 = buildPk(partitioner, 1, 2);
            this.pk13 = buildPk(partitioner, 1, 3);
            this.pk110 = buildPk(partitioner, 1, 10);
        }

        PrimaryKey beforeFirst()
        {
            return pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(firstToken - 1));
        }

        PrimaryKey exactFirstRow()
        {
            return firstPk;
        }

        PrimaryKey exactLastRow()
        {
            return lastPk;
        }

        PrimaryKey afterLastToken()
        {
            return pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(lastToken + 1));
        }

        PrimaryKey exactPk1Ck1()
        {
            return pk11;
        }

        PrimaryKey exactPk1Ck2()
        {
            return pk12;
        }

        PrimaryKey exactPk1Ck3()
        {
            return pk13;
        }

        PrimaryKey exactPk1Ck10()
        {
            return pk110;
        }

        PrimaryKey betweenPk1Ck3AndCk10()
        {
            return buildPk(partitioner, 1, 4);
        }

        PrimaryKey afterLastCkInPk1()
        {
            return buildPk(partitioner, 1, Integer.MAX_VALUE);
        }

        public long getId11()
        {
            if (id11 == -1)
                id11 = rowIdFromPKMethod.apply(buildPk(partitioner, 1, 1));
            return id11;
        }

        public long getId12()
        {
            if (id12 == -1)
                id12 = rowIdFromPKMethod.apply(buildPk(partitioner, 1, 2));
            return id12;
        }

        public long getId13()
        {
            if (id13 == -1)
                id13 = rowIdFromPKMethod.apply(buildPk(partitioner, 1, 3));
            return id13;
        }

        public long getId110()
        {
            if (id110 == -1)
                id110 = rowIdFromPKMethod.apply(buildPk(partitioner, 1, 10));
            return id110;
        }

        @Override
        protected void testExactRowIdOrInvertedCeiling()
        {
            assertResult(beforeFirst(), -1, "before first expects the inverted first");
            assertResult(exactFirstRow(), 0, "exact first row");

            assertResult(exactPk1Ck1(), getId11(), "exact pk=1, ck=1");
            assertResult(exactPk1Ck2(), getId11() + 1, "pk=1, ck=2 expects next after pk=1, ck=1");
            assertResult(exactPk1Ck3(), getId12() + 1, "exact pk=1, ck=3 expects next after pk=1, ck=2");
            assertResult(exactPk1Ck10(), getId13() + 1, "exact pk=1, ck=10 expects next after pk=1, ck=3");
            assertResult(betweenPk1Ck3AndCk10(), -getId110() - 1, "between pk=1 ck=3 and ck=10 expects inverted ck=10");
            assertResult(afterLastCkInPk1(), -(getId110() + 1) - 1, "after last ck in pk=1 expects inverted next partition first row");

            assertResult(exactLastRow(), count - 1, "exact last row");
            assertResult(afterLastToken(), Long.MIN_VALUE, "after last expects out of range");
        }

        @Override
        protected void testCeiling()
        {
            assertResult(beforeFirst(), 0, "before first expects the first");
            assertResult(exactFirstRow(), 0, "exact first row");

            assertResult(exactPk1Ck1(), getId11(), "exact pk=1, ck=1");
            assertResult(exactPk1Ck2(), getId11() + 1, "pk=1, ck=2 expects next after pk=1, ck=1");
            assertResult(exactPk1Ck3(), getId12() + 1, "exact pk=1, ck=3 expects next after pk=1, ck=2");
            assertResult(exactPk1Ck10(), getId13() + 1, "exact pk=1, ck=10 expects next after pk=1, ck=3");
            assertResult(betweenPk1Ck3AndCk10(), getId110(), "between pk=1 ck=3 and ck=10 expects ck=10");
            assertResult(afterLastCkInPk1(), getId110() + 1, "after last ck in pk=1 expects next partition first row");

            assertResult(exactLastRow(), count - 1, "exact last row");
            assertResult(afterLastToken(), -1, "after last expects out of range");
        }

        @Override
        protected void testFloor()
        {
            assertResult(beforeFirst(), -1, "before first expects out of range");
            assertResult(exactFirstRow(), 0, "exact first row");

            assertResult(exactPk1Ck1(), getId11(), "exact pk=1, ck=1");
            assertResult(exactPk1Ck2(), getId11() + 1, "pk=1, ck=2 expects next after pk=1, ck=1");
            assertResult(exactPk1Ck3(), getId12() + 1, "exact pk=1, ck=3 expects next after pk=1, ck=2");
            assertResult(exactPk1Ck10(), getId13() + 1, "exact pk=1, ck=10 expects next after pk=1, ck=3");
            assertResult(betweenPk1Ck3AndCk10(), getId13(), "between pk=1 ck=3 and ck=10 expects ck=3");
            assertResult(afterLastCkInPk1(), getId110(), "after last ck in pk=1 expects last ck in pk=1");

            assertResult(exactLastRow(), count - 1, "exact last row");
            assertResult(afterLastToken(), count - 1, "after last expects the last");
        }
    }
}
