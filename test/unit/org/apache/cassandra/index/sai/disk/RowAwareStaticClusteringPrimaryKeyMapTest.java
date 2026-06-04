/*
 * Copyright IBM Corp.
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
 * Tests for {@link PrimaryKeyMap} with tables that have static columns,
 * using the row-aware on-disk format. Static columns create special rows
 * with STATIC_CLUSTERING that need to be handled correctly.
 */
public class RowAwareStaticClusteringPrimaryKeyMapTest extends RowAwarePrimaryKeyMapTester
{
    private final IndexContext intContext = SAITester.createIndexContext("int_index", Int32Type.instance);
    private final IndexContext textContext = SAITester.createIndexContext("text_index", UTF8Type.instance);
    private final IndexContext staticContext = SAITester.createIndexContext("static_index", UTF8Type.instance);

    @Override
    protected Set<IndexContext> getIndexContexts()
    {
        return Set.of(intContext, textContext, staticContext);
    }

    @Override
    protected void createTableSchema()
    {
        // Create a table with static columns and clustering columns
        createTable("CREATE TABLE %s (pk int, ck int, static_value text static, int_value int, text_value text, PRIMARY KEY (pk, ck))");
        execute("CREATE CUSTOM INDEX int_index ON %s(int_value) USING 'StorageAttachedIndex'");
        execute("CREATE CUSTOM INDEX text_index ON %s(text_value) USING 'StorageAttachedIndex'");
        execute("CREATE CUSTOM INDEX static_index ON %s(static_value) USING 'StorageAttachedIndex'");
    }

    @Override
    protected void insertTestData()
    {
        // Insert data with static columns
        // Partition pk=1: static row + multiple regular rows
        execute("INSERT INTO %s (pk, static_value) VALUES (?, ?)", 1, "static1");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1, 1, 11, "a1");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1, 2, 12, "a2");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1, 3, 13, "a3");

        // Partition pk=2: static row + single regular row
        execute("INSERT INTO %s (pk, static_value) VALUES (?, ?)", 2, "static2");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 2, 1, 21, "b1");

        // Partition pk=1000: static row + multiple regular rows
        execute("INSERT INTO %s (pk, static_value) VALUES (?, ?)", 1000, "static1000");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1000, 1, 1001, "c1");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1000, 2, 1002, "c2");
        execute("INSERT INTO %s (pk, ck, int_value, text_value) VALUES (?, ?, ?, ?)", 1000, 3, 1003, "c3");
    }

    @Override
    protected MapWalker createMapWalker(PrimaryKeyMap map, PrimaryKeyMapFunction function)
    {
        return new StaticClusteringMapWalker(map, function);
    }

    private class StaticClusteringMapWalker extends MapWalker
    {
        private final PrimaryKey pk1Static;
        private final PrimaryKey pk1Ck1;
        private final PrimaryKey pk1Ck2;
        private final PrimaryKey pk1Ck3;
        private final PrimaryKey afterLastPk;
        private long idPk1Static = -1;
        private long idPk1Ck1 = -1;
        private long idPk1Ck2 = -1;
        private long idPk1Ck3 = -1;

        StaticClusteringMapWalker(PrimaryKeyMap map, PrimaryKeyMapFunction rowIdFromPKMethod)
        {
            super(map, rowIdFromPKMethod);
            // Pre-compute primary keys for testing
            this.pk1Static = buildPkStatic(partitioner, 1);
            this.pk1Ck1 = buildPk(partitioner, 1, 1);
            this.pk1Ck2 = buildPk(partitioner, 1, 2);
            this.pk1Ck3 = buildPk(partitioner, 1, 3);
            this.afterLastPk = buildPk(partitioner, 1000, 11);
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

        PrimaryKey afterLastRow()
        {
            return afterLastPk;
        }

        PrimaryKey afterLastToken()
        {
            return pkFactory.createTokenOnly(partitioner.getTokenFactory().fromLongValue(lastToken + 1));
        }

        PrimaryKey exactPk1Static()
        {
            return pk1Static;
        }

        PrimaryKey exactPk1Ck1()
        {
            return pk1Ck1;
        }

        PrimaryKey exactPk1Ck2()
        {
            return pk1Ck2;
        }

        PrimaryKey exactPk1Ck3()
        {
            return pk1Ck3;
        }

        PrimaryKey betweenPk1StaticAndCk1()
        {
            // Create a clustering value between STATIC_CLUSTERING and ck=1
            // STATIC_CLUSTERING sorts BEFORE all regular clustering values
            // Use ck=0 which sorts between static and ck=1
            return buildPk(partitioner, 1, 0);
        }

        PrimaryKey afterPk1Ck3()
        {
            // A key that sorts after ck=3 (the last clustering in pk=1)
            return buildPk(partitioner, 1, Integer.MAX_VALUE);
        }

        public long getIdPk1Static()
        {
            if (idPk1Static == -1)
                idPk1Static = rowIdFromPKMethod.apply(pk1Static);
            return idPk1Static;
        }

        public long getIdPk1Ck1()
        {
            if (idPk1Ck1 == -1)
                idPk1Ck1 = rowIdFromPKMethod.apply(pk1Ck1);
            return idPk1Ck1;
        }

        public long getIdPk1Ck2()
        {
            if (idPk1Ck2 == -1)
                idPk1Ck2 = rowIdFromPKMethod.apply(pk1Ck2);
            return idPk1Ck2;
        }

        public long getIdPk1Ck3()
        {
            if (idPk1Ck3 == -1)
                idPk1Ck3 = rowIdFromPKMethod.apply(pk1Ck3);
            return idPk1Ck3;
        }

        @Override
        protected void testExactRowIdOrInvertedCeiling()
        {
            assertResult(beforeFirst(), -1, "before first expects the inverted first");
            assertResult(exactFirstRow(), 0, "exact first row");

            // Test static row lookup
            assertResult(exactPk1Static(), getIdPk1Static(), "exact pk=1 static row");

            // Test regular clustering rows
            assertResult(exactPk1Ck1(), getIdPk1Ck1(), "exact pk=1, ck=1");
            assertResult(exactPk1Ck2(), getIdPk1Ck1() + 1, "pk=1, ck=2 expects next after pk=1, ck=1");
            assertResult(exactPk1Ck3(), getIdPk1Ck2() + 1, "exact pk=1, ck=3 expects next after pk=1, ck=2");

            // Test between static and first clustering row
            assertResult(betweenPk1StaticAndCk1(), -getIdPk1Ck1() - 1, "between static and ck=1 expects inverted ck=1");

            // Test after last clustering in partition
            assertResult(afterPk1Ck3(), -(getIdPk1Ck3() + 1) - 1, "after pk=1 ck=3 expects inverted next partition first row");

            assertResult(exactLastRow(), count - 1, "exact last row");
            assertResult(afterLastToken(), Long.MIN_VALUE, "after last expects out of range");
        }

        @Override
        protected void testCeiling()
        {
            assertResult(beforeFirst(), 0, "before first expects the first");
            assertResult(exactFirstRow(), 0, "exact first row");

            // Test static row lookup
            assertResult(exactPk1Static(), getIdPk1Static(), "exact pk=1 static row");

            // Test regular clustering rows
            assertResult(exactPk1Ck1(), getIdPk1Ck1(), "exact pk=1, ck=1");
            assertResult(exactPk1Ck2(), getIdPk1Ck1() + 1, "pk=1, ck=2 expects next after pk=1, ck=1");
            assertResult(exactPk1Ck3(), getIdPk1Ck2() + 1, "exact pk=1, ck=3 expects next after pk=1, ck=2");

            // Test between static and first clustering - ceiling should return first clustering
            assertResult(betweenPk1StaticAndCk1(), getIdPk1Ck1(), "between static and ck=1 expects ck=1 (ceiling)");

            // Test after last clustering in partition - should go to next partition the first row
            assertResult(afterPk1Ck3(), getIdPk1Ck3() + 1, "after pk=1 ck=3 expects next partition first row");

            assertResult(exactLastRow(), count - 1, "exact last row");
            assertResult(afterLastToken(), -1, "after last expects out of range");
        }

        @Override
        protected void testFloor()
        {
            assertResult(beforeFirst(), -1, "before first expects out of range");
            assertResult(exactFirstRow(), getIdPk1Static(), "exact first row means the last row in the first partition");

            // Test static row lookup
            assertResult(exactPk1Static(), getIdPk1Static(), "exact pk=1 static row");

            // Test regular clustering rows
            assertResult(exactPk1Ck1(), getIdPk1Ck1(), "exact pk=1, ck=1");
            assertResult(exactPk1Ck2(), getIdPk1Ck1() + 1, "pk=1, ck=2 expects next after pk=1, ck=1");
            assertResult(exactPk1Ck3(), getIdPk1Ck2() + 1, "exact pk=1, ck=3 expects next after pk=1, ck=2");

            // Test between static and first clustering
            // ck=0 sorts after STATIC_CLUSTERING, but before ck=1
            // floor(ck=0) should return the row at position 0
            assertResult(betweenPk1StaticAndCk1(), 0, "between static and ck=1 expects row 0 (floor)");

            // Test after last clustering in partition - floor should return last clustering
            assertResult(afterPk1Ck3(), getIdPk1Ck3(), "after pk=1 ck=3 expects ck=3 (floor)");

            assertResult(exactLastRow(), count - 1, "exact last row");
            assertResult(afterLastRow(), count - 1, "after last row in last partition expects the last row");
            assertResult(afterLastToken(), count - 1, "after last expects the last row");
        }
    }
}
