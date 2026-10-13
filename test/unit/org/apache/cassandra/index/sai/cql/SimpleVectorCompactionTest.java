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

package org.apache.cassandra.index.sai.cql;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;

import static org.apache.cassandra.index.sai.cql.VectorTypeTest.assertContainsInt;
import static org.assertj.core.api.Assertions.assertThat;

public class SimpleVectorCompactionTest extends VectorTester
{
    @Test
    public void testMergeTwoSSTablesOneVectorEach()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, val vector<float, 2>)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        // SSTable 1: 1 row with 1 vector
        execute("INSERT INTO %s (pk, val) VALUES (1, [1.0, 2.0])");
        flush();

        // SSTable 2: 1 row with 1 vector (different row)
        execute("INSERT INTO %s (pk, val) VALUES (2, [3.0, 4.0])");
        flush();

        // Compact the 2 SSTables into 1
        compact();

        // Verify ANN query finds both vectors in the compacted SSTable
        UntypedResultSet result = execute("SELECT pk FROM %s ORDER BY val ANN OF [1.0, 2.0] LIMIT 2");
        assertThat(result).hasSize(2);
        assertContainsInt(result, "pk", 1);
        assertContainsInt(result, "pk", 2);
    }

    @Test
    public void testMergeTwoSSTablesSameVectorEach()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, val vector<float, 2>)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        // SSTable 1: 1 row with 1 vector
        execute("INSERT INTO %s (pk, val) VALUES (1, [1.0, 2.0])");
        flush();

        // SSTable 2: 1 row with 1 vector (different row)
        execute("INSERT INTO %s (pk, val) VALUES (2, [1.0, 2.0])");
        flush();

        // Compact the 2 SSTables into 1
        compact();

        // Verify ANN query finds both vectors in the compacted SSTable
        UntypedResultSet result = execute("SELECT pk FROM %s ORDER BY val ANN OF [1.0, 2.0] LIMIT 2");
        assertThat(result).hasSize(2);
        assertContainsInt(result, "pk", 1);
        assertContainsInt(result, "pk", 2);
    }


    @Test
    public void testMergeTwoSSTablesSameRowUpdatedVector()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, val vector<float, 2>)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        // SSTable 1: pk 1 with initial vector
        execute("INSERT INTO %s (pk, val) VALUES (1, [1.0, 2.0])");
        flush();

        // SSTable 2: same pk 1 overwritten with a different vector
        execute("INSERT INTO %s (pk, val) VALUES (1, [5.0, 6.0])");
        flush();

        // Compact the 2 SSTables into 1 (the winning row in SSTable 2 replaces SSTable 1's vector)
        compact();

        // Verify ANN query finds the single merged row with the updated vector
        UntypedResultSet result = execute("SELECT pk, val FROM %s ORDER BY val ANN OF [5.0, 6.0] LIMIT 1");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 1);
        assertRows(result, row(1, vector(5.0f, 6.0f)));
    }

    @Test
    public void testMergeTwoSSTablesWithVectorDeletion()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, val vector<float, 2>)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        // SSTable 1: 2 rows with vectors
        execute("INSERT INTO %s (pk, val) VALUES (1, [1.0, 2.0])");
        execute("INSERT INTO %s (pk, val) VALUES (2, [3.0, 4.0])");
        flush();

        // SSTable 2: delete row pk = 1
        execute("DELETE FROM %s WHERE pk = 1");
        flush();

        // Compact the 2 SSTables into 1 (tombstone removes pk = 1)
        compact();

        // Verify ANN query only returns the non-deleted row pk = 2
        UntypedResultSet result = execute("SELECT pk, val FROM %s ORDER BY val ANN OF [1.0, 2.0] LIMIT 2");
        assertThat(result).hasSize(1);
        assertContainsInt(result, "pk", 2);
        assertRows(result, row(2, vector(3.0f, 4.0f)));
    }

    @Test
    public void testMergeTwoSSTablesRandomOverlappingRows()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, val vector<float, 2>)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        Map<Integer, Vector<Float>> expected = new HashMap<>();

        // SSTable 1: 10 random inserts for pk in [0, 20)
        for (int i = 0; i < 10; i++)
        {
            int pk = getRandom().nextIntBetween(0, 20);
            Vector<Float> vec = randomVectorBoxed(2);
            execute("INSERT INTO %s (pk, val) VALUES (?, ?)", pk, vec);
            expected.put(pk, vec);
        }
        flush();

        // SSTable 2: 10 random inserts for pk in [0, 20) (overwrites earlier values for overlapping keys)
        for (int i = 0; i < 10; i++)
        {
            int pk = getRandom().nextIntBetween(0, 20);
            Vector<Float> vec = randomVectorBoxed(2);
            execute("INSERT INTO %s (pk, val) VALUES (?, ?)", pk, vec);
            expected.put(pk, vec);
        }
        flush();

        // Compact the 2 SSTables into 1
        compact();

        // Validate all rows: each pk must have its latest winning vector
        UntypedResultSet allRows = execute("SELECT pk, val FROM %s");
        assertThat(allRows).hasSize(expected.size());

        for (Map.Entry<Integer, Vector<Float>> entry : expected.entrySet())
        {
            int pk = entry.getKey();
            Vector<Float> expectedVec = entry.getValue();

            UntypedResultSet rowResult = execute("SELECT pk, val FROM %s WHERE pk = ?", pk);
            assertThat(rowResult).hasSize(1);
            assertRows(rowResult, row(pk, expectedVec));
        }

        // Validate via ANN query returning all expected rows
        UntypedResultSet annResult = execute("SELECT pk, val FROM %s ORDER BY val ANN OF [1.0, 1.0] LIMIT 30");
        assertThat(annResult).hasSize(expected.size());
        for (int pk : expected.keySet())
            assertContainsInt(annResult, "pk", pk);
    }

    @Test
    public void testMergeTwoSSTablesRandomOverlappingRowsWithNullVectors()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, val vector<float, 2>, txt text)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        Map<Integer, Vector<Float>> expectedVectors = new HashMap<>();
        Map<Integer, String> expectedTexts = new HashMap<>();

        // SSTable 1: 10 random inserts for pk in [0, 20), some with null vectors
        for (int i = 0; i < 10; i++)
        {
            int pk = getRandom().nextIntBetween(0, 20);
            boolean hasVector = getRandom().nextBoolean();
            Vector<Float> vec = hasVector ? randomVectorBoxed(2) : null;
            String txt = "txt_sstable1_" + i;
            execute("INSERT INTO %s (pk, val, txt) VALUES (?, ?, ?)", pk, vec, txt);
            expectedVectors.put(pk, vec);
            expectedTexts.put(pk, txt);
        }
        flush();

        // SSTable 2: 10 random inserts for pk in [0, 20), some with null vectors (overwrites earlier values)
        for (int i = 0; i < 10; i++)
        {
            int pk = getRandom().nextIntBetween(0, 20);
            boolean hasVector = getRandom().nextBoolean();
            Vector<Float> vec = hasVector ? randomVectorBoxed(2) : null;
            String txt = "txt_sstable2_" + i;
            execute("INSERT INTO %s (pk, val, txt) VALUES (?, ?, ?)", pk, vec, txt);
            expectedVectors.put(pk, vec);
            expectedTexts.put(pk, txt);
        }
        flush();

        // Compact the 2 SSTables into 1
        compact();

        // Validate all rows: each pk must have its latest winning vector (or null) and text
        UntypedResultSet allRows = execute("SELECT pk, val, txt FROM %s");
        assertThat(allRows).hasSize(expectedTexts.size());

        int expectedVectorCount = 0;
        for (Map.Entry<Integer, Vector<Float>> entry : expectedVectors.entrySet())
        {
            int pk = entry.getKey();
            Vector<Float> expectedVec = entry.getValue();
            String expectedTxt = expectedTexts.get(pk);

            UntypedResultSet rowResult = execute("SELECT pk, val, txt FROM %s WHERE pk = ?", pk);
            assertThat(rowResult).hasSize(1);
            assertRows(rowResult, row(pk, expectedVec, expectedTxt));

            if (expectedVec != null)
                expectedVectorCount++;
        }

        // Validate via ANN query: only rows with non-null vectors should be returned
        UntypedResultSet annResult = execute("SELECT pk, val FROM %s ORDER BY val ANN OF [1.0, 1.0] LIMIT 30");
        assertThat(annResult).hasSize(expectedVectorCount);
        for (Map.Entry<Integer, Vector<Float>> entry : expectedVectors.entrySet())
        {
            if (entry.getValue() != null)
                assertContainsInt(annResult, "pk", entry.getKey());
        }
    }
}
