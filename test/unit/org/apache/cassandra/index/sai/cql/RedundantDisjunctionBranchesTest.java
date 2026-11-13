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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that the queries with the OR operator return the same rows when the query plan removes redundant branches,
 * such as the query that relaxes its criteria in each branch: (A AND B AND C) OR (A AND B) OR A.
 */
@RunWith(Parameterized.class)
public class RedundantDisjunctionBranchesTest extends SAITester
{
    private static final String SAI = "CREATE CUSTOM INDEX ON %%s(%s) USING 'StorageAttachedIndex'";

    private static final String RELAXING_CRITERIA = "(tax = 't1' AND last = 'l1' AND dob = '1980-01-01') OR " +
                                                    "(tax = 't1' AND last = 'l1') OR " +
                                                    "(tax = 't1')";

    @Parameterized.Parameters(name = "version={0}")
    public static List<Object> data()
    {
        return Stream.of(Version.AA, SAIUtil.currentVersion(), Version.LATEST).map(v -> new Object[]{ v }).collect(Collectors.toList());
    }

    @Parameterized.Parameter
    public Version version;

    @Before
    public void setup() throws Throwable
    {
        SAIUtil.setCurrentVersion(version);
    }

    private void createPeopleTable()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, tax text, last text, dob date)");
        createIndex(String.format(SAI, "tax"));
        createIndex(String.format(SAI, "last"));
        createIndex(String.format(SAI, "dob"));
    }

    @Test
    public void testBranchesRelaxingTheSameCriteria() throws Throwable
    {
        createPeopleTable();
        execute("INSERT INTO %s (id, tax, last, dob) VALUES (1, 't1', 'l1', '1980-01-01')");
        execute("INSERT INTO %s (id, tax, last, dob) VALUES (2, 't1', 'l1', '1990-01-01')");
        execute("INSERT INTO %s (id, tax, last, dob) VALUES (3, 't1', 'l2', '1980-01-01')");
        execute("INSERT INTO %s (id, tax, last, dob) VALUES (4, 't2', 'l1', '1980-01-01')");

        beforeAndAfterFlush(() -> assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE " + RELAXING_CRITERIA),
                                                          row(1), row(2), row(3)));
    }

    @Test
    public void testBranchesWithDifferentValues() throws Throwable
    {
        createPeopleTable();
        execute("INSERT INTO %s (id, tax, last, dob) VALUES (1, 't1', 'l1', '1980-01-01')");
        execute("INSERT INTO %s (id, tax, last, dob) VALUES (2, 't1', 'l1', '1990-01-01')");
        execute("INSERT INTO %s (id, tax, last, dob) VALUES (3, 't2', 'l2', '1980-01-01')");
        execute("INSERT INTO %s (id, tax, last, dob) VALUES (4, 't2', 'l3', '1980-01-01')");
        execute("INSERT INTO %s (id, tax, last, dob) VALUES (5, 't3', 'l4', '1980-01-01')");
        execute("INSERT INTO %s (id, tax, last, dob) VALUES (6, 't4', 'l4', '1980-01-01')");

        beforeAndAfterFlush(() -> assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE " +
                                                                  "(tax = 't1' AND last = 'l1' AND dob = '1980-01-01') OR " +
                                                                  "(tax = 't2' AND last = 'l2') OR " +
                                                                  "(tax = 't3')"),
                                                          row(1), row(3), row(5)));
    }

    /**
     * A branch is redundant only if all of the conditions of another branch are among its own.
     */
    @Test
    public void testBranchesThatLookRedundant() throws Throwable
    {
        createPeopleTable();
        execute("INSERT INTO %s (id, tax, last, dob) VALUES (1, 't1', 'l1', '1980-01-01')");
        execute("INSERT INTO %s (id, tax, last, dob) VALUES (2, 't1', 'l2', '1990-01-01')");
        execute("INSERT INTO %s (id, tax, last, dob) VALUES (3, 't2', 'l1', '1990-01-01')");
        execute("INSERT INTO %s (id, tax, last, dob) VALUES (4, 't2', 'l2', '1980-01-01')");

        beforeAndAfterFlush(() -> {
            // no branch includes the conditions of the other
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE (tax = 't1' AND last = 'l1') OR (last = 'l1' AND dob = '1990-01-01')"),
                                    row(1), row(3));
            // same branches
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE tax = 't1' OR tax = 't1'"), row(1), row(2));
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE (tax = 't1' AND last = 'l1') OR (last = 'l1' AND tax = 't1')"), row(1));
            // the disjunction is in a branch of a conjunction
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE dob = '1980-01-01' AND (tax = 't2' OR (tax = 't2' AND last = 'l2'))"), row(4));
            // the branch to keep has a nested disjunction, so it's narrower than its expressions alone
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE (tax = 't1' AND (last = 'l1' OR dob = '1990-01-01')) OR (tax = 't1' AND last = 'l2')"),
                                    row(1), row(2));
            // a different operator on the same value
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE dob != '1980-01-01' OR (dob = '1980-01-01' AND last = 'l1')"), row(1), row(2), row(3));
        });
    }

    @Test
    public void testAnalyzedColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, name text, code int)");
        createIndex("CREATE CUSTOM INDEX ON %s(name) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");
        createIndex(String.format(SAI, "code"));

        execute("INSERT INTO %s (id, name, code) VALUES (1, 'john smith', 1)");
        execute("INSERT INTO %s (id, name, code) VALUES (2, 'john', 2)");
        execute("INSERT INTO %s (id, name, code) VALUES (3, 'smith', 3)");
        execute("INSERT INTO %s (id, name, code) VALUES (4, 'bob', 4)");

        beforeAndAfterFlush(() -> {
            // the tokens of a term must all match, also under a disjunction
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE name : 'john smith' OR code = 4"), row(1), row(4));
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE (name : 'john smith' AND code = 1) OR (name : 'john smith')"), row(1));
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE (name : 'john' AND code = 2) OR (name : 'smith')"), row(1), row(2), row(3));
        });
    }

    @Test
    public void testRowsSplitAcrossSSTables()
    {
        createPeopleTable();
        disableCompaction(KEYSPACE);

        execute("INSERT INTO %s (id, tax) VALUES (1, 't1')");
        execute("INSERT INTO %s (id, last) VALUES (2, 'l1')");
        flush();
        execute("INSERT INTO %s (id, last) VALUES (1, 'l1')");
        execute("INSERT INTO %s (id, tax) VALUES (2, 't2')");
        flush();

        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE (tax = 't1' AND last = 'l1') OR (tax = 't2' AND last = 'l1')"), row(1), row(2));
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE (tax = 't1' AND last = 'l1') OR tax = 't1'"), row(1));
    }

    @Test
    public void testRowsThatStopMatching()
    {
        createPeopleTable();
        disableCompaction(KEYSPACE);

        execute("INSERT INTO %s (id, tax, last) VALUES (1, 't1', 'l1')");
        execute("INSERT INTO %s (id, tax, last) VALUES (2, 't1', 'l1')");
        execute("INSERT INTO %s (id, tax, last) VALUES (3, 't1', 'l1')");
        flush();
        execute("UPDATE %s SET tax = 't2', last = 'l2' WHERE id = 1");
        execute("DELETE FROM %s WHERE id = 2");
        execute("UPDATE %s SET last = 'l2' WHERE id = 3");

        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE tax = 't1' OR (tax = 't1' AND last = 'l1')"), row(3));
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE last = 'l1' OR (tax = 't1' AND last = 'l1')"));
        flush();
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE tax = 't1' OR (tax = 't1' AND last = 'l1')"), row(3));
        assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE last = 'l1' OR (tax = 't1' AND last = 'l1')"));
    }

    @Test
    public void testPagingAndLimit() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, a int, b int, PRIMARY KEY(pk, ck))");
        createIndex(String.format(SAI, "a"));
        createIndex(String.format(SAI, "b"));

        // all the rows match the first branch, half of them match the second too, which is redundant
        int rows = 0;
        for (int pk = 0; pk < 20; pk++)
            for (int ck = 0; ck < 5; ck++, rows++)
                execute("INSERT INTO %s (pk, ck, a, b) VALUES (?, ?, 1, ?)", pk, ck, rows % 2);
        int expected = rows;

        beforeAndAfterFlush(() -> {
            for (int pageSize : new int[]{ 1, 3, 10, 1000 })
            {
                ResultSet result = executeNetWithPaging("SELECT pk, ck FROM %s WHERE a = 1 OR (a = 1 AND b = 1)", pageSize);
                List<String> keys = new ArrayList<>();
                for (Row row : result)
                    keys.add(row.getInt("pk") + ":" + row.getInt("ck"));
                assertThat(keys).hasSize(expected);
                assertThat(new HashSet<>(keys)).hasSize(expected);
            }

            assertThat(execute("SELECT pk, ck FROM %s WHERE a = 1 OR (a = 1 AND b = 1) LIMIT 7").size()).isEqualTo(7);
            assertThat(execute("SELECT pk, ck FROM %s WHERE a = 1 OR (a = 1 AND b = 1) PER PARTITION LIMIT 1").size()).isEqualTo(20);
        });
    }

    @Test
    public void testBM25OrderIsNotAffectedByRedundantBranches() throws Throwable
    {
        // BM25 needs a version that AA doesn't support, and the query is the same for all the others
        Assume.assumeTrue(version.onOrAfter(Version.BM25_EARLIEST));

        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text, code int)");
        createIndex("CREATE CUSTOM INDEX ON %s(body) USING 'StorageAttachedIndex' WITH OPTIONS = {'index_analyzer': 'standard'}");
        createIndex(String.format(SAI, "code"));

        execute("INSERT INTO %s (id, body, code) VALUES (1, 'apple', 1)");
        execute("INSERT INTO %s (id, body, code) VALUES (2, 'apple apple apple', 1)");
        execute("INSERT INTO %s (id, body, code) VALUES (3, 'pear', 2)");

        beforeAndAfterFlush(() -> {
            Object[][] plain = getRows(execute("SELECT id FROM %s WHERE code = 1 ORDER BY body BM25 OF 'apple' LIMIT 10"));
            Object[][] redundant = getRows(execute("SELECT id FROM %s WHERE code = 1 OR (code = 1 AND body : 'apple') ORDER BY body BM25 OF 'apple' LIMIT 10"));
            assertThat(redundant).isDeepEqualTo(plain);
            assertThat(plain.length).isEqualTo(2);
        });
    }
}
