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
package org.apache.cassandra.cql3.validation.operations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;

import org.junit.Test;

public class SelectSingleColumnRelationTest extends CQLTester
{
    @Test
    public void testInvalidCollectionEqualityRelation()
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b set<int>, c list<int>, d map<int, int>)");
        createIndex("CREATE INDEX ON %s (b)");
        createIndex("CREATE INDEX ON %s (c)");
        createIndex("CREATE INDEX ON %s (d)");

        assertInvalidMessage("Collection column 'b' (set<int>) cannot be restricted by a '=' relation",
                             "SELECT * FROM %s WHERE a = 0 AND b=?", set(0));
        assertInvalidMessage("Collection column 'c' (list<int>) cannot be restricted by a '=' relation",
                             "SELECT * FROM %s WHERE a = 0 AND c=?", list(0));
        assertInvalidMessage("Collection column 'd' (map<int, int>) cannot be restricted by a '=' relation",
                             "SELECT * FROM %s WHERE a = 0 AND d=?", map(0, 0));
    }

    @Test
    public void testInvalidCollectionNonEQRelation()
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b set<int>, c int)");
        createIndex("CREATE INDEX ON %s (c)");
        execute("INSERT INTO %s (a, b, c) VALUES (0, {0}, 0)");

        // non-EQ operators
        assertInvalidMessage("Collection column 'b' (set<int>) cannot be restricted by a '>' relation",
                             "SELECT * FROM %s WHERE c = 0 AND b > ?", set(0));
        assertInvalidMessage("Collection column 'b' (set<int>) cannot be restricted by a '>=' relation",
                             "SELECT * FROM %s WHERE c = 0 AND b >= ?", set(0));
        assertInvalidMessage("Collection column 'b' (set<int>) cannot be restricted by a '<' relation",
                             "SELECT * FROM %s WHERE c = 0 AND b < ?", set(0));
        assertInvalidMessage("Collection column 'b' (set<int>) cannot be restricted by a '<=' relation",
                             "SELECT * FROM %s WHERE c = 0 AND b <= ?", set(0));
        assertInvalidMessage("Collection column 'b' (set<int>) cannot be restricted by a 'IN' relation",
                             "SELECT * FROM %s WHERE c = 0 AND b IN (?)", set(0));
        assertInvalidMessage("Collection column 'b' (set<int>) cannot be restricted by a 'NOT IN' relation",
                             "SELECT * FROM %s WHERE c = 0 AND b NOT IN (?)", set(0));
        assertInvalidMessage("Collection column 'b' (set<int>) cannot be restricted by a '!=' relation",
                "SELECT * FROM %s WHERE c = 0 AND b != 5");
        assertInvalidMessage("Unsupported restriction: b IS NOT NULL",
                "SELECT * FROM %s WHERE c = 0 AND b IS NOT NULL");
    }

    @Test
    public void testClusteringColumnRelations() throws Throwable
    {
        createTable("CREATE TABLE %s (a text, b int, c int, d int, primary key(a, b, c))");
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 1, 5, 1);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 2, 6, 2);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 3, 7, 3);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "second", 4, 8, 4);

        assertRows(execute("select * from %s where a in (?, ?)", "first", "second"),
                   row("first", 1, 5, 1),
                   row("first", 2, 6, 2),
                   row("first", 3, 7, 3),
                   row("second", 4, 8, 4));

        assertRows(execute("select * from %s where a = ? and b = ? and c in (?, ?)", "first", 2, 6, 7),
                   row("first", 2, 6, 2));

        assertRows(execute("select * from %s where a = ? and b in (?, ?) and c in (?, ?)", "first", 2, 3, 6, 7),
                   row("first", 2, 6, 2),
                   row("first", 3, 7, 3));

        assertRows(execute("select * from %s where a = ? and b in (?, ?) and c in (?, ?)", "first", 3, 2, 7, 6),
                   row("first", 2, 6, 2),
                   row("first", 3, 7, 3));

        assertRows(execute("select * from %s where a = ? and c in (?, ?) and b in (?, ?)", "first", 7, 6, 3, 2),
                   row("first", 2, 6, 2),
                   row("first", 3, 7, 3));

        assertRows(execute("select c, d from %s where a = ? and c in (?, ?) and b in (?, ?)", "first", 7, 6, 3, 2),
                   row(6, 2),
                   row(7, 3));

        assertRows(execute("select c, d from %s where a = ? and c in (?, ?) and b in (?, ?, ?)", "first", 7, 6, 3, 2, 3),
                   row(6, 2),
                   row(7, 3));

        assertRows(execute("select * from %s where a = ? and b in (?, ?) and c = ?", "first", 3, 2, 7),
                   row("first", 3, 7, 3));

        assertRows(execute("select * from %s where a = ? and b in ? and c in ?",
                           "first", Arrays.asList(3, 2), Arrays.asList(7, 6)),
                   row("first", 2, 6, 2),
                   row("first", 3, 7, 3));

        assertInvalidMessage("Invalid null value for column b",
                             "select * from %s where a = ? and b in ? and c in ?", "first", null, Arrays.asList(7, 6));

        assertRows(execute("select * from %s where a = ? and c >= ? and b in (?, ?)", "first", 6, 3, 2),
                   row("first", 2, 6, 2),
                   row("first", 3, 7, 3));

        assertRows(execute("select * from %s where a = ? and c > ? and b in (?, ?)", "first", 6, 3, 2),
                   row("first", 3, 7, 3));

        assertRows(execute("select * from %s where a = ? and c <= ? and b in (?, ?)", "first", 6, 3, 2),
                   row("first", 2, 6, 2));

        assertRows(execute("select * from %s where a = ? and c < ? and b in (?, ?)", "first", 7, 3, 2),
                   row("first", 2, 6, 2));

        assertRows(execute("select * from %s where a = ? and c >= ? and c <= ? and b in (?, ?)", "first", 6, 7, 3, 2),
                   row("first", 2, 6, 2),
                   row("first", 3, 7, 3));

        assertRows(execute("select * from %s where a = ? and c > ? and c <= ? and b in (?, ?)", "first", 6, 7, 3, 2),
                   row("first", 3, 7, 3));

        assertEmpty(execute("select * from %s where a = ? and c > ? and c < ? and b in (?, ?)", "first", 6, 7, 3, 2));

        assertInvalidMessage("Column \"c\" cannot be restricted by both an equality and an inequality relation",
                             "select * from %s where a = ? and c > ? and c = ? and b in (?, ?)", "first", 6, 7, 3, 2);

        assertInvalidMessage("c cannot be restricted by more than one relation if it includes an Equal",
                             "select * from %s where a = ? and c = ? and c > ?  and b in (?, ?)", "first", 6, 7, 3, 2);

        assertRows(execute("select * from %s where a = ? and c in (?, ?) and b in (?, ?) order by b DESC",
                           "first", 7, 6, 3, 2),
                   row("first", 3, 7, 3),
                   row("first", 2, 6, 2));

        assertInvalidMessage("More than one restriction was found for the start bound on b",
                             "select * from %s where a = ? and b > ? and b > ?", "first", 6, 3, 2);

        assertInvalidMessage("More than one restriction was found for the end bound on b",
                             "select * from %s where a = ? and b < ? and b <= ?", "first", 6, 3, 2);
    }

    @Test
    public void testPartitionKeyColumnRelations()
    {
        createTable("CREATE TABLE %s (a text, b int, c int, d int, primary key((a, b), c))");
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 1, 1, 1);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 2, 2, 2);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 3, 3, 3);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 4, 4, 4);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "second", 1, 1, 1);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "second", 4, 4, 4);

        assertRows(execute("select * from %s where a = ? and b = ?", "first", 2),
                   row("first", 2, 2, 2));

        assertRows(execute("select * from %s where a in (?, ?) and b in (?, ?)", "first", "second", 2, 3),
                   row("first", 2, 2, 2),
                   row("first", 3, 3, 3));

        assertRows(execute("select * from %s where a in (?, ?) and b = ?", "first", "second", 4),
                   row("first", 4, 4, 4),
                   row("second", 4, 4, 4));

        assertRows(execute("select * from %s where a = ? and b in (?, ?)", "first", 3, 4),
                   row("first", 3, 3, 3),
                   row("first", 4, 4, 4));

        assertRows(execute("select * from %s where a in (?, ?) and b in (?, ?)", "first", "second", 1, 4),
                   row("first", 1, 1, 1),
                   row("first", 4, 4, 4),
                   row("second", 1, 1, 1),
                   row("second", 4, 4, 4));

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "select * from %s where a in (?, ?)", "first", "second");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "select * from %s where a = ?", "first");
        assertInvalidMessage("b cannot be restricted by more than one relation if it includes a IN",
                             "select * from %s where a = ? AND b IN (?, ?) AND b = ?", "first", 2, 2, 3);
        assertInvalidMessage("b cannot be restricted by more than one relation if it includes an Equal",
                             "select * from %s where a = ? AND b = ? AND b IN (?, ?)", "first", 2, 2, 3);
        assertInvalidMessage("a cannot be restricted by more than one relation if it includes a IN",
                             "select * from %s where a IN (?, ?) AND a = ? AND b = ?", "first", "second", "first", 3);
        assertInvalidMessage("a cannot be restricted by more than one relation if it includes an Equal",
                             "select * from %s where a = ? AND a IN (?, ?) AND b IN (?, ?)", "first", "second", "first", 2, 3);
    }

    @Test
    public void testClusteringColumnRelationsWithClusteringOrder()
    {
        createTable("CREATE TABLE %s (a text, b int, c int, d int, primary key(a, b, c)) WITH CLUSTERING ORDER BY (b DESC, c ASC);");
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 1, 5, 1);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 2, 6, 2);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "first", 3, 7, 3);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "second", 4, 8, 4);

        assertRows(execute("select * from %s where a = ? and c in (?, ?) and b in (?, ?) order by b DESC",
                           "first", 7, 6, 3, 2),
                   row("first", 3, 7, 3),
                   row("first", 2, 6, 2));

        assertRows(execute("select * from %s where a = ? and c in (?, ?) and b in (?, ?) order by b ASC",
                           "first", 7, 6, 3, 2),
                   row("first", 2, 6, 2),
                   row("first", 3, 7, 3));
    }

    @Test
    public void testAllowFilteringWithClusteringColumn()
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");

        execute("INSERT INTO %s (k, c, v) VALUES(?, ?, ?)", 1, 2, 1);
        execute("INSERT INTO %s (k, c, v) VALUES(?, ?, ?)", 1, 3, 2);
        execute("INSERT INTO %s (k, c, v) VALUES(?, ?, ?)", 2, 2, 3);

        // Don't require filtering, always allowed
        assertRows(execute("SELECT * FROM %s WHERE k = ?", 1),
                   row(1, 2, 1),
                   row(1, 3, 2));

        assertRows(execute("SELECT * FROM %s WHERE k = ? AND c > ?", 1, 2), row(1, 3, 2));

        assertRows(execute("SELECT * FROM %s WHERE k = ? AND c = ?", 1, 2), row(1, 2, 1));

        assertRows(execute("SELECT * FROM %s WHERE k = ? ALLOW FILTERING", 1),
                   row(1, 2, 1),
                   row(1, 3, 2));

        assertRows(execute("SELECT * FROM %s WHERE k = ? AND c > ? ALLOW FILTERING", 1, 2), row(1, 3, 2));

        assertRows(execute("SELECT * FROM %s WHERE k = ? AND c = ? ALLOW FILTERING", 1, 2), row(1, 2, 1));

        // Require filtering, allowed only with ALLOW FILTERING
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c = ?", 2);
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE c > ? AND c <= ?", 2, 4);

        assertRows(execute("SELECT * FROM %s WHERE c = ? ALLOW FILTERING", 2),
                   row(1, 2, 1),
                   row(2, 2, 3));

        assertRows(execute("SELECT * FROM %s WHERE c > ? AND c <= ? ALLOW FILTERING", 2, 4), row(1, 3, 2));
    }

    @Test
    public void testAllowFilteringWithIndexedColumn()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int)");
        createIndex("CREATE INDEX ON %s(a)");

        execute("INSERT INTO %s(k, a, b) VALUES(?, ?, ?)", 1, 10, 100);
        execute("INSERT INTO %s(k, a, b) VALUES(?, ?, ?)", 2, 20, 200);
        execute("INSERT INTO %s(k, a, b) VALUES(?, ?, ?)", 3, 30, 300);
        execute("INSERT INTO %s(k, a, b) VALUES(?, ?, ?)", 4, 40, 400);

        // Don't require filtering, always allowed
        assertRows(execute("SELECT * FROM %s WHERE k = ?", 1), row(1, 10, 100));
        assertRows(execute("SELECT * FROM %s WHERE a = ?", 20), row(2, 20, 200));
        assertRows(execute("SELECT * FROM %s WHERE k = ? ALLOW FILTERING", 1), row(1, 10, 100));
        assertRows(execute("SELECT * FROM %s WHERE a = ? ALLOW FILTERING", 20), row(2, 20, 200));

        assertInvalid("SELECT * FROM %s WHERE a = ? AND b = ?");
        assertRows(execute("SELECT * FROM %s WHERE a = ? AND b = ? ALLOW FILTERING", 20, 200), row(2, 20, 200));
    }

    @Test
    public void testAllowFilteringWithIndexedColumnAndStaticColumns()
    {
        createTable("CREATE TABLE %s (a int, b int, c int, s int static, PRIMARY KEY(a, b))");
        createIndex("CREATE INDEX ON %s(c)");

        execute("INSERT INTO %s(a, b, c, s) VALUES(?, ?, ?, ?)", 1, 1, 1, 1);
        execute("INSERT INTO %s(a, b, c) VALUES(?, ?, ?)", 1, 2, 1);
        execute("INSERT INTO %s(a, s) VALUES(?, ?)", 3, 3);
        execute("INSERT INTO %s(a, b, c, s) VALUES(?, ?, ?, ?)", 2, 1, 1, 2);

        assertRows(execute("SELECT * FROM %s WHERE c = ? AND s > ? ALLOW FILTERING", 1, 1),
                   row(2, 1, 2, 1));

        assertRows(execute("SELECT * FROM %s WHERE c = ? AND s < ? ALLOW FILTERING", 1, 2),
                   row(1, 1, 1, 1),
                   row(1, 2, 1, 1));
    }

    @Test
    public void testIndexQueriesOnComplexPrimaryKey()
    {
        createTable("CREATE TABLE %s (pk0 int, pk1 int, ck0 int, ck1 int, ck2 int, value int, PRIMARY KEY ((pk0, pk1), ck0, ck1, ck2))");

        createIndex("CREATE INDEX ON %s (ck1)");
        createIndex("CREATE INDEX ON %s (ck2)");
        createIndex("CREATE INDEX ON %s (pk0)");
        createIndex("CREATE INDEX ON %s (ck0)");

        execute("INSERT INTO %s (pk0, pk1, ck0, ck1, ck2, value) VALUES (?, ?, ?, ?, ?, ?)", 0, 1, 2, 3, 4, 5);
        execute("INSERT INTO %s (pk0, pk1, ck0, ck1, ck2, value) VALUES (?, ?, ?, ?, ?, ?)", 1, 2, 3, 4, 5, 0);
        execute("INSERT INTO %s (pk0, pk1, ck0, ck1, ck2, value) VALUES (?, ?, ?, ?, ?, ?)", 2, 3, 4, 5, 0, 1);
        execute("INSERT INTO %s (pk0, pk1, ck0, ck1, ck2, value) VALUES (?, ?, ?, ?, ?, ?)", 3, 4, 5, 0, 1, 2);
        execute("INSERT INTO %s (pk0, pk1, ck0, ck1, ck2, value) VALUES (?, ?, ?, ?, ?, ?)", 4, 5, 0, 1, 2, 3);
        execute("INSERT INTO %s (pk0, pk1, ck0, ck1, ck2, value) VALUES (?, ?, ?, ?, ?, ?)", 5, 0, 1, 2, 3, 4);

        assertRows(execute("SELECT value FROM %s WHERE pk0 = 2"), row(1));
        assertRows(execute("SELECT value FROM %s WHERE ck0 = 0"), row(3));
        assertRows(execute("SELECT value FROM %s WHERE pk0 = 3 AND pk1 = 4 AND ck1 = 0"), row(2));
        assertRows(execute("SELECT value FROM %s WHERE pk0 = 5 AND pk1 = 0 AND ck0 = 1 AND ck2 = 3 ALLOW FILTERING"), row(4));
    }

    @Test
    public void testIndexOnClusteringColumns()
    {
        createTable("CREATE TABLE %s (id1 int, id2 int, author text, time bigint, v1 text, v2 text, PRIMARY KEY ((id1, id2), author, time))");
        createIndex("CREATE INDEX ON %s(time)");
        createIndex("CREATE INDEX ON %s(id2)");

        execute("INSERT INTO %s(id1, id2, author, time, v1, v2) VALUES(0, 0, 'bob', 0, 'A', 'A')");
        execute("INSERT INTO %s(id1, id2, author, time, v1, v2) VALUES(0, 0, 'bob', 1, 'B', 'B')");
        execute("INSERT INTO %s(id1, id2, author, time, v1, v2) VALUES(0, 1, 'bob', 2, 'C', 'C')");
        execute("INSERT INTO %s(id1, id2, author, time, v1, v2) VALUES(0, 0, 'tom', 0, 'D', 'D')");
        execute("INSERT INTO %s(id1, id2, author, time, v1, v2) VALUES(0, 1, 'tom', 1, 'E', 'E')");

        assertRows(execute("SELECT v1 FROM %s WHERE time = 1"), row("B"), row("E"));

        assertRows(execute("SELECT v1 FROM %s WHERE id2 = 1"), row("C"), row("E"));

        assertRows(execute("SELECT v1 FROM %s WHERE id1 = 0 AND id2 = 0 AND author = 'bob' AND time = 0"), row("A"));

        // Test for CASSANDRA-8206
        execute("UPDATE %s SET v2 = null WHERE id1 = 0 AND id2 = 0 AND author = 'bob' AND time = 1");

        assertRows(execute("SELECT v1 FROM %s WHERE id2 = 0"), row("A"), row("B"), row("D"));

        assertRows(execute("SELECT v1 FROM %s WHERE time = 1"), row("B"), row("E"));

        assertRows(execute("SELECT v1 FROM %s WHERE id2 = 0 and time IN (1, 2) ALLOW FILTERING"),
                   row("B"));

        assertRows(execute("SELECT v1 FROM %s WHERE author > 'ted' AND time = 1 ALLOW FILTERING"), row("E"));
        assertRows(execute("SELECT v1 FROM %s WHERE author > 'amy' AND author < 'zoe' AND time = 0 ALLOW FILTERING"),
                           row("A"), row("D"));
    }

    @Test
    public void testCompositeIndexWithPrimaryKey() throws Throwable
    {
        createTable("CREATE TABLE %s (blog_id int, time1 int, time2 int, author text, content text, PRIMARY KEY (blog_id, time1, time2))");

        createIndex("CREATE INDEX ON %s(author)");

        String req = "INSERT INTO %s (blog_id, time1, time2, author, content) VALUES (?, ?, ?, ?, ?)";
        execute(req, 1, 0, 0, "foo", "bar1");
        execute(req, 1, 0, 1, "foo", "bar2");
        execute(req, 2, 1, 0, "foo", "baz");
        execute(req, 3, 0, 1, "gux", "qux");

        assertRows(execute("SELECT blog_id, content FROM %s WHERE author='foo'"),
                   row(1, "bar1"),
                   row(1, "bar2"),
                   row(2, "baz"));
        assertRows(execute("SELECT blog_id, content FROM %s WHERE time1 > 0 AND author='foo' ALLOW FILTERING"), row(2, "baz"));
        assertRows(execute("SELECT blog_id, content FROM %s WHERE time1 = 1 AND author='foo' ALLOW FILTERING"), row(2, "baz"));
        assertRows(execute("SELECT blog_id, content FROM %s WHERE time1 = 1 AND time2 = 0 AND author='foo' ALLOW FILTERING"),
                   row(2, "baz"));
        assertEmpty(execute("SELECT content FROM %s WHERE time1 = 1 AND time2 = 1 AND author='foo' ALLOW FILTERING"));
        assertEmpty(execute("SELECT content FROM %s WHERE time1 = 1 AND time2 > 0 AND author='foo' ALLOW FILTERING"));

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT content FROM %s WHERE time2 >= 0 AND author='foo'");
    }

    @Test
    public void testRangeQueryOnIndex()
    {
        createTable("CREATE TABLE %s (id int primary key, row int, setid int);");
        createIndex("CREATE INDEX ON %s (setid)");

        String q = "INSERT INTO %s (id, row, setid) VALUES (?, ?, ?);";
        execute(q, 0, 0, 0);
        execute(q, 1, 1, 0);
        execute(q, 2, 2, 0);
        execute(q, 3, 3, 0);

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE setid = 0 AND row < 1;");
        assertRows(execute("SELECT * FROM %s WHERE setid = 0 AND row < 1 ALLOW FILTERING;"), row(0, 0, 0));
    }

    @Test
    public void testEmptyIN() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, v int, PRIMARY KEY (k1, k2))");

        for (int i = 0; i <= 2; i++)
            for (int j = 0; j <= 2; j++)
                execute("INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", i, j, i + j);

        assertEmpty(execute("SELECT v FROM %s WHERE k1 IN ()"));
        assertEmpty(execute("SELECT v FROM %s WHERE k1 = 0 AND k2 IN ()"));
    }

    @Test
    public void testINWithDuplicateValue()
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, v int, PRIMARY KEY (k1, k2))");
        execute("INSERT INTO %s (k1,  k2, v) VALUES (?, ?, ?)", 1, 1, 1);

        assertRows(execute("SELECT * FROM %s WHERE k1 IN (?, ?)", 1, 1),
                   row(1, 1, 1));

        assertRows(execute("SELECT * FROM %s WHERE k1 IN (?, ?) AND k2 IN (?, ?)", 1, 1, 1, 1),
                   row(1, 1, 1));

        assertRows(execute("SELECT * FROM %s WHERE k1 = ? AND k2 IN (?, ?)", 1, 1, 1),
                   row(1, 1, 1));
    }

    @Test
    public void testLargeClusteringINValues()
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");
        execute("INSERT INTO %s (k, c, v) VALUES (0, 0, 0)");
        List<Integer> inValues = new ArrayList<>(10000);
        for (int i = 0; i < 10000; i++)
            inValues.add(i);
        assertRows(execute("SELECT * FROM %s WHERE k=? AND c IN ?", 0, inValues),
                row(0, 0, 0));
    }

    @Test
    public void testMultiplePartitionKeyWithIndex()
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, f int, PRIMARY KEY ((a, b), c, d, e))");
        createIndex("CREATE INDEX ON %s (c)");
        createIndex("CREATE INDEX ON %s (f)");

        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 0, 0, 0, 0);
        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 0, 1, 0, 1);
        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 0, 1, 1, 2);

        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 1, 0, 0, 3);
        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 1, 1, 0, 4);
        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 1, 1, 1, 5);

        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)", 0, 0, 2, 0, 0, 5);

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a = ? AND c = ?", 0, 1);
        assertRows(execute("SELECT * FROM %s WHERE a = ? AND c = ? ALLOW FILTERING", 0, 1),
                   row(0, 0, 1, 0, 0, 3),
                   row(0, 0, 1, 1, 0, 4),
                   row(0, 0, 1, 1, 1, 5));

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a = ? AND c = ? AND d = ?", 0, 1, 1);
        assertRows(execute("SELECT * FROM %s WHERE a = ? AND c = ? AND d = ? ALLOW FILTERING", 0, 1, 1),
                   row(0, 0, 1, 1, 0, 4),
                   row(0, 0, 1, 1, 1, 5));

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND c IN (?) AND  d IN (?) ALLOW FILTERING", 0, 1, 1),
                row(0, 0, 1, 1, 0, 4),
                row(0, 0, 1, 1, 1, 5));

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND (c, d) >= (?, ?) ALLOW FILTERING", 0, 1, 1),
                row(0, 0, 1, 1, 0, 4),
                row(0, 0, 1, 1, 1, 5),
                row(0, 0, 2, 0, 0, 5));

        assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE, 'c'),
                             "SELECT * FROM %s WHERE a = ? AND c IN (?, ?) AND f = ?", 0, 0, 1, 5);
        assertRows(execute("SELECT * FROM %s WHERE a = ? AND c IN (?, ?) AND f = ? ALLOW FILTERING", 0, 1, 3, 5),
                   row(0, 0, 1, 1, 1, 5));

        assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE, 'c'),
                             "SELECT * FROM %s WHERE a = ? AND c IN (?, ?) AND f = ?", 0, 1, 2, 5);
        assertRows(execute("SELECT * FROM %s WHERE a = ? AND c IN (?, ?) AND f = ? ALLOW FILTERING", 0, 1, 2, 5),
                   row(0, 0, 1, 1, 1, 5),
                   row(0, 0, 2, 0, 0, 5));

        assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE, 'c'),
                             "SELECT * FROM %s WHERE a = ? AND c IN (?, ?) AND d IN (?) AND f = ?", 0, 1, 3, 0, 3);
        assertRows(execute("SELECT * FROM %s WHERE a = ? AND c IN (?, ?) AND d IN (?) AND f = ? ALLOW FILTERING", 0, 1, 3, 0, 3),
                   row(0, 0, 1, 0, 0, 3));

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND c >= ? ALLOW FILTERING", 0, 1),
                row(0, 0, 1, 0, 0, 3),
                row(0, 0, 1, 1, 0, 4),
                row(0, 0, 1, 1, 1, 5),
                row(0, 0, 2, 0, 0, 5));

        assertInvalidMessage(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE, 'c'),
                             "SELECT * FROM %s WHERE a = ? AND c >= ? AND f = ?", 0, 1, 5);
        assertRows(execute("SELECT * FROM %s WHERE a = ? AND b = ? AND c >= ? AND f = ?", 0, 0, 1, 5),
                   row(0, 0, 1, 1, 1, 5),
                   row(0, 0, 2, 0, 0, 5));

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND c >= ? AND f = ? ALLOW FILTERING", 0, 1, 5),
                   row(0, 0, 1, 1, 1, 5),
                   row(0, 0, 2, 0, 0, 5));

        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a = ? AND c = ? AND d >= ? AND f = ?", 0, 1, 1, 5);

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND b = ? AND c = ? AND d >= ? AND f = ?", 0, 0, 1, 1, 5),
                   row(0, 0, 1, 1, 1, 5));

        assertRows(execute("SELECT * FROM %s WHERE a = ? AND c = ? AND d >= ? AND f = ? ALLOW FILTERING", 0, 1, 1, 5),
                   row(0, 0, 1, 1, 1, 5));
    }

    @Test
    public void testFunctionCallWithUnset()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, s text, i int)");

        assertInvalidMessage("Invalid unset value for argument in call to function token",
                             "SELECT * FROM %s WHERE token(k) >= token(?)", unset());
        assertInvalidMessage("Invalid unset value for argument in call to function blobasint",
                             "SELECT * FROM %s WHERE k = blobAsInt(?)", unset());
    }

    @Test
    public void testLimitWithUnset()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, i int)");
        execute("INSERT INTO %s (k, i) VALUES (1, 1)");
        execute("INSERT INTO %s (k, i) VALUES (2, 1)");
        assertRows(execute("SELECT k FROM %s LIMIT ?", unset()), // treat as 'unlimited'
                row(1),
                row(2)
        );
    }

    @Test
    public void testWithUnsetValues()
    {
        createTable("CREATE TABLE %s (k int, i int, j int, s text, PRIMARY KEY(k,i,j))");
        createIndex("CREATE INDEX s_index ON %s (s)");
        // partition key
        assertInvalidMessage("Invalid unset value for column k", "SELECT * from %s WHERE k = ?", unset());
        assertInvalidMessage("Invalid unset value for column k", "SELECT * from %s WHERE k IN ?", unset());
        assertInvalidMessage("Invalid unset value for column k", "SELECT * from %s WHERE k IN(?)", unset());
        assertInvalidMessage("Invalid unset value for column k", "SELECT * from %s WHERE k IN(?,?)", 1, unset());
        assertInvalidMessage("Invalid unset value for column k", "SELECT * from %s WHERE k NOT IN ? ALLOW FILTERING", unset());
        assertInvalidMessage("Unsupported unset value for column k", "SELECT * from %s WHERE k NOT IN(?) ALLOW FILTERING", unset());
        assertInvalidMessage("Unsupported unset value for column k", "SELECT * from %s WHERE k NOT IN(?,?) ALLOW FILTERING", 1, unset());
        // clustering column
        assertInvalidMessage("Invalid unset value for column i", "SELECT * from %s WHERE k = 1 AND i = ?", unset());
        assertInvalidMessage("Invalid unset value for column i", "SELECT * from %s WHERE k = 1 AND i IN ?", unset());
        assertInvalidMessage("Invalid unset value for column i", "SELECT * from %s WHERE k = 1 AND i IN(?)", unset());
        assertInvalidMessage("Invalid unset value for column i", "SELECT * from %s WHERE k = 1 AND i IN(?,?)", 1, unset());
        assertInvalidMessage("Invalid unset value for column i", "SELECT * from %s WHERE k = 1 AND i NOT IN ?", unset());
        assertInvalidMessage("Invalid unset value for column i", "SELECT * from %s WHERE k = 1 AND i NOT IN(?)", unset());
        assertInvalidMessage("Invalid unset value for column i", "SELECT * from %s WHERE k = 1 AND i NOT IN(?,?)", 1, unset());
        assertInvalidMessage("Invalid unset value for column i", "SELECT * from %s WHERE i = ? ALLOW FILTERING", unset());
        // indexed column
        assertInvalidMessage("Unsupported unset value for column s", "SELECT * from %s WHERE s = ?", unset());
        // range
        assertInvalidMessage("Invalid unset value for column i", "SELECT * from %s WHERE k = 1 AND i > ?", unset());
    }

    @Test
    public void testInvalidSliceRestrictionOnPartitionKey()
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int, c text)");
        assertInvalidMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE,
                             "SELECT * FROM %s WHERE a >= 1 and a < 4");
        assertInvalidMessage("Multi-column relations can only be applied to clustering columns but was applied to: a",
                             "SELECT * FROM %s WHERE (a) >= (1) and (a) < (4)");
    }

    @Test
    public void testInvalidMulticolumnSliceRestrictionOnPartitionKey()
    {
        createTable("CREATE TABLE %s (a int, b int, c text, PRIMARY KEY ((a, b)))");
        assertInvalidMessage("Multi-column relations can only be applied to clustering columns but was applied to: a",
                             "SELECT * FROM %s WHERE (a, b) >= (1, 1) and (a, b) < (4, 1)");
        assertInvalidMessage("Multi-column relations can only be applied to clustering columns but was applied to: a",
                             "SELECT * FROM %s WHERE a >= 1 and (a, b) < (4, 1)");
        assertInvalidMessage("Multi-column relations can only be applied to clustering columns but was applied to: a",
                             "SELECT * FROM %s WHERE b >= 1 and (a, b) < (4, 1)");
        assertInvalidMessage("Multi-column relations can only be applied to clustering columns but was applied to: a",
                             "SELECT * FROM %s WHERE (a, b) >= (1, 1) and (b) < (4)");
        assertInvalidMessage("Multi-column relations can only be applied to clustering columns but was applied to: b",
                             "SELECT * FROM %s WHERE (b) < (4) and (a, b) >= (1, 1)");
        assertInvalidMessage("Multi-column relations can only be applied to clustering columns but was applied to: a",
                             "SELECT * FROM %s WHERE (a, b) >= (1, 1) and a = 1");
    }

    @Test
    public void testInvalidColumnNames()
    {
        createTable("CREATE TABLE %s (a int, b int, c map<int, int>, PRIMARY KEY (a, b))");
        assertInvalidMessage("Undefined column name d", "SELECT * FROM %s WHERE d = 0");
        assertInvalidMessage("Undefined column name d", "SELECT * FROM %s WHERE d IN (0, 1)");
        assertInvalidMessage("Undefined column name d", "SELECT * FROM %s WHERE d > 0 and d <= 2");
        assertInvalidMessage("Undefined column name d", "SELECT * FROM %s WHERE d CONTAINS 0");
        assertInvalidMessage("Undefined column name d", "SELECT * FROM %s WHERE d CONTAINS KEY 0");
        assertInvalidMessage("Undefined column name d", "SELECT * FROM %s WHERE d NOT CONTAINS 0");
        assertInvalidMessage("Undefined column name d", "SELECT * FROM %s WHERE d NOT CONTAINS KEY 0");
        assertInvalidMessage("Undefined column name d", "SELECT a AS d FROM %s WHERE d = 0");
        assertInvalidMessage("Undefined column name d", "SELECT b AS d FROM %s WHERE d IN (0, 1)");
        assertInvalidMessage("Undefined column name d", "SELECT b AS d FROM %s WHERE d NOT IN (0, 1)");
        assertInvalidMessage("Undefined column name d", "SELECT b AS d FROM %s WHERE d > 0 and d <= 2");
        assertInvalidMessage("Undefined column name d", "SELECT c AS d FROM %s WHERE d CONTAINS 0");
        assertInvalidMessage("Undefined column name d", "SELECT c AS d FROM %s WHERE d CONTAINS KEY 0");
        assertInvalidMessage("Undefined column name d", "SELECT c AS d FROM %s WHERE d NOT CONTAINS 0");
        assertInvalidMessage("Undefined column name d", "SELECT c AS d FROM %s WHERE d NOT CONTAINS KEY 0");
        assertInvalidMessage("Undefined column name d", "SELECT d FROM %s WHERE a = 0");
    }

    @Test
    public void testInvalidNonFrozenUDTRelation()
    {
        String type = createType("CREATE TYPE %s (a int)");
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b " + type + ')');
        Object udt = userType("a", 1);

        // All operators
        String msg = "Non-frozen UDT column 'b' (" + type + ") cannot be restricted by any relation";
        assertInvalidMessage(msg, "SELECT * FROM %s WHERE b = ?", udt);
        assertInvalidMessage(msg, "SELECT * FROM %s WHERE b > ?", udt);
        assertInvalidMessage(msg, "SELECT * FROM %s WHERE b < ?", udt);
        assertInvalidMessage(msg, "SELECT * FROM %s WHERE b >= ?", udt);
        assertInvalidMessage(msg, "SELECT * FROM %s WHERE b <= ?", udt);
        assertInvalidMessage(msg, "SELECT * FROM %s WHERE b IN (?)", udt);
        assertInvalidMessage(msg, "SELECT * FROM %s WHERE b NOT IN (?)", udt);
        assertInvalidMessage(msg, "SELECT * FROM %s WHERE b LIKE ?", udt);
        assertInvalidMessage(msg, "SELECT * FROM %s WHERE b != {a: 0}", udt);
        assertInvalidMessage("Unsupported restriction: b IS NOT NULL",
                             "SELECT * FROM %s WHERE b IS NOT NULL", udt);
        assertInvalidMessage("Cannot use CONTAINS on non-collection column b",
                             "SELECT * FROM %s WHERE b CONTAINS ?", udt);
        assertInvalidMessage("Cannot use NOT CONTAINS on non-collection column b",
                             "SELECT * FROM %s WHERE b NOT CONTAINS ?", udt);
    }

    @Test
    public void testInRestrictionWithClusteringColumn()
    {
        createTable("CREATE TABLE %s (key int, c1 int, c2 int, s1 text static, PRIMARY KEY ((key, c1), c2))");

        execute("INSERT INTO %s (key, c1, c2, s1) VALUES ( 10, 11, 1, 's1')");
        execute("INSERT INTO %s (key, c1, c2, s1) VALUES ( 10, 12, 2, 's2')");
        execute("INSERT INTO %s (key, c1, c2, s1) VALUES ( 10, 13, 3, 's3')");
        execute("INSERT INTO %s (key, c1, c2, s1) VALUES ( 10, 13, 4, 's4')");
        execute("INSERT INTO %s (key, c1, c2, s1) VALUES ( 20, 21, 1, 's1')");
        execute("INSERT INTO %s (key, c1, c2, s1) VALUES ( 20, 22, 2, 's2')");
        execute("INSERT INTO %s (key, c1, c2, s1) VALUES ( 20, 22, 3, 's3')");

        assertRows(execute("SELECT * from %s WHERE key = ? AND c1 IN (?, ?)", 10, 21, 13),
                   row(10, 13, 3, "s4"),
                   row(10, 13, 4, "s4"));

        assertRows(execute("SELECT * from %s WHERE key = ? AND c2 IN (?, ?) ALLOW FILTERING", 20, 1, 2),
                   row(20, 22, 2, "s3"),
                   row(20, 21, 1, "s1"));

        assertRows(execute("SELECT * from %s WHERE c1 = ? AND c2 IN (?, ?) ALLOW FILTERING", 13, 2, 3),
                   row(10, 13, 3, "s4"));

        assertRowsIgnoringOrder(execute("SELECT * from %s WHERE c2 IN (?, ?) ALLOW FILTERING", 1, 2),
                                row(10, 11, 1, "s1"),
                                row(10, 12, 2, "s2"),
                                row(20, 21, 1, "s1"),
                                row(20, 22, 2, "s3"));

        assertInvalidMessage("Invalid null value in condition for column c2",
                             "SELECT * from %s WHERE key = 10 AND c2 IN (1, null) ALLOW FILTERING");
    }

    @Test
    public void testInRestrictionsWithAllowFiltering()
    {
        createTable("CREATE TABLE %s (pk1 int, pk2 int, c text, s int static, v int, primary key((pk1, pk2), c))");
        execute("INSERT INTO %s (pk1, pk2, c, s, v) values (?, ?, ?, ?, ?)", 1, 0, "5", 1, 3);
        execute("INSERT INTO %s (pk1, pk2, c, s, v) values (?, ?, ?, ?, ?)", 1, 0, "7", 1, 2);
        execute("INSERT INTO %s (pk1, pk2, c, s, v) values (?, ?, ?, ?, ?)", 1, 1, "7", 1, 3);
        execute("INSERT INTO %s (pk1, pk2, c, s, v) values (?, ?, ?, ?, ?)", 2, 0, "4", 2, 1);
        execute("INSERT INTO %s (pk1, pk2, c, s, v) values (?, ?, ?, ?, ?)", 2, 3, "6", 2, 8);

        // Test filtering on regular columns
        assertRows(execute("SELECT * FROM %s WHERE v IN (?, ?) ALLOW FILTERING", 4, 3),
                   row(1, 0, "5", 1, 3),
                   row(1, 1, "7", 1, 3));

        assertRows(execute("SELECT * FROM %s WHERE v IN ? ALLOW FILTERING", list(4, 3)),
                   row(1, 0, "5", 1, 3),
                   row(1, 1, "7", 1, 3));

        // Test filtering on clustering columns
        assertRows(execute("SELECT * FROM %s WHERE c IN (?, ?, ?) ALLOW FILTERING", "7", "6", "8"),
                   row(2, 3, "6", 2, 8),
                   row(1, 0, "7", 1, 2),
                   row(1, 1, "7", 1, 3));

        assertRows(execute("SELECT * FROM %s WHERE c IN ? ALLOW FILTERING", list("7", "6", "8")),
                   row(2, 3, "6", 2, 8),
                   row(1, 0, "7", 1, 2),
                   row(1, 1, "7", 1, 3));

        // Test filtering on partition keys
        assertRows(execute("SELECT * FROM %s WHERE pk1 IN (?, ?) ALLOW FILTERING", 1, 3),
                   row(1, 0, "5", 1, 3),
                   row(1, 0, "7", 1, 2),
                   row(1, 1, "7", 1, 3));

        assertRows(execute("SELECT * FROM %s WHERE pk1 IN ? ALLOW FILTERING", list(1, 3)),
                   row(1, 0, "5", 1, 3),
                   row(1, 0, "7", 1, 2),
                   row(1, 1, "7", 1, 3));

        // Test filtering on static columns
        assertRows(execute("SELECT * FROM %s WHERE s IN (?, ?) ALLOW FILTERING", 1, 3),
                   row(1, 0, "5", 1, 3),
                   row(1, 0, "7", 1, 2),
                   row(1, 1, "7", 1, 3));

        assertRows(execute("SELECT * FROM %s WHERE s IN ? ALLOW FILTERING", list(1, 3)),
                   row(1, 0, "5", 1, 3),
                   row(1, 0, "7", 1, 2),
                   row(1, 1, "7", 1, 3));
    }

    @Test
    public void testInRestrictionsWithAllowFilteringAndOrdering()
    {
        createTable("CREATE TABLE %s (pk int, c text, v int, primary key(pk, c)) WITH CLUSTERING ORDER BY (c DESC)");
        execute("INSERT INTO %s (pk, c, v) values (?, ?, ?)", 1, "0", 5);
        execute("INSERT INTO %s (pk, c, v) values (?, ?, ?)", 1, "1", 7);
        execute("INSERT INTO %s (pk, c, v) values (?, ?, ?)", 1, "2", 7);
        execute("INSERT INTO %s (pk, c, v) values (?, ?, ?)", 2, "0", 4);
        execute("INSERT INTO %s (pk, c, v) values (?, ?, ?)", 2, "2", 6);

        assertRows(execute("SELECT * FROM %s WHERE pk = ? AND c IN (?, ?, ?) ALLOW FILTERING", 1, "2", "0", "8"),
                   row(1, "2", 7),
                   row(1, "0", 5));

        assertRows(execute("SELECT * FROM %s WHERE pk = ? AND c IN ? ORDER BY c ASC ALLOW FILTERING", 2, list("2", "8", "0")),
                   row(2, "0", 4),
                   row(2, "2", 6));

        assertRows(execute("SELECT * FROM %s WHERE pk IN (?, ?) AND c IN (?, ?, ?) ALLOW FILTERING", 1, 2, "2", "0", "8"),
                   row(1, "2", 7),
                   row(1, "0", 5),
                   row(2, "2", 6),
                   row(2, "0", 4));

        assertRows(execute("SELECT * FROM %s WHERE pk IN ? AND c IN ? ORDER BY c ASC ALLOW FILTERING", list(1, 2), list("2", "8", "0")),
                   row(1, "0", 5),
                   row(2, "0", 4),
                   row(1, "2", 7),
                   row(2, "2", 6));
    }

    @Test
    public void testInRestrictionsWithCountersAndAllowFiltering() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, v counter, primary key (pk))");

        assertEmpty(execute("SELECT * FROM %s WHERE v IN (?, ?) ALLOW FILTERING", 0L, 1L));

        execute("UPDATE %s SET v = v + 1 WHERE pk = 1");
        execute("UPDATE %s SET v = v + 2 WHERE pk = 2");
        execute("UPDATE %s SET v = v + 1 WHERE pk = 3");

        assertRows(execute("SELECT * FROM %s WHERE v IN (?, ?) ALLOW FILTERING", 0L, 1L),
                   row(1, 1L),
                   row(3, 1L));
    }

    @Test
    public void testClusteringSlicesWithNotIn()
    {
        createTable("CREATE TABLE %s (a text, b int, c int, d int, primary key(a, b, c))");
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "key", 1, 4, 1);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "key", 2, 5, 2);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "key", 2, 6, 3);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "key", 2, 7, 4);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "key", 3, 8, 5);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "key", 3, 9, 6);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "key", 4, 1, 7);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "key", 4, 2, 8);

        // restrict first clustering column by NOT IN
        assertRows(execute("select * from %s where a = ? and b not in ?", "key", list(2, 4, 5)),
                   row("key", 1, 4, 1),
                   row("key", 3, 8, 5),
                   row("key", 3, 9, 6));
        assertRows(execute("select * from %s where a = ? and b not in (?, ?, ?)", "key", 2, 4, 5),
                   row("key", 1, 4, 1),
                   row("key", 3, 8, 5),
                   row("key", 3, 9, 6));

        // use different order of items in NOT IN list:
        assertRows(execute("select * from %s where a = ? and b not in (?, ?, ?)", "key", 5, 2, 4),
                   row("key", 1, 4, 1),
                   row("key", 3, 8, 5),
                   row("key", 3, 9, 6));
        assertRows(execute("select * from %s where a = ? and b not in (?, ?, ?)", "key", 5, 4, 2),
                   row("key", 1, 4, 1),
                   row("key", 3, 8, 5),
                   row("key", 3, 9, 6));

        // restrict last clustering column by NOT IN
        assertRows(execute("select * from %s where a = ? and b = ? and c not in ?", "key", 2, list(5, 6)),
                   row("key", 2, 7, 4));
        assertRows(execute("select * from %s where a = ? and b = ? and c not in (?, ?)", "key", 2, 5, 6),
                   row("key", 2, 7, 4));

        // empty NOT IN should have no effect:
        assertRows(execute("select * from %s where a = ? and b = ? and c not in ?", "key", 2, list()),
                   row("key", 2, 5, 2),
                   row("key", 2, 6, 3),
                   row("key", 2, 7, 4));
        assertRows(execute("select * from %s where a = ? and b = ? and c not in ()", "key", 2),
                   row("key", 2, 5, 2),
                   row("key", 2, 6, 3),
                   row("key", 2, 7, 4));

        // NOT IN value that doesn't match any data should have no effect:
        assertRows(execute("select * from %s where a = ? and b = ? and c not in (?)", "key", 2, 0),
                   row("key", 2, 5, 2),
                   row("key", 2, 6, 3),
                   row("key", 2, 7, 4));

        // Duplicate NOT IN values:
        assertRows(execute("select * from %s where a = ? and b = ? and c not in (?, ?)", "key", 2, 5, 5),
                   row("key", 2, 6, 3),
                   row("key", 2, 7, 4));

        // mix NOT IN and '<' and '<=' comparison on the same column
        assertRows(execute("select * from %s where a = ? and b not in ? and b < ?", "key", list(2, 5), 1)); // empty
        assertRows(execute("select * from %s where a = ? and b not in ? and b < ?", "key", list(2, 5), 3),
                   row("key", 1, 4, 1));
        assertRows(execute("select * from %s where a = ? and b not in ? and b <= ?", "key", list(2), 2),
                   row("key", 1, 4, 1));
        assertRows(execute("select * from %s where a = ? and b not in ? and b <= ?", "key", list(2), 3),
                   row("key", 1, 4, 1),
                   row("key", 3, 8, 5),
                   row("key", 3, 9, 6));
        assertRows(execute("select * from %s where a = ? and b not in ? and b <= ?", "key", list(2), 10),
                   row("key", 1, 4, 1),
                   row("key", 3, 8, 5),
                   row("key", 3, 9, 6),
                   row("key", 4, 1, 7),
                   row("key", 4, 2, 8));

        // mix NOT IN and '>' and '>=' comparison on the same column
        assertRows(execute("select * from %s where a = ? and b not in ? and b > ?", "key", list(2), 1),
                   row("key", 3, 8, 5),
                   row("key", 3, 9, 6),
                   row("key", 4, 1, 7),
                   row("key", 4, 2, 8));
        assertRows(execute("select * from %s where a = ? and b not in ? and b > ?", "key", list(2), 2),
                   row("key", 3, 8, 5),
                   row("key", 3, 9, 6),
                   row("key", 4, 1, 7),
                   row("key", 4, 2, 8));
        assertRows(execute("select * from %s where a = ? and b not in ? and b >= ?", "key", list(2), 2),
                   row("key", 3, 8, 5),
                   row("key", 3, 9, 6),
                   row("key", 4, 1, 7),
                   row("key", 4, 2, 8));
        assertRows(execute("select * from %s where a = ? and b not in ? and b >= ?", "key", list(2), 4),
                   row("key", 4, 1, 7),
                   row("key", 4, 2, 8));
        assertRows(execute("select * from %s where a = ? and b not in ? and b >= ?", "key", list(2), 0),
                   row("key", 1, 4, 1),
                   row("key", 3, 8, 5),
                   row("key", 3, 9, 6),
                   row("key", 4, 1, 7),
                   row("key", 4, 2, 8));

        // mix NOT IN and range slice
        assertRows(execute("select * from %s where a = ? and b not in ? and b > ? and b < ?", "key", list(2), 1, 4),
                   row("key", 3, 8, 5),
                   row("key", 3, 9, 6));
        assertRows(execute("select * from %s where a = ? and b not in ? and b >= ? and b < ?", "key", list(2), 1, 4),
                   row("key", 1, 4, 1),
                   row("key", 3, 8, 5),
                   row("key", 3, 9, 6));
        assertRows(execute("select * from %s where a = ? and b not in ? and b > ? and b <= ?", "key", list(2), 1, 4),
                   row("key", 3, 8, 5),
                   row("key", 3, 9, 6),
                   row("key", 4, 1, 7),
                   row("key", 4, 2, 8));
        assertRows(execute("select * from %s where a = ? and b not in ? and b >= ? and b <= ?", "key", list(2), 1, 4),
                   row("key", 1, 4, 1),
                   row("key", 3, 8, 5),
                   row("key", 3, 9, 6),
                   row("key", 4, 1, 7),
                   row("key", 4, 2, 8));

        // Collision between a slice bound and NOT IN value:
        assertRows(execute("select * from %s where a = ? and b not in ? and b < ?", "key", list(2), 2),
                   row("key", 1, 4, 1));
        assertRows(execute("select * from %s where a = ? and b not in ? and b > ?", "key", list(2), 2),
                   row("key", 3, 8, 5),
                   row("key", 3, 9, 6),
                   row("key", 4, 1, 7),
                   row("key", 4, 2, 8));

        // NOT IN value outside of the slice range:
        assertRows(execute("select * from %s where a = ? and b not in ? and b > ? and b < ?", "key", list(0), 2, 4),
                   row("key", 3, 8, 5),
                   row("key", 3, 9, 6));
        assertRows(execute("select * from %s where a = ? and b not in ? and b > ? and b < ?", "key", list(10), 2, 4),
                   row("key", 3, 8, 5),
                   row("key", 3, 9, 6));

        // multiple NOT IN on the same column, use different ways of passing a list
        assertRows(execute("select * from %s where a = ? and b not in ? and b not in ?", "key", list(1, 2), list(4)),
                   row("key", 3, 8, 5),
                   row("key", 3, 9, 6));
        assertRows(execute("select * from %s where a = ? and b not in (?, ?) and b not in (?)", "key", 1, 2, 4),
                   row("key", 3, 8, 5),
                   row("key", 3, 9, 6));
        assertRows(execute("select * from %s where a = ? and b not in (?, ?) and b not in ?", "key", 1, 2, list(4)),
                   row("key", 3, 8, 5),
                   row("key", 3, 9, 6));

        // mix IN and NOT IN
        assertRows(execute("select * from %s where a = ? and b in ? and c not in ?", "key", list(2, 3), list(5, 6, 9)),
                   row("key", 2, 7, 4),
                   row("key", 3, 8, 5));

    }

    @Test
    public void testClusteringSlicesWithNotInAndReverseOrdering()
    {
        createTable("CREATE TABLE %s (a text, b int, c int, d int, primary key(a, b, c)) with clustering order by (b desc, c desc)");
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "key", 1, 4, 1);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "key", 2, 5, 2);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "key", 2, 6, 3);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "key", 2, 7, 4);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "key", 3, 8, 5);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "key", 3, 9, 6);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "key", 4, 1, 7);
        execute("insert into %s (a, b, c, d) values (?, ?, ?, ?)", "key", 4, 2, 8);

        // restrict first clustering column by NOT IN
        assertRows(execute("select * from %s where a = ? and b not in ?", "key", list(2, 4, 5)),
                   row("key", 3, 9, 6),
                   row("key", 3, 8, 5),
                   row("key", 1, 4, 1));
        assertRows(execute("select * from %s where a = ? and b not in (?, ?, ?)", "key", 2, 4, 5),
                   row("key", 3, 9, 6),
                   row("key", 3, 8, 5),
                   row("key", 1, 4, 1));

        // restrict last clustering column by NOT IN
        assertRows(execute("select * from %s where a = ? and b = ? and c not in ?", "key", 2, list(5, 6)),
                   row("key", 2, 7, 4));
        assertRows(execute("select * from %s where a = ? and b = ? and c not in (?, ?)", "key", 2, 5, 6),
                   row("key", 2, 7, 4));

        // empty NOT IN should have no effect:
        assertRows(execute("select * from %s where a = ? and b = ? and c not in ?", "key", 2, list()),
                   row("key", 2, 7, 4),
                   row("key", 2, 6, 3),
                   row("key", 2, 5, 2));
        assertRows(execute("select * from %s where a = ? and b = ? and c not in ()", "key", 2),
                   row("key", 2, 7, 4),
                   row("key", 2, 6, 3),
                   row("key", 2, 5, 2));

        // NOT IN value that doesn't match any data should have no effect:
        assertRows(execute("select * from %s where a = ? and b = ? and c not in (?)", "key", 2, 0),
                   row("key", 2, 7, 4),
                   row("key", 2, 6, 3),
                   row("key", 2, 5, 2));

        // Duplicate NOT IN values:
        assertRows(execute("select * from %s where a = ? and b = ? and c not in (?, ?)", "key", 2, 5, 5),
                   row("key", 2, 7, 4),
                   row("key", 2, 6, 3));

        // mix NOT IN and '<' and '<=' comparison on the same column
        assertRows(execute("select * from %s where a = ? and b not in ? and b < ?", "key", list(2, 5), 1)); // empty
        assertRows(execute("select * from %s where a = ? and b not in ? and b < ?", "key", list(2, 5), 3),
                   row("key", 1, 4, 1));
        assertRows(execute("select * from %s where a = ? and b not in ? and b <= ?", "key", list(2), 2),
                   row("key", 1, 4, 1));
        assertRows(execute("select * from %s where a = ? and b not in ? and b <= ?", "key", list(2), 3),
                   row("key", 3, 9, 6),
                   row("key", 3, 8, 5),
                   row("key", 1, 4, 1));
        assertRows(execute("select * from %s where a = ? and b not in ? and b <= ?", "key", list(2), 10),
                   row("key", 4, 2, 8),
                   row("key", 4, 1, 7),
                   row("key", 3, 9, 6),
                   row("key", 3, 8, 5),
                   row("key", 1, 4, 1));

        // mix NOT IN and '>' and '>=' comparison on the same column
        assertRows(execute("select * from %s where a = ? and b not in ? and b > ?", "key", list(2), 1),
                   row("key", 4, 2, 8),
                   row("key", 4, 1, 7),
                   row("key", 3, 9, 6),
                   row("key", 3, 8, 5));
        assertRows(execute("select * from %s where a = ? and b not in ? and b > ?", "key", list(2), 2),
                   row("key", 4, 2, 8),
                   row("key", 4, 1, 7),
                   row("key", 3, 9, 6),
                   row("key", 3, 8, 5));
        assertRows(execute("select * from %s where a = ? and b not in ? and b >= ?", "key", list(2), 2),
                   row("key", 4, 2, 8),
                   row("key", 4, 1, 7),
                   row("key", 3, 9, 6),
                   row("key", 3, 8, 5));
        assertRows(execute("select * from %s where a = ? and b not in ? and b >= ?", "key", list(2), 4),
                   row("key", 4, 2, 8),
                   row("key", 4, 1, 7));
        assertRows(execute("select * from %s where a = ? and b not in ? and b >= ?", "key", list(2), 0),
                   row("key", 4, 2, 8),
                   row("key", 4, 1, 7),
                   row("key", 3, 9, 6),
                   row("key", 3, 8, 5),
                   row("key", 1, 4, 1));

        // mix NOT IN and range slice
        assertRows(execute("select * from %s where a = ? and b not in ? and b > ? and b < ?", "key", list(2), 1, 4),
                   row("key", 3, 9, 6),
                   row("key", 3, 8, 5));
        assertRows(execute("select * from %s where a = ? and b not in ? and b >= ? and b < ?", "key", list(2), 1, 4),
                   row("key", 3, 9, 6),
                   row("key", 3, 8, 5),
                   row("key", 1, 4, 1));
        assertRows(execute("select * from %s where a = ? and b not in ? and b > ? and b <= ?", "key", list(2), 1, 4),
                   row("key", 4, 2, 8),
                   row("key", 4, 1, 7),
                   row("key", 3, 9, 6),
                   row("key", 3, 8, 5));
        assertRows(execute("select * from %s where a = ? and b not in ? and b >= ? and b <= ?", "key", list(2), 1, 4),
                   row("key", 4, 2, 8),
                   row("key", 4, 1, 7),
                   row("key", 3, 9, 6),
                   row("key", 3, 8, 5),
                   row("key", 1, 4, 1));

        // Collision between a slice bound and NOT IN value:
        assertRows(execute("select * from %s where a = ? and b not in ? and b < ?", "key", list(2), 2),
                   row("key", 1, 4, 1));
        assertRows(execute("select * from %s where a = ? and b not in ? and b > ?", "key", list(2), 2),
                   row("key", 4, 2, 8),
                   row("key", 4, 1, 7),
                   row("key", 3, 9, 6),
                   row("key", 3, 8, 5));

        // NOT IN value outside of the slice range:
        assertRows(execute("select * from %s where a = ? and b not in ? and b > ? and b < ?", "key", list(0), 2, 4),
                   row("key", 3, 9, 6),
                   row("key", 3, 8, 5));
        assertRows(execute("select * from %s where a = ? and b not in ? and b > ? and b < ?", "key", list(10), 2, 4),
                   row("key", 3, 9, 6),
                   row("key", 3, 8, 5));

        // multiple NOT IN on the same column, use different ways of passing a list
        assertRows(execute("select * from %s where a = ? and b not in ? and b not in ?", "key", list(1, 2), list(4)),
                   row("key", 3, 9, 6),
                   row("key", 3, 8, 5));
        assertRows(execute("select * from %s where a = ? and b not in (?, ?) and b not in (?)", "key", 1, 2, 4),
                   row("key", 3, 9, 6),
                   row("key", 3, 8, 5));
        assertRows(execute("select * from %s where a = ? and b not in (?, ?) and b not in ?", "key", 1, 2, list(4)),
                   row("key", 3, 9, 6),
                   row("key", 3, 8, 5));

        // mix IN and NOT IN
        assertRows(execute("select * from %s where a = ? and b in ? and c not in ?", "key", list(2, 3), list(5, 6, 9)),
                   row("key", 3, 8, 5),
                   row("key", 2, 7, 4));
    }

    @Test
    public void testNotInRestrictionsWithAllowFiltering()
    {
        createTable("CREATE TABLE %s (pk int, c int, v int, primary key(pk, c))");
        execute("insert into %s (pk, c, v) values (?, ?, ?)", 1, 1, 1);
        execute("insert into %s (pk, c, v) values (?, ?, ?)", 1, 2, 2);
        execute("insert into %s (pk, c, v) values (?, ?, ?)", 1, 3, 3);
        execute("insert into %s (pk, c, v) values (?, ?, ?)", 1, 4, 4);
        execute("insert into %s (pk, c, v) values (?, ?, ?)", 1, 5, 5);

        // empty NOT IN set
        assertRows(execute("select * from %s where pk = ? and v not in ? allow filtering", 1, list()),
                   row(1, 1, 1),
                   row(1, 2, 2),
                   row(1, 3, 3),
                   row(1, 4, 4),
                   row(1, 5, 5));
        assertRows(execute("select * from %s where pk = ? and v not in () allow filtering", 1),
                   row(1, 1, 1),
                   row(1, 2, 2),
                   row(1, 3, 3),
                   row(1, 4, 4),
                   row(1, 5, 5));

        // NOT IN with values that don't match any data
        assertRows(execute("select * from %s where pk = ? and v not in (?, ?) allow filtering", 1, -6, 20),
                   row(1, 1, 1),
                   row(1, 2, 2),
                   row(1, 3, 3),
                   row(1, 4, 4),
                   row(1, 5, 5));

        // NOT IN that excludes a few values
        assertRows(execute("select * from %s where pk = ? and v not in ? allow filtering", 1, list(2, 3)),
                   row(1, 1, 1),
                   row(1, 4, 4),
                   row(1, 5, 5));
        assertRows(execute("select * from %s where pk = ? and v not in (?, ?) allow filtering", 1, 2, 3),
                   row(1, 1, 1),
                   row(1, 4, 4),
                   row(1, 5, 5));

        // NOT IN with one-sided slice filters:
        assertRows(execute("select * from %s where pk = ? and v not in ? and v < ? allow filtering", 1, list(2, 3), 5),
                   row(1, 1, 1),
                   row(1, 4, 4));
        assertRows(execute("select * from %s where pk = ? and v not in ? and v < ? allow filtering", 1, list(2, 3, 10), 5),
                   row(1, 1, 1),
                   row(1, 4, 4));
        assertRows(execute("select * from %s where pk = ? and v not in ? and v <= ? allow filtering", 1, list(2, 3), 5),
                   row(1, 1, 1),
                   row(1, 4, 4),
                   row(1, 5, 5));
        assertRows(execute("select * from %s where pk = ? and v not in ? and v <= ? allow filtering", 1, list(2, 3, 10), 5),
                   row(1, 1, 1),
                   row(1, 4, 4),
                   row(1, 5, 5));
        assertRows(execute("select * from %s where pk = ? and v not in ? and v > ? allow filtering", 1, list(2, 3), 1),
                   row(1, 4, 4),
                   row(1, 5, 5));
        assertRows(execute("select * from %s where pk = ? and v not in ? and v > ? allow filtering", 1, list(0, 2, 3), 1),
                   row(1, 4, 4),
                   row(1, 5, 5));
        assertRows(execute("select * from %s where pk = ? and v not in ? and v >= ? allow filtering", 1, list(2, 3), 1),
                   row(1, 1, 1),
                   row(1, 4, 4),
                   row(1, 5, 5));
        assertRows(execute("select * from %s where pk = ? and v not in ? and v >= ? allow filtering", 1, list(0, 2, 3), 1),
                   row(1, 1, 1),
                   row(1, 4, 4),
                   row(1, 5, 5));

        // NOT IN with range filters:
        assertRows(execute("select * from %s where pk = ? and v not in ? and v > ? and v < ? allow filtering", 1, list(2, 3), 1, 4)); // empty
        assertRows(execute("select * from %s where pk = ? and v not in ? and v > ? and v < ? allow filtering", 1, list(2, 3), 1, 4)); // empty
        assertRows(execute("select * from %s where pk = ? and v not in ? and v > ? and v < ? allow filtering", 1, list(2, 3), 0, 5),
                   row(1, 1, 1),
                   row(1, 4, 4));
        assertRows(execute("select * from %s where pk = ? and v not in ? and v >= ? and v < ? allow filtering", 1, list(2, 3), 1, 4),
                   row(1, 1, 1));
        assertRows(execute("select * from %s where pk = ? and v not in ? and v > ? and v <= ? allow filtering", 1, list(2, 3), 1, 4),
                   row(1, 4, 4));
        assertRows(execute("select * from %s where pk = ? and v not in ? and v >= ? and v <= ? allow filtering", 1, list(2, 3), 1, 4),
                   row(1, 1, 1),
                   row(1, 4, 4));

        // more than one NOT IN clause
        assertRows(execute("select * from %s where pk = ? and v not in ? and v not in ? allow filtering", 1, list(2), list(3)),
                   row(1, 1, 1),
                   row(1, 4, 4),
                   row(1, 5, 5));

    }

    @Test
    public void testNotInRestrictionsWithOrAndAllowFiltering()
    {
        createTable("CREATE TABLE %s (pk int, c int, v int, PRIMARY KEY(pk, c))");
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", 1, 1, 1);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", 1, 2, 2);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", 1, 3, 3);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", 1, 4, 4);
        execute("INSERT INTO %s (pk, c, v) VALUES (?, ?, ?)", 1, 5, 5);

        assertRows(execute("SELECT * FROM %s WHERE v = ? OR v not in ? ALLOW FILTERING", 0, list()),
                   row(1, 1, 1),
                   row(1, 2, 2),
                   row(1, 3, 3),
                   row(1, 4, 4),
                   row(1, 5, 5));

        assertRows(execute("SELECT * FROM %s WHERE v NOT IN ? OR v NOT IN ? ALLOW FILTERING", list(), list(1, 2)),
                   row(1, 1, 1),
                   row(1, 2, 2),
                   row(1, 3, 3),
                   row(1, 4, 4),
                   row(1, 5, 5));

        assertRows(execute("SELECT * FROM %s WHERE v = ? OR v NOT IN ? ALLOW FILTERING", 0, list(1, 2, 3)),
                   row(1, 4, 4),
                   row(1, 5, 5));

        assertRows(execute("SELECT * FROM %s WHERE v = ? OR v NOT IN ? ALLOW FILTERING", 1, list(1, 2, 3)),
                   row(1, 1, 1),
                   row(1, 4, 4),
                   row(1, 5, 5));

        // Multiple NOT IN:
        assertRows(execute("SELECT * FROM %s WHERE v NOT IN ? OR v NOT IN ? ALLOW FILTERING", list(1, 2), list(3, 4)),
                   row(1, 1, 1),
                   row(1, 2, 2),
                   row(1, 3, 3),
                   row(1, 4, 4),
                   row(1, 5, 5));
        assertRows(execute("SELECT * FROM %s WHERE v NOT IN ? OR v NOT IN ? ALLOW FILTERING", list(1, 2, 3, 4), list()),
                   row(1, 1, 1),
                   row(1, 2, 2),
                   row(1, 3, 3),
                   row(1, 4, 4),
                   row(1, 5, 5));


        // Mixed IN / NOT IN with AND and OR:
        assertRows(execute("SELECT * FROM %s WHERE v IN ? OR v NOT IN ? AND v NOT IN ? ALLOW FILTERING", list(1), list(1, 2, 3), list(2, 3, 4)),
                   row(1, 1, 1),
                   row(1, 5, 5));

        assertRows(execute("SELECT * FROM %s WHERE v NOT IN ? AND (v IN ? OR v NOT IN ?) ALLOW FILTERING", list(), list(3), list(5)),
                   row(1, 1, 1),
                   row(1, 2, 2),
                   row(1, 3, 3),
                   row(1, 4, 4));

        assertRows(execute("SELECT * FROM %s WHERE v NOT IN ? AND (v IN ? OR v NOT IN ?) ALLOW FILTERING", list(1, 2), list(3), list(5)),
                   row(1, 3, 3),
                   row(1, 4, 4));

        assertRows(execute("SELECT * FROM %s WHERE v IN ? AND (v IN ? OR v NOT IN ?) ALLOW FILTERING", list(1, 3), list(5), list(3)),
                   row(1, 1, 1));

        assertRows(execute("SELECT * FROM %s WHERE v IN ? AND (v IN ? OR v NOT IN ?) ALLOW FILTERING", list(1, 3), list(5), list()),
                   row(1, 1, 1),
                   row(1, 3, 3));
    }


    @Test
    public void testNonEqualsRelationWithFiltering()
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 0, 0);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 1, 1);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 2, 2);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, 3, 3);

        assertRows(execute("SELECT a, b FROM %s WHERE a = ? AND c != ? ALLOW FILTERING", 0, 0),
                   row(0, 1),
                   row(0, 2),
                   row(0, 3)
        );
        assertRows(execute("SELECT a, b FROM %s WHERE a = ? AND c != ? ALLOW FILTERING", 0, 1),
                   row(0, 0),
                   row(0, 2),
                   row(0, 3)
        );
        assertRows(execute("SELECT a, b FROM %s WHERE a = ? AND c != ? ALLOW FILTERING", 0, -1),
                   row(0, 0),
                   row(0, 1),
                   row(0, 2),
                   row(0, 3)
        );
        assertRows(execute("SELECT a, b FROM %s WHERE a = ? AND c != ? ALLOW FILTERING", 0, 5),
                   row(0, 0),
                   row(0, 1),
                   row(0, 2),
                   row(0, 3)
        );
        assertRows(execute("SELECT a, b FROM %s WHERE a = ? AND c != ? AND c != ? ALLOW FILTERING", 0, 1, 2),
                   row(0, 0),
                   row(0, 3)
        );
        assertRows(execute("SELECT a, b FROM %s WHERE a = ? AND c != ? AND c < ? ALLOW FILTERING", 0, 1, 2),
                   row(0, 0)
        );
        assertRows(execute("SELECT a, b FROM %s WHERE a = ? AND c != ? AND c <= ? ALLOW FILTERING", 0, 1, 2),
                   row(0, 0),
                   row(0, 2)
        );
        assertRows(execute("SELECT a, b FROM %s WHERE a = ? AND c != ? AND c > ? ALLOW FILTERING", 0, 2, 0),
                   row(0, 1),
                   row(0, 3)
        );
        assertRows(execute("SELECT a, b FROM %s WHERE a = ? AND c != ? AND c >= ? ALLOW FILTERING", 0, 2, 1),
                   row(0, 1),
                   row(0, 3)
        );
    }

}
