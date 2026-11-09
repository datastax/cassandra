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

package org.apache.cassandra.db.memtable;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Deleting a whole non-frozen collection (or non-frozen UDT) column produces a row whose only content is a
 * complex column deletion: no cells, no row deletion and, for an UPDATE-written row, no primary key liveness.
 * Such a deletion must survive a memtable flush, otherwise the data in older SSTables is resurrected.
 * <p>
 * The default memtable ({@link TrieMemtable}) used to lose the deletion: the partition was counted as having no rows
 * and no tombstones, the flush skipped it and the old value came back. For a static column the deletion was not even
 * visible in the memtable before the flush.
 */
@RunWith(Parameterized.class)
public class ComplexDeletionFlushTest extends CQLTester
{
    @Parameterized.Parameter
    public String memtable;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object> parameters()
    {
        // "default" means no memtable option on the table, i.e. TrieMemtable
        return Arrays.asList("default", "trie", "trie_stage1", "trie_stage2", "trie_stage3", "skiplist");
    }

    private String withMemtable(String createTable)
    {
        if ("default".equals(memtable))
            return createTable;
        return createTable + " WITH memtable = '" + memtable + "'";
    }

    private void flushAndCheckSSTables(int expected)
    {
        flush();
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        assertEquals("live sstables after flush", expected, cfs.getLiveSSTables().size());
        for (SSTableReader sstable : cfs.getLiveSSTables())
            assertTrue("Data.db must not be empty: " + sstable, sstable.onDiskLength() > 0);
    }

    @Test
    public void testSetInsert() throws Throwable
    {
        createTable(withMemtable("CREATE TABLE %s (k int PRIMARY KEY, s set<text>)"));
        disableCompaction();
        execute("INSERT INTO %s (k, s) VALUES (1, {'a', 'b'})");
        flushAndCheckSSTables(1);

        execute("DELETE s FROM %s WHERE k = 1");
        // the row survives through its primary key liveness, the column is gone
        assertRows(execute("SELECT k, s FROM %s WHERE k = 1"), row(1, null));
        flushAndCheckSSTables(2);
        assertRows(execute("SELECT k, s FROM %s WHERE k = 1"), row(1, null));
    }

    @Test
    public void testSetUpdate() throws Throwable
    {
        createTable(withMemtable("CREATE TABLE %s (k int PRIMARY KEY, s set<text>)"));
        disableCompaction();
        // no primary key liveness: deleting the column removes the row
        execute("UPDATE %s SET s = {'a', 'b'} WHERE k = 1");
        execute("UPDATE %s SET s = {'c'} WHERE k = 2");
        flushAndCheckSSTables(1);

        execute("DELETE s FROM %s WHERE k = 1");
        execute("DELETE s FROM %s WHERE k = 2");
        assertEmpty(execute("SELECT * FROM %s WHERE k = 1"));
        flushAndCheckSSTables(2);
        assertEmpty(execute("SELECT * FROM %s WHERE k = 1"));
        assertEmpty(execute("SELECT * FROM %s"));
    }

    @Test
    public void testList() throws Throwable
    {
        createTable(withMemtable("CREATE TABLE %s (k int PRIMARY KEY, l list<text>)"));
        disableCompaction();
        execute("INSERT INTO %s (k, l) VALUES (1, ['a', 'b'])");
        flushAndCheckSSTables(1);

        execute("DELETE l FROM %s WHERE k = 1");
        assertRows(execute("SELECT k, l FROM %s WHERE k = 1"), row(1, null));
        flushAndCheckSSTables(2);
        assertRows(execute("SELECT k, l FROM %s WHERE k = 1"), row(1, null));
    }

    @Test
    public void testMap() throws Throwable
    {
        createTable(withMemtable("CREATE TABLE %s (k int PRIMARY KEY, m map<text, text>)"));
        disableCompaction();
        execute("INSERT INTO %s (k, m) VALUES (1, {'a': 'x', 'b': 'y'})");
        flushAndCheckSSTables(1);

        execute("DELETE m FROM %s WHERE k = 1");
        assertRows(execute("SELECT k, m FROM %s WHERE k = 1"), row(1, null));
        flushAndCheckSSTables(2);
        assertRows(execute("SELECT k, m FROM %s WHERE k = 1"), row(1, null));
    }

    @Test
    public void testNonFrozenUdt() throws Throwable
    {
        String type = createType("CREATE TYPE %s (a text, b int)");
        createTable(withMemtable("CREATE TABLE %s (k int PRIMARY KEY, u " + type + ')'));
        disableCompaction();
        execute("INSERT INTO %s (k, u) VALUES (1, {a: 'x', b: 1})");
        flushAndCheckSSTables(1);

        execute("DELETE u FROM %s WHERE k = 1");
        assertRows(execute("SELECT k, u FROM %s WHERE k = 1"), row(1, null));
        flushAndCheckSSTables(2);
        assertRows(execute("SELECT k, u FROM %s WHERE k = 1"), row(1, null));
    }

    @Test
    public void testOtherRegularColumns() throws Throwable
    {
        createTable(withMemtable("CREATE TABLE %s (k int, c int, v int, s set<text>, PRIMARY KEY (k, c))"));
        disableCompaction();
        execute("UPDATE %s SET v = 10, s = {'a', 'b'} WHERE k = 1 AND c = 1");
        execute("UPDATE %s SET v = 20, s = {'c'} WHERE k = 1 AND c = 2");
        flushAndCheckSSTables(1);

        execute("DELETE s FROM %s WHERE k = 1 AND c = 1");
        assertRows(execute("SELECT c, v, s FROM %s WHERE k = 1"), row(1, 10, null), row(2, 20, set("c")));
        flushAndCheckSSTables(2);
        assertRows(execute("SELECT c, v, s FROM %s WHERE k = 1"), row(1, 10, null), row(2, 20, set("c")));
    }

    @Test
    public void testStaticCollection() throws Throwable
    {
        createTable(withMemtable("CREATE TABLE %s (k int, c int, v int, st set<text> static, PRIMARY KEY (k, c))"));
        disableCompaction();
        execute("UPDATE %s SET st = {'a', 'b'} WHERE k = 1");
        execute("INSERT INTO %s (k, c, v) VALUES (1, 1, 10)");
        flushAndCheckSSTables(1);

        execute("DELETE st FROM %s WHERE k = 1");
        assertRows(execute("SELECT k, c, v, st FROM %s WHERE k = 1"), row(1, 1, 10, null));
        flushAndCheckSSTables(2);
        assertRows(execute("SELECT k, c, v, st FROM %s WHERE k = 1"), row(1, 1, 10, null));
    }

    @Test
    public void testStaticCollectionWithoutRows() throws Throwable
    {
        createTable(withMemtable("CREATE TABLE %s (k int, c int, v int, st set<text> static, PRIMARY KEY (k, c))"));
        disableCompaction();
        execute("UPDATE %s SET st = {'a', 'b'} WHERE k = 1");
        flushAndCheckSSTables(1);

        // the partition in the new memtable holds only the deletion in its static row
        execute("DELETE st FROM %s WHERE k = 1");
        assertEmpty(execute("SELECT * FROM %s WHERE k = 1"));
        flushAndCheckSSTables(2);
        assertEmpty(execute("SELECT * FROM %s WHERE k = 1"));
    }

    @Test
    public void testStaticCollectionSameMemtable() throws Throwable
    {
        createTable(withMemtable("CREATE TABLE %s (k int, c int, v int, st set<text> static, PRIMARY KEY (k, c))"));
        disableCompaction();
        execute("UPDATE %s SET st = {'a', 'b'} WHERE k = 1");
        execute("INSERT INTO %s (k, c, v) VALUES (1, 1, 10)");
        execute("DELETE st FROM %s WHERE k = 1");
        flushAndCheckSSTables(1);

        assertRows(execute("SELECT k, c, v, st FROM %s WHERE k = 1"), row(1, 1, 10, null));
    }
}
