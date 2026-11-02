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

package org.apache.cassandra.cql3.validation.operations;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.partitions.TriePartitionUpdate;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * A conditional update whose only effect is to delete a collection, regular or static, on a trie memtable table. Paxos
 * does not commit a proposal that is empty, and such an update holds the collection's deletion and nothing else: no
 * row liveness and no row deletion. The proposal is a trie partition update only where the storage compatibility mode
 * lets the table's updates be (NONE), so the test runs in that mode whatever the mode of the run. The statements go
 * through the native protocol, as executing them internally applies them without Paxos.
 */
public class UpdateIfConditionCollectionDeletionTest extends CQLTester
{
    private static Config.PaxosVariant initialVariant;
    private static StorageCompatibilityMode initialMode;

    @BeforeClass
    public static void setUpNetwork()
    {
        requireNetwork();
        initialVariant = Paxos.getPaxosVariant();
        initialMode = DatabaseDescriptor.getStorageCompatibilityMode();
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.NONE);
    }

    @AfterClass
    public static void restoreStorageCompatibilityMode()
    {
        DatabaseDescriptor.setStorageCompatibilityMode(initialMode);
    }

    @After
    public void restorePaxosVariant()
    {
        Paxos.setPaxosVariant(initialVariant);
    }

    @Test
    public void testSetCollectionToNull() throws Throwable
    {
        assertClearsRegularCollection("UPDATE %s SET m = null WHERE k = ? AND c = 0 IF v = 1");
    }

    @Test
    public void testSetCollectionToEmpty() throws Throwable
    {
        assertClearsRegularCollection("UPDATE %s SET m = {} WHERE k = ? AND c = 0 IF v = 1");
    }

    @Test
    public void testDeleteCollection() throws Throwable
    {
        assertClearsRegularCollection("DELETE m FROM %s WHERE k = ? AND c = 0 IF v = 1");
    }

    /**
     * With the collection in an sstable, the deletion is all the memtable holds of the partition, and has to be
     * flushed to delete it.
     */
    @Test
    public void testSetFlushedCollectionToNull() throws Throwable
    {
        assertClears("UPDATE %s SET m = null WHERE k = ? AND c = 0 IF v = 1", row(null, map(1, 1)), true);
    }

    @Test
    public void testSetStaticCollectionToNull() throws Throwable
    {
        assertClearsStaticCollection("UPDATE %s SET sm = null WHERE k = ? IF sv = 1");
    }

    @Test
    public void testSetStaticCollectionToEmpty() throws Throwable
    {
        assertClearsStaticCollection("UPDATE %s SET sm = {} WHERE k = ? IF sv = 1");
    }

    @Test
    public void testDeleteStaticCollection() throws Throwable
    {
        assertClearsStaticCollection("DELETE sm FROM %s WHERE k = ? IF sv = 1");
    }

    private void assertClearsRegularCollection(String statement) throws Throwable
    {
        assertClears(statement, row(null, map(1, 1)), false);
    }

    private void assertClearsStaticCollection(String statement) throws Throwable
    {
        assertClears(statement, row(map(1, 1), null), false);
    }

    /**
     * Applies the statement to a partition of its own with each Paxos variant, and checks the regular and the static
     * collection of the partition's row afterwards. If {@code flushed}, the partition's data is flushed before the
     * statement, and the statement's after it, and the row is checked again.
     */
    private void assertClears(String statement, Object[] expected, boolean flushed) throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, m map<int, int>, sv int static, sm map<int, int> static, " +
                    "PRIMARY KEY (k, c)) WITH memtable = 'trie'");
        assertSame(TriePartitionUpdate.FACTORY, currentTableMetadata().partitionUpdateFactory());
        Config.PaxosVariant[] variants = { Config.PaxosVariant.v1, Config.PaxosVariant.v2 };
        for (int key = 0; key < variants.length; ++key)
        {
            execute("INSERT INTO %s (k, c, v, m, sv, sm) VALUES (?, 0, 1, {1: 1}, 1, {1: 1})", key);
            if (flushed)
                flush();
            Paxos.setPaxosVariant(variants[key]);
            String shape = variants[key] + ": " + statement;
            assertTrue(shape, executeNet(statement, key).wasApplied());
            try
            {
                assertRows(execute("SELECT m, sm FROM %s WHERE k = ? AND c = 0", key), expected);
                if (flushed)
                {
                    flush();
                    assertRows(execute("SELECT m, sm FROM %s WHERE k = ? AND c = 0", key), expected);
                }
            }
            catch (AssertionError e)
            {
                throw new AssertionError(shape, e);
            }
        }
    }
}
