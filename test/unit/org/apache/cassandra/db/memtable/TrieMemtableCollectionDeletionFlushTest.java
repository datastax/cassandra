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

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

/**
 * A trie memtable partition whose only content is the deletion of a collection counts no row and no tombstone. A flush
 * leaves out the partitions that are empty, and such a partition is not: without its deletion, the collection comes
 * back from the sstable written before.
 */
public class TrieMemtableCollectionDeletionFlushTest extends CQLTester
{
    @Test
    public void testCollectionDeletionIsFlushed() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, m map<int, int>, PRIMARY KEY (k, c)) WITH memtable = 'trie'");
        execute("INSERT INTO %s (k, c, m) VALUES (0, 0, {1: 1})");
        flush();

        execute("UPDATE %s SET m = null WHERE k = 0 AND c = 0");
        assertRows(execute("SELECT m FROM %s WHERE k = 0 AND c = 0"), row((Object) null));
        flush();
        assertRows(execute("SELECT m FROM %s WHERE k = 0 AND c = 0"), row((Object) null));
    }
}
