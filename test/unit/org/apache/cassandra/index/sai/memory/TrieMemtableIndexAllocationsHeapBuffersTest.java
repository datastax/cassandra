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

package org.apache.cassandra.index.sai.memory;

import org.junit.BeforeClass;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.io.compress.BufferType;

import static org.junit.Assert.assertEquals;

public class TrieMemtableIndexAllocationsHeapBuffersTest extends TrieMemtableIndexTestBase
{
    @BeforeClass
    public static void setUpClass()
    {
        System.setProperty("cassandra.trie.memtable.shard.count", "8");
        setup(Config.MemtableAllocationType.heap_buffers);
        assertEquals(TrieMemtable.BUFFER_TYPE, BufferType.ON_HEAP);
    }
}
