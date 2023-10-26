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

package org.apache.cassandra.index.sai.disk.v1.trie;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public class TrieRangeIteratorTest extends SaiRandomizedTest
{
    private IndexDescriptor indexDescriptor;
    private IndexContext indexContext;

    @Before
    public void setup() throws Throwable
    {
        indexDescriptor = newIndexDescriptor();
        indexContext = SAITester.createIndexContext(newIndex(), UTF8Type.instance);
    }

    private long makeTrie() throws Exception
    {
        try (TrieTermsDictionaryWriter writer = new TrieTermsDictionaryWriter(indexDescriptor, indexContext, false))
        {
            writer.add(asByteComparable("bb"), 0);
            writer.add(asByteComparable("bbbb"), 1);
            writer.add(asByteComparable("bbbbbb"), 2);
            return writer.complete(new MutableLong());
        }
    }

    @Test
    public void testExactPrefixMatchWithAdmitPrefix() throws Exception
    {
        long fp = makeTrie();

        var prefix = asByteComparable("bb");
        var suffix = asByteComparable("bbbbbb");

        try (FileHandle input = indexDescriptor.createPerIndexFileHandle(IndexComponent.TERMS_DATA, indexContext);
             TrieRangeIterator reader = new TrieRangeIterator(input.instantiateRebufferer(), fp, prefix, suffix, true, true))
        {
            var iter = reader.iterator();
            assertEquals(0, iter.next().right.longValue());
            assertEquals(1, iter.next().right.longValue());
            assertEquals(2, iter.next().right.longValue());
        }
    }

    @Test
    public void testExactPrefixMatchWithNotAdmitPrefix() throws Exception
    {
        long fp = makeTrie();

        var prefix1 = ByteComparable.of("bbb");
        var suffix1 = asByteComparable("c");

        try (FileHandle input = indexDescriptor.createPerIndexFileHandle(IndexComponent.TERMS_DATA, indexContext);
             TrieRangeIterator reader = new TrieRangeIterator(input.instantiateRebufferer(), fp, prefix1, suffix1, false, true))
        {
            var iter = reader.iterator();
            assertEquals(1, iter.next().right.longValue());
            assertEquals(2, iter.next().right.longValue());
        }
    }
    @Test
    public void testLowerBoundTerminatesAsPrefixMatchNoPayloadWithNotAdmitPrefix() throws Exception
    {
        long fp = makeTrie();
        var prefix2 = ByteComparable.of("bbb");
        var suffix2 = asByteComparable("c");

        try (FileHandle input = indexDescriptor.createPerIndexFileHandle(IndexComponent.TERMS_DATA, indexContext);
             TrieRangeIterator reader = new TrieRangeIterator(input.instantiateRebufferer(), fp, prefix2, suffix2, true, true))
        {
            var iter = reader.iterator();
            assertEquals(1, iter.next().right.longValue());
            assertEquals(2, iter.next().right.longValue());
        }
    }

    private ByteComparable asByteComparable(String s)
    {
        return ByteComparable.fixedLength(ByteBufferUtil.bytes(s));
    }

}
