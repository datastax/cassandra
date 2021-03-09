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

package org.apache.cassandra.io.tries;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

public class WalkerTest extends AbstractTrieTestBase
{
    @Test
    public void testWithoutBounds() throws IOException
    {
        DataOutputBuffer buf = new AbstractTrieTestBase.DataOutputBufferPaged();
        IncrementalTrieWriter<Integer> builder = makeTrie(buf);
        long rootPos = builder.complete();

        Rebufferer source = new ByteBufRebufferer(buf.asNewBuffer());

        InternalIterator it = new InternalIterator(source, rootPos);

        DataOutputBuffer dumpBuf = new DataOutputBuffer();
        it.dumpTrie(new PrintStream(dumpBuf), (buf1, payloadPos, payloadFlags) -> String.format("%d/%d", payloadPos, payloadFlags));
        logger.info("Trie dump: \n{}", new String(dumpBuf.getData()));
        logger.info("Trie toString: {}", it.toString());

        it.goMax(rootPos);
        assertEquals(7, it.payloadFlags());
        assertEquals(TrieNode.PAYLOAD_ONLY.ordinal, it.nodeTypeOrdinal());
        assertEquals(1, it.nodeSize());
        assertFalse(it.hasChildren());

        it.goMin(rootPos);
        assertEquals(1, it.payloadFlags());
        assertEquals(TrieNode.PAYLOAD_ONLY.ordinal, it.nodeTypeOrdinal());
        assertEquals(1, it.nodeSize());
        assertFalse(it.hasChildren());

        assertEquals(-1, it.follow(source("151")));
        assertEquals(2, it.payloadFlags());

        assertEquals('3', it.follow(source("135")));

        assertEquals('3', it.followWithGreater(source("135")));
        it.goMin(it.greaterBranch);
        assertEquals(2, it.payloadFlags());

        assertEquals('3', it.followWithLesser(source("135")));
        it.goMax(it.lesserBranch);
        assertEquals(1, it.payloadFlags());

        assertEquals(3, (Object) it.prefix(source("155"), (walker, payloadPosition, payloadFlags) -> payloadFlags));
        assertNull(it.prefix(source("516"), (walker, payloadPosition, payloadFlags) -> payloadFlags));
        assertEquals(5, (Object) it.prefix(source("5151"), (walker, payloadPosition, payloadFlags) -> payloadFlags));
        assertEquals(1, (Object) it.prefix(source("1151"), (walker, payloadPosition, payloadFlags) -> payloadFlags));

        assertEquals(3, (Object) it.prefixAndNeighbours(source("155"), (walker, payloadPosition, payloadFlags) -> payloadFlags));
        assertNull(it.prefixAndNeighbours(source("516"), (walker, payloadPosition, payloadFlags) -> payloadFlags));
        assertEquals(5, (Object) it.prefixAndNeighbours(source("5151"), (walker, payloadPosition, payloadFlags) -> payloadFlags));
        assertEquals(1, (Object) it.prefixAndNeighbours(source("1151"), (walker, payloadPosition, payloadFlags) -> payloadFlags));

        assertEquals(3, (Object) it.prefixAndNeighbours(source("1555"), (walker, payloadPosition, payloadFlags) -> payloadFlags));
        it.goMax(it.lesserBranch);
        assertEquals(2, it.payloadFlags());
        it.goMin(it.greaterBranch);
        assertEquals(4, it.payloadFlags());
    }

    @Test
    public void testWithBounds() throws IOException
    {
        DataOutputBuffer buf = new AbstractTrieTestBase.DataOutputBufferPaged();
        IncrementalTrieWriter<Integer> builder = makeTrie(buf);
        long rootPos = builder.complete();

        Rebufferer source = new ByteBufRebufferer(buf.asNewBuffer());

        InternalIterator it = new InternalIterator(source, rootPos, source("151"), source("515"), false);
        long pos;
        assertNotEquals(-1, pos = it.nextPayloadedNode());
        assertEquals(3, TrieNode.at(buf.asNewBuffer(), (int) pos).payloadFlags(buf.asNewBuffer(), (int) pos));
        assertNotEquals(-1, pos = it.nextPayloadedNode());
        assertEquals(4, TrieNode.at(buf.asNewBuffer(), (int) pos).payloadFlags(buf.asNewBuffer(), (int) pos));
        assertNotEquals(-1, pos = it.nextPayloadedNode());
        assertEquals(5, TrieNode.at(buf.asNewBuffer(), (int) pos).payloadFlags(buf.asNewBuffer(), (int) pos));

        assertEquals(-1, it.nextPayloadedNode());
    }

    @Test
    public void testWithBoundsAndAdmitPrefix() throws IOException
    {
        DataOutputBuffer buf = new AbstractTrieTestBase.DataOutputBufferPaged();
        IncrementalTrieWriter<Integer> builder = makeTrie(buf);
        long rootPos = builder.complete();

        Rebufferer source = new ByteBufRebufferer(buf.asNewBuffer());

        InternalIterator it = new InternalIterator(source, rootPos, source("151"), source("515"), true);
        long pos;
        assertNotEquals(-1, pos = it.nextPayloadedNode());
        assertEquals(2, TrieNode.at(buf.asNewBuffer(), (int) pos).payloadFlags(buf.asNewBuffer(), (int) pos));
        assertNotEquals(-1, pos = it.nextPayloadedNode());
        assertEquals(3, TrieNode.at(buf.asNewBuffer(), (int) pos).payloadFlags(buf.asNewBuffer(), (int) pos));
        assertNotEquals(-1, pos = it.nextPayloadedNode());
        assertEquals(4, TrieNode.at(buf.asNewBuffer(), (int) pos).payloadFlags(buf.asNewBuffer(), (int) pos));
        assertNotEquals(-1, pos = it.nextPayloadedNode());
        assertEquals(5, TrieNode.at(buf.asNewBuffer(), (int) pos).payloadFlags(buf.asNewBuffer(), (int) pos));

        assertEquals(-1, it.nextPayloadedNode());
    }

    @Test
    public void testPartialTail() throws IOException
    {
        DataOutputBuffer buf = new AbstractTrieTestBase.DataOutputBufferPaged();
        IncrementalTrieWriter<Integer> builder = makeTrie(buf);
        IncrementalTrieWriter.PartialTail ptail = builder.makePartialRoot();
        long rootPos = builder.complete();
        Rebufferer source = new ByteBufRebufferer(buf.asNewBuffer());
        InternalIterator it = new InternalIterator(source, rootPos, source("151"), source("515"), true);

        Rebufferer partialSource = new ByteBufRebufferer(ptail.tail());
        InternalIterator tailIt = new InternalIterator(partialSource, ptail.root(), source("151"), source("515"), true);

        while (true)
        {
            long i1 = it.nextPayloadedNode();
            long i2 = tailIt.nextPayloadedNode();
            if (i1 == -1 || i2 == -1)
                break;

            int f1 = TrieNode.at(buf.asNewBuffer(), (int) i1).payloadFlags(buf.asNewBuffer(), (int) i1);
            int f2 = TrieNode.at(ptail.tail(), (int) i2).payloadFlags(ptail.tail(), (int) i2);
            assertEquals(f1, f2);
        }
    }

    @Test
    public void testBigTrie() throws IOException
    {
        DataOutputBuffer buf = new AbstractTrieTestBase.DataOutputBufferPaged();
        IncrementalTrieWriter<Integer> builder = IncrementalTrieWriter.open(serializer, buf);
        payloadSize = 0;
        makeBigTrie(builder);
        builder.reset();
        payloadSize = 200;
        makeBigTrie(builder);

        long rootPos = builder.complete();
        Rebufferer source = new ByteBufRebufferer(buf.asNewBuffer());
        InternalIterator it = new InternalIterator(source, rootPos);

        while (true)
        {
            long i1 = it.nextPayloadedNode();
            if (i1 == -1)
                break;

            TrieNode node = TrieNode.at(buf.asNewBuffer(), (int) i1);
            assertNotEquals(0, node.payloadFlags(buf.asNewBuffer(), (int) i1));
        }
    }


    private IncrementalTrieWriter<Integer> makeTrie(DataOutputBuffer out) throws IOException
    {
        IncrementalTrieWriter<Integer> builder = IncrementalTrieWriter.open(serializer, out);
        dump = true;
        builder.add(source("115"), 1);
        builder.add(source("151"), 2);
        builder.add(source("155"), 3);
        builder.add(source("511"), 4);
        builder.add(source("515"), 5);
        builder.add(source("551"), 6);
        builder.add(source("555555555555555555555555555555555555555555555555555555555555555555"), 7);
        return builder;
    }

    private void makeBigTrie(IncrementalTrieWriter<Integer> builder) throws IOException
    {
        dump = false;
        for (int shift = 0; shift < 8; shift++)
            for (long i = 1; i < 80; i++)
                builder.add(longSource(i, shift * 8, 100), (int) (i % 7) + 1);
    }

    private ByteComparable longSource(long l, int shift, int size)
    {
        String s = StringUtils.leftPad(toBase(l), 8, '0');
        s = StringUtils.rightPad(s, 8 + shift, '0');
        s = StringUtils.leftPad(s, size, '0');
        return source(s);
    }
}
