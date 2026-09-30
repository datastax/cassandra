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

package org.apache.cassandra.io.tries;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.PageAware;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Checks that a {@link ReverseValueIterator} whose construction fails while descending the trie (e.g. on a corrupted
 * page) releases every buffer it acquired and closes its reader exactly once.
 */
public class ReverseValueIteratorFailureTest extends AbstractTrieTestBase
{
    static class InjectedFailure extends RuntimeException
    {
        InjectedFailure(long position)
        {
            super("Injected failure reading page at " + position);
        }
    }

    /**
     * Serves the buffer one page at a time, counting acquired and released buffers and closeReader calls, and fails
     * when asked for the given page.
     */
    static class CountingPagedRebufferer implements Rebufferer
    {
        final ByteBuffer buffer;
        final long failingPage;
        int acquired;
        int released;
        int readerCloses;

        CountingPagedRebufferer(ByteBuffer buffer, long failingPage)
        {
            this.buffer = buffer;
            this.failingPage = failingPage;
        }

        @Override
        public BufferHolder rebuffer(long position)
        {
            long pageStart = PageAware.pageStart(position);
            if (pageStart == failingPage)
                throw new InjectedFailure(position);
            ByteBuffer page = buffer.duplicate();
            page.position((int) pageStart).limit((int) Math.min(buffer.limit(), pageStart + PageAware.PAGE_SIZE));
            ByteBuffer slice = page.slice();
            ++acquired;
            return new BufferHolder()
            {
                boolean isReleased;

                @Override
                public ByteBuffer buffer()
                {
                    return slice.duplicate();
                }

                @Override
                public long offset()
                {
                    return pageStart;
                }

                @Override
                public void release()
                {
                    assertTrue("Buffer at " + pageStart + " released twice", !isReleased);
                    isReleased = true;
                    ++released;
                }
            };
        }

        @Override
        public void closeReader()
        {
            ++readerCloses;
        }

        @Override
        public void close()
        {
        }

        @Override
        public ChannelProxy channel()
        {
            return null;
        }

        @Override
        public long fileLength()
        {
            return buffer.limit();
        }

        @Override
        public double getCrcCheckChance()
        {
            return 0;
        }

        @Override
        public long adjustPosition(long position)
        {
            return position;
        }
    }

    @Test
    public void testConstructionFailureReleasesResources() throws IOException
    {
        // the rebufferer serves single pages, which requires nodes not to cross page boundaries
        Assume.assumeTrue(writerClass != TestClass.SIMPLE);
        DataOutputBuffer out = new DataOutputBufferPaged();
        IncrementalTrieWriter<Integer> builder = newTrieWriter(serializer, out);
        List<ByteComparable> keys = new ArrayList<>();
        for (int shift = 0; shift < 8; shift++)
        {
            for (long i = 1; i < 80; i++)
            {
                ByteComparable key = longSource(i, shift * 8, 100);
                builder.add(key, (int) (i % 7) + 1);
                keys.add(key);
            }
        }
        long root = builder.complete();
        ByteBuffer buffer = out.asNewBuffer();
        long pages = (buffer.limit() + PageAware.PAGE_SIZE - 1) / PageAware.PAGE_SIZE;
        assertTrue("Expected a trie spanning several pages, got " + buffer.limit() + " bytes", pages > 2);

        int failures = 0;
        for (long page = 0; page < pages; page++)
        {
            long failingPage = page * PageAware.PAGE_SIZE;
            if (failingPage == PageAware.pageStart(root))
                continue;   // a failure reading the root page is handled by the Walker constructor
            for (ByteComparable end : keys)
            {
                CountingPagedRebufferer source = new CountingPagedRebufferer(buffer, failingPage);
                ReverseValueIterator<?> iterator;
                try
                {
                    iterator = new ReverseValueIterator<>(source, root, null, end, ValueIterator.LeftBoundTreatment.ADMIT_PREFIXES, true, version);
                }
                catch (InjectedFailure e)
                {
                    ++failures;
                    assertEquals("Buffers not released after a failed construction", source.acquired, source.released);
                    assertEquals("Reader not closed exactly once after a failed construction", 1, source.readerCloses);
                    continue;
                }
                iterator.close();
                iterator.close(); // closing is idempotent
                assertEquals(source.acquired, source.released);
                assertEquals(1, source.readerCloses);
            }
        }
        assertTrue("No construction failed", failures > 0);
    }

    private ByteComparable longSource(long l, int shift, int size)
    {
        String s = StringUtils.leftPad(toBase(l), 8, '0');
        s = StringUtils.rightPad(s, 8 + shift, '0');
        s = StringUtils.leftPad(s, size, '0');
        return source(s);
    }
}
