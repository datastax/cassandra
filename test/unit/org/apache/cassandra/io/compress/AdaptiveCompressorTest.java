/*
 * Copyright DataStax, Inc.
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

package org.apache.cassandra.io.compress;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import org.apache.cassandra.io.util.RandomAccessReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class AdaptiveCompressorTest
{

    @Test(expected = IllegalArgumentException.class)
    public void badCompressionLevelParamThrowsExceptionMin()
    {
        AdaptiveCompressor.create(ImmutableMap.of(AdaptiveCompressor.MIN_COMPRESSION_LEVEL_OPTION_NAME, Integer.toString(AdaptiveCompressor.MIN_COMPRESSION_LEVEL - 1)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void badCompressionLevelParamThrowsExceptionMax()
    {
        AdaptiveCompressor.create(ImmutableMap.of(AdaptiveCompressor.MAX_COMPRESSION_LEVEL_OPTION_NAME, Integer.toString(AdaptiveCompressor.MAX_COMPRESSION_LEVEL + 1)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void badMaxCompactionQueueLengthParamThrowsExceptionMin()
    {
        AdaptiveCompressor.create(ImmutableMap.of(AdaptiveCompressor.MAX_COMPACTION_QUEUE_LENGTH_OPTION_NAME, "-1"));
    }

    @Test
    public void averageRelativeTimeCompressingIsMeasuredProperly() throws IOException, InterruptedException
    {
        var params = new AdaptiveCompressor.Params(ICompressor.Uses.GENERAL, 15, 15, 0);
        AdaptiveCompressor c1 = new AdaptiveCompressor(params, () -> 0.0);
        ByteBuffer src = getSrcByteBuffer();
        ByteBuffer dest = getDstByteBuffer(c1);
        for (int i = 0; i < 20000; i++)
        {
            compress(c1, src, dest);
        }
        assertTrue(c1.getThreadLocalState().getRelativeTimeSpentCompressing() > 0.8);
        assertTrue(c1.getThreadLocalState().getRelativeTimeSpentCompressing() < 1.0);


        var params2 = new AdaptiveCompressor.Params(ICompressor.Uses.GENERAL, 0, 0, 0);
        AdaptiveCompressor c2 = new AdaptiveCompressor(params2, () -> 0.0);
        for (int i = 0; i < 100; i++)
        {
            Thread.sleep(1);
            compress(c2, src, dest);
        }
        assertTrue(c2.getThreadLocalState().getRelativeTimeSpentCompressing() < 0.02);
        assertTrue(c2.getThreadLocalState().getRelativeTimeSpentCompressing() > 0.0);
    }

    @Test
    public void compressionLevelAdaptsToWritePressure() throws IOException
    {
        var params = new AdaptiveCompressor.Params(ICompressor.Uses.GENERAL, 2, 8, 0);
        double[] load = { 1.0 };

        AdaptiveCompressor c = new AdaptiveCompressor(params, () -> load[0]);
        ByteBuffer src = getSrcByteBuffer();
        ByteBuffer dest = getDstByteBuffer(c);

        for (int i = 0; i < 10; i++)
            compress(c, src, dest);

        assertEquals(2, c.getThreadLocalState().currentCompressionLevel);

        // Low load; compression level must be increased back to max:
        load[0] = 0L;

        for (int i = 0; i < 10; i++)
            compress(c, src, dest);

        assertEquals(8, c.getThreadLocalState().currentCompressionLevel);
    }

    @Test
    public void compressionLevelDoesNotDecreaseWhenCompressionIsNotABottleneck() throws IOException, InterruptedException
    {
        var params = new AdaptiveCompressor.Params(ICompressor.Uses.GENERAL, 2, 8, 0);
        // Simulate high write load
        AdaptiveCompressor c = new AdaptiveCompressor(params, () -> 1.0);
        ByteBuffer src = getSrcByteBuffer();
        ByteBuffer dest = getDstByteBuffer(c);

        for (int i = 0; i < 200; i++)
        {
            Thread.sleep(1); // creates artificial bottleneck that is much slower than compression
            compress(c, src, dest);
        }

        assertEquals(8, c.getThreadLocalState().currentCompressionLevel);
    }

    private static ByteBuffer getDstByteBuffer(ICompressor compressor)
    {
        return ByteBuffer.allocateDirect(compressor.initialCompressedBufferLength(RandomAccessReader.DEFAULT_BUFFER_SIZE));
    }

    private static ByteBuffer getSrcByteBuffer()
    {
        int n = RandomAccessReader.DEFAULT_BUFFER_SIZE;
        byte[] srcData = new byte[n];
        new Random().nextBytes(srcData);

        ByteBuffer src = ByteBuffer.allocateDirect(n);
        src.put(srcData, 0, n);
        src.flip().position(0);
        return src;
    }

    private static void compress(AdaptiveCompressor c, ByteBuffer src, ByteBuffer dest) throws IOException
    {
        c.compress(src, dest);
        src.rewind();
        dest.rewind();
    }

}
