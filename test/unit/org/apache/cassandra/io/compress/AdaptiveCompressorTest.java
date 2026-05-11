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
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import org.apache.cassandra.io.util.RandomAccessReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;


public class AdaptiveCompressorTest
{

    @Test
    public void testCreateWithDifferentParamsMakeSeparateInstances()
    {
        Random rnd = new Random(1);
        for (int i = 0; i < 1000; i++)
        {
            Map<String, String> options = new HashMap<>();
            int maxCompressionLevel = rnd.nextInt(AdaptiveCompressor.MAX_COMPRESSION_LEVEL) + 1;
            int minCompressionLevel = rnd.nextInt(maxCompressionLevel);
            int maxQueueLen = rnd.nextInt(20) + 1;
            options.put(AdaptiveCompressor.MIN_COMPRESSION_LEVEL_OPTION_NAME, Integer.toString(minCompressionLevel));
            options.put(AdaptiveCompressor.MAX_COMPRESSION_LEVEL_OPTION_NAME, Integer.toString(maxCompressionLevel));
            options.put(AdaptiveCompressor.MAX_COMPACTION_QUEUE_LENGTH_OPTION_NAME, Integer.toString(maxQueueLen));

            AdaptiveCompressor compressor = AdaptiveCompressor.create(options);
            assertEquals(maxCompressionLevel, compressor.params.maxCompressionLevel);
            assertEquals(minCompressionLevel, compressor.params.minCompressionLevel);
            assertEquals(maxQueueLen, compressor.params.maxCompactionQueueLength);

            AdaptiveCompressor compressorForFlush = AdaptiveCompressor.createForFlush(options);
            assertEquals(maxCompressionLevel, compressorForFlush.params.maxCompressionLevel);
            assertEquals(minCompressionLevel, compressorForFlush.params.minCompressionLevel);
        }
    }

    @Test
    public void testCreateWithSameParamsReturnsTheSameInstance()
    {
        Map<String, String> options = new HashMap<>();
        int maxCompressionLevel = 10;
        int minCompressionLevel = 0;
        int maxQueueLen = 20;
        options.put(AdaptiveCompressor.MIN_COMPRESSION_LEVEL_OPTION_NAME, Integer.toString(minCompressionLevel));
        options.put(AdaptiveCompressor.MAX_COMPRESSION_LEVEL_OPTION_NAME, Integer.toString(maxCompressionLevel));
        options.put(AdaptiveCompressor.MAX_COMPACTION_QUEUE_LENGTH_OPTION_NAME, Integer.toString(maxQueueLen));
        AdaptiveCompressor compressor1 = AdaptiveCompressor.create(options);
        AdaptiveCompressor compressor2 = AdaptiveCompressor.create(options);
        assertSame(compressor1, compressor2);
    }

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

    @Test
    public void testParamsEqualsAndHashCode()
    {
        AdaptiveCompressor.Params[] params = new AdaptiveCompressor.Params[16];
        int index = 0;
        for (ICompressor.Uses use : new ICompressor.Uses[]{ICompressor.Uses.GENERAL, ICompressor.Uses.FAST_COMPRESSION})
            for (int min : new int[]{5, 6})
                for (int max : new int[]{10, 11})
                    for (int queue : new int[]{15, 16})
                        params[index++] = new AdaptiveCompressor.Params(use, min, max, queue);

        for (int i = 0; i < params.length; i++)
        {
            for (int j = 0; j < params.length; j++)
            {
                if (i == j)
                {
                    assertEquals(params[i], params[j]);
                    assertEquals(params[i].hashCode(), params[j].hashCode());
                }
                else
                {
                    assertNotEquals(params[i], params[j]);
                }
            }
        }

        // cannot change to assertNotEquals, because we want to expicitly call Params.equals here
        assertFalse(params[0].equals(null));
        assertFalse(params[0].equals(new Object()));
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
