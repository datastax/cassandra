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
package org.apache.cassandra.schema;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.ICompressor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class DefaultCompressionSelectorTest
{
    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Before
    public void prepare() throws Exception
    {
        DatabaseDescriptor.setFlushCompression(null);
        CassandraRelevantProperties.SSTABLE_COMPRESSION_SELECTOR_CLASS.reset();
        CassandraRelevantProperties.DEFAULT_SSTABLE_COMPRESSION.reset();
    }

    // --- CompressionParams.Selector loading via system property ---

    @Test
    public void testFromPropertyLoadsDefaultSelectorWhenPropertyEmpty()
    {
        CompressionParams.Selector selector = CompressionParams.Selector.fromProperty();
        assertSame(DefaultCompressionSelector.class, selector.getClass());
    }

    @Test
    public void testFromPropertyLoadsCustomSelectorClass()
    {
        System.setProperty(CassandraRelevantProperties.SSTABLE_COMPRESSION_SELECTOR_CLASS.getKey(),
                           TestCustomSelector.class.getName());
        CompressionParams.Selector selector = CompressionParams.Selector.fromProperty();
        assertSame(TestCustomSelector.class, selector.getClass());
    }

    // --- Most common default configuration ---

    @Test
    public void testDefaultCompressionWhenNoPropertiesSet()
    {
        assertSame(CompressionParams.FAST, CompressionParams.forNewTables("any_keyspace"));
        assertSame(CompressionParams.FAST, CompressionParams.forFlush("any_keyspace", CompressionParams.deflate()));
    }

    @Test
    public void testDefaultCompressionWhenGlobalAdaptiveCompressionSet()
    {
        CassandraRelevantProperties.DEFAULT_SSTABLE_COMPRESSION.setString("adaptive");
        assertSame(CompressionParams.ADAPTIVE, CompressionParams.forNewTables("any_keyspace"));
        assertSame(CompressionParams.FAST_ADAPTIVE, CompressionParams.forFlush("any_keyspace", CompressionParams.deflate()));
    }

    // --- Additional flushCompression tests ---

    @Test
    public void testFlushCompressionReturnsNoneIfNewTableCompressionNotSet()
    {
        assertEquals(CompressionParams.noCompression(),
                     CompressionParams.forFlush("any_keyspace", CompressionParams.noCompression()));
    }

    @Test
    public void testFlushCompressionReturnsNoopIfGlobalFlushCompressionSetToNone()
    {
        DatabaseDescriptor.setFlushCompression(Config.FlushCompression.none);
        DefaultCompressionSelector selector = new DefaultCompressionSelector();
        // regardless of what the table uses, none → NOOP
        assertSame(CompressionParams.NOOP, selector.flushCompression("ks", CompressionParams.FAST));
        assertSame(CompressionParams.NOOP, selector.flushCompression("ks", CompressionParams.ADAPTIVE));
        assertSame(CompressionParams.NOOP, selector.flushCompression("ks", CompressionParams.FAST_ADAPTIVE));
    }

    @Test
    public void testFlushCompressionReturnsFastWhenTableCompressorDoesntSupportFastCompression()
    {
        // ADAPTIVE (general AdaptiveCompressor) does not advertise FAST_COMPRESSION use
        DatabaseDescriptor.setFlushCompression(Config.FlushCompression.fast);
        DefaultCompressionSelector selector = new DefaultCompressionSelector();
        assertSame(CompressionParams.FAST, selector.flushCompression("ks", CompressionParams.ADAPTIVE));
    }

    @Test
    public void testFlushCompressionFallsThroughToTableWhenTableCompressorSupportsFastCompression()
    {
        // LZ4 fast compressor advertises FAST_COMPRESSION; forUse returns itself → result is tableParams
        DatabaseDescriptor.setFlushCompression(Config.FlushCompression.fast);
        DefaultCompressionSelector selector = new DefaultCompressionSelector();
        assertSame(CompressionParams.FAST, selector.flushCompression("ks", CompressionParams.FAST));

        DatabaseDescriptor.setFlushCompression(Config.FlushCompression.adaptive);
        // FAST_ADAPTIVE also advertises FAST_COMPRESSION
        assertSame(CompressionParams.FAST_ADAPTIVE, selector.flushCompression("ks", CompressionParams.FAST_ADAPTIVE));
    }

    @Test
    public void testFlushCompressionAdaptiveReturnsFastAdaptive()
    {
        DatabaseDescriptor.setFlushCompression(Config.FlushCompression.adaptive);
        DefaultCompressionSelector selector = new DefaultCompressionSelector();
        assertSame(CompressionParams.FAST_ADAPTIVE, selector.flushCompression("ks", CompressionParams.ADAPTIVE));
    }

    @Test
    public void testFlushCompressionFallsBackToNewTableCompression()
    {
        // Slow compressor + flush compression set to table -> use the table compressor as-is
        DatabaseDescriptor.setFlushCompression(Config.FlushCompression.table);
        DefaultCompressionSelector selector = new DefaultCompressionSelector();
        assertEquals(CompressionParams.deflate(), selector.flushCompression("ks", CompressionParams.deflate()));
    }

    @Test
    public void testFlushCompressionUsesFasterVariantOfGeneralCompressionIfAvailable()
    {
        // If slow compressor is selected, check we attempt to adapt it for fast operation by callling
        // forUse(FAST_COMPRESSION).
        DatabaseDescriptor.setFlushCompression(Config.FlushCompression.table);
        DefaultCompressionSelector selector = new DefaultCompressionSelector();
        assertEquals(CompressionParams.ADAPTIVE.forUse(ICompressor.Uses.FAST_COMPRESSION),
                     selector.flushCompression("ks", CompressionParams.ADAPTIVE));
    }

    // --- compactionCompression tests ---

    @Test
    public void testCompactionCompressionDefaultReturnsTableParams()
    {
        DefaultCompressionSelector selector = new DefaultCompressionSelector();
        assertSame(CompressionParams.FAST, selector.compactionCompression("ks", CompressionParams.FAST));
        assertSame(CompressionParams.ADAPTIVE, selector.compactionCompression("ks", CompressionParams.ADAPTIVE));
        assertSame(CompressionParams.NOOP, selector.compactionCompression("ks", CompressionParams.NOOP));
    }

    @Test
    public void testForCompactionDelegatesToSelector()
    {
        assertSame(CompressionParams.FAST, CompressionParams.forCompaction("any_keyspace", CompressionParams.FAST));
        assertSame(CompressionParams.ADAPTIVE, CompressionParams.forCompaction("any_keyspace", CompressionParams.ADAPTIVE));
    }

    /** A test-only selector that always returns NOOP compression. */
    public static class TestCustomSelector implements CompressionParams.Selector
    {
        @Override
        public CompressionParams newTableCompression(String keyspace)
        {
            return CompressionParams.NOOP;
        }

        @Override
        public CompressionParams flushCompression(String keyspace, CompressionParams tableParams)
        {
            return CompressionParams.NOOP;
        }

        @Override
        public CompressionParams compactionCompression(String keyspace, CompressionParams tableParams)
        {
            return CompressionParams.NOOP;
        }
    }
}
