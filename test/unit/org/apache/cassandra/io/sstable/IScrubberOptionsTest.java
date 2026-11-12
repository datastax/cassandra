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

package org.apache.cassandra.io.sstable;

import org.junit.Test;

import org.apache.cassandra.io.sstable.IScrubber.OverwriteTTLMode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link IScrubber.Options} and its builder, in particular the interaction between the TTL overwrite modes
 * and the {@code bumpTTLTimestamp} flag.
 */
public class IScrubberOptionsTest
{
    @Test
    public void testDefaults()
    {
        IScrubber.Options options = IScrubber.options().build();

        assertFalse(options.checkData);
        assertFalse(options.skipCorrupted);
        assertFalse(options.overrideTxnIsOffline);
        assertFalse(options.reinsertOverflowedTTLRows);
        assertFalse(options.bumpTTLTimestamp);
        assertEquals(OverwriteTTLMode.NONE, options.overwriteTTLMode);
    }

    @Test
    public void testSimpleFlags()
    {
        IScrubber.Options options = IScrubber.options().checkData().skipCorrupted().overrideTxnIsOffline().build();
        assertTrue(options.checkData);
        assertTrue(options.skipCorrupted);
        assertTrue(options.overrideTxnIsOffline);

        options = IScrubber.options()
                           .checkData(true).skipCorrupted(true).overrideTxnIsOffline(true)
                           .checkData(false).skipCorrupted(false).overrideTxnIsOffline(false)
                           .build();
        assertFalse(options.checkData);
        assertFalse(options.skipCorrupted);
        assertFalse(options.overrideTxnIsOffline);
    }

    @Test
    public void testBumpTTLTimestampWithNoTTL()
    {
        IScrubber.Options options = IScrubber.options()
                                             .overwriteTTLMode(OverwriteTTLMode.NO_TTL)
                                             .bumpTTLTimestamp(true)
                                             .build();
        assertEquals(OverwriteTTLMode.NO_TTL, options.overwriteTTLMode);
        assertTrue(options.bumpTTLTimestamp);
        assertFalse(options.reinsertOverflowedTTLRows);

        options = IScrubber.options().overwriteTTLMode(OverwriteTTLMode.NO_TTL).bumpTTLTimestamp(false).build();
        assertEquals(OverwriteTTLMode.NO_TTL, options.overwriteTTLMode);
        assertFalse(options.bumpTTLTimestamp);

        // the flag is independent from the order in which the builder methods are called
        options = IScrubber.options().bumpTTLTimestamp(true).overwriteTTLMode(OverwriteTTLMode.NO_TTL).build();
        assertTrue(options.bumpTTLTimestamp);
    }

    @Test
    public void testBumpTTLTimestampRejectedWithoutNoTTL()
    {
        assertBumpRejected(IScrubber.options().bumpTTLTimestamp(true));
        assertBumpRejected(IScrubber.options().overwriteTTLMode(OverwriteTTLMode.NONE).bumpTTLTimestamp(true));
        assertBumpRejected(IScrubber.options().overwriteTTLMode(OverwriteTTLMode.REINSERT_OVERFLOWED_TTL).bumpTTLTimestamp(true));
        assertBumpRejected(IScrubber.options().reinsertOverflowedTTLRows().bumpTTLTimestamp(true));
        assertBumpRejected(IScrubber.options().overwriteTTLMode(null).bumpTTLTimestamp(true));
    }

    private static void assertBumpRejected(IScrubber.Options.Builder builder)
    {
        try
        {
            builder.build();
            fail("bumpTTLTimestamp must be rejected unless the mode is NO_TTL");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("bumpTTLTimestamp"));
            assertTrue(e.getMessage(), e.getMessage().contains("NO_TTL"));
        }
    }

    @Test
    public void testReinsertOverflowedTTLRows()
    {
        IScrubber.Options options = IScrubber.options().reinsertOverflowedTTLRows().build();
        assertTrue(options.reinsertOverflowedTTLRows);
        assertEquals(OverwriteTTLMode.REINSERT_OVERFLOWED_TTL, options.overwriteTTLMode);

        options = IScrubber.options().overwriteTTLMode(OverwriteTTLMode.REINSERT_OVERFLOWED_TTL).build();
        assertTrue(options.reinsertOverflowedTTLRows);

        // enabling it twice is harmless
        options = IScrubber.options().reinsertOverflowedTTLRows(true).reinsertOverflowedTTLRows(true).build();
        assertEquals(OverwriteTTLMode.REINSERT_OVERFLOWED_TTL, options.overwriteTTLMode);

        // disabling it goes back to NONE
        options = IScrubber.options().reinsertOverflowedTTLRows(true).reinsertOverflowedTTLRows(false).build();
        assertFalse(options.reinsertOverflowedTTLRows);
        assertEquals(OverwriteTTLMode.NONE, options.overwriteTTLMode);

        // disabling it must not touch another mode
        options = IScrubber.options().overwriteTTLMode(OverwriteTTLMode.NO_TTL).reinsertOverflowedTTLRows(false).build();
        assertEquals(OverwriteTTLMode.NO_TTL, options.overwriteTTLMode);
    }

    @Test
    public void testConflictingModesAreRejected()
    {
        try
        {
            IScrubber.options().overwriteTTLMode(OverwriteTTLMode.NO_TTL).reinsertOverflowedTTLRows(true);
            fail("NO_TTL and reinsertOverflowedTTLRows are mutually exclusive");
        }
        catch (IllegalArgumentException expected)
        {
            assertTrue(expected.getMessage().contains("NO_TTL"));
        }

        try
        {
            IScrubber.options().reinsertOverflowedTTLRows(true).overwriteTTLMode(OverwriteTTLMode.NO_TTL);
            fail("NO_TTL and reinsertOverflowedTTLRows are mutually exclusive");
        }
        catch (IllegalArgumentException expected)
        {
            assertTrue(expected.getMessage().contains("NO_TTL"));
        }
    }

    @Test
    public void testNullModeMeansNone()
    {
        IScrubber.Options options = IScrubber.options().overwriteTTLMode(OverwriteTTLMode.NO_TTL).build();
        assertEquals(OverwriteTTLMode.NO_TTL, options.overwriteTTLMode);

        assertEquals(OverwriteTTLMode.NONE, IScrubber.options().overwriteTTLMode(null).build().overwriteTTLMode);
    }

    @Test
    public void testToString()
    {
        String string = IScrubber.options()
                                 .checkData()
                                 .overwriteTTLMode(OverwriteTTLMode.NO_TTL)
                                 .bumpTTLTimestamp(true)
                                 .build()
                                 .toString();

        assertTrue(string, string.contains("checkData=true"));
        assertTrue(string, string.contains("overwriteTTLMode=NO_TTL"));
        assertTrue(string, string.contains("bumpTTLTimestamp=true"));

        assertTrue(IScrubber.options().build().toString().contains("bumpTTLTimestamp=false"));
    }
}
