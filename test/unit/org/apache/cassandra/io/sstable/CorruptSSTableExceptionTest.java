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

package org.apache.cassandra.io.sstable;

import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.io.util.File;

import static org.apache.cassandra.io.sstable.CorruptSSTableException.maybeWrapInCorruptSSTableException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

public class CorruptSSTableExceptionTest
{
    private static final File FILE = new File("ks/tbl/nb-1-big-Partitions.db");
    private static final File OTHER_FILE = new File("ks/tbl/nb-1-big-Rows.db");

    @Test
    public void testIOExceptionIsWrappedForFile()
    {
        IOException failure = new IOException("read failed");
        CorruptSSTableException wrapped = maybeWrapInCorruptSSTableException(failure, FILE);
        assertSame(failure, wrapped.getCause());
        assertEquals(FILE, wrapped.file);
        assertEquals(new CorruptSSTableException(failure, FILE).getMessage(), wrapped.getMessage());
    }

    @Test
    public void testIOExceptionIsWrappedForPath()
    {
        IOException failure = new IOException("read failed");
        CorruptSSTableException wrapped = maybeWrapInCorruptSSTableException(failure, FILE.path());
        assertSame(failure, wrapped.getCause());
        assertEquals(FILE, wrapped.file);
        assertEquals(new CorruptSSTableException(failure, FILE.path()).getMessage(), wrapped.getMessage());
    }

    @Test
    public void testUncheckedFailureIsWrapped()
    {
        IllegalArgumentException failure = new IllegalArgumentException("garbage");
        CorruptSSTableException wrapped = maybeWrapInCorruptSSTableException(failure, FILE);
        assertNotSame(failure, wrapped);
        assertSame(failure, wrapped.getCause());
        assertEquals(FILE, wrapped.file);
    }

    @Test
    public void testCorruptSSTableExceptionIsReturnedUnchanged()
    {
        CorruptSSTableException failure = new CorruptSSTableException(new IOException("bad checksum"), OTHER_FILE);
        // the corrupted file reported by the lower level is kept, not replaced by the one given here
        assertSame(failure, maybeWrapInCorruptSSTableException(failure, FILE));
        assertSame(failure, maybeWrapInCorruptSSTableException(failure, FILE.path()));
        assertEquals(OTHER_FILE, failure.file);
    }
}
