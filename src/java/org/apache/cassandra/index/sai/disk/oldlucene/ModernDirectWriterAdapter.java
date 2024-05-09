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

package org.apache.cassandra.index.sai.disk.oldlucene;

import java.io.IOException;

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.packed.DirectWriter;

/**
 * Minimal wrapper arount DirectWriter to allow it to be used in a common interface with LegacyDirectWriter.
 */
public class ModernDirectWriterAdapter implements DirectWriterAdapter
{
    private final org.apache.lucene.util.packed.DirectWriter delegate;

    public ModernDirectWriterAdapter(DataOutput output, long numValues, int bitsPerValue)
    {
        this.delegate = DirectWriter.getInstance(output, numValues, bitsPerValue);
    }

    @Override
    public void add(long l) throws IOException
    {
        delegate.add(l);
    }

    @Override
    public void finish() throws IOException
    {
        delegate.finish();
    }
}
