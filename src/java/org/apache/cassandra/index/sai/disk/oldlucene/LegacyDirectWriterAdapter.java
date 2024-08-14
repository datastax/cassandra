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

import org.apache.lucene.backward_codecs.packed.LegacyDirectWriter;

/**
 * Minimal wrapper around Lucene's LegacyDirectWriter, which doesn't share an interface with DirectWriter.
 */
public class LegacyDirectWriterAdapter implements DirectWriterAdapter
{
    private final LegacyDirectWriter delegate;

    public LegacyDirectWriterAdapter(org.apache.lucene.store.DataOutput output, long numValues, int bitsPerValue)
    {
        this.delegate = LegacyDirectWriter.getInstance(output, numValues, bitsPerValue);
    }

    public void add(long l) throws IOException
    {
        delegate.add(l);
    }

    public void finish() throws IOException
    {
        delegate.finish();
    }
}
