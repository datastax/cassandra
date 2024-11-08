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

package org.apache.cassandra.index.sai.iterators;

import java.io.IOException;
import java.util.function.Supplier;

import org.apache.cassandra.index.sai.utils.PrimaryKey;

/**
 * Delays creating an iterator to the first use.
 */
public class KeyRangeLazyIterator extends KeyRangeIterator
{
    private KeyRangeIterator inner;
    private final Supplier<KeyRangeIterator> factory;

    public KeyRangeLazyIterator(Supplier<KeyRangeIterator> factory, PrimaryKey min, PrimaryKey max, long count)
    {
        super(min, max, count);
        this.factory = factory;
    }

    @Override
    protected void performSkipTo(PrimaryKey nextKey)
    {
        maybeInitialize();
        inner.skipTo(nextKey);
    }

    @Override
    protected PrimaryKey computeNext()
    {
        maybeInitialize();
        return inner.hasNext() ? inner.next() : endOfData();
    }

    @Override
    public void close() throws IOException
    {
        if (inner != null)
            inner.close();
    }

    private void maybeInitialize()
    {
        if (inner == null)
        {
            inner = factory.get();
            assert inner != null;
        }
    }
}
