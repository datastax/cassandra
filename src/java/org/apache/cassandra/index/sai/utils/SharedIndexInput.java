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

package org.apache.cassandra.index.sai.utils;

import java.io.IOException;

import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.SharedCloseable;
import org.apache.lucene.store.IndexInput;

public class SharedIndexInput extends IndexInput implements SharedCloseable
{
    private final Ref<?> ref;
    private final IndexInput input;

    public SharedIndexInput(IndexInput input)
    {
        super(input.toString());
        ref = new Ref<>(null, new SharedTidy().add(input));
        this.input = input;
    }

    public SharedIndexInput(SharedIndexInput copy)
    {
        super(copy.input.toString());
        this.ref = copy.ref.ref();
        this.input = copy.input;
    }

    @Override
    public SharedIndexInput sharedCopy()
    {
        return new SharedIndexInput(this);
    }

    @Override
    public Throwable close(Throwable accumulate)
    {
        return ref.ensureReleased(accumulate);
    }

    @Override
    public void addTo(Ref.IdentityCollection identities)
    {
        identities.add(ref);
    }

    @Override
    public void close() throws IOException
    {
        ref.ensureReleased();
    }

    @Override
    public long getFilePointer()
    {
        return input.getFilePointer();
    }

    @Override
    public void seek(long l) throws IOException
    {
        input.seek(l);
    }

    @Override
    public long length()
    {
        return input.length();
    }

    @Override
    public IndexInput slice(String s, long l, long l1) throws IOException
    {
        return input.slice(s, l, l1);
    }

    @Override
    public byte readByte() throws IOException
    {
        return input.readByte();
    }

    @Override
    public void readBytes(byte[] bytes, int i, int i1) throws IOException
    {
        input.readBytes(bytes, i, i1);
    }
}
