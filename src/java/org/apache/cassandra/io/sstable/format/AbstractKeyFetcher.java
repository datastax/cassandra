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

package org.apache.cassandra.io.sstable.format;

import java.io.IOException;
import java.util.Objects;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.IKeyFetcher;
import org.apache.cassandra.io.util.RandomAccessReader;

public abstract class AbstractKeyFetcher implements IKeyFetcher
{
    private final RandomAccessReader reader;

    protected AbstractKeyFetcher(RandomAccessReader reader)
    {
        this.reader = reader;
    }

    @Override
    public DecoratedKey apply(long keyOffset)
    {
        if (keyOffset < 0)
            return null;

        try
        {
            reader.seek(keyOffset);
            if (reader.isEOF())
                return null;

            return readKey(reader);
        }
        catch (IOException e)
        {
            throw new FSReadError(new IOException("Failed to read key from " + reader.getChannel().file(), e), reader.getChannel().file());
        }
    }

    public abstract DecoratedKey readKey(RandomAccessReader reader) throws IOException;

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        AbstractKeyFetcher that = (AbstractKeyFetcher) o;
        return Objects.equals(reader.getChannel().file(), that.reader.getChannel().file());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(reader.getChannel().file());
    }

    @Override
    public String toString()
    {
        return String.format("KeyFetcher{file=%s}", reader.getChannel().file());
    }

    @Override
    public void close()
    {
        reader.close();
    }
}
