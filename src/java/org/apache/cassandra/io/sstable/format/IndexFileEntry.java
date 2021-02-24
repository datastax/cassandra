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

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * An entry in the sstable index file, this is just to group
 * a {@link DecoratedKey} with its {@link RowIndexEntry}.
 */
public class IndexFileEntry
{
    public final static IndexFileEntry EMPTY = new IndexFileEntry();

    public final DecoratedKey key;
    public final RowIndexEntry entry;

    public IndexFileEntry()
    {
        this(null, null);
    }

    public IndexFileEntry(DecoratedKey key, RowIndexEntry entry)
    {
        this.key = key;
        this.entry = entry;
    }

    @Override
    public String toString()
    {
        return String.format("[key: %s, indexed: %s, rows: %d]",
                             key == null ? "null" : new String(ByteBufferUtil.getArrayUnsafe(key.getTempKey())),
                             entry == null ? false : entry.isIndexed(),
                             entry == null ? 0 : entry.rowIndexCount());
    }
}
