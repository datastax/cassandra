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

package org.apache.cassandra.db.tries;

import java.io.IOException;

import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.SequentialWriter;

/// Serializes a plain or range trie in the on-disk trie format; see [OnDiskBaseTrieWriter] and
/// [OnDiskTrie.md](./OnDiskTrie.md). Content goes in the descent- or ascent-side slot of its node, according to the
/// path the walk presents it on.
public class OnDiskTrieWriter<T> extends OnDiskBaseTrieWriter<T>
{
    public interface DataSerializer<T>
    {
        /// Writes `value` to `out`. The writer takes the content's length from how far `out` moved.
        void serialize(DataOutputPlus out, T value) throws IOException;
    }

    public OnDiskTrieWriter(DataOutputPlus out, DataSerializer<T> dataSerializer, boolean swapAscentAndDescentSides)
    {
        super(out, dataSerializer, swapAscentAndDescentSides);
    }

    private void content(T content, long position)
    {
        if (Cursor.isOnReturnPath(position))
            ascentContent(content);
        else
            descentContent(content);
    }

    /// Write the trie the given fresh cursor walks, which must be in [Direction#REVERSE]. A writer can write several
    /// tries this way, one after the other.
    void writeFrom(Cursor<T> c) throws IOException
    {
        // A finished walk has written every node on its path, but leaves the path's length as it ended.
        assert lastNodeOnPath == -1 : "Previous walk did not finish";
        resetPathLength(0);

        long position = c.encodedPosition();
        T content = c.content();   // handle content on the root node
        if (content != null)
            content(content, position);

        while (!Cursor.isExhausted(position = step(c, position)))
        {
            content = c.content();
            if (content != null)
                content(content, position);
        }
    }

    /// Serialize a trie into `out`. Returns the position of the root, which is the position of `out` when this
    /// returns, or 0 if the trie is empty; a non-empty trie always writes at least one byte, so its root is never 0.
    /// The value can be given to the readers as is: they take a root of 0 as the empty trie.
    public static <T> long write(BaseTrie<T, ?, ?> trie, boolean isOrdered, DataSerializer<T> serializer, DataOutputPlus out) throws IOException
    {
        OnDiskTrieWriter<T> fw = new OnDiskTrieWriter<>(out, serializer, isOrdered);
        long startPosition = out.position();

        try (Cursor<T> c = trie.cursor(Direction.REVERSE))
        {
            fw.writeFrom(c);
        }
        // An empty trie leaves the stream untouched, and the position there is not a node.
        long position = out.position();
        return position > startPosition ? position : 0;
    }

    public static <T> File write(BaseTrie<T, ?, ?> trie, boolean isOrdered, DataSerializer<T> serializer, File file) throws IOException
    {
        try (SequentialWriter writer = new SequentialWriter(file))
        {
            write(trie, isOrdered, serializer, writer);
            writer.finish();
            return file;
        }
    }
}
