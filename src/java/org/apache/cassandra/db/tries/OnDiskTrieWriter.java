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
/// path the walk presents it on. The layout of the result is determined by the [OnDiskNodeWriter] the nodes are
/// given to.
public class OnDiskTrieWriter<T> extends OnDiskBaseTrieWriter<T>
{
    public interface DataSerializer<T>
    {
        /// Writes `value` to `out`. The writer takes the content's length from how far `out` moved.
        ///
        /// Note: Implementations cannot assume that `out` is the file the trie is written to. In particular, its
        /// position will likely be different from the position where the content ends up in the file.
        void serialize(DataOutputPlus out, T value) throws IOException;
    }

    /// Create a writer that writes each node as soon as it is complete.
    public OnDiskTrieWriter(DataOutputPlus out, DataSerializer<T> dataSerializer, boolean swapAscentAndDescentSides)
    {
        this(new OnDiskNodeWriterImmediate<>(out, dataSerializer), swapAscentAndDescentSides);
    }

    /// Create a writer that gives the nodes of the trie to the given node writer.
    public OnDiskTrieWriter(OnDiskNodeWriter<T> nodeWriter, boolean swapAscentAndDescentSides)
    {
        super(nodeWriter, swapAscentAndDescentSides);
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
    ///
    /// Returns the position of the root of the trie, which is where the output is left, or 0 if the trie is empty.
    /// A writer of nested tries (see [OnDiskNodeWriter#nested]) also returns 0 for a trie whose root is not placed
    /// yet; [#hasRoot] tells the two apart.
    long writeFrom(Cursor<T> c) throws IOException
    {
        start();

        long position = c.encodedPosition();
        do
        {
            T content = Cursor.content(c, position);
            if (content != null)
                content(content, position);
            position = step(c, position);
        }
        while (!Cursor.isExhausted(position));
        return rootPosition;
    }

    /// Serialize a trie into `out`, writing each node as soon as it is complete. Returns the position of the root,
    /// which is the position of `out` when this returns, or 0 if the trie is empty; a non-empty trie always writes at
    /// least one byte, so its root is never 0. The value can be given to the readers as is: they take a root of 0 as
    /// the empty trie.
    public static <T> long write(BaseTrie<T, ?, ?> trie, boolean isOrdered, DataSerializer<T> serializer, DataOutputPlus out) throws IOException
    {
        return write(trie, isOrdered, serializer, out, OnDiskNodeWriter.Type.IMMEDIATE);
    }

    /// Serialize a trie into `out` with the layout the given type of node writer produces. Returns the position of
    /// the root as above.
    public static <T> long write(BaseTrie<T, ?, ?> trie, boolean isOrdered, DataSerializer<T> serializer, DataOutputPlus out, OnDiskNodeWriter.Type nodeWriterType) throws IOException
    {
        OnDiskTrieWriter<T> fw = new OnDiskTrieWriter<>(nodeWriterType.create(out, serializer), isOrdered);
        try (Cursor<T> c = trie.cursor(Direction.REVERSE))
        {
            // An empty trie leaves the stream untouched and has no root.
            return fw.writeFrom(c);
        }
    }

    public static <T> File write(BaseTrie<T, ?, ?> trie, boolean isOrdered, DataSerializer<T> serializer, File file) throws IOException
    {
        return write(trie, isOrdered, serializer, file, OnDiskNodeWriter.Type.IMMEDIATE);
    }

    public static <T> File write(BaseTrie<T, ?, ?> trie, boolean isOrdered, DataSerializer<T> serializer, File file, OnDiskNodeWriter.Type nodeWriterType) throws IOException
    {
        try (SequentialWriter writer = new SequentialWriter(file))
        {
            write(trie, isOrdered, serializer, writer, nodeWriterType);
            writer.finish();
            return file;
        }
    }
}
