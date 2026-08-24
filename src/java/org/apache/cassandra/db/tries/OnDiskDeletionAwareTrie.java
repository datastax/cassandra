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

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.ChunkReader;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.SimpleChunkReader;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.io.util.RandomAccessReader.DEFAULT_BUFFER_SIZE;

/// Reads a [DeletionAwareTrie] written by [DeletionAwareFileWriter].
///
/// The data trie is read as an ordinary on-disk trie whose payload is a
/// [DeletionAwareFileWriter.Payload] — live content plus the position of the node's deletion
/// branch. A deletion branch is itself a complete range trie in the same stream, so
/// [Cursor#deletionBranchCursor] is just an [OnDiskCursor.Range] opened at that position.
///
/// See [DeletionAwareFileWriter] for why the branch position lives in the payload rather than
/// in the node encoding.
public class OnDiskDeletionAwareTrie<T, D extends RangeState<D>>
implements DeletionAwareTrie<T, D>, Closeable
{
    final Rebufferer rebufferer;
    final OnDiskCursor.DataDeserializer<DeletionAwareFileWriter.Payload<T>> payloadDeserializer;
    final OnDiskCursor.DataDeserializer<D> deletionDeserializer;
    final ByteComparable.Version byteComparableVersion;
    final long root;
    private final boolean ownsChannel;
    private final ChannelProxy channel;

    OnDiskDeletionAwareTrie(Rebufferer rebufferer,
                            OnDiskCursor.DataDeserializer<T> contentDeserializer,
                            OnDiskCursor.DataDeserializer<D> deletionDeserializer,
                            ByteComparable.Version byteComparableVersion,
                            long root,
                            ChannelProxy channel)
    {
        this.rebufferer = rebufferer;
        this.payloadDeserializer = new PayloadDeserializer<>(contentDeserializer);
        this.deletionDeserializer = deletionDeserializer;
        this.byteComparableVersion = byteComparableVersion;
        this.root = root;
        this.channel = channel;
        this.ownsChannel = channel != null;
    }

    /// Reads the payload written by [DeletionAwareFileWriter]: the branch position as a vint,
    /// followed by the content occupying whatever of the payload remains.
    static class PayloadDeserializer<T> implements OnDiskCursor.DataDeserializer<DeletionAwareFileWriter.Payload<T>>
    {
        final OnDiskCursor.DataDeserializer<T> contentDeserializer;

        PayloadDeserializer(OnDiskCursor.DataDeserializer<T> contentDeserializer)
        {
            this.contentDeserializer = contentDeserializer;
        }

        @Override
        public DeletionAwareFileWriter.Payload<T> deserialize(DataInputPlus rdr, int length) throws IOException
        {
            long encoded = rdr.readUnsignedVInt();
            long branchRoot = encoded - 1;
            // DataInputPlus does not expose a position, but a vint's encoded length is a function
            // of its value, so the bytes consumed can be recomputed exactly.
            int contentLength = length - VIntCoding.computeUnsignedVIntSize(encoded);
            T content = contentLength > 0 ? contentDeserializer.deserialize(rdr, contentLength) : null;
            return new DeletionAwareFileWriter.Payload<>(content, branchRoot);
        }
    }

    @Override
    public DeletionAwareCursor<T, D> makeCursor(Direction direction)
    {
        if (root == 0)
            return new DeletionAwareCursor.Empty<>(direction, byteComparableVersion);
        return new OnDiskDeletionAwareCursor<>(this, direction, root);
    }

    public ByteComparable.Version byteComparableVersion()
    {
        return byteComparableVersion;
    }

    /// Mirrors [OnDiskBaseTrie.WithOwnChannel]: the reader must be released before the rebufferer
    /// is closed, or `BufferManagingRebufferer.close` trips its outstanding-buffer assertion.
    /// Cursors are not closeable; the caller must ensure none are still in use.
    @Override
    public void close()
    {
        try
        {
            rebufferer.closeReader();
        }
        finally
        {
            if (ownsChannel)
            {
                try
                {
                    rebufferer.close();
                }
                finally
                {
                    channel.close();
                }
            }
        }
    }

    public static <T, D extends RangeState<D>> OnDiskDeletionAwareTrie<T, D> open(File file,
                                                                                  OnDiskCursor.DataDeserializer<T> contentDeserializer,
                                                                                  OnDiskCursor.DataDeserializer<D> deletionDeserializer,
                                                                                  ByteComparable.Version version,
                                                                                  long root)
    {
        ChannelProxy channel = new ChannelProxy(file);
        try
        {
            ChunkReader reader = new SimpleChunkReader(channel, -1, BufferType.OFF_HEAP, DEFAULT_BUFFER_SIZE);
            Rebufferer rebufferer = reader.instantiateRebufferer(false);
            return new OnDiskDeletionAwareTrie<>(rebufferer, contentDeserializer, deletionDeserializer, version,
                                                 root >= 0 ? root : reader.fileLength(), channel);
        }
        catch (Throwable t)
        {
            channel.close();
            throw t;
        }
    }

    /// Presents the payload trie as a deletion-aware cursor.
    ///
    /// The underlying cursor sets [Cursor#MAY_HAVE_CONTENT_BIT] whenever a payload is present, but a
    /// payload is also written for a node that carries only a deletion branch. Every returned position
    /// is therefore re-flagged from the decoded payload, so that the flags mean the same thing they do
    /// on an in-memory deletion-aware cursor. This costs a payload decode where a node-level encoding
    /// would not — see [DeletionAwareFileWriter] on that trade.
    static class OnDiskDeletionAwareCursor<T, D extends RangeState<D>> implements DeletionAwareCursor<T, D>
    {
        final OnDiskDeletionAwareTrie<T, D> trie;
        final OnDiskCursor<DeletionAwareFileWriter.Payload<T>> source;

        OnDiskDeletionAwareCursor(OnDiskDeletionAwareTrie<T, D> trie, Direction direction, long root)
        {
            this.trie = trie;
            this.source = new OnDiskCursor<>(trie.payloadDeserializer, trie.rebufferer,
                                             trie.byteComparableVersion, direction, false, root);
        }

        private DeletionAwareFileWriter.Payload<T> payload()
        {
            return (source.encodedPosition() & MAY_HAVE_CONTENT_BIT) != 0 ? source.content() : null;
        }

        /// Replace the source's single "has payload" flag with the two flags a deletion-aware cursor
        /// is expected to expose.
        private long reflag(long position)
        {
            if ((position & MAY_HAVE_CONTENT_BIT) == 0)
                return position;    // no payload here at all, nothing to correct

            DeletionAwareFileWriter.Payload<T> p = source.content();
            long flags = 0;
            if (p != null)
            {
                if (p.content != null)
                    flags |= MAY_HAVE_CONTENT_BIT;
                if (p.deletionBranchRoot >= 0)
                    flags |= MAY_HAVE_DELETION_BRANCH_BIT;
            }
            return (position & ~(MAY_HAVE_CONTENT_BIT | MAY_HAVE_DELETION_BRANCH_BIT)) | flags;
        }

        @Override
        public RangeCursor<D> deletionBranchCursor(Direction direction)
        {
            DeletionAwareFileWriter.Payload<T> p = payload();
            if (p == null || p.deletionBranchRoot < 0)
                return null;
            return new OnDiskCursor.Range<>(trie.deletionDeserializer, trie.rebufferer,
                                            trie.byteComparableVersion, direction, p.deletionBranchRoot);
        }

        @Override
        public T content()
        {
            DeletionAwareFileWriter.Payload<T> p = payload();
            return p != null ? p.content : null;
        }

        @Override
        public long encodedPosition()
        {
            return reflag(source.encodedPosition());
        }

        @Override
        public ByteComparable.Version byteComparableVersion()
        {
            return source.byteComparableVersion();
        }

        @Override
        public long advance()
        {
            return reflag(source.advance());
        }

        @Override
        public long advanceMultiple(TransitionsReceiver receiver)
        {
            return reflag(source.advanceMultiple(receiver));
        }

        @Override
        public long skipTo(long encodedSkipPosition)
        {
            return reflag(source.skipTo(encodedSkipPosition));
        }

        @Override
        public DeletionAwareCursor<T, D> tailCursor(Direction direction)
        {
            return new OnDiskDeletionAwareCursor<>(trie, direction, source.currentFullNode);
        }
    }
}
