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

package org.apache.cassandra.index.sai.disk.vector;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;

/**
 * Identifies one vector for the duration of a single graph merge, without ever reading the vector itself.
 * It is the key of the merge's postings map ({@code handle -> output rowids}).
 *
 * <p>Today a handle is the {@code (source segment index, node id)} of the node in a source graph whose
 * cell won the merge for an output row. Callers treat it as opaque: everything that depends on how a
 * vector is identified (the key layout, its serialized width, and the packing used for compact
 * per-ordinal arrays) lives here, so the identity can later become, for example, a content hash
 * without touching the merge code.
 *
 * @param source the index of the source segment in the merger's source list
 * @param nodeId the node id (ordinal) within that source's graph
 */
public record VectorHandle(int source, int nodeId)
{
    /** Serialized width of a handle in the postings map. Constant, so the map can use a fixed key size. */
    public static final int BYTES = 2 * Integer.BYTES;

    /** Packs this handle into a single {@code long}, for primitive arrays indexed by merged ordinal. */
    public long pack()
    {
        return ((long) source << 32) | (nodeId & 0xFFFFFFFFL);
    }

    /** Inverse of {@link #pack()}. */
    public static VectorHandle unpack(long packed)
    {
        return new VectorHandle((int) (packed >>> 32), (int) packed);
    }

    /** Chronicle key marshaller; every handle serializes to exactly {@link #BYTES} bytes. */
    public static class Marshaller implements BytesReader<VectorHandle>, BytesWriter<VectorHandle>
    {
        @Override
        public void write(Bytes out, VectorHandle handle)
        {
            out.writeInt(handle.source);
            out.writeInt(handle.nodeId);
        }

        @Override
        public VectorHandle read(Bytes in, VectorHandle using)
        {
            // records are immutable, so `using` cannot be reused
            return new VectorHandle(in.readInt(), in.readInt());
        }
    }
}
