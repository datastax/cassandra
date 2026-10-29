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

package org.apache.cassandra.config;

/**
 * Which foreign internode dialect this node is willing to speak, on top of its own.
 * <p>
 * {@link #DSE_6} is a transitional mode for the in-place upgrade of a DataStax Enterprise 6.9 (or 6.8)
 * cluster. It keeps the node's on-disk output readable by DSE so that a node can be rolled back (trie
 * sstables of version {@link #DSE_6_TRIE_INDEX_FORMAT_VERSION} with numeric generations), and requires
 * {@code storage_compatibility_mode: HCD_1}, the {@code bti} sstable format and {@code paxos_variant: v1}.
 * Offline tools and clients inherit the on-disk half only. A node in the mode also speaks, with DSE 6 peers, the
 * pre-4.0 handshake and the Cassandra 3.0.14 messaging dialect DSE uses with non-DSE peers.
 * <p>
 * The mode is removed with a rolling restart once every node runs this software.
 */
public enum InternodeCompatibilityMode
{
    /** Only the native dialects of this software; the default. */
    NONE,

    /** The in-place upgrade from DataStax Enterprise 6.8/6.9; see the class comment. */
    DSE_6;

    /**
     * The trie index format version written while in {@link #DSE_6} mode: DSE's own current version.
     * DSE decodes every trie index with its own byte-comparable encoding, so a file written in a
     * newer version would be unreadable after a rollback, however compatible its metadata is.
     */
    public static final String DSE_6_TRIE_INDEX_FORMAT_VERSION = "bb";

    public boolean isDse6()
    {
        return this == DSE_6;
    }
}
