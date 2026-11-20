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
 * pre-4.0 handshake and the Cassandra 3.0.14 messaging dialect DSE uses with non-DSE peers, with DSE's gossip
 * states, and refuses the operations a mixed cluster cannot run (topology changes, repair, sstable rewrites, schema
 * changes).
 * <p>
 * {@link #DSE_5_1} is the mode of the in-place upgrade of a DataStax Enterprise 5.1.51 cluster. It requires
 * {@code storage_compatibility_mode: DSE_5_1} (which requires it in turn), the {@code big} sstable format and
 * {@code paxos_variant: v1}, writes numeric sstable generations, refuses the compressors DSE 5.1 does not read (and a
 * {@code min_compress_ratio}), the column types it does not read, a checksum type other than CRC32, transient
 * replication and incremental repair, and refuses the authenticators and authorizers that read tables of
 * {@code system_auth} DSE 5.1 does not have. At start it refuses the files a node in the mode never writes (sstables of
 * the {@code bti} format or another format than {@code big}, big sstables newer than {@link #DSE_51_BIG_VERSION},
 * non-numeric generations, commit log segments above version 7), writes the marker of the upgrade from DSE 5.1 (which
 * refuses a later start with {@code NONE} or {@code DSE_6}) and moves aside the commit log segments of DSE 5.1 it
 * replayed; it writes the transaction logs and the sstable statistics as DSE 5.1 reads them. The node writes big
 * sstables of version {@link #DSE_51_BIG_VERSION}, the version DSE 5.1 writes and reads; its code reads every big
 * version up to {@code oa}, but a start in the mode refuses the big sstables newer than {@link #DSE_51_BIG_VERSION}, an
 * entire-sstable stream of any version but the big 3.x ones is refused, and the guards of the mode refuse every import
 * and refresh. A node in the mode speaks, with DSE 5.1 peers, the pre-4.0 handshake and the Apache Cassandra 3.11
 * dialect DSE 5.1 uses (messaging version 11, with Apache Cassandra 3.11's limit ordinals), with this software's gossip
 * ordinals, and applies the operational guards and the schema freeze of {@link #DSE_6} (a schema statement, a schema
 * change received or a JMX compression change that sets a compressor DSE 5.1 does not read is refused too), except
 * that {@code upgradesstables} and {@code recompress_sstables} run, since every rewrite writes big
 * {@link #DSE_51_BIG_VERSION}; it neither sends its schema to DSE 5.1 nodes nor applies theirs, and refuses their
 * repair messages and their reads that carry row-level access control. It refuses to start, as {@link #DSE_6} does,
 * while DSE access controls it does not enforce exist (row-level grants, tables and views restricted to rows) or any
 * trace of the DSE features it does not support (Search, Graph, Analytics, DSEFS, CFS, Advanced Replication, tiered
 * storage, in-memory tables); it also refuses, unlike {@link #DSE_6}, a table or view on a compaction strategy this
 * software no longer has, and creates no SAI index, even with the schema freeze lifted. Offline tools inherit only the
 * checks of the on-disk settings and the transaction logs, statistics and sstable version of the mode (they write big
 * {@link #DSE_51_BIG_VERSION} too, without the storage mode); clients, which do not read {@code cassandra.yaml}, get
 * the {@code big} format and numeric generations forced, and write big {@link #DSE_51_BIG_VERSION}.
 * <p>
 * The mode is removed with a rolling restart once every node runs this software.
 */
public enum InternodeCompatibilityMode
{
    /** Only the native dialects of this software; the default. */
    NONE(null),

    /** The in-place upgrade from DataStax Enterprise 6.8/6.9; see the class comment. */
    DSE_6("DSE 6"),

    /** The in-place upgrade from DataStax Enterprise 5.1.51; see the class comment. */
    DSE_5_1("DSE 5.1");

    /**
     * The trie index format version written while in {@link #DSE_6} mode: DSE's own current version.
     * DSE decodes every trie index with its own byte-comparable encoding, so a file written in a
     * newer version would be unreadable after a rollback, however compatible its metadata is.
     */
    public static final String DSE_6_TRIE_INDEX_FORMAT_VERSION = "bb";

    /**
     * The big sstable version written while in {@link #DSE_5_1} mode (the storage mode of the same name), by the node,
     * the offline tools run with its configuration and the clients: DSE 5.1's own current version, the newest it reads.
     */
    public static final String DSE_51_BIG_VERSION = "me";

    private final String source;

    InternodeCompatibilityMode(String source)
    {
        this.source = source;
    }

    /** True for {@link #DSE_6}. */
    public boolean isDse6()
    {
        return this == DSE_6;
    }

    /** True for {@link #DSE_5_1}. */
    public boolean isDse51()
    {
        return this == DSE_5_1;
    }

    /** True for a mode of an upgrade from DSE: {@link #DSE_6} or {@link #DSE_5_1}. */
    public boolean isDse()
    {
        return source != null;
    }

    /**
     * The DSE release the mode upgrades from, as the texts of the mode name it ({@code DSE 6}, {@code DSE 5.1});
     * null for {@link #NONE}.
     */
    public String source()
    {
        return source;
    }
}
