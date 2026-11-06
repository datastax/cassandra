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

package org.apache.cassandra.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.compress.DeflateCompressor;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.SnappyCompressor;

/**
 * The compressors of the tables in the DSE 5.1 compatibility mode. DSE 5.1.51 reads three compressors of this software,
 * {@link LZ4Compressor}, {@link SnappyCompressor} and {@link DeflateCompressor}, by its source. An sstable compressed
 * with any other one cannot be opened by a DSE 5.1 node: it reports the sstable as corrupt and, with its default
 * {@code disk_failure_policy: stop}, does not start. The node flushes and compacts with the compressor of each table
 * (the compression selector and {@code flush_compression} are checked by {@link DatabaseDescriptor}), so in
 * {@code internode_compatibility_mode: DSE_5_1} it refuses to start while a table or a view of its schema is
 * compressed with another compressor, or encrypted: the encryption of this software is not supported in the mode. The
 * check runs once the schema is loaded and before any keyspace is opened; what the node wrote for such a table while it
 * ran in the mode (a table created or altered then) stays on disk until its sstables are rewritten.
 */
public final class Dse51Compressors
{
    /** The compressors DSE 5.1 reads, by class. */
    private static final Set<Class<? extends ICompressor>> READ_BY_DSE51 = Set.of(LZ4Compressor.class,
                                                                                  SnappyCompressor.class,
                                                                                  DeflateCompressor.class);

    private Dse51Compressors()
    {
    }

    /** Runs the check in {@code DSE_5_1}; does nothing in the other modes. */
    public static void verifyAtStartup() throws StartupException
    {
        if (!DatabaseDescriptor.isDse51CompatibilityMode())
            return;
        List<KeyspaceMetadata> keyspaces = new ArrayList<>();
        for (String name : Schema.instance.getKeyspaces())
        {
            KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(name);
            if (keyspace != null)
                keyspaces.add(keyspace);
        }
        List<String> unreadable = unreadable(keyspaces);
        List<String> encrypted = encrypted(keyspaces);
        if (!unreadable.isEmpty() || !encrypted.isEmpty())
            throw new StartupException(StartupException.ERR_WRONG_CONFIG, refusal(unreadable, encrypted));
    }

    /**
     * Every table and view of {@code keyspaces} compressed with a compressor DSE 5.1 does not read, as
     * {@code keyspace.table (Compressor)}, sorted: the simple name for a compressor of
     * {@code org.apache.cassandra.io.compress}, the full class name for any other. An encrypted table is not in it
     * ({@link #encrypted}), and a table without compression is read by DSE 5.1.
     */
    @VisibleForTesting
    static List<String> unreadable(Iterable<KeyspaceMetadata> keyspaces)
    {
        return tables(keyspaces, false);
    }

    /**
     * Every table and view of {@code keyspaces} encrypted by this software, as {@code keyspace.table (Class)},
     * sorted.
     */
    @VisibleForTesting
    static List<String> encrypted(Iterable<KeyspaceMetadata> keyspaces)
    {
        return tables(keyspaces, true);
    }

    private static List<String> tables(Iterable<KeyspaceMetadata> keyspaces, boolean encrypted)
    {
        List<String> tables = new ArrayList<>();
        for (KeyspaceMetadata keyspace : keyspaces)
        {
            for (TableMetadata table : keyspace.tablesAndViews())
            {
                CompressionParams compression = table.params.compression;
                if (!compression.isEnabled())
                    continue;
                boolean isEncrypted = compression.getSstableCompressor().encryptionOnly() != null;
                boolean listed = encrypted ? isEncrypted : !isEncrypted && !READ_BY_DSE51.contains(compression.klass());
                if (listed)
                {
                    // a class of another package by its full name, so that it is not taken for one DSE 5.1 reads
                    Class<?> klass = compression.klass();
                    String compressor = LZ4Compressor.class.getPackage().equals(klass.getPackage())
                                        ? klass.getSimpleName() : klass.getName();
                    tables.add(table.keyspace + '.' + table.name + " (" + compressor + ')');
                }
            }
        }
        tables.sort(null);
        return tables;
    }

    /** The refusal of the start, naming every table and view {@link #unreadable} or {@link #encrypted} found. */
    @VisibleForTesting
    static String refusal(List<String> unreadable, List<String> encrypted)
    {
        List<String> reasons = new ArrayList<>();
        if (!unreadable.isEmpty())
            reasons.add(String.join(", ", unreadable) + (unreadable.size() == 1 ? " is" : " are") + " compressed " +
                        "with a compressor DSE 5.1 does not read, so a DSE 5.1 node could not open the sstables " +
                        "this node writes for " + (unreadable.size() == 1 ? "it" : "them"));
        if (!encrypted.isEmpty())
            reasons.add(String.join(", ", encrypted) + (encrypted.size() == 1 ? " is" : " are") + " encrypted, " +
                        "which this mode does not support");
        return "internode_compatibility_mode DSE_5_1 refused: " + String.join("; ", reasons) + ". DSE 5.1 reads " +
               "LZ4Compressor, SnappyCompressor and DeflateCompressor. Start this node without the mode, keeping " +
               "sstable.selected_format: big and uuid_sstable_identifiers_enabled: false, change the " +
               "compression of the tables named to one of them, rewrite their sstables (nodetool upgradesstables " +
               "-a), then start the node in the mode again";
    }
}
