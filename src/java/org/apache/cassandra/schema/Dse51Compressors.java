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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.compress.DeflateCompressor;
import org.apache.cassandra.io.compress.Encryptor;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.net.DseCompatRefusals;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

/**
 * The compressors of the tables in the DSE 5.1 compatibility mode. DSE 5.1.51 reads three compressors of this software,
 * {@link LZ4Compressor}, {@link SnappyCompressor} and {@link DeflateCompressor}, by its source. An sstable compressed
 * with any other one cannot be opened by a DSE 5.1 node: it reports the sstable as corrupt and, with its default
 * {@code disk_failure_policy: stop}, does not start. The node flushes and compacts with the compressor of each table
 * (the compression selector and {@code flush_compression} are checked by {@link DatabaseDescriptor}), so in
 * {@code internode_compatibility_mode: DSE_5_1} it refuses to start while a table or a view of its schema is
 * compressed with another compressor, or encrypted: the encryption of this software is not supported in the mode. The
 * check runs once the schema is loaded and before any keyspace is opened.
 * <p>
 * The same check of one table's compression ({@link #check(CompressionParams)}) refuses, in the mode, a schema
 * statement that would give a table or a view such a compression ({@link #refusedStatementChanges}, called for every
 * schema statement, {@code allow_ddl} or not), and a schema change carrying one that reaches the node from another node
 * of this software ({@link #refusedSchemaMutations}, which the update handler does not apply): only a node not in the
 * mode can send one, a configuration mistake or a flag drop under way. It also refuses such compression parameters set
 * on a table by JMX ({@link #refusedLocalCompression}).
 */
public final class Dse51Compressors
{
    /** The compressors DSE 5.1 reads, by class. */
    private static final Set<Class<? extends ICompressor>> READ_BY_DSE51 = Set.of(LZ4Compressor.class,
                                                                                  SnappyCompressor.class,
                                                                                  DeflateCompressor.class);

    /** The reason under which the refused statements and the schema changes not applied are counted. */
    public static final String REFUSED_SCHEMA_CHANGE = "schema change with a compressor DSE 5.1 does not read";

    /** What the check of one table's compression finds ({@link #check(CompressionParams)}). */
    public enum Finding
    {
        /** No compression, or a compressor DSE 5.1 reads. */
        READABLE,
        /** A compressor DSE 5.1 does not read. */
        UNREADABLE,
        /** Compressed and encrypted by this software, which the mode does not support. */
        ENCRYPTED
    }

    private Dse51Compressors()
    {
    }

    /** The check of one table's compression, shared by the start, the schema statements and the changes received. */
    public static Finding check(CompressionParams compression)
    {
        if (!compression.isEnabled())
            return Finding.READABLE;
        if (compression.getSstableCompressor().encryptionOnly() != null)
            return Finding.ENCRYPTED;
        return READ_BY_DSE51.contains(compression.klass()) ? Finding.READABLE : Finding.UNREADABLE;
    }

    /**
     * A table named with its compressor, {@code keyspace.table (Compressor)}: the simple name for a compressor of
     * {@code org.apache.cassandra.io.compress}, the full class name for any other, so that it is not taken for one DSE
     * 5.1 reads.
     */
    private static String describe(String keyspace, String table, String compressorClass)
    {
        String prefix = LZ4Compressor.class.getPackage().getName() + '.';
        boolean ofThePackage = compressorClass.startsWith(prefix) && compressorClass.indexOf('.', prefix.length()) < 0;
        String compressor = ofThePackage ? compressorClass.substring(prefix.length()) : compressorClass;
        return keyspace + '.' + table + " (" + compressor + ')';
    }

    /**
     * In {@code DSE_5_1}, the refusal of the compression parameters set on a table by JMX
     * ({@code ColumnFamilyStoreMBean.setCompressionParameters}, a local override the schema does not hold, so that the
     * start check does not see it) when {@link #check} does not accept them, counted under the reason of the refused
     * schema changes ({@link #REFUSED_SCHEMA_CHANGE}: a change of a table's compression too); null when they are
     * accepted, or outside the mode.
     */
    public static String refusedLocalCompression(String keyspace, String table, CompressionParams params)
    {
        if (!DatabaseDescriptor.isDse51CompatibilityMode())
            return null;
        Finding finding = check(params);
        if (finding == Finding.READABLE)
            return null;
        DseCompatRefusals.refused(REFUSED_SCHEMA_CHANGE);
        String why = finding == Finding.ENCRYPTED
                     ? "they encrypt, which this mode does not support"
                     : "a DSE 5.1 node could not open the sstables this node writes with them";
        return "internode_compatibility_mode DSE_5_1 refuses the compression parameters of " +
               describe(keyspace, table, params.klass().getName()) + ": " + why + ". DSE 5.1 reads LZ4Compressor, " +
               "SnappyCompressor and DeflateCompressor; use one of them, or no compression";
    }

    /**
     * In {@code DSE_5_1}, the refusal of a schema statement that changes {@code before} into {@code after} so that a
     * table or a view is created, or its compression changed, with a compression {@link #check} does not accept; null
     * when there is none, or outside the mode. The refusal names each such table with its compressor
     * ({@link #unreadable} and {@link #encrypted} name them so) and the compressors DSE 5.1 reads. Only the tables and
     * views whose compression the change sets are checked: the start check refuses a schema that holds another one.
     */
    public static String refusedStatementChanges(Keyspaces before, Keyspaces after)
    {
        if (!DatabaseDescriptor.isDse51CompatibilityMode())
            return null;
        List<KeyspaceMetadata> changed = new ArrayList<>();
        for (KeyspaceMetadata keyspace : after)
        {
            KeyspaceMetadata previous = before.getNullable(keyspace.name);
            if (previous == keyspace)
                continue;
            Tables.Builder tables = Tables.builder();
            Views.Builder views = Views.builder();
            for (TableMetadata table : keyspace.tables)
                if (setsCompression(previous == null ? null : previous.getTableOrViewNullable(table.name), table))
                    tables.add(table);
            for (ViewMetadata view : keyspace.views)
                if (setsCompression(previous == null ? null : previous.getTableOrViewNullable(view.name()),
                                    view.metadata))
                    views.put(view);
            changed.add(keyspace.withSwapped(tables.build()).withSwapped(views.build()));
        }
        List<String> unreadable = unreadable(changed);
        List<String> encrypted = encrypted(changed);
        if (unreadable.isEmpty() && encrypted.isEmpty())
            return null;
        DseCompatRefusals.refused(REFUSED_SCHEMA_CHANGE);
        return statementRefusal(unreadable, encrypted);
    }

    private static boolean setsCompression(TableMetadata before, TableMetadata after)
    {
        return before == null || !before.params.compression.equals(after.params.compression);
    }

    /** The text of a refused schema statement, naming the tables and views {@link #refusedStatementChanges} found. */
    @VisibleForTesting
    static String statementRefusal(List<String> unreadable, List<String> encrypted)
    {
        List<String> reasons = new ArrayList<>();
        if (!unreadable.isEmpty())
            reasons.add(String.join(", ", unreadable) + " would be compressed with a compressor DSE 5.1 does not " +
                        "read, so a DSE 5.1 node could not open the sstables this node writes for " +
                        (unreadable.size() == 1 ? "it" : "them"));
        if (!encrypted.isEmpty())
            reasons.add(String.join(", ", encrypted) + " would be encrypted, which this mode does not support");
        return "internode_compatibility_mode DSE_5_1 refuses the statement: " + String.join("; ", reasons) +
               ". DSE 5.1 reads LZ4Compressor, SnappyCompressor and DeflateCompressor; use one of them, or no " +
               "compression";
    }

    /**
     * In {@code DSE_5_1}, the tables and views to which the schema {@code mutations} give a compression {@link #check}
     * does not accept, named as {@link #unreadable} names them (an encrypted one followed by {@code , encrypted}),
     * sorted; empty when there is none, or outside the mode. Read from the rows of {@code system_schema.tables} and
     * {@code system_schema.views} they carry, whose {@code compression} map every schema change of a table or a view
     * writes in full. A map that names no class this node can load is taken for one DSE 5.1 does not read.
     */
    public static List<String> refusedSchemaMutations(Collection<Mutation> mutations)
    {
        List<String> refused = new ArrayList<>();
        if (!DatabaseDescriptor.isDse51CompatibilityMode())
            return refused;
        for (Mutation mutation : mutations)
        {
            if (!SchemaConstants.SCHEMA_KEYSPACE_NAME.equals(mutation.getKeyspaceName()))
                continue;
            for (PartitionUpdate update : mutation.getPartitionUpdates())
            {
                String table = update.metadata().name;
                if (!SchemaKeyspaceTables.TABLES.equals(table) && !SchemaKeyspaceTables.VIEWS.equals(table))
                    continue;
                ColumnMetadata compressionColumn = update.metadata().getColumn(ByteBufferUtil.bytes("compression"));
                String keyspace = UTF8Type.instance.compose(update.partitionKey().getKey());
                for (Row row : (Iterable<Row>) update::rowIterator)
                {
                    Cell<?> cell = compressionColumn == null ? null : row.getCell(compressionColumn);
                    if (cell == null || !cell.isLive(FBUtilities.nowInSeconds()))
                        continue;
                    @SuppressWarnings("unchecked")
                    Map<String, String> map = (Map<String, String>) compressionColumn.type.compose(cell.buffer());
                    String name = UTF8Type.instance.compose(row.clustering().bufferAt(0));
                    String refusal = refusal(keyspace, name, map);
                    if (refusal != null)
                        refused.add(refusal);
                }
            }
        }
        refused.sort(null);
        return refused;
    }

    /**
     * The refusal of a table of a schema change by its compression map, or null when DSE 5.1 reads it. An encrypted
     * table is told by its class alone (the {@link Encryptor} or a subclass, resolved without initialising it), so that
     * the key provider of a schema this node refuses is not loaded here; the map of another class is parsed as the
     * stored schema is ({@link CompressionParams#fromStoredMap}), and one this node cannot parse is named by its class.
     */
    @VisibleForTesting
    static String refusal(String keyspace, String table, Map<String, String> compression)
    {
        String klass = compression.get(CompressionParams.CLASS);
        Class<?> compressorClass = klass == null ? null : compressorClass(klass);
        if (compressorClass != null && Encryptor.class.isAssignableFrom(compressorClass))
            return describe(keyspace, table, compressorClass.getName()) + ", encrypted";
        CompressionParams params;
        try
        {
            params = CompressionParams.fromStoredMap(compression, keyspace, table);
        }
        catch (RuntimeException e)
        {
            return describe(keyspace, table, String.valueOf(klass));
        }
        switch (check(params))
        {
            case UNREADABLE:
                return describe(keyspace, table, params.klass().getName());
            case ENCRYPTED:
                return describe(keyspace, table, params.klass().getName()) + ", encrypted";
            default:
                return null;
        }
    }

    /** The class a compression map names, as {@code CompressionParams} resolves it, not initialised; null if none. */
    private static Class<?> compressorClass(String name)
    {
        String className = name.contains(".") ? name : LZ4Compressor.class.getPackage().getName() + '.' + name;
        try
        {
            return Class.forName(className, false, Dse51Compressors.class.getClassLoader());
        }
        catch (ClassNotFoundException | LinkageError e)
        {
            return null;
        }
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
     * sorted, the class named as in {@link #unreadable}.
     */
    @VisibleForTesting
    static List<String> encrypted(Iterable<KeyspaceMetadata> keyspaces)
    {
        return tables(keyspaces, true);
    }

    private static List<String> tables(Iterable<KeyspaceMetadata> keyspaces, boolean encrypted)
    {
        List<String> tables = new ArrayList<>();
        Finding listed = encrypted ? Finding.ENCRYPTED : Finding.UNREADABLE;
        for (KeyspaceMetadata keyspace : keyspaces)
        {
            for (TableMetadata table : keyspace.tablesAndViews())
            {
                CompressionParams compression = table.params.compression;
                if (check(compression) == listed)
                    tables.add(describe(table.keyspace, table.name, compression.klass().getName()));
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
               "LZ4Compressor, SnappyCompressor and DeflateCompressor. Start this node without the mode, with -D" +
               CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEERS.getKey() + "=true (the " +
               "DSE 5.1 upgrade marker this node wrote at its start in the mode refuses that start otherwise), " +
               "keeping storage_compatibility_mode: HCD_1, sstable.selected_format: big and " +
               "uuid_sstable_identifiers_enabled: false, change the compression of the tables named to one of them, " +
               "rewrite their sstables (nodetool upgradesstables -a), then start the node in the mode again without " +
               "that property";
    }
}
