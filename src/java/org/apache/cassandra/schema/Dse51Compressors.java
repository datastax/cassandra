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

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Directories;
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
import org.apache.cassandra.io.util.File;
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
 * check runs once the schema is loaded and before any keyspace is opened. A table given a {@code min_compress_ratio}
 * is refused too: the big {@code me} sstables written for DSE 5.1 have no uncompressed chunk (DSE 5.1 decompresses
 * every chunk), so the node would ignore it. Later in the start it also refuses to start while an sstable on disk
 * names such a compressor, or encryption, in its compression info, whatever the schema of its table says
 * ({@link #verifySSTablesAtStartup}).
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
    private static final Logger logger = LoggerFactory.getLogger(Dse51Compressors.class);

    /** The compressors DSE 5.1 reads, by class. */
    private static final Set<Class<? extends ICompressor>> READ_BY_DSE51 = Set.of(LZ4Compressor.class,
                                                                                  SnappyCompressor.class,
                                                                                  DeflateCompressor.class);

    /** The end of every refusal text: what DSE 5.1 reads. */
    private static final String READ_BY_DSE51_TEXT = "DSE 5.1 reads LZ4Compressor, SnappyCompressor and " +
                                                     "DeflateCompressor, every chunk compressed";

    /**
     * How the advice has the sstables rewritten: by {@code nodetool upgradesstables -a}, which the guards of the mode
     * let run in {@code DSE_5_1}, and which rewrites every sstable of the table, and of the tables of its secondary
     * indexes, which take its compression, at the version the node writes (big {@code me}) with its new compression,
     * purging nothing a compaction would not; a table refused for its ratio alone has sstables DSE 5.1 reads already.
     */
    private static final String REWRITE_COMMANDS =
        "nodetool upgradesstables -a <keyspace> <table>, which rewrites the sstables of the table and of its " +
        "secondary indexes";

    /** The rewrite in the advice of the refusals of the schema, see {@link #REWRITE_COMMANDS}. */
    private static final String REWRITE_TEXT =
        "rewrite the sstables of those with a compressor DSE 5.1 does not read or encrypted (" + REWRITE_COMMANDS +
        "; a table named for its min_compress_ratio alone needs no rewrite)";

    /** Why a table's {@code min_compress_ratio}, named in the refusals, is ignored. */
    private static final String RATIO_IGNORED_REASON =
        "the sstables this node writes for DSE 5.1 have no uncompressed chunk";

    /** Why a {@code min_compress_ratio} is refused. */
    private static final String MIN_COMPRESS_RATIO_REASON = "the sstables this node writes for DSE 5.1 have no " +
                                                            "uncompressed chunk, so it would be ignored";

    /** The start of the refusal of a schema statement in the mode, for its compression or for a column type. */
    public static final String STATEMENT_REFUSAL = "internode_compatibility_mode DSE_5_1 refuses the statement: ";

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
        ENCRYPTED,
        /**
         * A compressor DSE 5.1 reads, with a {@code min_compress_ratio}, which would store uncompressed chunks: the
         * sstables written for DSE 5.1 cannot have any, so the node ignores it.
         */
        MIN_COMPRESS_RATIO
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
        if (!READ_BY_DSE51.contains(compression.klass()))
            return Finding.UNREADABLE;
        return compression.maxCompressedLength() == Integer.MAX_VALUE ? Finding.READABLE : Finding.MIN_COMPRESS_RATIO;
    }

    /** {@link #describe} for a table {@link #check} finds {@code finding} in: the ratio named, for its own finding. */
    private static String describe(String keyspace, String table, CompressionParams compression, Finding finding)
    {
        String described = describe(keyspace, table, compression.klass().getName());
        if (finding == Finding.MIN_COMPRESS_RATIO)
            return described + ", " + CompressionParams.MIN_COMPRESS_RATIO + ' ' +
                   compression.asMap().get(CompressionParams.MIN_COMPRESS_RATIO);
        return described;
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
        String why;
        switch (finding)
        {
            case ENCRYPTED:
                why = "they encrypt, which this mode does not support";
                break;
            case MIN_COMPRESS_RATIO:
                why = MIN_COMPRESS_RATIO_REASON;
                break;
            default:
                why = "a DSE 5.1 node could not open the sstables this node writes with them";
        }
        return "internode_compatibility_mode DSE_5_1 refuses the compression parameters of " +
               describe(keyspace, table, params, finding) + ": " + why + ". " + READ_BY_DSE51_TEXT +
               "; use one of them without " + CompressionParams.MIN_COMPRESS_RATIO + ", or no compression";
    }

    /**
     * In {@code DSE_5_1}, the refusal of a schema statement that changes {@code before} into {@code after} so that a
     * table or a view is created, or its compression changed, with a compression {@link #check} does not accept; null
     * when there is none, or outside the mode. The refusal names each such table with its compressor
     * ({@link #unreadable}, {@link #encrypted} and {@link #withMinCompressRatio} name them so) and the compressors
     * DSE 5.1 reads. Only the tables and
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
        List<String> withRatio = withMinCompressRatio(changed);
        if (unreadable.isEmpty() && encrypted.isEmpty() && withRatio.isEmpty())
            return null;
        DseCompatRefusals.refused(REFUSED_SCHEMA_CHANGE);
        return statementRefusal(unreadable, encrypted, withRatio);
    }

    private static boolean setsCompression(TableMetadata before, TableMetadata after)
    {
        return before == null || !before.params.compression.equals(after.params.compression);
    }

    /** The text of a refused schema statement, naming the tables and views {@link #refusedStatementChanges} found. */
    @VisibleForTesting
    static String statementRefusal(List<String> unreadable, List<String> encrypted, List<String> withRatio)
    {
        List<String> reasons = new ArrayList<>();
        if (!unreadable.isEmpty())
            reasons.add(String.join(", ", unreadable) + " would be compressed with a compressor DSE 5.1 does not " +
                        "read, so a DSE 5.1 node could not open the sstables this node writes for " +
                        (unreadable.size() == 1 ? "it" : "them"));
        if (!encrypted.isEmpty())
            reasons.add(String.join(", ", encrypted) + " would be encrypted, which this mode does not support");
        if (!withRatio.isEmpty())
            reasons.add(String.join(", ", withRatio) + " would be ignored: " + RATIO_IGNORED_REASON);
        return STATEMENT_REFUSAL + String.join("; ", reasons) + ". " + READ_BY_DSE51_TEXT + "; use one of them " +
               "without " + CompressionParams.MIN_COMPRESS_RATIO + ", or no compression";
    }

    /**
     * In {@code DSE_5_1}, the tables and views to which the schema {@code mutations} give a compression {@link #check}
     * does not accept, named as {@link #unreadable} names them (an encrypted one followed by {@code , encrypted}),
     * sorted; empty when there is none, or outside the mode. Read from the rows of {@code system_schema.tables} and
     * {@code system_schema.views} they carry, whose {@code compression} map every schema change of a table or a view
     * writes in full. A map that names no class this node can load is taken for one DSE 5.1 does not read; one with a
     * {@code min_compress_ratio} is named followed by it.
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
            case MIN_COMPRESS_RATIO:
                return describe(keyspace, table, params, Finding.MIN_COMPRESS_RATIO);
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

    /**
     * Runs the check of the schema in {@code DSE_5_1}; does nothing in the other modes. With
     * {@code -Dcassandra.internode_compatibility_mode.ignore_dse51_compression=true} what the check finds is logged at
     * WARN instead, for the start that changes the compression of the tables named. The sstables on disk are checked
     * later in the start, by {@link #verifySSTablesAtStartup}.
     */
    public static void verifyAtStartup() throws StartupException
    {
        if (!DatabaseDescriptor.isDse51CompatibilityMode())
            return;
        List<KeyspaceMetadata> keyspaces = keyspaces();
        List<String> unreadable = unreadable(keyspaces);
        List<String> encrypted = encrypted(keyspaces);
        List<String> withRatio = withMinCompressRatio(keyspaces);
        if (unreadable.isEmpty() && encrypted.isEmpty() && withRatio.isEmpty())
            return;
        if (CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE51_COMPRESSION.getBoolean())
        {
            logger.warn("{}", ignoredRefusal(unreadable, encrypted, withRatio));
            return;
        }
        throw new StartupException(StartupException.ERR_WRONG_CONFIG, refusal(unreadable, encrypted, withRatio));
    }

    /**
     * Runs the check of the sstables on disk in {@code DSE_5_1} ({@link #sstablesOnDisk}); does nothing in the other
     * modes. It runs once the leftovers of the transactions that did not finish are removed from the data directories
     * (so that the files of a rewrite that finished, and those of one that did not, are not taken for sstables) and
     * before any keyspace is opened. With
     * {@code -Dcassandra.internode_compatibility_mode.ignore_dse51_compression=true} what it finds is logged at WARN
     * instead. A compression info file that cannot be read, or a directory that cannot be listed, is logged at WARN
     * ({@link #notReadWarning}), neither refused nor counted in {@code compatstatus}: an sstable whose compression
     * info cannot be read is damaged, which the regular open of the sstables handles under the
     * {@code disk_failure_policy}, and the next start in the mode checks it again.
     */
    public static void verifySSTablesAtStartup() throws StartupException
    {
        if (!DatabaseDescriptor.isDse51CompatibilityMode())
            return;
        List<TableMetadata> tables = new ArrayList<>();
        for (KeyspaceMetadata keyspace : keyspaces())
            keyspace.tablesAndViews().forEach(tables::add);
        SSTablesOnDisk onDisk = sstablesOnDisk(tables);
        if (!onDisk.notRead.isEmpty())
            logger.warn("{}", notReadWarning(onDisk.notRead));
        if (onDisk.unreadableByDse51.isEmpty())
            return;
        if (CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE51_COMPRESSION.getBoolean())
        {
            logger.warn("{}", ignoredFilesRefusal(onDisk.unreadableByDse51));
            return;
        }
        throw new StartupException(StartupException.ERR_WRONG_CONFIG, filesRefusal(onDisk.unreadableByDse51));
    }

    private static List<KeyspaceMetadata> keyspaces()
    {
        List<KeyspaceMetadata> keyspaces = new ArrayList<>();
        for (String name : Schema.instance.getKeyspaces())
        {
            KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(name);
            if (keyspace != null)
                keyspaces.add(keyspace);
        }
        return keyspaces;
    }

    /** What {@link #sstablesOnDisk} finds. */
    @VisibleForTesting
    static final class SSTablesOnDisk
    {
        /**
         * By table ({@code keyspace.table}, or {@code keyspace.table index <name>} for the table of a secondary index),
         * the compression info files whose compressor DSE 5.1 does not read, or that are encrypted, as
         * {@code <file> (<compressor>)}, sorted.
         */
        final SortedMap<String, List<String>> unreadableByDse51 = new TreeMap<>();

        /**
         * The compression info files whose compressor cannot be read, and the table directories that cannot be
         * listed, as {@code <path> (<error>)}, sorted.
         */
        final List<String> notRead = new ArrayList<>();
    }

    /**
     * The compression info files of the sstables of {@code tables} (and of their secondary indexes, in the
     * subdirectories of their directories named after them), in every data directory, symbolic links followed: the
     * compressor class is the first field of the file, the class loaded without being initialised. Snapshots, backups
     * and any other subdirectory are not inspected, nor the directories of a table or an index no longer in the schema,
     * which DSE 5.1 does not open either. The directories of each table are those {@link Directories} gives, which
     * creates those missing, as the keyspaces opened next in the start do. Such an sstable is left when a table's
     * compression was changed and its sstables not all rewritten since (an {@code upgradesstables} that did not run, or
     * did not finish): the check of the schema alone would accept it.
     */
    @VisibleForTesting
    static SSTablesOnDisk sstablesOnDisk(Iterable<TableMetadata> tables)
    {
        SSTablesOnDisk onDisk = new SSTablesOnDisk();
        for (TableMetadata table : tables)
        {
            String name = table.keyspace + '.' + table.name;
            Set<String> indexDirectories = new HashSet<>();
            for (IndexMetadata index : table.indexes)
                indexDirectories.add(Directories.SECONDARY_INDEX_NAME_SEPARATOR + index.name);
            for (File directory : new Directories(table).getCFDirectories())
            {
                Path tableDirectory = directory.toPath();
                try
                {
                    // <table directory>/<sstable>, and <table directory>/.<index>/<sstable>; a file or a directory
                    // that cannot be read is named and the walk goes on
                    Files.walkFileTree(tableDirectory, EnumSet.of(FileVisitOption.FOLLOW_LINKS), 2,
                                       new SimpleFileVisitor<>()
                    {
                        @Override
                        public FileVisitResult preVisitDirectory(Path path, BasicFileAttributes attributes)
                        {
                            // the directories of the table's indexes, not snapshots, backups or any other
                            boolean walked = path.equals(tableDirectory) ||
                                             indexDirectories.contains(path.getFileName().toString());
                            return walked ? FileVisitResult.CONTINUE : FileVisitResult.SKIP_SUBTREE;
                        }

                        @Override
                        public FileVisitResult visitFile(Path path, BasicFileAttributes attributes)
                        {
                            String fileName = path.getFileName().toString();
                            if (attributes.isDirectory() || !fileName.endsWith("-CompressionInfo.db"))
                                return FileVisitResult.CONTINUE;
                            String compressor;
                            try
                            {
                                compressor = compressorOf(path);
                            }
                            catch (IOException e)
                            {
                                onDisk.notRead.add(path + " (" + describe(path, e) + ')');
                                return FileVisitResult.CONTINUE;
                            }
                            if (isReadByDse51(compressor))
                                return FileVisitResult.CONTINUE;
                            Path relative = tableDirectory.relativize(path);
                            String owner = relative.getNameCount() > 1
                                           ? name + " index " + relative.getName(0).toString().substring(1)
                                           : name;
                            onDisk.unreadableByDse51.computeIfAbsent(owner, k -> new ArrayList<>())
                                                    .add(path + " (" + compressor + ')');
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult visitFileFailed(Path path, IOException e)
                        {
                            onDisk.notRead.add(path + " (" + describe(path, e) + ')');
                            return FileVisitResult.CONTINUE;
                        }
                    });
                }
                catch (IOException e)
                {
                    onDisk.notRead.add(tableDirectory + " (" + describe(tableDirectory, e) + ')');
                }
            }
        }
        onDisk.unreadableByDse51.values().forEach(files -> files.sort(null));
        onDisk.notRead.sort(null);
        return onDisk;
    }

    /** The error of {@code path}: its class, and its message unless it is the path alone. */
    private static String describe(Path path, IOException e)
    {
        String message = e.getMessage();
        return message == null || message.equals(path.toString()) ? e.getClass().getSimpleName()
                                                                  : e.getClass().getSimpleName() + ": " + message;
    }

    /** The compressor class name at the head of a compression info file. */
    private static String compressorOf(Path compressionInfo) throws IOException
    {
        try (DataInputStream in = new DataInputStream(Files.newInputStream(compressionInfo)))
        {
            return in.readUTF();
        }
    }

    /**
     * Whether DSE 5.1 reads the compressor of {@code name} (a simple or a full class name), the class loaded without
     * being initialised.
     */
    private static boolean isReadByDse51(String name)
    {
        Class<?> klass = compressorClass(name);
        return klass != null && READ_BY_DSE51.contains(klass);
    }

    /** The most compression info files of one table a text names. */
    private static final int FILES_NAMED_PER_TABLE = 3;

    /**
     * The sstables {@link #sstablesOnDisk} found DSE 5.1 cannot read, by table: how many, and the first
     * {@value #FILES_NAMED_PER_TABLE} compression info files.
     */
    private static String describeFiles(SortedMap<String, List<String>> unreadableByDse51)
    {
        List<String> tables = new ArrayList<>();
        unreadableByDse51.forEach((table, files) -> {
            List<String> named = files.subList(0, Math.min(files.size(), FILES_NAMED_PER_TABLE));
            StringBuilder text = new StringBuilder(table).append(": ").append(files.size())
                                                         .append(files.size() == 1 ? " sstable, " : " sstables, ")
                                                         .append(String.join(", ", named));
            if (files.size() > FILES_NAMED_PER_TABLE)
                text.append(" and ").append(files.size() - FILES_NAMED_PER_TABLE).append(" more");
            tables.add(text.toString());
        });
        return String.join("; ", tables);
    }

    private static int count(SortedMap<String, List<String>> unreadableByDse51)
    {
        return unreadableByDse51.values().stream().mapToInt(List::size).sum();
    }

    /** The refusal of the start while sstables {@link #sstablesOnDisk} found DSE 5.1 cannot read remain. */
    @VisibleForTesting
    static String filesRefusal(SortedMap<String, List<String>> unreadableByDse51)
    {
        int count = count(unreadableByDse51);
        boolean oneTable = unreadableByDse51.size() == 1;
        return "internode_compatibility_mode DSE_5_1 refused: " + count + (count == 1 ? " sstable" : " sstables") +
               " on disk " + (count == 1 ? "is" : "are") + " compressed with a compressor DSE 5.1 does not read, or " +
               "encrypted, although the schema of " + (oneTable ? "its table" : "their tables") + " is not (" +
               describeFiles(unreadableByDse51) + "): a rewrite of them did not finish, or did not run since the " +
               "compression of " + (oneTable ? "that table" : "those tables") + " changed, and a DSE 5.1 node " +
               "could not open them. " + READ_BY_DSE51_TEXT + ". Start this node once more in the mode with -D" +
               CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE51_COMPRESSION.getKey() + "=true, " +
               "which logs this at WARN instead; then rewrite the sstables of " +
               (oneTable ? "that table" : "those tables") + " (" + REWRITE_COMMANDS + "), and restart the node in " +
               "the mode without that property";
    }

    /** The WARN of a start with {@code ignore_dse51_compression} while such sstables remain. */
    @VisibleForTesting
    static String ignoredFilesRefusal(SortedMap<String, List<String>> unreadableByDse51)
    {
        int count = count(unreadableByDse51);
        boolean oneTable = unreadableByDse51.size() == 1;
        return "internode_compatibility_mode DSE_5_1 starts with -D" +
               CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE51_COMPRESSION.getKey() + "=true " +
               "although " + count + (count == 1 ? " sstable" : " sstables") + " on disk " +
               (count == 1 ? "is" : "are") + " compressed with a compressor DSE 5.1 does not read, or encrypted (" +
               describeFiles(unreadableByDse51) + "): rewrite the sstables of " +
               (oneTable ? "that table" : "those tables") + " (" + REWRITE_COMMANDS + "), and restart the node " +
               "without that property: until then a DSE 5.1 node could not open them";
    }

    /** The most paths {@link #notReadWarning} names. */
    private static final int PATHS_NOT_READ_NAMED = 10;

    /**
     * The WARN naming the files and directories {@link #sstablesOnDisk} could not read, the first
     * {@value #PATHS_NOT_READ_NAMED} of them.
     */
    @VisibleForTesting
    static String notReadWarning(List<String> notRead)
    {
        List<String> named = notRead.subList(0, Math.min(notRead.size(), PATHS_NOT_READ_NAMED));
        int more = notRead.size() - named.size();
        return "internode_compatibility_mode DSE_5_1 could not check the compressor of the sstables at " +
               String.join(", ", named) + (more > 0 ? " and " + more + " more" : "") +
               ": whether a DSE 5.1 node could open them is not known; check those paths before a rollback to DSE 5.1";
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
        return tables(keyspaces, Finding.UNREADABLE);
    }

    /**
     * Every table and view of {@code keyspaces} encrypted by this software, as {@code keyspace.table (Class)},
     * sorted, the class named as in {@link #unreadable}.
     */
    @VisibleForTesting
    static List<String> encrypted(Iterable<KeyspaceMetadata> keyspaces)
    {
        return tables(keyspaces, Finding.ENCRYPTED);
    }

    /**
     * Every table and view of {@code keyspaces} compressed with a compressor DSE 5.1 reads and a
     * {@code min_compress_ratio}, as {@code keyspace.table (Compressor), min_compress_ratio <ratio>}, sorted.
     */
    @VisibleForTesting
    static List<String> withMinCompressRatio(Iterable<KeyspaceMetadata> keyspaces)
    {
        return tables(keyspaces, Finding.MIN_COMPRESS_RATIO);
    }

    private static List<String> tables(Iterable<KeyspaceMetadata> keyspaces, Finding listed)
    {
        List<String> tables = new ArrayList<>();
        for (KeyspaceMetadata keyspace : keyspaces)
        {
            for (TableMetadata table : keyspace.tablesAndViews())
            {
                CompressionParams compression = table.params.compression;
                if (check(compression) == listed)
                    tables.add(describe(table.keyspace, table.name, compression, listed));
            }
        }
        tables.sort(null);
        return tables;
    }

    /**
     * The refusal of the start, naming every table and view {@link #unreadable}, {@link #encrypted} or
     * {@link #withMinCompressRatio} found, with the remedy, which keeps every sstable at the version DSE 5.1 reads: one
     * more start in the mode with {@code ignore_dse51_compression}, in which their compression is changed and their
     * sstables are rewritten.
     */
    @VisibleForTesting
    static String refusal(List<String> unreadable, List<String> encrypted, List<String> withRatio)
    {
        return "internode_compatibility_mode DSE_5_1 refused: " + reasons(unreadable, encrypted, withRatio) + ". " +
               READ_BY_DSE51_TEXT + ". Start this node once more in the mode with -D" +
               CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE51_COMPRESSION.getKey() + "=true, " +
               "which logs this at WARN instead; then change the compression of the tables named to one of them " +
               "without " + CompressionParams.MIN_COMPRESS_RATIO + ", or to none (ALTER TABLE or ALTER MATERIALIZED " +
               "VIEW, with -D" + CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL.getKey() +
               "=true too), " + REWRITE_TEXT + ", and restart the node in the mode without those properties";
    }

    /** The WARN of a start with {@code ignore_dse51_compression}, naming what {@link #refusal} would refuse. */
    @VisibleForTesting
    static String ignoredRefusal(List<String> unreadable, List<String> encrypted, List<String> withRatio)
    {
        return "internode_compatibility_mode DSE_5_1 starts with -D" +
               CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE51_COMPRESSION.getKey() + "=true " +
               "although " + reasons(unreadable, encrypted, withRatio) + ". " + READ_BY_DSE51_TEXT + ". Change the " +
               "compression of the tables named to one of them without " + CompressionParams.MIN_COMPRESS_RATIO +
               ", or to none (ALTER TABLE or ALTER MATERIALIZED VIEW, with -D" +
               CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_ALLOW_DDL.getKey() + "=true), " + REWRITE_TEXT +
               ", and restart the node without those properties" +
               (unreadable.isEmpty() && encrypted.isEmpty()
                ? ""
                : ": until then a DSE 5.1 node could not open the sstables this node writes for the tables of " +
                  "those compressors");
    }

    private static String reasons(List<String> unreadable, List<String> encrypted, List<String> withRatio)
    {
        List<String> reasons = new ArrayList<>();
        if (!unreadable.isEmpty())
            reasons.add(String.join(", ", unreadable) + (unreadable.size() == 1 ? " is" : " are") + " compressed " +
                        "with a compressor DSE 5.1 does not read, so a DSE 5.1 node could not open the sstables " +
                        "this node writes for " + (unreadable.size() == 1 ? "it" : "them"));
        if (!encrypted.isEmpty())
            reasons.add(String.join(", ", encrypted) + (encrypted.size() == 1 ? " is" : " are") + " encrypted, " +
                        "which this mode does not support");
        if (!withRatio.isEmpty())
            reasons.add(String.join(", ", withRatio) + (withRatio.size() == 1 ? " is" : " are") + " ignored: " +
                        RATIO_IGNORED_REASON);
        return String.join("; ", reasons);
    }
}
