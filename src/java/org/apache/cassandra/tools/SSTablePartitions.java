/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 *
 */
package org.apache.cassandra.tools;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

/**
 * Export SSTables to JSON format.
 */
public class SSTablePartitions
{

    private static final String KEY_OPTION = "k";
    private static final String EXCLUDE_KEY_OPTION = "x";
    private static final String RECURSIVE_OPTION = "r";
    private static final String SNAPSHOTS_OPTION = "s";
    private static final String BACKUPS_OPTION = "b";
    private static final String PARTITIONS_ONLY_OPTION = "y";
    private static final String SIZE_THRESHOLD_OPTION = "t";
    private static final String TOMBSTONE_THRESHOLD_OPTION = "o";
    private static final String CELL_THRESHOLD_OPTION = "c";
    private static final String CSV_OPTION = "m";
    private static final String CURRENT_TIMESTAMP_OPTION = "u";

    private static final Options options = new Options();

    private static final UUID EMPTY_TABLE_ID = new UUID(0L, 0L);

    static
    {
        Config.setClientMode(true);

        Option optKey = new Option(KEY_OPTION, "key", true, "Partition keys to include");
        // Number of times -k <key> can be passed on the command line.
        optKey.setArgs(Option.UNLIMITED_VALUES);
        options.addOption(optKey);

        Option excludeKey = new Option(EXCLUDE_KEY_OPTION, "exclude-key", true, "Excluded partition key(s) from partition detailed row/cell/tombstone information (irrelevant, if --partitions-only is given)");
        // Number of times -x <key> can be passed on the command line.
        excludeKey.setArgs(Option.UNLIMITED_VALUES);
        options.addOption(excludeKey);

        Option thresholdKey = new Option(SIZE_THRESHOLD_OPTION, "min-size", true, "partition size threshold");
        options.addOption(thresholdKey);

        Option tombstoneKey = new Option(TOMBSTONE_THRESHOLD_OPTION, "min-tombstones", true, "partition tombstone count threshold");
        options.addOption(tombstoneKey);

        Option cellKey = new Option(CELL_THRESHOLD_OPTION, "min-cells", true, "partition cell count threshold");
        options.addOption(cellKey);

        Option currentTimestampKey = new Option(CURRENT_TIMESTAMP_OPTION, "current-timestamp", true, "timestamp (seconds since epoch, unit time) for TTL expired calculation");
        options.addOption(currentTimestampKey);

        Option recursiveKey = new Option(RECURSIVE_OPTION, "recursive", false, "scan for sstables recursively");
        options.addOption(recursiveKey);

        Option snapshotsKey = new Option(SNAPSHOTS_OPTION, "snapshots", false, "include snapshots present in data directories (recursive scans)");
        options.addOption(snapshotsKey);

        Option backupsKey = new Option(BACKUPS_OPTION, "backups", false, "include backups present in data directories (recursive scans)");
        options.addOption(backupsKey);

        Option partitionsOnlyKey = new Option(PARTITIONS_ONLY_OPTION, "partitions-only", false, "Do not process per-partition detailed row/cell/tombstone information, only brief information");
        options.addOption(partitionsOnlyKey);

        Option csvKey = new Option(CSV_OPTION, "csv", false, "CSV output (machine readable)");
        options.addOption(csvKey);
    }

    static String toHexString(UUID id)
    {
        return ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(id));
    }

    static UUID fromHexString(String nonDashUUID)
    {
        ByteBuffer bytes = ByteBufferUtil.hexToBytes(nonDashUUID);
        long msb = bytes.getLong(0);
        long lsb = bytes.getLong(8);
        return new UUID(msb, lsb);
    }

    static Pair<String, UUID> tableNameAndIdFromFilename(String filename)
    {
        int dash = filename.lastIndexOf('-');
        if (dash <= 0 || dash != filename.length() - 32 - 1)
            return null;

        UUID id = fromHexString(filename.substring(dash + 1));
        String tableName = filename.substring(0, dash);

        return Pair.create(tableName, id);
    }

    /**
     * Construct table schema from info stored in SSTable's Stats.db
     *
     * @param desc SSTable's descriptor
     * @return Restored CFMetaData
     * @throws IOException when Stats.db cannot be read
     */
    static CFMetaData metadataFromSSTable(Descriptor desc, String keyspace, String table) throws IOException
    {
        if (!desc.version.storeRows())
            throw new IOException("pre-3.0 SSTable is not supported.");

        EnumSet<MetadataType> types = EnumSet.of(MetadataType.STATS, MetadataType.HEADER);
        Map<MetadataType, MetadataComponent> sstableMetadata = desc.getMetadataSerializer().deserialize(desc, types);
        SerializationHeader.Component header = (SerializationHeader.Component) sstableMetadata.get(MetadataType.HEADER);
        IPartitioner partitioner = FBUtilities.newPartitioner(desc);

        CFMetaData.Builder builder = CFMetaData.Builder.create(keyspace == null ? "keyspace" : keyspace,
                                                               table == null ? "table" : table).withPartitioner(partitioner);
        header.getStaticColumns().entrySet().stream()
              .forEach(entry -> {
                  ColumnIdentifier ident = ColumnIdentifier.getInterned(UTF8Type.instance.getString(entry.getKey()), true);
                  builder.addStaticColumn(ident, entry.getValue());
              });
        header.getRegularColumns().entrySet().stream()
              .forEach(entry -> {
                  ColumnIdentifier ident = ColumnIdentifier.getInterned(UTF8Type.instance.getString(entry.getKey()), true);
                  builder.addRegularColumn(ident, entry.getValue());
              });
        builder.addPartitionKey("PartitionKey", header.getKeyType());
        for (int i = 0; i < header.getClusteringTypes().size(); i++)
        {
            builder.addClusteringColumn("clustering" + (i > 0 ? i : ""), header.getClusteringTypes().get(i));
        }
        return builder.build();
    }

    /**
     * Given arguments specifying an SSTable, and optionally an output file, export the contents of the SSTable to JSON.
     *
     * @param args command lines arguments
     * @throws ConfigurationException on configuration failure (wrong params given)
     */
    public static void main(String[] args) throws ConfigurationException
    {
        CommandLineParser parser = new PosixParser();
        CommandLine cmd;
        try
        {
            cmd = parser.parse(options, args);
        }
        catch (ParseException e1)
        {
            System.err.println(e1.getMessage());
            printUsage();
            System.exit(1);
            return;
        }

        if (cmd.getArgs().length == 0)
        {
            System.err.println("You must supply at least one sstable or directory");
            printUsage();
            System.exit(1);
        }

        int ec = processArguments(cmd);

        System.exit(ec);
    }

    private static int processArguments(CommandLine cmd)
    {
        String[] keys = cmd.getOptionValues(KEY_OPTION);
        HashSet<String> excludes = new HashSet<>(Arrays.asList(cmd.getOptionValues(EXCLUDE_KEY_OPTION) == null
                                                               ? new String[0]
                                                               : cmd.getOptionValues(EXCLUDE_KEY_OPTION)));

        boolean scanRecursive = cmd.hasOption(RECURSIVE_OPTION);
        boolean withSnapshots = cmd.hasOption(SNAPSHOTS_OPTION);
        boolean withBackups = cmd.hasOption(BACKUPS_OPTION);
        boolean csv = cmd.hasOption(CSV_OPTION);
        boolean partitionsOnly = cmd.hasOption(PARTITIONS_ONLY_OPTION);

        long sizeThreshold = Long.MAX_VALUE;
        int cellCountThreshold = Integer.MAX_VALUE;
        int tombstoneCountThreshold = Integer.MAX_VALUE;
        long currentTime = System.currentTimeMillis() / 1000L;

        try
        {
            if (cmd.hasOption(SIZE_THRESHOLD_OPTION))
                sizeThreshold = Long.parseLong(cmd.getOptionValue(SIZE_THRESHOLD_OPTION));
            if (cmd.hasOption(CELL_THRESHOLD_OPTION))
                cellCountThreshold = Integer.parseInt(cmd.getOptionValue(CELL_THRESHOLD_OPTION));
            if (cmd.hasOption(TOMBSTONE_THRESHOLD_OPTION))
                tombstoneCountThreshold = Integer.parseInt(cmd.getOptionValue(TOMBSTONE_THRESHOLD_OPTION));
            if (cmd.hasOption(CURRENT_TIMESTAMP_OPTION))
                currentTime = Integer.parseInt(cmd.getOptionValue(CURRENT_TIMESTAMP_OPTION));
        }
        catch (NumberFormatException e)
        {
            System.err.printf("Invalid threshold argument: %s%n", e.getMessage());
            return 1;
        }

        if (sizeThreshold < 0 || cellCountThreshold < 0 || tombstoneCountThreshold < 0 || currentTime < 0)
        {
            System.err.println("Negative values are not allowed");
            return 1;
        }

        List<File> directories = new ArrayList<>();
        List<ExtendedDescriptor> descriptors = new ArrayList<>();

        if (!argumentsToFiles(cmd.getArgs(), descriptors, directories))
            return 1;

        for (File directory : directories)
            processDirectory(scanRecursive, withSnapshots, withBackups, directory,
                             descriptors);

        if (csv)
            System.out.println("key,keyBinary,live," +
                               "offset,size,rowCount,cellCount," +
                               "tombstoneCount,rowTombstoneCount,rangeTombstoneCount,complexTombstoneCount," +
                               "cellTombstoneCount,rowTtlExpired,cellTtlExpired," +
                               "directory," +
                               "keyspace,table," +
                               "index," +
                               "snapshot,backup," +
                               "generation," +
                               "format,version");

        Collections.sort(descriptors);

        for (ExtendedDescriptor desc : descriptors)
            processSstable(keys, excludes, desc,
                           sizeThreshold, cellCountThreshold, tombstoneCountThreshold, partitionsOnly,
                           csv, currentTime);

        return 0;
    }

    private static void processDirectory(boolean scanRecursive,
                                         boolean withSnapshots,
                                         boolean withBackups,
                                         File dir,
                                         List<ExtendedDescriptor> descriptors)
    {
        File[] files = dir.listFiles();
        if (files == null)
            return;
        for (File file : files)
        {
            if (file.isFile())
            {
                try
                {
                    if (Descriptor.componentFromFilename(file) != Component.DATA)
                        continue;

                    ExtendedDescriptor desc = ExtendedDescriptor.guessFromFile(file);
                    if (desc.snapshot != null && !withSnapshots)
                        continue;
                    if (desc.backup != null && !withBackups)
                        continue;

                    descriptors.add(desc);
                }
                catch (IllegalArgumentException e)
                {
                    // ignore that error when scanning directories
                }
            }
            if (scanRecursive && file.isDirectory())
            {
                processDirectory(true,
                                 withSnapshots, withBackups,
                                 file,
                                 descriptors);
            }
        }
    }

    private static boolean argumentsToFiles(String[] args, List<ExtendedDescriptor> descriptors, List<File> directories)
    {
        boolean err = false;
        for (String arg : args)
        {
            File fArg = new File(arg);
            if (!fArg.exists())
            {
                System.err.printf("Argument '%s' does not resolve to a file or directory%n", arg);
                err = true;
            }

            if (!fArg.canRead())
            {
                System.err.printf("Argument '%s' is not a readable file or directory (check permissions)%n", arg);
                err = true;
                continue;
            }

            if (fArg.isFile())
            {
                try
                {
                    descriptors.add(ExtendedDescriptor.guessFromFile(fArg));
                }
                catch (IllegalArgumentException e)
                {
                    System.err.printf("Argument '%s' is not an sstable%n", arg);
                    err = true;
                }
            }
            if (fArg.isDirectory())
                directories.add(fArg);
        }
        return !err;
    }

    private static void processSstable(String[] keys, HashSet<String> excludes, ExtendedDescriptor desc,
                                       long sizeThreshold, int cellCountThreshold, int tombstoneCountThreshold,
                                       boolean partitionsOnly, boolean csv, long currentTime)
    {
        try
        {
            long t0 = System.nanoTime();

            CFMetaData metadata = metadataFromSSTable(desc.descriptor, desc.keyspace, desc.table);

            SSTableReader sstable = SSTableReader.openNoValidation(desc.descriptor, metadata);

            if (!csv)
                System.out.printf("%nProcessing %s (%d bytes uncompressed, %d bytes on disk)%n", desc, sstable.uncompressedLength(), sstable.onDiskLength());

            List<PartitionStatistics> matches = new ArrayList<>();

            SstableStatistics sstableStatistics = new SstableStatistics();

            try
            {

                IPartitioner partitioner = sstable.getPartitioner();
                ISSTableScanner currentScanner = null;
                if (keys != null && keys.length > 0)
                {
                    try
                    {
                        List<AbstractBounds<PartitionPosition>> bounds = Arrays.stream(keys)
                                                                               .filter(key -> !excludes.contains(key))
                                                                               .map(metadata.getKeyValidator()::fromString)
                                                                               .map(partitioner::decorateKey)
                                                                               .sorted()
                                                                               .map(DecoratedKey::getToken)
                                                                               .map(token -> new Bounds<>(token.minKeyBound(), token.maxKeyBound()))
                                                                               .collect(Collectors.toList());
                        currentScanner = sstable.getScanner(bounds.iterator());
                    }
                    catch (RuntimeException e)
                    {
                        System.err.printf("Cannot use one or more partition keys in %s for the partition key type ('%s') of the underlying table: %s%n",
                                          Arrays.toString(keys), metadata.getKeyValidator().asCQL3Type(), e);
                    }
                }
                if (currentScanner == null)
                    currentScanner = sstable.getScanner();

                try
                {
                    PartitionStatistics statistics = null;
                    while (currentScanner.hasNext())
                    {
                        UnfilteredRowIterator partition = currentScanner.next();

                        // checkMatch() is always called _after_ any partition and checks for a partition size or
                        // cell count or tombstone count match for the previous partition, because then we have all
                        // the information we need to actually perform the check in 'statistics'.
                        checkMatch(sizeThreshold, cellCountThreshold, tombstoneCountThreshold, csv,
                                   partitionsOnly, currentScanner, matches, sstableStatistics, statistics, metadata, desc);

                        statistics = new PartitionStatistics(partition.partitionKey().getKey(),
                                                             currentScanner.getCurrentPosition(),
                                                             partition.partitionLevelDeletion().isLive());
                        if (excludes.contains(metadata.getKeyValidator().getString(statistics.key)))
                            continue;

                        if (!partitionsOnly)
                            perPartitionDetails(desc, currentTime, statistics, partition);
                    }

                    // See comment above - for the last partition.
                    checkMatch(sizeThreshold, cellCountThreshold, tombstoneCountThreshold, csv, partitionsOnly,
                               currentScanner, matches, sstableStatistics, statistics, metadata, desc);
                }
                finally
                {
                    currentScanner.close();
                }
            }
            catch (RuntimeException e)
            {
                System.err.printf("Failure processing sstable %s: %s%n", desc.descriptor, e);
            }
            finally
            {
                sstable.selfRef().release();
            }

            long t = System.nanoTime() - t0;

            if (!csv)
            {
                if (!matches.isEmpty())
                {
                    System.out.printf("Summary of %s:%n" +
                                      "  File: %s%n" +
                                      "  %d partitions match%n" +
                                      "  Keys:", desc, desc.descriptor.filenameFor(Component.DATA), matches.size());
                    for (PartitionStatistics match : matches)
                    {
                        System.out.print(" " + maybeEscapeKeyForSummary(metadata, match.key));
                    }
                    System.out.println();
                }

                if (partitionsOnly)
                    System.out.printf("        %20s%n", "Partition size");
                else
                    System.out.printf("        %20s %20s %20s %20s%n", "Partition size", "Row count", "Cell count", "Tombstone count");
                printPercentile(partitionsOnly, sstableStatistics, "p50", h -> h.percentile(.5d));
                printPercentile(partitionsOnly, sstableStatistics, "p75", h -> h.percentile(.75d));
                printPercentile(partitionsOnly, sstableStatistics, "p90", h -> h.percentile(.90d));
                printPercentile(partitionsOnly, sstableStatistics, "p95", h -> h.percentile(.95d));
                printPercentile(partitionsOnly, sstableStatistics, "p99", h -> h.percentile(.99d));
                printPercentile(partitionsOnly, sstableStatistics, "p999", h -> h.percentile(.999d));
                printPercentile(partitionsOnly, sstableStatistics, "min", EstimatedHistogram::min);
                printPercentile(partitionsOnly, sstableStatistics, "max", EstimatedHistogram::max);
                System.out.printf("  count %20d%n", sstableStatistics.partitionSizeHistogram.count());
                System.out.printf("  time  %20d%n", TimeUnit.NANOSECONDS.toMillis(t));
            }
        }
        catch (IOException e)
        {
            // throwing exception outside main with broken pipe causes windows cmd to hang
            e.printStackTrace(System.err);
        }
    }

    private static void perPartitionDetails(ExtendedDescriptor desc, long currentTime, PartitionStatistics statistics, UnfilteredRowIterator partition)
    {
        while (partition.hasNext())
        {
            Unfiltered unfiltered = partition.next();

            if (unfiltered instanceof Row)
            {
                Row row = (Row) unfiltered;
                statistics.rowCount++;

                if (!row.deletion().isLive())
                    statistics.rowTombstoneCount++;

                LivenessInfo liveInfo = row.primaryKeyLivenessInfo();
                if (!liveInfo.isEmpty() && liveInfo.isExpiring() && liveInfo.localExpirationTime() < currentTime)
                    statistics.rowTtlExpired++;

                for (ColumnData cd : row)
                {

                    if (cd.column().isSimple())
                    {
                        cellStats((int) currentTime, statistics, liveInfo, (Cell) cd);
                    }
                    else
                    {
                        ComplexColumnData complexData = (ComplexColumnData) cd;
                        if (!complexData.complexDeletion().isLive())
                            statistics.complexTombstoneCount++;

                        for (Cell cell : complexData)
                            cellStats((int) currentTime, statistics, liveInfo, cell);
                    }
                }
            }
            else if (unfiltered instanceof RangeTombstoneMarker)
            {
                statistics.rangeTombstoneCount++;
            }
            else
            {
                throw new UnsupportedOperationException("Unknown kind " + unfiltered.kind() + " in sstable " + desc.descriptor);
            }
        }
    }

    private static void cellStats(int currentTime, PartitionStatistics statistics, LivenessInfo liveInfo, Cell cell)
    {
        statistics.cellCount++;
        if (cell.isTombstone())
            statistics.cellTombstoneCount++;
        if (cell.isExpiring() && (liveInfo.isEmpty() || cell.ttl() != liveInfo.ttl()) && !cell.isLive(currentTime))
            statistics.cellTtlExpired++;
    }

    private static void printPercentile(boolean partitionsOnly,
                                        SstableStatistics sstableStatistics,
                                        String header,
                                        Function<EstimatedHistogram, Long> value)
    {
        if (partitionsOnly)
            System.out.printf("  %-4s  %20d%n",
                              header,
                              value.apply(sstableStatistics.partitionSizeHistogram));
        else
            System.out.printf("  %-4s  %20d %20d %20d %20d%n",
                              header,
                              value.apply(sstableStatistics.partitionSizeHistogram),
                              value.apply(sstableStatistics.rowCountHistogram),
                              value.apply(sstableStatistics.cellCountHistogram),
                              value.apply(sstableStatistics.tombstoneCountHistogram));
    }

    private static String maybeEscapeKeyForSummary(CFMetaData metadata, ByteBuffer key)
    {
        String s = metadata.getKeyValidator().getString(key);
        if (s.indexOf(' ') == -1)
            return s;
        return "\"" + s.replaceAll("\"", "\"\"") + "\"";
    }

    private static void checkMatch(long sizeThreshold, int cellCountThreshold, int tombstoneCountThreshold,
                                   boolean csv,
                                   boolean partitionsOnly,
                                   ISSTableScanner currentScanner,
                                   List<PartitionStatistics> matches,
                                   SstableStatistics sstableStatistics,
                                   PartitionStatistics statistics,
                                   CFMetaData metadata,
                                   ExtendedDescriptor desc)
    {
        if (statistics != null)
        {
            statistics.endOfPartition(currentScanner.getCurrentPosition());

            sstableStatistics.partitionSizeHistogram.add(statistics.partitionSize);
            sstableStatistics.rowCountHistogram.add(statistics.rowCount);
            sstableStatistics.cellCountHistogram.add(statistics.cellCount);
            sstableStatistics.tombstoneCountHistogram.add(statistics.tombstoneCount());

            if (statistics.matchesThreshold(sizeThreshold, cellCountThreshold, tombstoneCountThreshold))
            {
                matches.add(statistics);
                if (csv)
                    statistics.printPartitionInfoCSV(metadata, desc);
                else
                    statistics.printPartitionInfo(metadata, partitionsOnly);
            }
        }
    }

    private static void printUsage()
    {
        String usage = String.format("sstablepartitions <options> <sstable files or directories>%n");
        String header = "Print partition statistics of one or more sstables.";
        new HelpFormatter().printHelp(usage, header, options, "");
    }

    static final class SstableStatistics
    {
        EstimatedHistogram partitionSizeHistogram = new EstimatedHistogram();
        EstimatedHistogram rowCountHistogram = new EstimatedHistogram();
        EstimatedHistogram cellCountHistogram = new EstimatedHistogram();
        EstimatedHistogram tombstoneCountHistogram = new EstimatedHistogram();
    }

    static final class PartitionStatistics
    {
        final ByteBuffer key;
        final long offset;
        final boolean live;
        long partitionSize;
        int rowCount;
        int cellCount;
        int rowTombstoneCount;
        int rangeTombstoneCount;
        int complexTombstoneCount;
        int cellTombstoneCount;
        int rowTtlExpired;
        int cellTtlExpired;

        PartitionStatistics(ByteBuffer key, long offset, boolean live)
        {
            this.key = key;
            this.offset = offset;
            this.partitionSize = -1;
            this.live = live;
            this.rowCount = 0;
            this.cellCount = 0;
            this.rowTombstoneCount = 0;
            this.rangeTombstoneCount = 0;
            this.complexTombstoneCount = 0;
            this.cellTombstoneCount = 0;
            this.rowTtlExpired = 0;
            this.cellTtlExpired = 0;
        }

        void endOfPartition(long position)
        {
            this.partitionSize = position - offset;
        }

        int tombstoneCount()
        {
            return rowTombstoneCount + rangeTombstoneCount + complexTombstoneCount + cellTombstoneCount + rowTtlExpired + cellTtlExpired;
        }

        boolean matchesThreshold(long sizeThreshold, int cellCountThreshold, int tombstoneCountThreshold)
        {
            return partitionSize >= sizeThreshold
                   || cellCount >= cellCountThreshold
                   || tombstoneCount() >= tombstoneCountThreshold;
        }

        void printPartitionInfo(CFMetaData metadata, boolean partitionsOnly)
        {
            String key = metadata.getKeyValidator().getString(this.key);
            if (partitionsOnly)
                System.out.printf("  Partition: '%s' (%s) %s, position: %d, size: %d%n",
                                  key, ByteBufferUtil.bytesToHex(this.key), live ? "live" : "not live",
                                  offset, partitionSize);
            else
                System.out.printf("  Partition: '%s' (%s) %s, position: %d, size: %d, rows: %d, cells: %d, tombstones: %d (row:%d, range:%d, complex:%d, cell:%d, row-TTLd:%d, cell-TTLd:%d)%n",
                                  key, ByteBufferUtil.bytesToHex(this.key), live ? "live" : "not live",
                                  offset, partitionSize,
                                  rowCount, cellCount,
                                  tombstoneCount(),
                                  rowTombstoneCount, rangeTombstoneCount, complexTombstoneCount, cellTombstoneCount, rowTtlExpired, cellTtlExpired);
        }

        void printPartitionInfoCSV(CFMetaData metadata, ExtendedDescriptor desc)
        {
            System.out.printf("\"%s\",%s,%s," +
                              "%d,%d,%d,%d," +
                              "%d,%d,%d,%d," +
                              "%d,%d,%d," +
                              "%s," +
                              "%s,%s," +
                              "%s," +
                              "%s,%s," +
                              "%d,%s,%s%n",
                              maybeEscapeKeyForSummary(metadata, this.key), ByteBufferUtil.bytesToHex(this.key), live ? "true" : "false",
                              offset, partitionSize, rowCount, cellCount,
                              tombstoneCount(), rowTombstoneCount, rangeTombstoneCount, complexTombstoneCount,
                              cellTombstoneCount, rowTtlExpired, cellTtlExpired,
                              desc.descriptor.filenameFor(Component.DATA),
                              notNull(desc.keyspace), notNull(desc.table),
                              notNull(desc.index),
                              notNull(desc.snapshot), notNull(desc.backup),
                              desc.descriptor.generation,
                              desc.descriptor.formatType.name,
                              desc.descriptor.version.getVersion());
        }
    }

    static final class ExtendedDescriptor implements Comparable<ExtendedDescriptor>
    {
        final String keyspace;
        final String table;
        final String index;
        final String snapshot;
        final String backup;
        final UUID tableId;
        final Descriptor descriptor;

        ExtendedDescriptor(String keyspace, String table, UUID tableId, String index, String snapshot, String backup, Descriptor descriptor)
        {
            this.keyspace = keyspace;
            this.table = table;
            this.tableId = tableId;
            this.index = index;
            this.snapshot = snapshot;
            this.backup = backup;
            this.descriptor = descriptor;
        }

        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            if (backup != null)
                sb.append("Backup:").append(backup).append(' ');
            if (snapshot != null)
                sb.append("Snapshot:").append(snapshot).append(' ');
            if (keyspace != null)
                sb.append(keyspace).append('.');
            if (table != null)
                sb.append(table);
            if (index != null)
                sb.append('.').append(index);
            if (tableId != null)
                sb.append('-').append(toHexString(tableId));
            sb.append(" #").append(descriptor.generation).append(" (").append(descriptor.formatType.name).append('-').append(descriptor.version.getVersion()).append(')');
            return sb.toString();
        }

        static ExtendedDescriptor guessFromFile(File fArg)
        {
            Descriptor desc = Descriptor.fromFilename(fArg.getParentFile(), fArg.getName()).left;

            String snapshot = null;
            String backup = null;
            String index = null;

            File parent = fArg.getParentFile();
            File parent2 = parent.getParentFile();
            String parentName = parent.getName();
            String parent2Name = parent2.getName();

            if (parentName.length() > 1 && parentName.startsWith(".") && parentName.charAt(1) != '.')
            {
                index = parentName.substring(1);
                parent = parent.getParentFile();
                parent2 = parent.getParentFile();
                parentName = parent.getName();
            }

            if (parent2Name.equals(Directories.SNAPSHOT_SUBDIR))
            {
                snapshot = parent.getName();
                parent = parent2.getParentFile();
                parent2 = parent.getParentFile();
                parentName = parent.getName();
                parent2Name = parent2.getName();
            }
            else if (parent2Name.equals(Directories.BACKUPS_SUBDIR))
            {
                backup = parent.getName();
                parent = parent2.getParentFile();
                parent2 = parent.getParentFile();
                parentName = parent.getName();
                parent2Name = parent2.getName();
            }

            try
            {
                Pair<String, UUID> tableNameAndId = tableNameAndIdFromFilename(parentName);
                if (tableNameAndId != null)
                {
                    String keyspace = parent2Name;

                    return new ExtendedDescriptor(keyspace,
                                                  tableNameAndId.left,
                                                  tableNameAndId.right,
                                                  index,
                                                  snapshot,
                                                  backup,
                                                  desc);
                }
            }
            catch (NumberFormatException e)
            {
                // ignore non-parseable table-IDs
            }

            return new ExtendedDescriptor(null,
                                          null,
                                          null,
                                          index,
                                          snapshot,
                                          backup,
                                          desc);
        }

        public int compareTo(ExtendedDescriptor o)
        {
            int c = descriptor.directory.toString().compareTo(o.descriptor.directory.toString());
            if (c != 0)
                return c;
            c = notNull(keyspace).compareTo(notNull(o.keyspace));
            if (c != 0)
                return c;
            c = notNull(table).compareTo(notNull(o.table));
            if (c != 0)
                return c;
            c = notNull(tableId).compareTo(notNull(o.tableId));
            if (c != 0)
                return c;
            c = notNull(index).compareTo(notNull(o.index));
            if (c != 0)
                return c;
            c = notNull(snapshot).compareTo(notNull(o.snapshot));
            if (c != 0)
                return c;
            c = notNull(backup).compareTo(notNull(o.backup));
            if (c != 0)
                return c;
            c = Integer.compare(descriptor.generation, o.descriptor.generation);
            if (c != 0)
                return c;
            return Integer.compare(System.identityHashCode(this), System.identityHashCode(o));
        }
    }

    static String notNull(String s)
    {
        return s != null ? s : "";
    }

    static UUID notNull(UUID s)
    {
        return s != null ? s : EMPTY_TABLE_ID;
    }
}
