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
package org.apache.cassandra.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.file.FileStore;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jpountz.lz4.LZ4Factory;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.config.StartupChecksOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLogDescriptor;
import org.apache.cassandra.db.compaction.LeveledCompactionStrategy;
import org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.io.sstable.SSTableIdFactory;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.UUIDBasedSSTableId;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.Ec2Snitch;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.locator.SnitchProperties;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JavaUtils;
import org.apache.cassandra.utils.INativeLibrary;
import org.apache.cassandra.utils.SigarLibrary;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_JMX_LOCAL_PORT;
import static org.apache.cassandra.config.CassandraRelevantProperties.COM_SUN_MANAGEMENT_JMXREMOTE_PORT;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVA_VERSION;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVA_VM_NAME;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

/**
 * Verifies that the system and environment is in a fit state to be started.
 * Used in CassandraDaemon#setup() to check various settings and invariants.
 *
 * Each individual test is modelled as an implementation of StartupCheck, these are run
 * at the start of CassandraDaemon#setup() before any local state is mutated. The default
 * checks are a mix of informational tests (inspectJvmOptions), initialization
 * (initSigarLibrary, checkCacheServiceInitialization) and invariant checking
 * (checkValidLaunchDate, checkSystemKeyspaceState, checkSSTablesFormat).
 *
 * In addition, if checkSystemKeyspaceState determines that the release version has
 * changed since last startup (i.e. the node has been upgraded) it snapshots the system
 * keyspace to make it easier to back out if necessary.
 *
 * If any check reports a failure, then the setup method exits with an error (after
 * logging any output from the tests). If all tests report success, setup can continue.
 * We should be careful in future to ensure anything which mutates local state (such as
 * writing new sstables etc) only happens after we've verified the initial setup.
 */
public class StartupChecks
{
    public enum StartupCheckType
    {
        // non-configurable check is always enabled for execution
        non_configurable_check,
        check_filesystem_ownership(true),
        check_dc,
        check_rack,
        check_data_resurrection(true);

        public final boolean disabledByDefault;

        StartupCheckType()
        {
            this(false);
        }

        StartupCheckType(boolean disabledByDefault)
        {
            this.disabledByDefault = disabledByDefault;
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(StartupChecks.class);
    // List of checks to run before starting up. If any test reports failure, startup will be halted.
    private final List<StartupCheck> preFlightChecks = new ArrayList<>();

    // The default set of pre-flight checks to run. Order is somewhat significant in that we probably
    // always want the system keyspace check run last, as this actually loads the schema for that
    // keyspace. All other checks should not require any schema initialization.
    private final List<StartupCheck> DEFAULT_TESTS = ImmutableList.of(checkJemalloc,
                                                                      checkLz4Native,
                                                                      checkValidLaunchDate,
                                                                      checkJMXPorts,
                                                                      checkJMXProperties,
                                                                      inspectJvmOptions,
                                                                      checkNativeLibraryInitialization,
                                                                      initSigarLibrary,
                                                                      checkMaxMapCount,
                                                                      checkReadAheadKbSetting,
                                                                      checkDataDirs,
                                                                      checkDsePeersWithoutFlag,
                                                                      loadDsePeersRecord,
                                                                      checkDse6Artefacts,
                                                                      checkSSTablesFormat,
                                                                      checkSystemKeyspaceState,
                                                                      checkDse6Ec2NamingScheme,
                                                                      checkDatacenter,
                                                                      checkRack,
                                                                      checkLegacyAuthTables,
                                                                      checkYamlConfig,
                                                                      checkTableSettings,
                                                                      new DataResurrectionCheck());

    private final static String WARN_SUFFIX = " This will impact your level of production support.";

    public StartupChecks withDefaultTests()
    {
        preFlightChecks.addAll(DEFAULT_TESTS);
        return this;
    }

    /**
     * Add system test to be run before schema is loaded during startup
     * @param test the system test to include
     */
    public StartupChecks withTest(StartupCheck test)
    {
        preFlightChecks.add(test);
        return this;
    }

    @VisibleForTesting
    List<StartupCheck> getPreFlightChecks()
    {
        return ImmutableList.copyOf(preFlightChecks);
    }

    /**
     * Run the configured tests and return a report detailing the results.
     * @throws StartupException if any test determines that the
     * system is not in an valid state to startup
     * @param options options to pass to respective checks for their configration
     */
    public void verify(StartupChecksOptions options) throws StartupException
    {
        for (StartupCheck test : preFlightChecks)
            test.execute(options);

        for (StartupCheck test : preFlightChecks)
        {
            try
            {
                test.postAction(options);
            }
            catch (Throwable t)
            {
                logger.warn("Failed to run startup check post-action on " + test.getStartupCheckType());
            }
        }
    }

    public static final StartupCheck checkJemalloc = new StartupCheck()
    {
        @Override
        public void execute(StartupChecksOptions options)
        {
            if (options.isDisabled(getStartupCheckType()))
                return;

            String jemalloc = CassandraRelevantProperties.LIBJEMALLOC.getString();
            if (jemalloc == null)
                logger.warn("jemalloc shared library could not be preloaded to speed up memory allocations");
            else if ("-".equals(jemalloc))
                logger.info("jemalloc preload explicitly disabled");
            else
                logger.info("jemalloc seems to be preloaded from {}", jemalloc);
        }
    };

    public static final StartupCheck checkLz4Native = new StartupCheck()
    {
        @Override
        public void execute(StartupChecksOptions options)
        {
            if (options.isDisabled(getStartupCheckType()))
                return;
            try
            {
                LZ4Factory.nativeInstance(); // make sure native loads
            }
            catch (AssertionError | LinkageError e)
            {
                logger.warn("lz4-java was unable to load native libraries; this will lower the performance of lz4 (network/sstables/etc.): {}", Throwables.getRootCause(e).getMessage());
            }
        }
    };

    public static final StartupCheck checkValidLaunchDate = new StartupCheck()
    {
        /**
         * The earliest legit timestamp a casandra instance could have ever launched.
         * Date roughly taken from http://perspectives.mvdirona.com/2008/07/12/FacebookReleasesCassandraAsOpenSource.aspx
         * We use this to ensure the system clock is at least somewhat correct at startup.
         */
        private static final long EARLIEST_LAUNCH_DATE = 1215820800000L;

        @Override
        public void execute(StartupChecksOptions options) throws StartupException
        {
            if (options.isDisabled(getStartupCheckType()))
                return;
            long now = currentTimeMillis();
            if (now < EARLIEST_LAUNCH_DATE)
                throw new StartupException(StartupException.ERR_WRONG_MACHINE_STATE,
                                           String.format("current machine time is %s, but that is seemingly incorrect. exiting now.",
                                                         new Date(now).toString()));
        }
    };

    public static final StartupCheck checkJMXPorts = new StartupCheck()
    {
        @Override
        public void execute(StartupChecksOptions options)
        {
            if (options.isDisabled(getStartupCheckType()))
                return;
            String jmxPort = CassandraRelevantProperties.CASSANDRA_JMX_REMOTE_PORT.getString();
            if (jmxPort == null)
            {
                logger.warn("JMX is not enabled to receive remote connections. Please see cassandra-env.sh for more info.");
                jmxPort = CassandraRelevantProperties.CASSANDRA_JMX_LOCAL_PORT.toString();
                if (jmxPort == null)
                    logger.error(CASSANDRA_JMX_LOCAL_PORT.getKey() + " missing from cassandra-env.sh, unable to start local JMX service.");
            }
            else
            {
                logger.info("JMX is enabled to receive remote connections on port: {}", jmxPort);
            }
        }
    };

    public static final StartupCheck checkJMXProperties = new StartupCheck()
    {
        @Override
        public void execute(StartupChecksOptions options)
        {
            if (options.isDisabled(getStartupCheckType()))
                return;
            if (COM_SUN_MANAGEMENT_JMXREMOTE_PORT.isPresent())
            {
                logger.warn("Use of com.sun.management.jmxremote.port at startup is deprecated. " +
                            "Please use cassandra.jmx.remote.port instead.");
            }
        }
    };

    public static final StartupCheck inspectJvmOptions = new StartupCheck()
    {
        @Override
        public void execute(StartupChecksOptions options)
        {
            if (options.isDisabled(getStartupCheckType()))
                return;
            // log warnings for different kinds of sub-optimal JVMs.  tldr use 64-bit Oracle >= 1.6u32
            if (!DatabaseDescriptor.hasLargeAddressSpace())
                logger.warn("32bit JVM detected.  It is recommended to run Cassandra on a 64bit JVM for better performance.");

            String javaVmName = JAVA_VM_NAME.getString();
            if (!(javaVmName.contains("HotSpot") || javaVmName.contains("OpenJDK")))
            {
                logger.warn("Non-Oracle JVM detected.  Some features, such as immediate unmap of compacted SSTables, may not work as intended");
            }
            else
            {
                checkOutOfMemoryHandling();
            }
        }

        /**
         * Checks that the JVM is configured to handle OutOfMemoryError
         */
        private void checkOutOfMemoryHandling()
        {
            if (JavaUtils.supportExitOnOutOfMemory(JAVA_VERSION.getString()))
            {
                if (!jvmOptionsContainsOneOf("-XX:OnOutOfMemoryError=", "-XX:+ExitOnOutOfMemoryError", "-XX:+CrashOnOutOfMemoryError"))
                    logger.warn("The JVM is not configured to stop on OutOfMemoryError which can cause data corruption."
                                + " Use one of the following JVM options to configure the behavior on OutOfMemoryError: "
                                + " -XX:+ExitOnOutOfMemoryError, -XX:+CrashOnOutOfMemoryError, or -XX:OnOutOfMemoryError=\"<cmd args>;<cmd args>\"");
            }
            else
            {
                if (!jvmOptionsContainsOneOf("-XX:OnOutOfMemoryError="))
                    logger.warn("The JVM is not configured to stop on OutOfMemoryError which can cause data corruption."
                            + " Either upgrade your JRE to a version greater or equal to 8u92 and use -XX:+ExitOnOutOfMemoryError/-XX:+CrashOnOutOfMemoryError"
                            + " or use -XX:OnOutOfMemoryError=\"<cmd args>;<cmd args>\" on your current JRE." + WARN_SUFFIX);
            }
        }

        /**
         * Checks if one of the specified options is being used.
         * @param optionNames The name of the options to check
         * @return {@code true} if one of the specified options is being used, {@code false} otherwise.
         */
        private boolean jvmOptionsContainsOneOf(String... optionNames)
        {
            RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
            List<String> inputArguments = runtimeMxBean.getInputArguments();
            for (String argument : inputArguments)
            {
                for (String optionName : optionNames)
                    if (argument.startsWith(optionName))
                        return true;
            }
            return false;
        }
    };

    public static final StartupCheck checkNativeLibraryInitialization = new StartupCheck()
    {
        @Override
        public void execute(StartupChecksOptions options) throws StartupException
        {
            if (options.isDisabled(getStartupCheckType()))
                return;
            // Fail-fast if the native library could not be linked.
            if (!INativeLibrary.instance.isAvailable())
                throw new StartupException(StartupException.ERR_WRONG_MACHINE_STATE, "The native library could not be initialized properly. ");
        }
    };

    public static final StartupCheck initSigarLibrary = new StartupCheck()
    {
        @Override
        public void execute(StartupChecksOptions options)
        {
            if (options.isDisabled(getStartupCheckType()))
                return;
            SigarLibrary.instance.warnIfRunningInDegradedMode();
        }
    };

    public static final StartupCheck checkReadAheadKbSetting = new StartupCheck()
    {
        // This value is in KB.
        private static final long MAX_RECOMMENDED_READ_AHEAD_KB_SETTING = 128;

        /**
         * Function to get the block device system path(Example: /dev/sda) from the
         * data directories defined in cassandra config.(cassandra.yaml)
         * @param dataDirectories list of data directories from cassandra.yaml
         * @return Map of block device path and data directory
         */
        private Map<String, String> getBlockDevices(String[] dataDirectories) {
            Map<String, String> blockDevices = new HashMap<String, String>();

            for (String dataDirectory : dataDirectories)
            {
                try
                {
                    Path p = File.getPath(dataDirectory);
                    FileStore fs = Files.getFileStore(p);

                    String blockDirectory = fs.name();
                    if(StringUtils.isNotEmpty(blockDirectory))
                    {
                        blockDevices.put(blockDirectory, dataDirectory);
                    }
                }
                catch (IOException e)
                {
                    logger.warn("IO exception while reading file {}.", dataDirectory, e);
                }
            }
            return blockDevices;
        }

        @Override
        public void execute(StartupChecksOptions options)
        {
            if (options.isDisabled(getStartupCheckType()) || !FBUtilities.isLinux)
                return;

            String[] dataDirectories = DatabaseDescriptor.getRawConfig().data_file_directories;
            Map<String, String> blockDevices = getBlockDevices(dataDirectories);

            for (Map.Entry<String, String> entry: blockDevices.entrySet())
            {
                String blockDeviceDirectory = entry.getKey();
                String dataDirectory = entry.getValue();
                try
                {
                    Path readAheadKBPath = StartupChecks.getReadAheadKBPath(blockDeviceDirectory);

                    if (readAheadKBPath == null || Files.notExists(readAheadKBPath))
                    {
                        logger.debug("No 'read_ahead_kb' setting found for device {} of data directory {}.", blockDeviceDirectory, dataDirectory);
                        continue;
                    }

                    final List<String> data = Files.readAllLines(readAheadKBPath);
                    if (data.isEmpty())
                        continue;

                    int readAheadKbSetting = Integer.parseInt(data.get(0));

                    if (readAheadKbSetting > MAX_RECOMMENDED_READ_AHEAD_KB_SETTING)
                    {
                        logger.warn("Detected high '{}' setting of {} for device '{}' of data directory '{}'. It is " +
                                    "recommended to set this value to 8KB (or lower) on SSDs or 64KB (or lower) on HDDs " +
                                    "to prevent excessive IO usage and page cache churn on read-intensive workloads.",
                                    readAheadKBPath, readAheadKbSetting, blockDeviceDirectory, dataDirectory);
                    }
                }
                catch (final IOException e)
                {
                    logger.warn("IO exception while reading file {}.", blockDeviceDirectory, e);
                }
            }
        }
    };

    public static final StartupCheck checkMaxMapCount = new StartupCheck()
    {
        private final long EXPECTED_MAX_MAP_COUNT = 1048575;
        private final String MAX_MAP_COUNT_PATH = "/proc/sys/vm/max_map_count";

        private long getMaxMapCount()
        {
            final Path path = File.getPath(MAX_MAP_COUNT_PATH);
            try (final BufferedReader bufferedReader = Files.newBufferedReader(path))
            {
                final String data = bufferedReader.readLine();
                if (data != null)
                {
                    try
                    {
                        return Long.parseLong(data);
                    }
                    catch (final NumberFormatException e)
                    {
                        logger.warn("Unable to parse {}.", path, e);
                    }
                }
            }
            catch (final IOException e)
            {
                logger.warn("IO exception while reading file {}.", path, e);
            }
            return -1;
        }

        @Override
        public void execute(StartupChecksOptions options)
        {
            if (options.isDisabled(getStartupCheckType()) || !FBUtilities.isLinux)
                return;

            if (DatabaseDescriptor.getDiskAccessMode() == Config.DiskAccessMode.standard &&
                DatabaseDescriptor.getIndexAccessMode() == Config.DiskAccessMode.standard)
                return; // no need to check if disk access mode is only standard and not mmap

            long maxMapCount = getMaxMapCount();
            if (maxMapCount < EXPECTED_MAX_MAP_COUNT)
                logger.warn("Maximum number of memory map areas per process (vm.max_map_count) {} " +
                            "is too low, recommended value: {}, you can change it with sysctl. {}",
                            maxMapCount, EXPECTED_MAX_MAP_COUNT, WARN_SUFFIX);
        }
    };

    public static final StartupCheck checkDataDirs = new StartupCheck()
    {
        @Override
        public void execute(StartupChecksOptions options) throws StartupException
        {
            if (options.isDisabled(getStartupCheckType()))
                return;
            // check all directories(data, commitlog, saved cache) for existence and permission
            Iterable<File> dirs = Iterables.concat(Arrays.asList(DatabaseDescriptor.getAllDataFileLocations()),
                                                     Arrays.asList(DatabaseDescriptor.getCommitLogLocation(),
                                                                   DatabaseDescriptor.getSavedCachesLocation(),
                                                                   DatabaseDescriptor.getHintsDirectory()));
            for (File dir : dirs) {
                logger.debug("Checking directory {}", dir);
    
                // check that directories exist.
                if (!dir.exists()) {
                    logger.warn("Directory {} doesn't exist", dir);
                    // if they don't, failing their creation, stop cassandra.
                    if (!dir.tryCreateDirectories())
                        throw new StartupException(StartupException.ERR_WRONG_DISK_STATE,
                                "Has no permission to create directory " + dir);
                }
    
                // if directories exist verify their permissions
                if (!Directories.verifyFullPermissions(dir))
                    throw new StartupException(StartupException.ERR_WRONG_DISK_STATE,
                            "Insufficient permissions on directory " + dir);
            }
            if (DatabaseDescriptor.getAllDataFileLocations().length > 1)
                logger.warn("Multiple {} data_dir configured.  Best practice is to use a (striped) LVM. {}",
                            DatabaseDescriptor.getAllDataFileLocations().length, WARN_SUFFIX);
        }
    };

    public static final StartupCheck checkSSTablesFormat = new StartupCheck()
    {
        @Override
        public void execute(StartupChecksOptions options) throws StartupException
        {
            if (options.isDisabled(getStartupCheckType()))
                return;
            final Set<String> invalid = new HashSet<>();
            final List<String> withIllegalGenId = new ArrayList<>();

            walkSSTableFiles(dir -> {
                // In very old versions of Cassandra, we wouldn't necessarily delete sstables from dropped system tables
                // which were removed in various major version upgrades (e.g system.Versions in 1.2)
                String tablePart = staleSystemTableName(dir);
                if (tablePart == null)
                    return false;

                String ksPart = SchemaConstants.SYSTEM_KEYSPACE_NAME;
                String canonicalPath = FileUtils.getCanonicalPath(new File(dir));

                // We can have snapshots of our system tables or snapshots created with a -t tag of "system" that would trigger
                // this potential warning, so we warn more softly in the case that it's probably a snapshot.
                if (canonicalPath.contains("snapshot"))
                {
                    logger.info("Found unknown system directory {}.{} at {} that contains the word snapshot. " +
                                "This may be left over from a previous version of Cassandra or may be normal. " +
                                " Consider removing after inspection if determined to be unnecessary.",
                                ksPart, tablePart, canonicalPath);
                }
                else
                {
                    logger.warn("Found unknown system directory {}.{} at {} - this is likely left over from a previous " +
                                "version of Cassandra and should be removed after inspection.",
                                ksPart, tablePart, canonicalPath);
                }
                return true;
            }, file -> {
                try
                {
                    Descriptor desc = Descriptor.fromFileWithComponent(file, false).left;
                    if (!desc.isCompatible())
                        invalid.add(file.toString());

                    if (!DatabaseDescriptor.isUUIDSSTableIdentifiersEnabled() && desc.id instanceof UUIDBasedSSTableId)
                        withIllegalGenId.add(file.toString());
                }
                catch (Exception e)
                {
                    invalid.add(file.toString());
                }
            });

            if (!invalid.isEmpty())
                throw new StartupException(StartupException.ERR_WRONG_DISK_STATE,
                                           String.format("Detected unreadable sstables %s, please check " +
                                                         "NEWS.txt and ensure that you have upgraded through " +
                                                         "all required intermediate versions, running " +
                                                         "upgradesstables",
                                                         Joiner.on(",").join(invalid)));

            if (!withIllegalGenId.isEmpty())
                throw new StartupException(StartupException.ERR_WRONG_CONFIG,
                                           "UUID sstable identifiers are disabled but some sstables have been " +
                                           "created with UUID identifiers. You have to either delete those " +
                                           "sstables or enable UUID based sstable identifers in cassandra.yaml " +
                                           "(uuid_sstable_identifiers_enabled). The list of affected sstables is: " +
                                           Joiner.on(", ").join(withIllegalGenId) + ". If you decide to delete sstables, " +
                                           "and have that data replicated over other healthy nodes, those will be brought" +
                                           "back during repair");
        }
    };

    /**
     * The table part of {@code dir} if it is the directory of a table of the {@code system} keyspace this software
     * does not know (for example one dropped by an earlier release), null otherwise.
     */
    private static String staleSystemTableName(Path dir)
    {
        String[] nameParts = FileUtils.getCanonicalPath(new File(dir)).split(java.io.File.separator);
        if (nameParts.length < 2)
            return null;

        String tablePart = nameParts[nameParts.length - 1];
        String ksPart = nameParts[nameParts.length - 2];

        if (tablePart.contains("-"))
            tablePart = tablePart.split("-")[0];

        return ksPart.equals(SchemaConstants.SYSTEM_KEYSPACE_NAME) && !SystemKeyspace.ALL_TABLE_NAMES.contains(tablePart)
               ? tablePart
               : null;
    }

    /**
     * Walks every data directory and hands each file with the sstable extension to {@code onSSTableFile}, skipping
     * snapshots, backups, the commit log, saved caches and hints directories (any of them may be nested in a data
     * directory) and every directory for which {@code skipDirectory} returns true.
     */
    private static void walkSSTableFiles(Predicate<Path> skipDirectory, Consumer<File> onSSTableFile) throws StartupException
    {
        final Set<String> nonSSTablePaths = new HashSet<>();
        nonSSTablePaths.add(FileUtils.getCanonicalPath(DatabaseDescriptor.getCommitLogLocation()));
        nonSSTablePaths.add(FileUtils.getCanonicalPath(DatabaseDescriptor.getSavedCachesLocation()));
        nonSSTablePaths.add(FileUtils.getCanonicalPath(DatabaseDescriptor.getHintsDirectory()));

        FileVisitor<Path> sstableVisitor = new SimpleFileVisitor<Path>()
        {
            public FileVisitResult visitFile(Path path, BasicFileAttributes attrs)
            {
                File file = new File(path);
                if (Descriptor.isValidFile(file))
                    onSSTableFile.accept(file);
                return FileVisitResult.CONTINUE;
            }

            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
            {
                if (skipDirectory.test(dir))
                    return FileVisitResult.SKIP_SUBTREE;

                String name = dir.getFileName().toString();
                return (name.equals(Directories.SNAPSHOT_SUBDIR)
                        || name.equals(Directories.BACKUPS_SUBDIR)
                        || nonSSTablePaths.contains(PathUtils.toCanonicalPath(dir).toString()))
                       ? FileVisitResult.SKIP_SUBTREE
                       : FileVisitResult.CONTINUE;
            }
        };

        for (File dataDir : DatabaseDescriptor.getAllDataFileLocations())
        {
            try
            {
                Files.walkFileTree(dataDir.toPath(), sstableVisitor);
            }
            catch (IOException e)
            {
                throw new StartupException(3, "Unable to verify sstable files on disk", e);
            }
        }
    }

    /** The newest big-format sstable version DSE 6 reads. */
    private static final String DSE_6_MAX_BIG_VERSION = "me";

    /**
     * With {@code internode_compatibility_mode: DSE_6}, refuses to start if the data directories or the commit log
     * directory hold a file that shows the node already ran without the mode:
     * <ul>
     *     <li>trie ({@code bti}) sstables of a version newer than {@code bb};</li>
     *     <li>big-format sstables of a version newer than {@code me};</li>
     *     <li>sstables of any other format;</li>
     *     <li>sstables with a non-numeric identifier (UUID or ULID): DSE 6 only parses numeric generations;</li>
     *     <li>commit log segments of a version above the 4.0 line, other than DSE's own {@code 680}.</li>
     * </ul>
     * The sstables listed are files a DSE 6 node could not read correctly, or at all, after a rollback of this node,
     * which would then only be possible from a snapshot. Snapshots and backups are not inspected. The commit log rule
     * only detects a run without the mode: DSE 6 cannot replay the version 7 segments this node writes in the mode
     * either, so the commit log, like the hints, is out of the scope of this check and is dealt with by the rollback
     * procedure (drain, then delete). Paxos v2 state is not looked for: a run without the mode that wrote it also
     * wrote trie sstables newer than {@code bb}, which are refused.
     * <p>
     * The sstable files are classified from the tokens of their names (version, identifier, format), not through
     * {@link Descriptor}: in this mode the trie format is initialised at version {@code bb} and big at the
     * {@code HCD_1} version, so {@link Descriptor} rejects a newer file as unreadable by this node before it could be
     * classified. For the same reason this check runs before {@link #checkSSTablesFormat}: the operator gets the
     * DSE-specific explanation rather than a request to run upgradesstables.
     * <p>
     * Once every check has passed, {@link StartupCheck#postAction} writes the upgrade marker (see
     * {@link DseUpgradeMarker}), so that a later start without the mode is refused by {@link #checkDsePeersWithoutFlag}.
     * <p>
     * The check is a {@code non_configurable_check} on purpose, like {@link #checkDsePeersWithoutFlag}: it only
     * fires in {@code DSE_6} mode, and an operator must not be able to switch the rollback guarantee off from the
     * yaml in the middle of the upgrade.
     */
    public static final StartupCheck checkDse6Artefacts = new StartupCheck()
    {
        @Override
        public void execute(StartupChecksOptions options) throws StartupException
        {
            if (!DatabaseDescriptor.isDse6CompatibilityMode())
                return;

            List<String> unreadableByDse = new ArrayList<>();
            // an unknown system table directory holds what DSE itself wrote; checkSSTablesFormat logs it
            walkSSTableFiles(dir -> staleSystemTableName(dir) != null, file -> {
                if (isSSTableUnreadableByDse6(file.name()))
                    unreadableByDse.add(file.toString());
            });

            File commitLogDirectory = DatabaseDescriptor.getCommitLogLocation();
            if (commitLogDirectory.exists())
            {
                File[] segments = commitLogDirectory.tryList();
                if (segments == null)
                    throw new StartupException(3, "Unable to verify commit log segments on disk: cannot list " + commitLogDirectory);
                for (File segment : segments)
                {
                    if (isCommitLogSegmentWrittenWithoutDse6Mode(segment.name()))
                        unreadableByDse.add(segment.toString());
                }
            }

            if (!unreadableByDse.isEmpty())
                throw new StartupException(StartupException.ERR_WRONG_DISK_STATE,
                                           "cannot enter DSE_6 compatibility mode: the node already ran without it, " +
                                           "these files could not be read correctly, or at all, by DSE 6 after a rollback: " +
                                           Joiner.on(", ").join(unreadableByDse) +
                                           ". Restore the node from its pre-upgrade snapshot. Deleting these files instead " +
                                           "loses this node's copy of their data: do it only if that data is replicated on " +
                                           "other nodes, and repair the node once the compatibility mode is dropped");
        }

        @Override
        public void postAction(StartupChecksOptions options)
        {
            if (DatabaseDescriptor.isDse6CompatibilityMode())
                writeUpgradeMarker(DseUpgradeMarker::writeIfAbsent);
        }
    };

    /** Creates the upgrade marker if it is absent; returns true if this call created it. */
    @VisibleForTesting
    interface UpgradeMarkerWrite
    {
        boolean writeIfAbsent() throws IOException;
    }

    /** Writes the upgrade marker with {@code write} and logs the outcome; a failure is logged, not thrown. */
    @VisibleForTesting
    static void writeUpgradeMarker(UpgradeMarkerWrite write)
    {
        try
        {
            if (write.writeIfAbsent())
                logger.info("Wrote the DSE 6 upgrade marker {}", DseUpgradeMarker.path());
        }
        catch (IOException | RuntimeException | FSError e)
        {
            if (Files.exists(DseUpgradeMarker.path()))
                logger.warn("Unable to write the content of the DSE 6 upgrade marker {}; the file exists, which is all " +
                            "that matters: a later start with internode_compatibility_mode NONE is refused while it does",
                            DseUpgradeMarker.path(), e);
            else
                logger.error("Unable to write the DSE 6 upgrade marker {}: unless the file exists, a later start with " +
                             "internode_compatibility_mode NONE will not be refused while DSE 6 peers may remain; create it by hand",
                             DseUpgradeMarker.path(), e);
        }
    }

    /**
     * Whether {@code fileName}, an sstable component name {@code <version>-<id>-<format>-<component>}, is one DSE 6
     * could not read: DSE reads trie up to {@code bb}, big up to {@code me}, numeric generations only.
     * A name that does not have the four parts, or whose version or identifier does not parse, is not classified
     * here; {@link #checkSSTablesFormat} reports it.
     */
    @VisibleForTesting
    static boolean isSSTableUnreadableByDse6(String fileName)
    {
        List<String> tokens = Splitter.on(Descriptor.FILENAME_SEPARATOR).splitToList(fileName);
        if (tokens.size() != 4)
            return false;

        String version = tokens.get(0);
        if (!Version.validate(version))
            return false;

        SSTableId id;
        try
        {
            id = SSTableIdFactory.instance.fromString(tokens.get(1));
        }
        catch (RuntimeException e)
        {
            return false;
        }
        if (!(id instanceof SequenceBasedSSTableId))
            return true;

        String format = tokens.get(2);
        if (BtiFormat.NAME.equals(format))
            return version.compareTo(InternodeCompatibilityMode.DSE_6_TRIE_INDEX_FORMAT_VERSION) > 0;
        if (BigFormat.NAME.equals(format))
            return version.compareTo(DSE_6_MAX_BIG_VERSION) > 0;
        return true;
    }

    /**
     * Whether {@code fileName} is a commit log segment written by a run without the DSE 6 mode: a version above the
     * 4.0 line other than DSE's own. It is not a readability rule: DSE 6 cannot replay version 7 either, which is what
     * this node writes in the mode. A name {@link CommitLogDescriptor} does not parse (a legacy segment without a
     * version, a number out of range) is not classified here; the commit log replay reports it.
     */
    @VisibleForTesting
    static boolean isCommitLogSegmentWrittenWithoutDse6Mode(String fileName)
    {
        if (!CommitLogDescriptor.isValid(fileName))
            return false;

        int version;
        try
        {
            version = CommitLogDescriptor.fromFileName(fileName).version();
        }
        catch (RuntimeException e)
        {
            return false;
        }
        return version > CommitLogDescriptor.VERSION_40 && version != CommitLogDescriptor.VERSION_DSE_68;
    }

    /**
     * With {@code internode_compatibility_mode: NONE}, refuses to start while the DSE 6 upgrade marker is present or
     * while the node's own record of DSE 6 peers ({@link DsePeersRecord}) is not empty: DSE peers may still be in the
     * cluster, and a node without the mode cannot talk to them. These two files are the only sources of truth here;
     * no guess is made from release versions. The record is what this node persisted the last time it ran: the
     * startup checks run before gossip and before any connection, so no live messaging version is known yet. The check
     * fails closed: a marker whose presence cannot be determined, or a record that cannot be read, refuses the start
     * as well.
     */
    public static final StartupCheck checkDsePeersWithoutFlag = new StartupCheck()
    {
        @Override
        public void execute(StartupChecksOptions options) throws StartupException
        {
            if (DatabaseDescriptor.isDse6CompatibilityMode())
                return;
            boolean marker = DseUpgradeMarker.exists();
            List<String> dsePeers = Collections.emptyList();
            String recordProblem = null;
            try
            {
                dsePeers = DsePeersRecord.persistedPeers();
            }
            catch (IOException | RuntimeException e)
            {
                // a record that cannot be read may list DSE peers: fail closed, and say so
                recordProblem = String.valueOf(e);
            }
            boolean record = !dsePeers.isEmpty() || recordProblem != null;
            if (!marker && !record)
                return;
            // DseUpgradeMarker.exists() counts a marker whose presence cannot be checked as present: say so rather than claim it is there
            boolean markerChecked = !marker || Files.exists(DseUpgradeMarker.path());
            List<String> evidence = new ArrayList<>(2);
            if (marker)
                evidence.add("the DSE 6 upgrade marker " + DseUpgradeMarker.path() + (markerChecked ? " is present" : " may be present (its presence cannot be checked)"));
            if (record)
                evidence.add("the record of DSE 6 peers " + DsePeersRecord.path() +
                             (recordProblem == null ? " listing " + dsePeers + " is present" : " cannot be read (" + recordProblem + ')'));
            String found = String.join(" and ", evidence);
            if (CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEERS.getBoolean())
            {
                logger.warn("{} but ignored ({}=true); DSE 6 peers, if any, will be unreachable",
                            Character.toUpperCase(found.charAt(0)) + found.substring(1), CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEERS.getKey());
                return;
            }
            StringBuilder remedy = new StringBuilder();
            if (marker)
                remedy.append(markerChecked ? "once every node runs this software, delete the marker"
                                            : "make the metadata directory readable so that the marker can be checked");
            if (recordProblem != null)
                remedy.append(marker ? "; " : "").append("make the record readable");
            else if (!dsePeers.isEmpty())
            {
                remedy.append(marker ? "; " : "")
                      .append("the record empties itself once this node has seen every listed peer at messaging version 4.0 or later ")
                      .append("(run once more with DSE_6 after the last upgrade); delete it by hand only after checking that the listed ")
                      .append("peers have left the cluster");
                List<String> notAddresses = new ArrayList<>();
                for (String entry : dsePeers)
                {
                    if (DsePeersRecord.parseEntry(entry) == null)
                        notAddresses.add(entry);
                }
                // such lines never leave the record by themselves
                if (!notAddresses.isEmpty())
                    remedy.append("; the entries ").append(notAddresses).append(" are not addresses with port (<ip>:<port>) and must be removed from it by hand");
            }
            // a configuration refusal whatever the evidence, an unreadable record included: the start with NONE is what is
            // refused, and DSE_6 is one of its remedies (loadDsePeersRecord, which cannot do without the record, reports
            // the same file as a disk state error)
            throw new StartupException(StartupException.ERR_WRONG_CONFIG,
                                       String.format("internode_compatibility_mode is NONE but %s: " +
                                                     "DSE 6 peers may still be in the cluster. Set internode_compatibility_mode: DSE_6, or %s, " +
                                                     "or start with -D%s=true",
                                                     found, remedy,
                                                     CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_DSE_PEERS.getKey()));
        }
    };

    /**
     * In {@code DSE_6} mode, loads the record of DSE 6 peers persisted by the previous run ({@link DsePeersRecord})
     * before any connection is made, so that a DSE peer that stays down during this run is not forgotten.
     */
    public static final StartupCheck loadDsePeersRecord = new StartupCheck()
    {
        @Override
        public void execute(StartupChecksOptions options) throws StartupException
        {
            try
            {
                DsePeersRecord.loadAtStartup();
            }
            catch (IOException | RuntimeException e)
            {
                throw new StartupException(StartupException.ERR_WRONG_DISK_STATE,
                                           "Unable to read the record of DSE 6 peers " + DsePeersRecord.path() +
                                           "; make it readable (it lists the DSE 6 peers of the last run)", e);
            }
        }
    };

    public static final StartupCheck checkSystemKeyspaceState = new StartupCheck()
    {
        @Override
        public void execute(StartupChecksOptions options) throws StartupException
        {
            if (options.isDisabled(getStartupCheckType()))
                return;
            // check the system keyspace to keep user from shooting self in foot by changing partitioner, cluster name, etc.
            // we do a one-off scrub of the system keyspace first; we can't load the list of the rest of the keyspaces,
            // until system keyspace is opened.

            for (TableMetadata cfm : Schema.instance.getTablesAndViews(SchemaConstants.SYSTEM_KEYSPACE_NAME))
                ColumnFamilyStore.scrubDataDirectories(cfm);

            try
            {
                SystemKeyspace.checkHealth();
            }
            catch (ConfigurationException e)
            {
                throw new StartupException(StartupException.ERR_WRONG_CONFIG, "Fatal exception during initialization", e);
            }
        }
    };

    /**
     * In {@code DSE_6} mode, warns when the snitch is {@link Ec2Snitch} (or {@code Ec2MultiRegionSnitch}) with the standard
     * EC2 names ({@code ec2_naming_scheme=standard}, the default): DSE 6 has no such setting and always used the legacy
     * names ({@code us-east} and {@code 1a} for the zone {@code us-east-1a}), so a node that
     * ran DSE gets another data center and rack, and {@link #checkDatacenter} and {@link #checkRack}, which run next,
     * refuse its start unless {@code cassandra.ignore_dc} and {@code cassandra.ignore_rack} are set
     * ({@code Ec2Snitch.validate} would refuse it later too). A warning, not a refusal: those checks already refuse a
     * node whose names changed; this one says why, and what to set, before them.
     */
    public static final StartupCheck checkDse6Ec2NamingScheme = new StartupCheck()
    {
        @Override
        public void execute(StartupChecksOptions options)
        {
            if (!DatabaseDescriptor.isDse6CompatibilityMode())
                return;
            IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
            if (snitch instanceof DynamicEndpointSnitch)
                snitch = ((DynamicEndpointSnitch) snitch).subsnitch;
            if (!(snitch instanceof Ec2Snitch) || ((Ec2Snitch) snitch).usesLegacyNaming())
                return;
            logger.warn("DSE 6 compatibility mode: {} names this node's data center {} and rack {} with ec2_naming_scheme=standard, the default; " +
                        "DSE 6 always used the legacy EC2 names (data center us-east and rack 1a for the zone us-east-1a), so the data center " +
                        "and rack checks refuse the start of a node that ran DSE, unless cassandra.ignore_dc and cassandra.ignore_rack are set. " +
                        "Set ec2_naming_scheme=legacy in {}, with the dc_suffix of DSE's cassandra-rackdc.properties when it sets one",
                        snitch.getClass().getSimpleName(), snitch.getLocalDatacenter(), snitch.getLocalRack(), SnitchProperties.RACKDC_PROPERTY_FILENAME);
        }
    };

    public static final StartupCheck checkDatacenter = new StartupCheck()
    {
        @Override
        public void execute(StartupChecksOptions options) throws StartupException
        {
            boolean enabled = options.isEnabled(getStartupCheckType());
            if (CassandraRelevantProperties.IGNORE_DC.isPresent())
            {
                logger.warn(String.format("Cassandra system property flag %s is deprecated and you should " +
                                          "use startup check configuration in cassandra.yaml",
                                          CassandraRelevantProperties.IGNORE_DC.getKey()));
                enabled = !CassandraRelevantProperties.IGNORE_DC.getBoolean();
            }
            if (enabled)
            {
                String storedDc = SystemKeyspace.getDatacenter();
                if (storedDc != null)
                {
                    String currentDc = DatabaseDescriptor.getEndpointSnitch().getLocalDatacenter();
                    if (!storedDc.equals(currentDc))
                    {
                        String formatMessage = "Cannot start node if snitch's data center (%s) differs from previous data center (%s). " +
                                               "Please fix the snitch configuration, decommission and rebootstrap this node or use the flag -Dcassandra.ignore_dc=true.";

                        throw new StartupException(StartupException.ERR_WRONG_CONFIG, String.format(formatMessage, currentDc, storedDc));
                    }
                }
            }
        }

        @Override
        public StartupCheckType getStartupCheckType()
        {
            return StartupCheckType.check_dc;
        }
    };

    public static final StartupCheck checkRack = new StartupCheck()
    {
        @Override
        public void execute(StartupChecksOptions options) throws StartupException
        {
            boolean enabled = options.isEnabled(getStartupCheckType());
            if (CassandraRelevantProperties.IGNORE_RACK.isPresent())
            {
                logger.warn(String.format("Cassandra system property flag %s is deprecated and you should " +
                                          "use startup check configuration in cassandra.yaml",
                                          CassandraRelevantProperties.IGNORE_RACK.getKey()));
                enabled = !CassandraRelevantProperties.IGNORE_RACK.getBoolean();
            }
            if (enabled)
            {
                String storedRack = SystemKeyspace.getRack();
                if (storedRack != null)
                {
                    String currentRack = DatabaseDescriptor.getEndpointSnitch().getLocalRack();
                    if (!storedRack.equals(currentRack))
                    {
                        String formatMessage = "Cannot start node if snitch's rack (%s) differs from previous rack (%s). " +
                                               "Please fix the snitch configuration, decommission and rebootstrap this node or use the flag -Dcassandra.ignore_rack=true.";

                        throw new StartupException(StartupException.ERR_WRONG_CONFIG, String.format(formatMessage, currentRack, storedRack));
                    }
                }
            }
        }

        @Override
        public StartupCheckType getStartupCheckType()
        {
            return StartupCheckType.check_rack;
        }
    };

    public static final StartupCheck checkLegacyAuthTables = new StartupCheck()
    {
        @Override
        public void execute(StartupChecksOptions options) throws StartupException
        {
            if (options.isDisabled(getStartupCheckType()))
                return;
            Optional<String> errMsg = checkLegacyAuthTablesMessage();
            if (errMsg.isPresent())
                throw new StartupException(StartupException.ERR_WRONG_CONFIG, errMsg.get());
        }
    };

    @VisibleForTesting
    public static Path getReadAheadKBPath(String blockDirectoryPath)
    {
        Path readAheadKBPath = null;

        final String READ_AHEAD_KB_SETTING_PATH = "/sys/block/%s/queue/read_ahead_kb";
        try
        {
            String[] blockDirComponents = blockDirectoryPath.split("/");
            if (blockDirComponents.length >= 2 && blockDirComponents[1].equals("dev"))
            {
                String deviceName = blockDirComponents[2].replaceAll("[0-9]*$", "");
                if (StringUtils.isNotEmpty(deviceName))
                {
                    readAheadKBPath = File.getPath(String.format(READ_AHEAD_KB_SETTING_PATH, deviceName));
                }
            }
        }
        catch (Exception e)
        {
            logger.error("Error retrieving device path for {}.", blockDirectoryPath);
        }

        return readAheadKBPath;
    }

    @VisibleForTesting
    static Optional<String> checkLegacyAuthTablesMessage()
    {
        List<String> existing = new ArrayList<>(SchemaConstants.LEGACY_AUTH_TABLES).stream().filter((legacyAuthTable) ->
            {
                UntypedResultSet result = QueryProcessor.executeOnceInternal(String.format("SELECT table_name FROM %s.%s WHERE keyspace_name='%s' AND table_name='%s'",
                                                                                           SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                                                                           "tables",
                                                                                           SchemaConstants.AUTH_KEYSPACE_NAME,
                                                                                           legacyAuthTable));
                return result != null && !result.isEmpty();
            }).collect(Collectors.toList());

        if (!existing.isEmpty())
            return Optional.of(String.format("Legacy auth tables %s in keyspace %s still exist and have not been properly migrated.",
                        Joiner.on(", ").join(existing), SchemaConstants.AUTH_KEYSPACE_NAME));
        else
            return Optional.empty();
    };

    public static final StartupCheck checkYamlConfig = new StartupCheck()
    {
        @Override
        public void execute(StartupChecksOptions options) throws StartupException
        {
            if (options.isDisabled(getStartupCheckType()))
                return;

            if (Murmur3Partitioner.instance != DatabaseDescriptor.getPartitioner())
                logger.warn("Not using murmur3 partitioner ({}). {}", DatabaseDescriptor.getPartitioner().getClass().getName(), WARN_SUFFIX);

            if (DatabaseDescriptor.getEndpointSnitch() instanceof SimpleSnitch)
                logger.warn("SimpleSnitch is only for dev/test environments. {}", WARN_SUFFIX);

            if (DatabaseDescriptor.getNumTokens() > 16)
                logger.warn("num_tokens {} too high. Values over 16 poorly impact repairs and node bootstrapping/decommissioning. {}",
                            DatabaseDescriptor.getNumTokens(), WARN_SUFFIX);

            if (!BtiFormat.NAME.equals(DatabaseDescriptor.getSelectedSSTableFormat().name()))
                logger.warn("Trie-based SSTables (bti) should always be the default (current is {}). {}", DatabaseDescriptor.getSelectedSSTableFormat().name(), WARN_SUFFIX);

            // ServerTestUtils.prepare() enables transient replication, so we have to skip this when we're inside a unit test
            if(DatabaseDescriptor.isTransientReplicationEnabled() && "cassandra.testtag_IS_UNDEFINED".equals(CassandraRelevantProperties.TEST_CASSANDRA_TESTTAG.getString()))
                throw new StartupException(StartupException.ERR_WRONG_CONFIG, "Transient Replication cannot be used in HCD.");

            if(DatabaseDescriptor.getMaterializedViewsEnabled())
                logger.warn("Materialised Views should not be enabled. {}", WARN_SUFFIX);

            // skip this when we're inside a unit test
            if(DatabaseDescriptor.getSASIIndexesEnabled() && "cassandra.testtag_IS_UNDEFINED".equals(CassandraRelevantProperties.TEST_CASSANDRA_TESTTAG.getString()))
                throw new StartupException(StartupException.ERR_WRONG_CONFIG, "SASI cannot be used in HCD. All SASI indexes must be dropped and this option disabled.");

            if(DatabaseDescriptor.enableDropCompactStorage())
                logger.warn("Using `DROP COMPACT STORAGE` on tables should not be enabled. {}", WARN_SUFFIX);

            if(null != DatabaseDescriptor.getDefaultCompaction()
                    && !UnifiedCompactionStrategy.class.getSimpleName().equals(DatabaseDescriptor.getDefaultCompaction().class_name))
                logger.warn("UnifiedCompactionStrategy should always be the default. {}", WARN_SUFFIX);

            if (DatabaseDescriptor.getGuardrailsConfig().getTombstoneFailThreshold() > 100000)
                logger.warn("Guardrails value {} for tombstone_failure_threshold is too high (>100000). {}",
                            DatabaseDescriptor.getGuardrailsConfig().getTombstoneFailThreshold(), WARN_SUFFIX);

            if (DatabaseDescriptor.getBatchSizeFailThresholdInKiB() > 640)
                logger.warn("Guardrails value {} for batch_size_fail_threshold_in_kb is too high (>640). {}",
                        DatabaseDescriptor.getBatchSizeFailThresholdInKiB(), WARN_SUFFIX);

            if (DatabaseDescriptor.getGuardrailsConfig().getColumnsPerTableFailThreshold() > 200)
                logger.warn("Guardrails value {} for columns_per_table_fail_threshold is too high (>200). {}",
                            DatabaseDescriptor.getGuardrailsConfig().getColumnsPerTableFailThreshold(), WARN_SUFFIX);

            if (DatabaseDescriptor.getGuardrailsConfig().getFieldsPerUDTFailThreshold() > 100)
                logger.warn("Guardrails value {} for fields_per_udt_fail_threshold is too high (>100). {}",
                            DatabaseDescriptor.getGuardrailsConfig().getFieldsPerUDTFailThreshold(), WARN_SUFFIX);

            DataStorageSpec.LongBytesBound collectionSizeWarnThreshold = DatabaseDescriptor.getGuardrailsConfig().getCollectionSizeWarnThreshold();
            if (collectionSizeWarnThreshold != null && collectionSizeWarnThreshold.toKibibytes() > 10480)
                logger.warn("Guardrails value {} for collection_size_warn_threshold is too high (>10480). {}",
                            collectionSizeWarnThreshold, WARN_SUFFIX);

            if (DatabaseDescriptor.getGuardrailsConfig().getItemsPerCollectionWarnThreshold() > 200)
                logger.warn("Guardrails value {} for items_per_collection_warn_threshold is too high (>200). {}",
                            DatabaseDescriptor.getGuardrailsConfig().getItemsPerCollectionWarnThreshold(), WARN_SUFFIX);

            if (DatabaseDescriptor.getGuardrailsConfig().getTablesWarnThreshold() > 100)
                logger.warn("Guardrails value {} for tables_warn_threshold is too high (>100). {}",
                            DatabaseDescriptor.getGuardrailsConfig().getTablesWarnThreshold(), WARN_SUFFIX);

            if (DatabaseDescriptor.getGuardrailsConfig().getTablesFailThreshold() > 200)
                logger.warn("Guardrails value {} for tables_fail_threshold is too high (>200). {}",
                            DatabaseDescriptor.getGuardrailsConfig().getTablesFailThreshold(), WARN_SUFFIX);

            if (DatabaseDescriptor.getGuardrailsConfig().getInSelectCartesianProductFailThreshold() > 25)
                logger.warn("Guardrails value {} for in_select_cartesian_product_fail_threshold is too high (>25). {}",
                            DatabaseDescriptor.getGuardrailsConfig().getInSelectCartesianProductFailThreshold(), WARN_SUFFIX);

            if (DatabaseDescriptor.getGuardrailsConfig().getPartitionKeysInSelectFailThreshold() > 20)
                logger.warn("Guardrails value {} for partition_keys_in_select_fail_threshold is too high (>20). {}",
                            DatabaseDescriptor.getGuardrailsConfig().getPartitionKeysInSelectFailThreshold(), WARN_SUFFIX);

            if (!DatabaseDescriptor.getGuardrailsConfig().getWriteConsistencyLevelsDisallowed().contains(ConsistencyLevel.ANY))
                logger.warn("Guardrails value \"{}\" for write_consistency_levels_disallowed does not contain \"ANY\". {}",
                            StringUtils.join(DatabaseDescriptor.getGuardrailsConfig().getWriteConsistencyLevelsDisallowed(), ','), WARN_SUFFIX);
        }
    };

    public static final StartupCheck checkTableSettings = new StartupCheck()
    {
        @Override
        public void execute(StartupChecksOptions options)
        {
            if (options.isDisabled(getStartupCheckType()))
                return;
                
            List<String> stcsOrLcsTables = new ArrayList<>();
            List<String> compactTables = new ArrayList<>();
            List<String> nonSAITables = new ArrayList<>();
            for (String ksName : Schema.instance.getUserKeyspaces())
            {
                KeyspaceMetadata ks = Schema.instance.getKeyspaceMetadata(ksName);
                if (ks == null)
                    continue;
                for (TableMetadata t : ks.tables)
                {
                    if (SizeTieredCompactionStrategy.class == t.params.compaction.klass() || LeveledCompactionStrategy.class == t.params.compaction.klass())
                        stcsOrLcsTables.add(t.keyspace + '.' + t.name);
                    if (t.isCompactTable())
                        compactTables.add(t.keyspace + '.' + t.name);
                    for (IndexMetadata i : t.indexes)
                        if (!StorageAttachedIndex.class.getName().equals(i.getIndexClassName()))
                            nonSAITables.add(t.keyspace + '.' + t.name);
                }
            }

            if (!stcsOrLcsTables.isEmpty())
                logger.warn("The following tables using STCS and LCS should be altered to use UnifiedCompactionStrategy (UCS): {}. {}",
                            StringUtils.join(stcsOrLcsTables, ','), WARN_SUFFIX);

            if (!compactTables.isEmpty())
                logger.warn("The following tables are `WITH COMPACT STORAGE` and need to be manually migrated to normal tables (Avoid using `DROP COMPACT STORAGE`): {}. {}",
                        StringUtils.join(compactTables, ','), WARN_SUFFIX);

            if (!nonSAITables.isEmpty())
                logger.warn("The following tables with non-SAI indexes should be altered to use SAI: {}. {}",
                        StringUtils.join(nonSAITables, ','), WARN_SUFFIX);
        }
    };
}
