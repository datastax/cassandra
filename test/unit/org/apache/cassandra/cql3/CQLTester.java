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
package org.apache.cassandra.cql3;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.rmi.server.RMISocketFactory;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.cassandra.service.ClientWarn;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import com.codahale.metrics.Gauge;
import com.datastax.driver.core.CloseFuture;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.NettyOptions;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.Statement;
import com.datastax.shaded.netty.channel.EventLoopGroup;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.auth.CassandraAuthorizer;
import org.apache.cassandra.auth.CassandraRoleManager;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.types.ParseUtils;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.db.virtual.SystemViewsKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.db.virtual.VirtualSchemaKeyspace;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.nodes.Nodes;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JMXServerUtils;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.TimeUUID;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;

import static com.datastax.driver.core.SocketOptions.DEFAULT_CONNECT_TIMEOUT_MILLIS;
import static com.datastax.driver.core.SocketOptions.DEFAULT_READ_TIMEOUT_MILLIS;
import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;
import static org.apache.cassandra.index.sai.SAITester.vector;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Base class for CQL tests.
 */
public abstract class CQLTester
{
    protected static final Logger logger = LoggerFactory.getLogger(CQLTester.class);

    public static final String KEYSPACE = "cql_test_keyspace";
    public static final String KEYSPACE_PER_TEST = "cql_test_keyspace_alt";
    protected static final boolean USE_PREPARED_VALUES = Boolean.valueOf(System.getProperty("cassandra.test.use_prepared", "true"));
    protected static final boolean REUSE_PREPARED = Boolean.valueOf(System.getProperty("cassandra.test.reuse_prepared", "true"));
    protected static final long ROW_CACHE_SIZE_IN_MB = Integer.valueOf(System.getProperty("cassandra.test.row_cache_size_in_mb", "0"));
    private static final AtomicInteger seqNumber = new AtomicInteger();
    protected static final ByteBuffer TOO_BIG = ByteBuffer.allocate(FBUtilities.MAX_UNSIGNED_SHORT + 1024);
    public static final String DATA_CENTER = ServerTestUtils.DATA_CENTER;
    public static final String DATA_CENTER_REMOTE = ServerTestUtils.DATA_CENTER_REMOTE;
    public static final String RACK1 = ServerTestUtils.RACK1;
    private static final int ASSERTION_TIMEOUT_SECONDS = 15;
    private static final User SUPER_USER = new User("cassandra", "cassandra");

    /**
     * Whether to use coordinator execution in {@link #execute(String, Object...)}, so queries get full validation and
     * go through reconciliation. When enabled, calls to {@link #execute(String, Object...)} will behave as calls to
     * {@link #executeWithCoordinator(String, Object...)}. Otherwise, they will behave as calls to
     * {@link #executeInternal(String, Object...)}.
     *
     * @see #execute
     */
    private static boolean coordinatorExecution = false;

    private static org.apache.cassandra.transport.Server server;
    private static JMXConnectorServer jmxServer;
    protected static String jmxHost;
    protected static int jmxPort;
    protected static MBeanServerConnection jmxConnection;

    private static Randomization random;

    protected static final int nativePort;
    protected static final InetAddress nativeAddr;
    protected static final Set<InetAddressAndPort> remoteAddrs = new HashSet<>();
    private static final Map<Pair<User, ProtocolVersion>, Cluster> clusters = new HashMap<>();
    protected static final Map<Pair<User, ProtocolVersion>, Session> sessions = new HashMap<>();

    private static final ThreadPoolExecutor schemaCleanup =
    new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());


    public static final List<ProtocolVersion> PROTOCOL_VERSIONS = new ArrayList<>(ProtocolVersion.SUPPORTED.size());

    private static final String CREATE_INDEX_NAME_REGEX = "(\\s*(\\w*|\"\\w*\")\\s*)";
    private static final String CREATE_INDEX_NAME_QUOTED_REGEX = "(\\s*(\\w*|\"[^\"]*\")\\s*)";
    private static final String CREATE_INDEX_REGEX = String.format("\\A\\s*CREATE(?:\\s+CUSTOM)?\\s+INDEX" +
                                                                   "(?:\\s+IF\\s+NOT\\s+EXISTS)?\\s*" +
                                                                   "%s?\\s*ON\\s+(%<s\\.)?%<s\\s*" +
                                                                   "(\\((?:\\s*\\w+\\s*\\()?%s\\))?",
                                                                   CREATE_INDEX_NAME_REGEX, CREATE_INDEX_NAME_QUOTED_REGEX);
    private static final Pattern CREATE_INDEX_PATTERN = Pattern.compile(CREATE_INDEX_REGEX, Pattern.CASE_INSENSITIVE);

    public static final NettyOptions IMMEDIATE_CONNECTION_SHUTDOWN_NETTY_OPTIONS = new NettyOptions()
    {
        @Override
        public void onClusterClose(EventLoopGroup eventLoopGroup)
        {
            // shutdown driver connection immediatelly
            eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS).syncUninterruptibly();
        }
    };

    /** Return the current server version if supported by the driver, else
     * the latest that is supported.
     *
     * @return - the preferred versions that is also supported by the driver
     */
    public static final ProtocolVersion getDefaultVersion()
    {
        return PROTOCOL_VERSIONS.contains(ProtocolVersion.CURRENT)
               ? ProtocolVersion.CURRENT
               : PROTOCOL_VERSIONS.get(PROTOCOL_VERSIONS.size() - 1);
    }

    static
    {
        checkProtocolVersion();

        nativeAddr = InetAddress.getLoopbackAddress();
        nativePort = getAutomaticallyAllocatedPort(nativeAddr);

        ServerTestUtils.daemonInitialization();
    }

    private List<String> keyspaces = new ArrayList<>();
    private List<String> tables = new ArrayList<>();
    private List<String> views = new ArrayList<>();
    private List<String> types = new ArrayList<>();
    private List<String> functions = new ArrayList<>();
    private List<String> aggregates = new ArrayList<>();
    private List<String> indexes = new ArrayList<>();
    private User user;

    // We don't use USE_PREPARED_VALUES in the code below so some test can foce value preparation (if the result
    // is not expected to be the same without preparation)
    private boolean usePrepared = USE_PREPARED_VALUES;
    private static boolean reusePrepared = REUSE_PREPARED;

    protected boolean usePrepared()
    {
        return usePrepared;
    }

    /**
     * Returns a port number that is automatically allocated,
     * typically from an ephemeral port range.
     *
     * @return a port number
     */
    private static int getAutomaticallyAllocatedPort(InetAddress address)
    {
        try
        {
            try (ServerSocket sock = new ServerSocket())
            {
                // A port number of {@code 0} means that the port number will be automatically allocated,
                // typically from an ephemeral port range.
                sock.bind(new InetSocketAddress(address, 0));
                return sock.getLocalPort();
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static void checkProtocolVersion()
    {
        // The latest versions might not be supported yet by the java driver
        for (ProtocolVersion version : ProtocolVersion.SUPPORTED)
        {
            try
            {
                com.datastax.driver.core.ProtocolVersion.fromInt(version.asInt());
                PROTOCOL_VERSIONS.add(version);
            }
            catch (IllegalArgumentException e)
            {
                logger.warn("Protocol Version {} not supported by java driver", version);
            }
        }
    }

    public static void prepareServer()
    {
        ServerTestUtils.prepareServer();
    }

    public static void cleanup()
    {
        ServerTestUtils.cleanup();
    }

    /**
     * Starts the JMX server. It's safe to call this method multiple times.
     */
    public static void startJMXServer() throws Exception
    {
        if (jmxServer != null)
            return;

        InetAddress loopback = InetAddress.getLoopbackAddress();
        jmxHost = loopback.getHostAddress();
        jmxPort = getAutomaticallyAllocatedPort(loopback);
        jmxServer = JMXServerUtils.createJMXServer(jmxPort, true);
        jmxServer.start();
    }

    public static void createMBeanServerConnection() throws Exception
    {
        assert jmxServer != null : "jmxServer not started";

        Map<String, Object> env = new HashMap<>();
        env.put("com.sun.jndi.rmi.factory.socket", RMISocketFactory.getDefaultSocketFactory());
        JMXConnector jmxc = JMXConnectorFactory.connect(getJMXServiceURL(), env);
        jmxConnection =  jmxc.getMBeanServerConnection();
    }

    public static JMXServiceURL getJMXServiceURL() throws MalformedURLException
    {
        assert jmxServer != null : "jmxServer not started";

        return new JMXServiceURL(String.format("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi", jmxHost, jmxPort));
    }

    public static Randomization getRandom()
    {
        if (random == null)
            random = new Randomization();
        return random;
    }

    @Rule
    public FailureWatcher failureRule = new FailureWatcher();

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.setAutoSnapshot(false);

        if (ROW_CACHE_SIZE_IN_MB > 0)
            DatabaseDescriptor.setRowCacheSizeInMB(ROW_CACHE_SIZE_IN_MB);
        StorageService.instance.setPartitionerUnsafe(Murmur3Partitioner.instance);

        // Once per-JVM is enough
        prepareServer();
    }

    @AfterClass
    public static void tearDownClass()
    {
        for (Session sess : sessions.values())
                sess.close();
        for (Cluster cl : clusters.values())
                cl.close();

        if (server != null)
            server.stop();

        // We use queryInternal for CQLTester so prepared statement will populate our internal cache (if reusePrepared is used; otherwise prepared
        // statements are not cached but re-prepared every time). So we clear the cache between test files to avoid accumulating too much.
        if (reusePrepared)
            QueryProcessor.clearInternalStatementsCache();

        TokenMetadata metadata = StorageService.instance.getTokenMetadata();
        metadata.clearUnsafe();

        if (jmxServer != null && jmxServer instanceof RMIConnectorServer)
        {
            try
            {
                ((RMIConnectorServer) jmxServer).stop();
            }
            catch (IOException e)
            {
                logger.warn("Error shutting down jmx", e);
            }
        }
    }

    @Before
    public void beforeTest() throws Throwable
    {
        Schema.instance.transform(schema -> schema.withAddedOrUpdated(KeyspaceMetadata.create(KEYSPACE_PER_TEST, KeyspaceParams.simple(1)))
                                                  .withAddedOrUpdated(KeyspaceMetadata.create(KEYSPACE, KeyspaceParams.simple(1))), false);
    }

    @After
    public void afterTest() throws Throwable
    {
        // Restore standard behavior in case it was changed
        usePrepared = USE_PREPARED_VALUES;
        reusePrepared = REUSE_PREPARED;

        final List<String> keyspacesToDrop = copy(keyspaces);
        final List<String> tablesToDrop = copy(tables);
        final List<String> viewsToDrop = copy(views);
        keyspaces = null;
        tables = null;
        views = null;
        types = null;
        functions = null;
        aggregates = null;
        user = null;

        try
        {
            Schema.instance.transform(schema -> schema.without(List.of(KEYSPACE_PER_TEST))
                                                      .withAddedOrUpdated(KeyspaceMetadata.create(KEYSPACE, KeyspaceParams.simple(1)))
                                                      .without(keyspacesToDrop), false);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Blocks until the previous schema cleanup task finished.
     */
    public void waitForSchemaCleanupCompleted(long timeout, TimeUnit unit)
    {
        Awaitility.await().atMost(timeout, unit).until(() -> schemaCleanup.getActiveCount() == 0);
    }

    public static List<String> buildNodetoolArgs(List<String> args)
    {
        int port = jmxPort == 0 ? Integer.getInteger("cassandra.jmx.local.port", 7199) : jmxPort;
        String host = jmxHost == null ? "127.0.0.1" : jmxHost;
        List<String> allArgs = new ArrayList<>();
        allArgs.add("bin/nodetool");
        allArgs.add("-p");
        allArgs.add(String.valueOf(port));
        allArgs.add("-h");
        allArgs.add(host);
        allArgs.addAll(args);
        return allArgs;
    }

    public static List<String> buildCqlshArgs(List<String> args)
    {
        List<String> allArgs = new ArrayList<>();
        allArgs.add("bin/cqlsh");
        allArgs.add(nativeAddr.getHostAddress());
        allArgs.add(Integer.toString(nativePort));
        allArgs.add("-e");
        allArgs.addAll(args);
        return allArgs;
    }

    public static List<String> buildCassandraStressArgs(List<String> args)
    {
        List<String> allArgs = new ArrayList<>();
        allArgs.add("tools/bin/cassandra-stress");
        allArgs.addAll(args);
        if (args.indexOf("-port") == -1)
        {
            allArgs.add("-port");
            allArgs.add("native=" + Integer.toString(nativePort));
        }
        return allArgs;
    }

    protected static void requireNetworkWithoutDriver()
    {
        if (server != null)
            return;

        startServices();
        startServer(server -> {});
    }

    protected static void requireAuthentication()
    {
        System.setProperty("cassandra.superuser_setup_delay_ms", "-1");

        DatabaseDescriptor.setAuthenticator(new PasswordAuthenticator());
        DatabaseDescriptor.setRoleManager(new CassandraRoleManager());
        DatabaseDescriptor.setAuthorizer(new CassandraAuthorizer());
    }

    // lazy initialization for all tests that require Java Driver
    protected static void requireNetwork() throws ConfigurationException
    {
        requireNetwork(server -> {});
    }

    // lazy initialization for all tests that require Java Driver
    protected static void requireNetwork(Consumer<Server.Builder> decorator) throws ConfigurationException
    {
        if (server != null)
            return;

        startServices();
        initializeNetwork(decorator, null);
        Nodes.Instance.persistLocalMetadata();
    }

    private static void startServices()
    {
        VirtualKeyspaceRegistry.instance.register(VirtualSchemaKeyspace.instance);
        VirtualKeyspaceRegistry.instance.register(SystemViewsKeyspace.instance);
        StorageService.instance.initServer();
        SchemaLoader.startGossiper();
    }

    protected static void reinitializeNetwork()
    {
        reinitializeNetwork(null);
    }

    protected static void reinitializeNetwork(Consumer<Cluster.Builder> clusterConfigurator)
    {
        if (server != null && server.isRunning())
        {
            server.stop();
            server = null;
        }
        List<CloseFuture> futures = new ArrayList<>();
        for (Cluster cluster : clusters.values())
            futures.add(cluster.closeAsync());
        for (Session session : sessions.values())
            futures.add(session.closeAsync());
        FBUtilities.waitOnFutures(futures);
        clusters.clear();
        sessions.clear();

        initializeNetwork(server -> {}, clusterConfigurator);
    }

    private static void initializeNetwork(Consumer<Server.Builder> decorator, Consumer<Cluster.Builder> clusterConfigurator)
    {
        startServer(decorator);
    }

    private static void startServer(Consumer<Server.Builder> decorator)
    {
        Server.Builder serverBuilder = new Server.Builder().withHost(nativeAddr).withPort(nativePort);
        decorator.accept(serverBuilder);
        server = serverBuilder.build();
        ClientMetrics.instance.init(Collections.singleton(server));
        server.start();
    }

    protected void dropPerTestKeyspace() throws Throwable
    {
        execute(String.format("DROP KEYSPACE IF EXISTS %s", KEYSPACE_PER_TEST));
    }

    /**
     * Returns a copy of the specified list.
     * @return a copy of the specified list.
     */
    private static List<String> copy(List<String> list)
    {
        return list.isEmpty() ? Collections.<String>emptyList() : new ArrayList<>(list);
    }

    public ColumnFamilyStore getCurrentColumnFamilyStore()
    {
        return getCurrentColumnFamilyStore(KEYSPACE);
    }

    public ColumnFamilyStore getCurrentColumnFamilyStore(String keyspace)
    {
        String currentTable = currentTable();
        return currentTable == null
             ? null
             : getColumnFamilyStore(keyspace, currentTable);
    }

    public ColumnFamilyStore getColumnFamilyStore(String keyspace, String table)
    {
        return Keyspace.open(keyspace).getColumnFamilyStore(table);
    }

    public ColumnMetadata getColumn(String name)
    {
        return getCurrentColumnFamilyStore().metadata.get().getColumn(ColumnIdentifier.getInterned(name, true));
    }

    public ColumnMetadata getDroppedColumn(String name)
    {
        return getCurrentColumnFamilyStore().metadata.get().getDroppedColumn(ColumnIdentifier.getInterned(name, true).bytes);
    }

    public void flush(boolean forceFlush)
    {
        if (forceFlush)
            flush();
    }

    public void flush()
    {
        flush(KEYSPACE);
    }

    public void flush(String keyspace)
    {
        flush(keyspace, currentTable());
    }

    public void flush(String keyspace, String table)
    {
        ColumnFamilyStore store = getColumnFamilyStore(keyspace, table);
        if (store != null)
            store.forceBlockingFlush(UNIT_TESTS);
    }

    public void disableCompaction(String keyspace)
    {
        disableCompaction(keyspace, currentTable());
    }

    public void disableCompaction(String keyspace, String table)
    {
        ColumnFamilyStore store = getColumnFamilyStore(keyspace, table);
        if (store != null)
            store.disableAutoCompaction();
    }

    public void compact()
    {
        compact(KEYSPACE, currentTable());
    }

    public void compact(String keyspace, String table)
    {
        ColumnFamilyStore store = getColumnFamilyStore(keyspace, table);
        if (store != null)
            store.forceMajorCompaction();
    }

    public void compact(String keyspace)
    {
        ColumnFamilyStore store = getCurrentColumnFamilyStore(keyspace);
        if (store != null)
            store.forceMajorCompaction();
    }

    public void disableCompaction()
    {
        disableCompaction(KEYSPACE);
    }

    public void enableCompaction(String keyspace)
    {
        ColumnFamilyStore store = getCurrentColumnFamilyStore(keyspace);
        if (store != null)
            store.enableAutoCompaction();
    }

    public void enableCompaction()
    {
        enableCompaction(KEYSPACE);
    }

    public void cleanupCache()
    {
        ColumnFamilyStore store = getCurrentColumnFamilyStore();
        if (store != null)
            store.cleanupCache();
    }

    public static FunctionName parseFunctionName(String qualifiedName)
    {
        int i = qualifiedName.indexOf('.');
        return i == -1
               ? FunctionName.nativeFunction(qualifiedName)
               : new FunctionName(qualifiedName.substring(0, i).trim(), qualifiedName.substring(i+1).trim());
    }

    public static String shortFunctionName(String f)
    {
        return parseFunctionName(f).name;
    }

    private static void removeAllSSTables(String ks, List<String> tables)
    {
        // clean up data directory which are stored as data directory/keyspace/data files
        for (File d : Directories.getKSChildDirectories(ks))
        {
            if (d.exists() && containsAny(d.name(), tables))
                FileUtils.deleteRecursive(d);
        }
    }

    private static boolean containsAny(String filename, List<String> tables)
    {
        for (int i = 0, m = tables.size(); i < m; i++)
            // don't accidentally delete in-use directories with the
            // same prefix as a table to delete, i.e. table_1 & table_11
            if (filename.contains(tables.get(i) + "-"))
                return true;
        return false;
    }

    protected String keyspace()
    {
        return KEYSPACE;
    }

    protected String currentTable()
    {
        if (tables.isEmpty())
            return null;
        return tables.get(tables.size() - 1);
    }

    protected String currentView()
    {
        if (views.isEmpty())
            return null;
        return views.get(views.size() - 1);
    }

    protected String currentKeyspace()
    {
        if (keyspaces.isEmpty())
            return null;
        return keyspaces.get(keyspaces.size() - 1);
    }

    protected String currentIndex()
    {
        if (indexes.isEmpty())
            return null;
        return indexes.get(indexes.size() - 1);
    }

    protected String getIndex(int i)
    {
        return indexes.get(i);
    }

    protected Index getIndex(String indexName)
    {
        return getCurrentColumnFamilyStore().indexManager.getIndexByName(indexName);
    }

    protected Collection<String> currentTables()
    {
        if (tables == null || tables.isEmpty())
            return ImmutableList.of();

        return new ArrayList<>(tables);
    }

    protected ByteBuffer unset()
    {
        return ByteBufferUtil.UNSET_BYTE_BUFFER;
    }

    protected void forcePreparedValues()
    {
        this.usePrepared = true;
    }

    protected void stopForcingPreparedValues()
    {
        this.usePrepared = USE_PREPARED_VALUES;
    }

    public static void disablePreparedReuseForTest()
    {
        reusePrepared = false;
    }

    protected String createType(String query)
    {
        return createType(KEYSPACE, query);
    }

    protected String createType(String keyspace, String query)
    {
        String typeName = createTypeName();
        String fullQuery = String.format(query, keyspace + "." + typeName);
        logger.info(fullQuery);
        schemaChange(fullQuery);
        return typeName;
    }

    protected String createTypeName()
    {
        String typeName = String.format("type_%02d", seqNumber.getAndIncrement());
        types.add(typeName);
        return typeName;
    }

    protected String createFunctionName(String keyspace)
    {
        return String.format("%s.function_%02d", keyspace, seqNumber.getAndIncrement());
    }

    protected void registerFunction(String functionName, String argTypes)
    {
        functions.add(functionName + '(' + argTypes + ')');
    }

    protected String createFunction(String keyspace, String argTypes, String query) throws Throwable
    {
        String functionName = createFunctionName(keyspace);
        createFunctionOverload(functionName, argTypes, query);
        return functionName;
    }

    protected void createFunctionOverload(String functionName, String argTypes, String query) throws Throwable
    {
        registerFunction(functionName, argTypes);
        String fullQuery = String.format(query, functionName);
        logger.info(fullQuery);
        schemaChange(fullQuery);
    }

    protected String createAggregateName(String keyspace)
    {
        return String.format("%s.aggregate_%02d", keyspace, seqNumber.getAndIncrement());
    }

    protected void registerAggregate(String aggregateName, String argTypes)
    {
        aggregates.add(aggregateName + '(' + argTypes + ')');
    }

    protected String createAggregate(String keyspace, String argTypes, String query) throws Throwable
    {
        String aggregateName = createAggregateName(keyspace);
        createAggregateOverload(aggregateName, argTypes, query);
        return aggregateName;
    }

    protected void createAggregateOverload(String aggregateName, String argTypes, String query) throws Throwable
    {
        String fullQuery = String.format(query, aggregateName);
        registerAggregate(aggregateName, argTypes);
        logger.info(fullQuery);
        schemaChange(fullQuery);
    }

    protected String createKeyspace(String query)
    {
        String currentKeyspace = createKeyspaceName();
        String fullQuery = String.format(query, currentKeyspace);
        logger.info(fullQuery);
        schemaChange(fullQuery);
        return currentKeyspace;
    }

    protected void alterKeyspace(String query)
    {
        String fullQuery = String.format(query, currentKeyspace());
        logger.info(fullQuery);
        schemaChange(fullQuery);
    }

    protected void alterKeyspaceMayThrow(String query) throws Throwable
    {
        String fullQuery = String.format(query, currentKeyspace());
        logger.info(fullQuery);
        QueryProcessor.executeOnceInternal(fullQuery);
    }

    protected String createKeyspaceName()
    {
        String currentKeyspace = String.format("keyspace_%02d", seqNumber.getAndIncrement());
        keyspaces.add(currentKeyspace);
        return currentKeyspace;
    }

    public String createTable(String query)
    {
        return createTable(KEYSPACE, query);
    }

    protected String createViewName()
    {
        String currentView = "view_" + seqNumber.getAndIncrement();
        views.add(currentView);
        return currentView;
    }

    protected String createTable(String keyspace, String query)
    {
        return createTable(keyspace, query, null);
    }

    protected String createTable(String keyspace, String query, String tableName)
    {
        String currentTable = createTableName(tableName);
        String fullQuery = formatQuery(keyspace, query);
        logger.info(fullQuery);
        schemaChange(fullQuery);
        return currentTable;
    }

    protected String createTableName()
    {
        return createTableName(null);
    }

    protected String createTableName(String tableName)
    {
        String currentTable = tableName == null ? String.format("table_%02d", seqNumber.getAndIncrement()) : tableName;
        tables.add(currentTable);
        return currentTable;
    }

    protected void createTableMayThrow(String query) throws Throwable
    {
        String currentTable = createTableName();
        String fullQuery = formatQuery(query);
        logger.info(fullQuery);
        QueryProcessor.executeOnceInternal(fullQuery);
    }

    protected void alterTable(String query)
    {
        String fullQuery = formatQuery(query);
        logger.info(fullQuery);
        schemaChange(fullQuery);
    }

    protected void alterTableMayThrow(String query) throws Throwable
    {
        String fullQuery = formatQuery(query);
        logger.info(fullQuery);
        QueryProcessor.executeOnceInternal(fullQuery);
    }

    protected void dropTable(String query)
    {
        dropFormattedTable(String.format(query, KEYSPACE + "." + currentTable()));
    }

    public void dropView(String view)
    {
        dropFormattedTable(String.format("DROP MATERIALIZED VIEW IF EXISTS %s.%s", KEYSPACE, view));
        views.remove(view);
    }

    protected void dropFormattedTable(String formattedQuery)
    {
        logger.info(formattedQuery);
        schemaChange(formattedQuery);
    }

    /**
     * Creates a secondary index, waiting for it to become queryable.
     *
     * @param query the index creation query
     * @return the name of the created index
     */
    public String createIndex(String query)
    {
        return createIndex(KEYSPACE, query);
    }

    /**
     * Creates a secondary index, waiting for it to become queryable.
     *
     * @param keyspace the keyspace the created index should belong to
     * @param query the index creation query
     * @return the name of the created index
     */
    protected String createIndex(String keyspace, String query)
    {
        String formattedQuery = formatQuery(keyspace, query);
        Pair<String, String> qualifiedIndexName = createFormattedIndex(keyspace, formattedQuery);
        waitForIndexQueryable(qualifiedIndexName.left, qualifiedIndexName.right);
        return qualifiedIndexName.right;
    }

    /**
     * Creates a secondary index without waiting for it to become queryable.
     *
     * @param query the index creation query
     * @return the name of the created index
     */
    protected String createIndexAsync(String query)
    {
        return createIndexAsync(KEYSPACE, query);
    }

    /**
     * Creates a secondary index without waiting for it to become queryable.
     *
     * @param keyspace the keyspace the created index should belong to
     * @param query the index creation query
     * @return the name of the created index
     */
    protected String createIndexAsync(String keyspace, String query)
    {
        String formattedQuery = formatQuery(keyspace, query);
        return createFormattedIndex(keyspace, formattedQuery).right;
    }

    private Pair<String, String> createFormattedIndex(String keyspace, String formattedQuery)
    {
        logger.info(formattedQuery);
        Pair<String, String> qualifiedIndexName = getCreateIndexName(keyspace, formattedQuery);
        indexes.add(qualifiedIndexName.right);
        schemaChange(formattedQuery);
        return qualifiedIndexName;
    }

    protected static Pair<String, String> getCreateIndexName(String keyspace, String formattedQuery)
    {
        Matcher matcher = CREATE_INDEX_PATTERN.matcher(formattedQuery);
        if (!matcher.find())
            throw new IllegalArgumentException("Expected valid create index query but found: " + formattedQuery);

        String parsedKeyspace = matcher.group(5);
        if (!Strings.isNullOrEmpty(parsedKeyspace))
            keyspace = parsedKeyspace;

        String index = matcher.group(2);
        boolean isQuotedGeneratedIndexName = false;
        if (Strings.isNullOrEmpty(index))
        {
            String table = matcher.group(7);
            if (Strings.isNullOrEmpty(table))
                throw new IllegalArgumentException("Table name should be specified: " + formattedQuery);

            String column = matcher.group(9);
            isQuotedGeneratedIndexName = ParseUtils.isQuoted(column, '\"');

            String baseName = Strings.isNullOrEmpty(column)
                              ? IndexMetadata.generateDefaultIndexName(table, null)
                              : IndexMetadata.generateDefaultIndexName(table, new ColumnIdentifier(column, true));

            KeyspaceMetadata ks = Schema.instance.getKeyspaceMetadata(keyspace);
            assertNotNull(ks);
            index = ks.findAvailableIndexName(baseName);
        }
        index = ParseUtils.isQuoted(index, '\"')
                ? ParseUtils.unDoubleQuote(index)
                : isQuotedGeneratedIndexName ? index : index.toLowerCase();

        return Pair.create(keyspace, index);
    }

    public void waitForTableIndexesQueryable()
    {
        waitForTableIndexesQueryable(60);
    }

    public void waitForTableIndexesQueryable(int seconds)
    {
        waitForTableIndexesQueryable(currentTable(), seconds);
    }

    public void waitForTableIndexesQueryable(String table, int seconds)
    {
        waitForTableIndexesQueryable(KEYSPACE, table, seconds);
    }

    public void waitForTableIndexesQueryable(String keyspace, String table)
    {
        waitForTableIndexesQueryable(keyspace, table, 60);
    }

    /**
     * Index creation is asynchronous. This method waits until all the indexes in the specified table are queryable.
     *
     * @param keyspace the table keyspace name
     * @param table the table name
     * @param seconds the maximum time to wait for the indexes to be queryable
     */
    public void waitForTableIndexesQueryable(String keyspace, String table, int seconds)
    {
        waitForAssert(() -> Assertions.assertThat(getNotQueryableIndexes(keyspace, table)).isEmpty(), seconds, TimeUnit.SECONDS);
    }

    public void waitForIndexQueryable(String index)
    {
        waitForIndexQueryable(KEYSPACE, index);
    }

    /**
     * Index creation is asynchronous. This method waits until the specified index is queryable.
     *
     * @param keyspace the index keyspace name
     * @param index the index name
     */
    public void waitForIndexQueryable(String keyspace, String index)
    {
        waitForIndexQueryable(keyspace, index, 1, TimeUnit.MINUTES);
    }

    /**
     * Index creation is asynchronous. This method waits until the specified index is queryable.
     *
     * @param keyspace the index keyspace name
     * @param index the index name
     * @param timeout the timeout
     * @param unit the timeout unit
     */
    public void waitForIndexQueryable(String keyspace, String index, long timeout, TimeUnit unit)
    {
        waitForAssert(() -> assertTrue(isIndexQueryable(keyspace, index)), timeout, unit);
    }

    protected void waitForIndexBuilds(String index)
    {
        waitForIndexBuilds(KEYSPACE, index);
    }

    /**
     * Index creation is asynchronous. This method waits until the specified index hasn't any building task running.
     * <p>
     * This method differs from {@link #waitForIndexQueryable(String, String)} in that it doesn't require the
     * index to be fully nor successfully built, so it can be used to wait for failing index builds.
     *
     * @param keyspace the index keyspace name
     * @param index the index name
     */
    protected void waitForIndexBuilds(String keyspace, String index)
    {
        waitForAssert(() -> assertFalse(isIndexBuilding(keyspace, index)), 60, TimeUnit.SECONDS);
    }

    /**
     * @return the names of the indexes in the current table that are not queryable
     */
    protected Set<String> getNotQueryableIndexes()
    {
        return getNotQueryableIndexes(KEYSPACE, currentTable());
    }

    /**
     * @param keyspace the table keyspace name
     * @param table the table name
     * @return the names of the indexes in the specified table that are not queryable
     */
    protected Set<String> getNotQueryableIndexes(String keyspace, String table)
    {
        SecondaryIndexManager sim = Keyspace.open(keyspace).getColumnFamilyStore(table).indexManager;
        return sim.listIndexes()
                  .stream()
                  .filter(index -> !sim.isIndexQueryable(index))
                  .map(index -> index.getIndexMetadata().name)
                  .collect(Collectors.toSet());
    }

    protected boolean isIndexBuilding(String keyspace, String indexName)
    {
        SecondaryIndexManager manager = getIndexManager(keyspace, indexName);
        assertNotNull(manager);

        return manager.isIndexBuilding(indexName);
    }

    protected boolean isIndexQueryable(String keyspace, String indexName)
    {
        SecondaryIndexManager manager = getIndexManager(keyspace, indexName);
        assertNotNull(manager);

        Index index = manager.getIndexByName(indexName);
        return manager.isIndexQueryable(index);
    }

    protected boolean areAllTableIndexesQueryable()
    {
        return areAllTableIndexesQueryable(KEYSPACE, currentTable());
    }

    protected boolean areAllTableIndexesQueryable(String keyspace, String table)
    {
        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        for (Index index : cfs.indexManager.listIndexes())
        {
            if (!cfs.indexManager.isIndexQueryable(index))
                return false;
        }
        return true;
    }

    protected boolean indexNeedsFullRebuild(String index)
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(currentTable());
        return cfs.indexManager.needsFullRebuild(index);
    }

    protected void verifyInitialIndexFailed(String indexName)
    {
        // Verify that the initial index build fails...
        waitForAssert(() -> assertTrue(indexNeedsFullRebuild(indexName)));
    }

    @Nullable
    protected SecondaryIndexManager getIndexManager(String keyspace, String indexName)
    {
        for (ColumnFamilyStore cfs : Keyspace.open(keyspace).getColumnFamilyStores())
        {
            Index index = cfs.indexManager.getIndexByName(indexName);
            if (index != null)
                return cfs.indexManager;
        }
        return null;
    }

    protected void waitForAssert(Runnable runnableAssert, long timeout, TimeUnit unit)
    {
        Awaitility.await().dontCatchUncaughtExceptions().atMost(timeout, unit).untilAsserted(runnableAssert::run);
    }

    protected void waitForAssert(Runnable assertion)
    {
        waitForAssert(assertion, ASSERTION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    protected void createIndexMayThrow(String query) throws Throwable
    {
        String fullQuery = formatQuery(query);
        logger.info(fullQuery);
        QueryProcessor.executeOnceInternal(fullQuery);
    }

    public void dropIndex(String query)
    {
        String fullQuery = String.format(query, KEYSPACE);
        logger.info(fullQuery);
        schemaChange(fullQuery);
    }

    /**
     *  Because the tracing executor is single threaded, submitting an empty event should ensure
     *  that all tracing events mutations have been applied.
     */
    protected void waitForTracingEvents()
    {
        try
        {
            Stage.TRACING.submit(() -> {}).get();
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.error("Failed to wait for tracing events: {}", t);
        }
    }

    protected static void assertSchemaChange(String query,
                                             Event.SchemaChange.Change expectedChange,
                                             Event.SchemaChange.Target expectedTarget,
                                             String expectedKeyspace,
                                             String expectedName,
                                             String... expectedArgTypes)
    {
        ResultMessage actual = schemaChange(query);
        Assert.assertTrue(actual instanceof ResultMessage.SchemaChange);
        Event.SchemaChange schemaChange = ((ResultMessage.SchemaChange) actual).change;
        Assert.assertSame(expectedChange, schemaChange.change);
        Assert.assertSame(expectedTarget, schemaChange.target);
        Assert.assertEquals(expectedKeyspace, schemaChange.keyspace);
        Assert.assertEquals(expectedName, schemaChange.name);
        Assert.assertEquals(expectedArgTypes != null ? Arrays.asList(expectedArgTypes) : null, schemaChange.argTypes);
    }

    protected static void assertWarningsContain(Message.Response response, String message)
    {
        assertWarningsContain(response.getWarnings(), message);
    }

    protected static void assertWarningsContain(List<String> warnings, String message)
    {
        Assert.assertNotNull(warnings);
        assertTrue(warnings.stream().anyMatch(s -> s.contains(message)));
    }

    protected static void assertWarningsEquals(ResultSet rs, String... messages)
    {
        assertWarningsEquals(rs.getExecutionInfo().getWarnings(), messages);
    }

    protected static void assertWarningsEquals(List<String> warnings, String... messages)
    {
        Assert.assertNotNull(warnings);
        Assertions.assertThat(messages).hasSameElementsAs(warnings);
    }

    protected static void assertNoWarningContains(Message.Response response, String message)
    {
        assertNoWarningContains(response.getWarnings(), message);
    }

    protected static void assertNoWarningContains(List<String> warnings, String message)
    {
        if (warnings != null)
        {
            assertFalse(warnings.stream().anyMatch(s -> s.contains(message)));
        }
    }

    protected static ResultMessage schemaChange(String query)
    {
        try
        {
            ClientState state = ClientState.forInternalCalls(SchemaConstants.SYSTEM_KEYSPACE_NAME);
            QueryState queryState = new QueryState(state);

            CQLStatement statement = QueryProcessor.parseStatement(query, queryState.getClientState());
            statement.validate(queryState);

            QueryOptions options = QueryOptions.forInternalCalls(Collections.<ByteBuffer>emptyList());

            return statement.executeLocally(queryState, options);
        }
        catch (Exception e)
        {
            logger.info("Error performing schema change", e);
            if (e instanceof InvalidRequestException)
                throw e;
            throw new RuntimeException(String.format("Error setting schema for test (query was: %s)", query), e);
        }
    }

    protected TableMetadata currentTableMetadata()
    {
        return Schema.instance.getTableMetadata(KEYSPACE, currentTable());
    }

    protected com.datastax.driver.core.ResultSet executeNet(ProtocolVersion protocolVersion, String query, Object... values)
    {
        return sessionNet(protocolVersion).execute(formatQuery(query), values);
    }

    protected com.datastax.driver.core.ResultSet executeNet(String query, Object... values)
    {
        return sessionNet().execute(formatQuery(query), values);
    }

    protected com.datastax.driver.core.ResultSet executeNet(Statement statement)
    {
        return executeNet(getDefaultVersion(), statement);
    }

    protected com.datastax.driver.core.ResultSet executeNet(ProtocolVersion protocolVersion, Statement statement)
    {
        return sessionNet(protocolVersion).execute(statement);
    }

    protected com.datastax.driver.core.ResultSet executeNetWithPaging(ProtocolVersion version, String query, int pageSize) throws Throwable
    {
        return sessionNet(version).execute(new SimpleStatement(formatQuery(query)).setFetchSize(pageSize));
    }

    protected com.datastax.driver.core.ResultSet executeNetWithPaging(ProtocolVersion version, String query, String KS, int pageSize)
    {
        return sessionNet(version).execute(new SimpleStatement(formatQuery(KS, query)).setKeyspace(KS).setFetchSize(pageSize));
    }

    protected com.datastax.driver.core.ResultSet executeNetWithPaging(String query, int pageSize) throws Throwable
    {
        return sessionNet().execute(new SimpleStatement(formatQuery(query)).setFetchSize(pageSize));
    }

    /**
     * Use the specified user for executing the queries over the network.
     * @param username the user name
     * @param password the user password
     */
    public void useUser(String username, String password)
    {
        this.user = new User(username, password);
    }

    /**
     * Use the super user for executing the queries over the network.
     */
    public void useSuperUser()
    {
        this.user = SUPER_USER;
    }

    public boolean isSuperUser()
    {
        return SUPER_USER.equals(user);
    }

    public Session sessionNet()
    {
        return sessionNet(getDefaultVersion());
    }

    protected Session sessionNet(ProtocolVersion protocolVersion)
    {
        requireNetwork();

        return getSession(protocolVersion);
    }

    protected Session getSession(ProtocolVersion protocolVersion)
    {
        Cluster cluster = getCluster(protocolVersion);
        return sessions.computeIfAbsent(Pair.create(user, protocolVersion), userProto -> cluster.connect());
    }

    private Cluster getCluster(ProtocolVersion protocolVersion)
    {
        return clusters.computeIfAbsent(Pair.create(user, protocolVersion),
                                        userProto -> initClientCluster(user, protocolVersion));
    }

    private Cluster initClientCluster(User user, ProtocolVersion version)
    {
        Pair<User, ProtocolVersion> key = Pair.create(user, version);
        Cluster cluster = clusters.get(key);
        if (cluster != null)
            return cluster;

        Cluster.Builder builder = clusterBuilder(version);
        if (user != null)
            builder.withCredentials(user.username, user.password);
        cluster = builder.build();

        logger.info("Started Java Driver session for {} with protocol version {}", user, version);

        return cluster;
    }

    protected void closeClientCluster(String username, String password)
    {
        // Close driver cluster belonging to user
        User user = new User(username, password);
        for (ProtocolVersion protocolVersion : PROTOCOL_VERSIONS)
        {
            closeClientCluster(user, protocolVersion);
        }
    }

    private void closeClientCluster(User user, ProtocolVersion protocolVersion)
    {
        Pair<User, ProtocolVersion> key = Pair.create(user, protocolVersion);
        Session session = sessions.remove(key);
        if (session != null)
        {
            session.close();
        }
        
        Cluster cluster = clusters.remove(key);
        if (cluster != null)
        {
            cluster.close();
        }

        logger.info("Closed Java Driver session for {} with protocol version {}", user, protocolVersion);
    }

    public static Cluster.Builder clusterBuilder(ProtocolVersion version)
    {
        Cluster.Builder builder = clusterBuilder();
        if (version.isBeta())
            builder = builder.allowBetaProtocolVersion();
        else
            builder = builder.withProtocolVersion(com.datastax.driver.core.ProtocolVersion.fromInt(version.asInt()));
        return builder;
    }

    public static Cluster.Builder clusterBuilder()
    {
        SocketOptions socketOptions = new SocketOptions()
            .setConnectTimeoutMillis(Integer.getInteger("cassandra.test.driver.connection_timeout_ms", DEFAULT_CONNECT_TIMEOUT_MILLIS)) // default is 5000
            .setReadTimeoutMillis(Integer.getInteger("cassandra.test.driver.read_timeout_ms", DEFAULT_READ_TIMEOUT_MILLIS)); // default is 12000

        logger.info("Timeouts: {} / {}", socketOptions.getConnectTimeoutMillis(), socketOptions.getReadTimeoutMillis());

        return Cluster.builder()
                      .addContactPoints(nativeAddr)
                      .withPort(nativePort)
                      .withClusterName("Test Cluster")
                      .withoutJMXReporting()
                      .withSocketOptions(socketOptions)
                      .withNettyOptions(IMMEDIATE_CONNECTION_SHUTDOWN_NETTY_OPTIONS)
                      .withPoolingOptions(new PoolingOptions().setPoolTimeoutMillis(10000));
    }

    protected SimpleClient newSimpleClient(ProtocolVersion version) throws IOException
    {
        return new SimpleClient(nativeAddr.getHostAddress(), nativePort, version, version.isBeta(), new EncryptionOptions().applyConfig())
               .connect(false, false);
    }

    public String formatQuery(String query)
    {
        return formatQuery(KEYSPACE, query);
    }

    protected final String formatQuery(String keyspace, String query)
    {
        String currentTable = currentTable();
        return currentTable == null ? query : String.format(query, keyspace + "." + currentTable);
    }

    protected CQLStatement parseStatement(String query)
    {
        String formattedQuery = formatQuery(query);
        return QueryProcessor.parseStatement(formattedQuery, ClientState.forInternalCalls());
    }

    protected ReadCommand parseReadCommand(String query)
    {
        SelectStatement select = (SelectStatement) parseStatement(query);
        ReadQuery readQuery = select.getQuery(QueryState.forInternalCalls(), QueryOptions.DEFAULT, FBUtilities.nowInSeconds());
        Assertions.assertThat(readQuery).isInstanceOf(ReadCommand.class);
        return  (ReadCommand) readQuery;
    }

    protected List<SinglePartitionReadCommand> parseReadCommandGroup(String query)
    {
        SelectStatement select = (SelectStatement) parseStatement(query);
        ReadQuery readQuery = select.getQuery(QueryState.forInternalCalls(), QueryOptions.DEFAULT, FBUtilities.nowInSeconds());
        Assertions.assertThat(readQuery).isInstanceOf(SinglePartitionReadCommand.Group.class);
        SinglePartitionReadCommand.Group commands = (SinglePartitionReadCommand.Group) readQuery;
        return commands.queries;
    }

    protected ResultMessage.Prepared prepare(String query) throws Throwable
    {
        return QueryProcessor.instance.prepare(formatQuery(query), ClientState.forInternalCalls());
    }

    /**
     * Enables coordinator execution in {@link #execute(String, Object...)}, so queries get full validation and go
     * through reconciliation. This makes calling {@link #execute(String, Object...)} equivalent to calling
     * {@link #executeWithCoordinator(String, Object...)}.
     */
    protected static void enableCoordinatorExecution()
    {
        requireNetworkWithoutDriver();
        coordinatorExecution = true;
    }

    /**
     * Disables coordinator execution in {@link #execute(String, Object...)}, so queries won't get full validation nor
     * go through reconciliation.This makes calling {@link #execute(String, Object...)} equivalent to calling
     * {@link #executeInternal(String, Object...)}.
     */
    protected static void disableCoordinatorExecution()
    {
        coordinatorExecution = false;
    }

    /**
     * Execute the specified query as either an internal query or a coordinator query depending on the value of
     * {@link #coordinatorExecution}.
     *
     * @param query a CQL query
     * @param values the values to bind to the query
     * @return the query results
     * @see #execute
     * @see #executeInternal
     */
    public UntypedResultSet execute(String query, Object... values)
    {
        return coordinatorExecution
               ? executeWithCoordinator(query, values)
               : executeInternal(query, values);
    }

    /**
     * Execute the specified query as an internal query only for the local node. This will skip reconciliation and some
     * validation.
     * </p>
     * For the particular case of {@code SELECT} queries using secondary indexes, the skipping of reconciliation means
     * that the query {@link org.apache.cassandra.db.filter.RowFilter} might not be fully applied to the index results.
     *
     * @param query a CQL query
     * @param values the values to bind to the query
     * @return the query results
     * @see CQLStatement#executeLocally
     */
    public UntypedResultSet executeInternal(String query, Object... values)
    {
        return executeFormattedQuery(formatQuery(query), false, values);
    }

    /**
     * Execute the specified query as an coordinator-side query meant for all the relevant nodes in the cluster, even if
     * {@link CQLTester} tests are single-node. This won't skip reconciliation and will do full validation.
     * </p>
     * For the particular case of {@code SELECT} queries using secondary indexes, applying reconciliation means that the
     * query {@link org.apache.cassandra.db.filter.RowFilter} will be fully applied to the index results.
     *
     * @param query a CQL query
     * @param values the values to bind to the query
     * @return the query results
     * @see CQLStatement#execute
     */
    public UntypedResultSet executeWithCoordinator(String query, Object... values)
    {
        return executeFormattedQuery(formatQuery(query), true, values);
    }

    public UntypedResultSet executeFormattedQuery(String query, Object... values)
    {
        return executeFormattedQuery(query, coordinatorExecution, values);
    }

    private UntypedResultSet executeFormattedQuery(String query, boolean useCoordinator, Object... values)
    {        
        if (useCoordinator)
            requireNetworkWithoutDriver();
        
        UntypedResultSet rs;
        if (usePrepared)
        {
            if (logger.isTraceEnabled())
                logger.trace("Executing: {} with values {}", query, formatAllValues(values));

            Object[] transformedValues = transformValues(values);

            if (reusePrepared)
            {
                rs = useCoordinator
                     ? QueryProcessor.execute(query, ConsistencyLevel.ONE, transformedValues)
                     : QueryProcessor.executeInternal(query, transformedValues);

                // If a test uses a "USE ...", then presumably its statements use relative table. In that case, a USE
                // change the meaning of the current keyspace, so we don't want a following statement to reuse a previously
                // prepared statement at this wouldn't use the right keyspace. To avoid that, we drop the previously
                // prepared statement.
                if (query.startsWith("USE"))
                    QueryProcessor.clearInternalStatementsCache();
            }
            else
            {
                rs = useCoordinator
                     ? QueryProcessor.executeOnce(query, ConsistencyLevel.ONE, transformedValues)
                     : QueryProcessor.executeOnceInternal(query, transformedValues);
            }
        }
        else
        {
            query = replaceValues(query, values);

            if (logger.isTraceEnabled())
                logger.trace("Executing: {}", query);

            rs = useCoordinator
                 ? QueryProcessor.executeOnce(query, ConsistencyLevel.ONE)
                 : QueryProcessor.executeOnceInternal(query);
        }
        if (rs != null)
        {
            if (logger.isTraceEnabled())
                logger.trace("Got {} rows", rs.size());
        }
        return rs;
    }

    protected void assertRowsNet(ResultSet result, Object[]... rows)
    {
        assertRowsNet(getDefaultVersion(), result, rows);
    }

    protected void assertRowsNet(ProtocolVersion protocolVersion, ResultSet result, Object[]... rows)
    {
        // necessary as we need cluster objects to supply CodecRegistry.
        // It's reasonably certain that the network setup has already been done
        // by the time we arrive at this point, but adding this check doesn't hurt
        requireNetwork();

        if (result == null)
        {
            if (rows.length > 0)
                Assert.fail(String.format("No rows returned by query but %d expected", rows.length));
            return;
        }

        ColumnDefinitions meta = result.getColumnDefinitions();
        Iterator<Row> iter = result.iterator();
        int i = 0;
        while (iter.hasNext() && i < rows.length)
        {
            Object[] expected = rows[i];
            Row actual = iter.next();

            Assert.assertEquals(String.format("Invalid number of (expected) values provided for row %d (using protocol version %s)",
                                              i, protocolVersion),
                                meta.size(), expected.length);

            for (int j = 0; j < meta.size(); j++)
            {
                DataType type = meta.getType(j);
                com.datastax.driver.core.TypeCodec<Object> codec = getCluster(protocolVersion).getConfiguration()
                                                                                              .getCodecRegistry()
                                                                                              .codecFor(type);
                ByteBuffer expectedByteValue = codec.serialize(expected[j], com.datastax.driver.core.ProtocolVersion.fromInt(protocolVersion.asInt()));
                int expectedBytes = expectedByteValue == null ? -1 : expectedByteValue.remaining();
                ByteBuffer actualValue = actual.getBytesUnsafe(meta.getName(j));
                int actualBytes = actualValue == null ? -1 : actualValue.remaining();
                if (!Objects.equal(expectedByteValue, actualValue))
                    Assert.fail(String.format("Invalid value for row %d column %d (%s of type %s), " +
                                              "expected <%s> (%d bytes) but got <%s> (%d bytes) " +
                                              "(using protocol version %s)",
                                              i, j, meta.getName(j), type,
                                              codec.format(expected[j]),
                                              expectedBytes,
                                              codec.format(codec.deserialize(actualValue, com.datastax.driver.core.ProtocolVersion.fromInt(protocolVersion.asInt()))),
                                              actualBytes,
                                              protocolVersion));
            }
            i++;
        }

        if (iter.hasNext())
        {
            while (iter.hasNext())
            {
                iter.next();
                i++;
            }
            Assert.fail(String.format("Got less rows than expected. Expected %d but got %d (using protocol version %s).",
                                      rows.length, i, protocolVersion));
        }

        Assert.assertTrue(String.format("Got %s rows than expected. Expected %d but got %d (using protocol version %s)",
                                        rows.length>i ? "less" : "more", rows.length, i, protocolVersion), i == rows.length);
    }

    protected void assertRowCountNet(ResultSet r1, int expectedCount)
    {
        Assert.assertFalse("Received a null resultset when expected count was > 0", expectedCount > 0 && r1 == null);
        int actualRowCount = Iterables.size(r1);
        Assert.assertEquals(String.format("expected %d rows but received %d", expectedCount, actualRowCount), expectedCount, actualRowCount);
    }

    public static void assertRows(UntypedResultSet result, Object[]... rows)
    {
        assertRows(result, false, rows);
    }

    public static void assertRows(UntypedResultSet result, boolean printAssertedRows, Object[]... rows)
    {
        // Useful for manual debugging, but generally unnecessary on
        if (printAssertedRows)
        {
            // Print all the rows
            for (Object[] row : rows)
            {
                System.out.print("Expected row:");
                for (Object column : row)
                    System.out.print("  " + column);
                System.out.println();
            }
        }

        if (result == null)
        {
            if (rows.length > 0)
                Assert.fail(String.format("No rows returned by query but %d expected", rows.length));
            return;
        }

        List<ColumnSpecification> meta = result.metadata();
        Iterator<UntypedResultSet.Row> iter = result.iterator();
        int i = 0;
        while (iter.hasNext() && i < rows.length)
        {
            if (rows[i] == null)
                throw new IllegalArgumentException(String.format("Invalid expected value for row: %d. A row cannot be null.", i));

            Object[] expected = rows[i];
            UntypedResultSet.Row actual = iter.next();

            Assert.assertEquals(String.format("Invalid number of (expected) values provided for row %d", i), expected.length, meta.size());

            for (int j = 0; j < meta.size(); j++)
            {
                ColumnSpecification column = meta.get(j);
                ByteBuffer expectedByteValue = makeByteBuffer(expected == null ? null : expected[j], column.type);
                ByteBuffer actualValue = actual.getBytes(column.name.toString());

                if (expectedByteValue != null)
                    expectedByteValue = expectedByteValue.duplicate();
                if (!Objects.equal(expectedByteValue, actualValue))
                {
                    Object actualValueDecoded = actualValue == null ? null : column.type.getSerializer().deserialize(actualValue);
                    if (!Objects.equal(expected != null ? expected[j] : null, actualValueDecoded))
                        Assert.fail(String.format("Invalid value for row %d column %d (%s of type %s), expected <%s> but got <%s>",
                                                  i,
                                                  j,
                                                  column.name,
                                                  column.type.asCQL3Type(),
                                                  formatValue(expectedByteValue != null ? expectedByteValue.duplicate() : null, column.type),
                                                  formatValue(actualValue, column.type)));
                }
            }
            i++;
        }

        if (iter.hasNext())
        {
            while (iter.hasNext())
            {
                UntypedResultSet.Row actual = iter.next();
                i++;

                StringBuilder str = new StringBuilder();
                for (int j = 0; j < meta.size(); j++)
                {
                    ColumnSpecification column = meta.get(j);
                    ByteBuffer actualValue = actual.getBytes(column.name.toString());
                    str.append(String.format("%s=%s ", column.name, formatValue(actualValue, column.type)));
                }
                logger.info("Extra row num {}: {}", i, str.toString());
            }
            Assert.fail(String.format("Got more rows than expected. Expected %d but got %d.", rows.length, i));
        }

        Assert.assertTrue(String.format("Got %s rows than expected. Expected %d but got %d", rows.length>i ? "less" : "more", rows.length, i), i == rows.length);
    }

    /**
     * Like assertRows(), but ignores the ordering of rows.
     */
    public static void assertRowsIgnoringOrder(UntypedResultSet result, Object[]... rows)
    {
        assertRowsIgnoringOrderInternal(result, false, rows);
    }

    public static void assertRowsIgnoringOrderAndExtra(UntypedResultSet result, Object[]... rows)
    {
        assertRowsIgnoringOrderInternal(result, true, rows);
    }

    private static void assertRowsIgnoringOrderInternal(UntypedResultSet result, boolean ignoreExtra, Object[]... rows)
    {
        if (result == null)
        {
            if (rows.length > 0)
                Assert.fail(String.format("No rows returned by query but %d expected", rows.length));
            return;
        }

        List<ColumnSpecification> meta = result.metadata();

        Set<List<ByteBuffer>> expectedRows = new HashSet<>(rows.length);
        for (Object[] expected : rows)
        {
            Assert.assertEquals("Invalid number of (expected) values provided for row", expected.length, meta.size());
            List<ByteBuffer> expectedRow = new ArrayList<>(meta.size());
            for (int j = 0; j < meta.size(); j++)
                expectedRow.add(makeByteBuffer(expected[j], meta.get(j).type));
            expectedRows.add(expectedRow);
        }

        Set<List<ByteBuffer>> actualRows = new HashSet<>(result.size());
        for (UntypedResultSet.Row actual : result)
        {
            List<ByteBuffer> actualRow = new ArrayList<>(meta.size());
            for (int j = 0; j < meta.size(); j++)
                actualRow.add(actual.getBytes(meta.get(j).name.toString()));
            actualRows.add(actualRow);
        }

        com.google.common.collect.Sets.SetView<List<ByteBuffer>> extra = com.google.common.collect.Sets.difference(actualRows, expectedRows);
        com.google.common.collect.Sets.SetView<List<ByteBuffer>> missing = com.google.common.collect.Sets.difference(expectedRows, actualRows);
        if ((!ignoreExtra && !extra.isEmpty()) || !missing.isEmpty())
        {
            List<String> extraRows = makeRowStrings(extra, meta);
            List<String> missingRows = makeRowStrings(missing, meta);
            StringBuilder sb = new StringBuilder();
            if (!extra.isEmpty())
            {
                sb.append("Got ").append(extra.size()).append(" extra row(s) ");
                if (!missing.isEmpty())
                    sb.append("and ").append(missing.size()).append(" missing row(s) ");
                sb.append("in result.  Extra rows:\n    ");
                sb.append(extraRows.stream().collect(Collectors.joining("\n    ")));
                if (!missing.isEmpty())
                    sb.append("\nMissing Rows:\n    ").append(missingRows.stream().collect(Collectors.joining("\n    ")));
                Assert.fail(sb.toString());
            }

            if (!missing.isEmpty())
                Assert.fail("Missing " + missing.size() + " row(s) in result: \n    " + missingRows.stream().collect(Collectors.joining("\n    ")));
        }

        assert ignoreExtra || expectedRows.size() == actualRows.size();
    }

    protected static List<String> makeRowStrings(UntypedResultSet resultSet)
    {
        List<List<ByteBuffer>> rows = new ArrayList<>();
        for (UntypedResultSet.Row row : resultSet)
        {
            List<ByteBuffer> values = new ArrayList<>();
            for (ColumnSpecification columnSpecification : resultSet.metadata())
            {
                values.add(row.getBytes(columnSpecification.name.toString()));
            }
            rows.add(values);
        }

        return makeRowStrings(rows, resultSet.metadata());
    }

    private static List<String> makeRowStrings(Iterable<List<ByteBuffer>> rows, List<ColumnSpecification> meta)
    {
        List<String> strings = new ArrayList<>();
        for (List<ByteBuffer> row : rows)
        {
            StringBuilder sb = new StringBuilder("row(");
            for (int j = 0; j < row.size(); j++)
            {
                ColumnSpecification column = meta.get(j);
                sb.append(column.name.toString()).append("=").append(formatValue(row.get(j), column.type));
                if (j < (row.size() - 1))
                    sb.append(", ");
            }
            strings.add(sb.append(")").toString());
        }
        return strings;
    }

    protected void assertRowCount(UntypedResultSet result, int numExpectedRows)
    {
        if (result == null)
        {
            if (numExpectedRows > 0)
                Assert.fail(String.format("No rows returned by query but %d expected", numExpectedRows));
            return;
        }

        List<ColumnSpecification> meta = result.metadata();
        Iterator<UntypedResultSet.Row> iter = result.iterator();
        int i = 0;
        while (iter.hasNext() && i < numExpectedRows)
        {
            UntypedResultSet.Row actual = iter.next();
            assertNotNull(actual);
            i++;
        }

        if (iter.hasNext())
        {
            while (iter.hasNext())
            {
                iter.next();
                i++;
            }
            Assert.fail(String.format("Got less rows than expected. Expected %d but got %d.", numExpectedRows, i));
        }

        Assert.assertTrue(String.format("Got %s rows than expected. Expected %d but got %d", numExpectedRows>i ? "less" : "more", numExpectedRows, i), i == numExpectedRows);
    }

    protected Object[][] getRows(UntypedResultSet result)
    {
        if (result == null)
            return new Object[0][];

        List<Object[]> ret = new ArrayList<>();
        List<ColumnSpecification> meta = result.metadata();

        Iterator<UntypedResultSet.Row> iter = result.iterator();
        while (iter.hasNext())
        {
            UntypedResultSet.Row rowVal = iter.next();
            Object[] row = new Object[meta.size()];
            for (int j = 0; j < meta.size(); j++)
            {
                ColumnSpecification column = meta.get(j);
                ByteBuffer val = rowVal.getBytes(column.name.toString());
                row[j] = val == null ? null : column.type.getSerializer().deserialize(val);
            }

            ret.add(row);
        }

        Object[][] a = new Object[ret.size()][];
        return ret.toArray(a);
    }

    protected void assertColumnNames(UntypedResultSet result, String... expectedColumnNames)
    {
        if (result == null)
        {
            Assert.fail("No rows returned by query.");
            return;
        }

        List<ColumnSpecification> metadata = result.metadata();
        Assert.assertEquals("Got less columns than expected.", expectedColumnNames.length, metadata.size());

        for (int i = 0, m = metadata.size(); i < m; i++)
        {
            ColumnSpecification columnSpec = metadata.get(i);
            Assert.assertEquals(expectedColumnNames[i], columnSpec.name.toString());
        }
    }

    protected void assertAllRows(Object[]... rows) throws Throwable
    {
        assertRows(execute("SELECT * FROM %s"), rows);
    }

    public static Object[] row(Object... expected)
    {
        return expected;
    }

    protected void assertEmpty(UntypedResultSet result)
    {
        if (result != null && !result.isEmpty())
            throw new AssertionError(String.format("Expected empty result but got %d rows: %s \n", result.size(), makeRowStrings(result)));
    }

    protected void assertInvalid(String query, Object... values)
    {
        assertInvalidMessage(null, query, values);
    }

    protected void assertInvalidMessage(String errorMessage, String query, Object... values)
    {
        assertInvalidThrowMessage(errorMessage, null, query, values);
    }

    protected void assertInvalidThrow(Class<? extends Throwable> exception, String query, Object... values)
    {
        assertInvalidThrowMessage(null, exception, query, values);
    }

    protected void assertInvalidThrowMessage(String errorMessage, Class<? extends Throwable> exception, String query, Object... values)
    {
        assertInvalidThrowMessage(Optional.empty(), errorMessage, exception, query, values);
    }

    // if a protocol version > Integer.MIN_VALUE is supplied, executes
    // the query via the java driver, mimicking a real client.
    protected void assertInvalidThrowMessage(Optional<ProtocolVersion> protocolVersion,
                                             String errorMessage,
                                             Class<? extends Throwable> exception,
                                             String query,
                                             Object... values)
    {
        try
        {
            if (!protocolVersion.isPresent())
                execute(query, values);
            else
                executeNet(protocolVersion.get(), query, values);

            String q = USE_PREPARED_VALUES
                       ? query + " (values: " + formatAllValues(values) + ")"
                       : replaceValues(query, values);
            Assert.fail("Query should be invalid but no error was thrown. Query is: " + q);
        }
        catch (Exception e)
        {
            if (exception != null && !exception.isAssignableFrom(e.getClass()))
            {
                Assert.fail("Query should be invalid but wrong error was thrown. " +
                            "Expected: " + exception.getName() + ", got: " + e.getClass().getName() + ". " +
                            "Query is: " + queryInfo(query, values));
            }
            if (errorMessage != null)
            {
                assertMessageContains(errorMessage, e);
            }
        }
    }

    public static List<String> warningsFromResultSet(List<String> ignoredWarnings, ResultSet rs)
    {
        return rs.getExecutionInfo().getWarnings()
                 .stream().filter(w -> ignoredWarnings.stream().noneMatch(w::contains))
                 .collect(Collectors.toList());
    }

    private static String queryInfo(String query, Object[] values)
    {
        return USE_PREPARED_VALUES
               ? query + " (values: " + formatAllValues(values) + ")"
               : replaceValues(query, values);
    }

    protected void assertValidSyntax(String query) throws Throwable
    {
        try
        {
            QueryProcessor.parseStatement(query);
        }
        catch(SyntaxException e)
        {
            Assert.fail(String.format("Expected query syntax to be valid but was invalid. Query is: %s; Error is %s",
                                      query, e.getMessage()));
        }
    }

    protected void assertInvalidSyntax(String query, Object... values) throws Throwable
    {
        assertInvalidSyntaxMessage(null, query, values);
    }

    protected void assertInvalidSyntaxMessage(String errorMessage, String query, Object... values) throws Throwable
    {
        try
        {
            execute(query, values);
            Assert.fail("Query should have invalid syntax but no error was thrown. Query is: " + queryInfo(query, values));
        }
        catch (SyntaxException e)
        {
            if (errorMessage != null)
            {
                assertMessageContains(errorMessage, e);
            }
        }
    }

    /**
     * Asserts that the message of the specified exception contains the specified text.
     *
     * @param text the text that the exception message must contains
     * @param e the exception to check
     */
    private static void assertMessageContains(String text, Exception e)
    {
        Assert.assertTrue("Expected error message to contain '" + text + "', but got '" + e.getMessage() + "'",
                e.getMessage().contains(text));
    }

    @FunctionalInterface
    public interface CheckedFunction {
        void apply() throws Throwable;
    }

    /**
     * Runs the given function before and after a flush of sstables.  This is useful for checking that behavior is
     * the same whether data is in memtables or sstables.
     * @param runnable
     * @throws Throwable
     */
    public void beforeAndAfterFlush(CheckedFunction runnable) throws Throwable
    {
        try
        {
            runnable.apply();
        }
        catch (Throwable t)
        {
            throw new AssertionError("Test failed before flush:\n" + t, t);
        }

        flush();

        try
        {
            runnable.apply();
        }
        catch (Throwable t)
        {
            throw new AssertionError("Test failed after flush:\n" + t, t);
        }
    }

    /**
     * Runs the given function before a flush, after a flush, and finally after a compaction of the table. This is
     * useful for checking that behavior is the same whether data is in memtables, memtable-flushed-sstbales,
     * compaction-built-sstables.
     * @param runnable
     * @throws Throwable
     */
    public void runThenFlushThenCompact(CheckedFunction runnable) throws Throwable
    {
        beforeAndAfterFlush(runnable);

        compact();

        try
        {
            runnable.apply();
        }
        catch (Throwable t)
        {
            throw new AssertionError("Test failed after compact:\n" + t, t);
        }
    }

    private static String replaceValues(String query, Object[] values)
    {
        StringBuilder sb = new StringBuilder();
        int last = 0;
        int i = 0;
        int idx;
        while ((idx = query.indexOf('?', last)) > 0)
        {
            if (i >= values.length)
                throw new IllegalArgumentException(String.format("Not enough values provided. The query has at least %d variables but only %d values provided", i, values.length));

            sb.append(query.substring(last, idx));

            Object value = values[i++];

            // When we have a .. IN ? .., we use a list for the value because that's what's expected when the value is serialized.
            // When we format as string however, we need to special case to use parenthesis. Hackish but convenient.
            if (idx >= 3 && value instanceof List && query.substring(idx - 3, idx).equalsIgnoreCase("IN "))
            {
                List l = (List)value;
                sb.append("(");
                for (int j = 0; j < l.size(); j++)
                {
                    if (j > 0)
                        sb.append(", ");
                    sb.append(formatForCQL(l.get(j)));
                }
                sb.append(")");
            }
            else
            {
                sb.append(formatForCQL(value));
            }
            last = idx + 1;
        }
        sb.append(query.substring(last));
        return sb.toString();
    }

    // We're rellly only returning ByteBuffers but this make the type system happy
    private static Object[] transformValues(Object[] values)
    {
        // We could partly rely on QueryProcessor.executeOnceInternal doing type conversion for us, but
        // it would complain with ClassCastException if we pass say a string where an int is excepted (since
        // it bases conversion on what the value should be, not what it is). For testing, we sometimes
        // want to pass value of the wrong type and assert that this properly raise an InvalidRequestException
        // and executeOnceInternal goes into way. So instead, we pre-convert everything to bytes here based
        // on the value.
        // Besides, we need to handle things like TupleValue that executeOnceInternal don't know about.

        Object[] buffers = new ByteBuffer[values.length];
        for (int i = 0; i < values.length; i++)
        {
            Object value = values[i];
            if (value == null)
            {
                buffers[i] = null;
                continue;
            }
            else if (value == ByteBufferUtil.UNSET_BYTE_BUFFER)
            {
                buffers[i] = ByteBufferUtil.UNSET_BYTE_BUFFER;
                continue;
            }

            try
            {
                buffers[i] = typeFor(value).decompose(serializeTuples(value));
            }
            catch (Exception ex)
            {
                logger.info("Error serializing query parameter {}:", value, ex);
                throw ex;
            }
        }
        return buffers;
    }

    private static Object serializeTuples(Object value)
    {
        if (value instanceof TupleValue)
        {
            return ((TupleValue)value).toByteBuffer();
        }

        // We need to reach inside collections for TupleValue and transform them to ByteBuffer
        // since otherwise the decompose method of the collection AbstractType won't know what
        // to do with them
        if (value instanceof List)
        {
            List l = (List)value;
            List n = new ArrayList(l.size());
            for (Object o : l)
                n.add(serializeTuples(o));
            return n;
        }

        if (value instanceof Set)
        {
            Set s = (Set)value;
            Set n = new LinkedHashSet(s.size());
            for (Object o : s)
                n.add(serializeTuples(o));
            return n;
        }

        if (value instanceof Map)
        {
            Map m = (Map)value;
            Map n = new LinkedHashMap(m.size());
            for (Object entry : m.entrySet())
                n.put(serializeTuples(((Map.Entry)entry).getKey()), serializeTuples(((Map.Entry)entry).getValue()));
            return n;
        }
        return value;
    }

    private static String formatAllValues(Object[] values)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < values.length; i++)
        {
            if (i > 0)
                sb.append(", ");
            sb.append(formatForCQL(values[i]));
        }
        sb.append("]");
        return sb.toString();
    }

    private static String formatForCQL(Object value)
    {
        if (value == null)
            return "null";

        if (value instanceof TupleValue)
            return ((TupleValue)value).toCQLString();

        // We need to reach inside collections for TupleValue. Besides, for some reason the format
        // of collection that CollectionType.getString gives us is not at all 'CQL compatible'
        if (value instanceof Collection || value instanceof Map)
        {
            StringBuilder sb = new StringBuilder();
            if (value instanceof List)
            {
                List l = (List)value;
                sb.append("[");
                for (int i = 0; i < l.size(); i++)
                {
                    if (i > 0)
                        sb.append(", ");
                    sb.append(formatForCQL(l.get(i)));
                }
                sb.append("]");
            }
            else if (value instanceof Set)
            {
                Set s = (Set)value;
                sb.append("{");
                Iterator iter = s.iterator();
                while (iter.hasNext())
                {
                    sb.append(formatForCQL(iter.next()));
                    if (iter.hasNext())
                        sb.append(", ");
                }
                sb.append("}");
            }
            else
            {
                Map m = (Map)value;
                sb.append("{");
                Iterator iter = m.entrySet().iterator();
                while (iter.hasNext())
                {
                    Map.Entry entry = (Map.Entry)iter.next();
                    sb.append(formatForCQL(entry.getKey())).append(": ").append(formatForCQL(entry.getValue()));
                    if (iter.hasNext())
                        sb.append(", ");
                }
                sb.append("}");
            }
            return sb.toString();
        }

        AbstractType type = typeFor(value);
        String s = type.getString(type.decompose(value));

        if (type instanceof InetAddressType || type instanceof TimestampType)
            return String.format("'%s'", s);
        else if (type instanceof UTF8Type)
            return String.format("'%s'", s.replaceAll("'", "''"));
        else if (type instanceof BytesType)
            return "0x" + s;

        return s;
    }

    protected static ByteBuffer makeByteBuffer(Object value, AbstractType type)
    {
        if (value == null)
            return null;

        if (value instanceof TupleValue)
            return ((TupleValue)value).toByteBuffer();

        if (value instanceof ByteBuffer)
            return (ByteBuffer)value;

        return type.decompose(serializeTuples(value));
    }

    private static String formatValue(ByteBuffer bb, AbstractType<?> type)
    {
        if (bb == null)
            return "null";

        if (type instanceof CollectionType)
        {
            // CollectionType override getString() to use hexToBytes. We can't change that
            // without breaking SSTable2json, but the serializer for collection have the
            // right getString so using it directly instead.
            TypeSerializer ser = type.getSerializer();
            return ser.toString(ser.deserialize(bb));
        }

        return type.getString(bb);
    }

    protected TupleValue tuple(Object...values)
    {
        return new TupleValue(values);
    }

    protected Object userType(Object... values)
    {
        if (values.length % 2 != 0)
            throw new IllegalArgumentException("userType() requires an even number of arguments");

        String[] fieldNames = new String[values.length / 2];
        Object[] fieldValues = new Object[values.length / 2];
        int fieldNum = 0;
        for (int i = 0; i < values.length; i += 2)
        {
            fieldNames[fieldNum] = (String) values[i];
            fieldValues[fieldNum] = values[i + 1];
            fieldNum++;
        }
        return new UserTypeValue(fieldNames, fieldValues);
    }

    protected Object list(Object...values)
    {
        return Arrays.asList(values);
    }

    /** @return a normalized vector with the given dimension */
    public static Vector<Float> randomVectorBoxed(int dimension)
    {
        var floats = randomVector(dimension);
        return vector(floats);
    }

    public static ByteBuffer randomVectorSerialized(int dimension)
    {
        var rawVector = randomVectorBoxed(dimension);
        return VectorType.getInstance(FloatType.instance, dimension).getSerializer().serialize(rawVector);
    }

    public static float[] randomVector(int dimension)
    {
        // this can be called from concurrent threads so don't use getRandom()
        var R = ThreadLocalRandom.current();

        var vector = new float[dimension];
        for (int i = 0; i < dimension; i++)
        {
            vector[i] = R.nextFloat();
        }
        normalize(vector);
        return vector;
    }

    /** Normalize the given vector in-place */
    protected static void normalize(float[] v)
    {
        var sum = 0.0f;
        for (int i = 0; i < v.length; i++)
        {
            sum += v[i] * v[i];
        }

        sum = (float) Math.sqrt(sum);
        for (int i = 0; i < v.length; i++)
            v[i] /= sum;
    }

    protected Object set(Object...values)
    {
        return ImmutableSet.copyOf(values);
    }

    // LinkedHashSets are iterable in insertion order, which is important for some tests
    protected LinkedHashSet<Object> linkedHashSet(Object...values)
    {
        LinkedHashSet<Object> s = new LinkedHashSet<>(values.length);
        s.addAll(Arrays.asList(values));
        return s;
    }

    protected Object map(Object...values)
    {
        return linkedHashMap(values);
    }

    // LinkedHashMaps are iterable in insertion order, which is important for some tests
    protected static LinkedHashMap<Object, Object> linkedHashMap(Object...values)
    {
        if (values.length % 2 != 0)
            throw new IllegalArgumentException("Invalid number of arguments, got " + values.length);

        int size = values.length / 2;
        LinkedHashMap<Object, Object> m = new LinkedHashMap<>(size);
        for (int i = 0; i < size; i++)
            m.put(values[2 * i], values[(2 * i) + 1]);
        return m;
    }

    protected com.datastax.driver.core.TupleType tupleTypeOf(ProtocolVersion protocolVersion, com.datastax.driver.core.DataType...types)
    {
        requireNetwork();
        return getCluster(protocolVersion).getMetadata().newTupleType(types);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected static Gauge<Integer> getPausedConnectionsGauge()
    {
        String metricName = "org.apache.cassandra.metrics.Client.PausedConnections";
        Map<String, Gauge> metrics = CassandraMetricsRegistry.Metrics.getGauges((name, metric) -> name.equals(metricName));
        if (metrics.size() != 1)
            fail(String.format("Expected a single registered metric for paused client connections, found %s",
                               metrics.size()));
        return metrics.get(metricName);
    }

    @Ignore // Check TinySegmentFlushingFailureTest for details why this annotation is needed here despite this is not a test
    public static class Vector<T> extends AbstractList<T>
    {
        private final T[] values;

        public Vector(T[] values)
        {
            this.values = values;
        }

        @Override
        public T get(int index)
        {
            return values[index];
        }

        @Override
        public int size()
        {
            return values.length;
        }
    }

    // Attempt to find an AbstracType from a value (for serialization/printing sake).
    // Will work as long as we use types we know of, which is good enough for testing
    private static AbstractType typeFor(Object value)
    {
        if (value instanceof ByteBuffer || value instanceof TupleValue || value == null)
            return BytesType.instance;

        if (value instanceof Byte)
            return ByteType.instance;

        if (value instanceof Short)
            return ShortType.instance;

        if (value instanceof Integer)
            return Int32Type.instance;

        if (value instanceof Long)
            return LongType.instance;

        if (value instanceof Float)
            return FloatType.instance;

        if (value instanceof Duration)
            return DurationType.instance;

        if (value instanceof Double)
            return DoubleType.instance;

        if (value instanceof BigInteger)
            return IntegerType.instance;

        if (value instanceof BigDecimal)
            return DecimalType.instance;

        if (value instanceof String)
            return UTF8Type.instance;

        if (value instanceof Boolean)
            return BooleanType.instance;

        if (value instanceof InetAddress)
            return InetAddressType.instance;

        if (value instanceof Date)
            return TimestampType.instance;

        if (value instanceof UUID)
            return UUIDType.instance;

        if (value instanceof TimeUUID)
            return TimeUUIDType.instance;

        // vector impl list, so have to check first
        if (value instanceof Vector)
        {
            Vector<?> v = (Vector<?>) value;
            return VectorType.getInstance(typeFor(v.values[0]), v.values.length);
        }

        if (value instanceof List)
        {
            List l = (List)value;
            AbstractType elt = l.isEmpty() ? BytesType.instance : typeFor(l.get(0));
            return ListType.getInstance(elt, true);
        }

        if (value instanceof Set)
        {
            Set s = (Set)value;
            AbstractType elt = s.isEmpty() ? BytesType.instance : typeFor(s.iterator().next());
            return SetType.getInstance(elt, true);
        }

        if (value instanceof Map)
        {
            Map m = (Map)value;
            AbstractType keys, values;
            if (m.isEmpty())
            {
                keys = BytesType.instance;
                values = BytesType.instance;
            }
            else
            {
                Map.Entry entry = (Map.Entry)m.entrySet().iterator().next();
                keys = typeFor(entry.getKey());
                values = typeFor(entry.getValue());
            }
            return MapType.getInstance(keys, values, true);
        }

        throw new IllegalArgumentException("Unsupported value type (value is " + value + ")");
    }

    private static class TupleValue
    {
        protected final Object[] values;

        TupleValue(Object[] values)
        {
            this.values = values;
        }

        public ByteBuffer toByteBuffer()
        {
            ByteBuffer[] bbs = new ByteBuffer[values.length];
            for (int i = 0; i < values.length; i++)
                bbs[i] = makeByteBuffer(values[i], typeFor(values[i]));
            return TupleType.buildValue(bbs);
        }

        public String toCQLString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("(");
            for (int i = 0; i < values.length; i++)
            {
                if (i > 0)
                    sb.append(", ");
                sb.append(formatForCQL(values[i]));
            }
            sb.append(")");
            return sb.toString();
        }

        public String toString()
        {
            return "TupleValue" + toCQLString();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TupleValue that = (TupleValue) o;
            return Arrays.equals(values, that.values);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(values);
        }
    }

    private static class UserTypeValue extends TupleValue
    {
        private final String[] fieldNames;

        UserTypeValue(String[] fieldNames, Object[] fieldValues)
        {
            super(fieldValues);
            this.fieldNames = fieldNames;
        }

        @Override
        public String toCQLString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            boolean haveEntry = false;
            for (int i = 0; i < values.length; i++)
            {
                if (values[i] != null)
                {
                    if (haveEntry)
                        sb.append(", ");
                    sb.append(ColumnIdentifier.maybeQuote(fieldNames[i]));
                    sb.append(": ");
                    sb.append(formatForCQL(values[i]));
                    haveEntry = true;
                }
            }
            assert haveEntry;
            sb.append("}");
            return sb.toString();
        }

        public String toString()
        {
            return "UserTypeValue" + toCQLString();
        }
    }

    @Ignore // Check TinySegmentFlushingFailureTest for details why this annotation is needed here despite this is not a test
    public static class Randomization
    {
        private long seed;
        private Random random;

        Randomization()
        {
            if (random == null)
            {
                seed = Long.getLong("cassandra.test.random.seed", System.nanoTime());
                random = new Random(seed);
            }
        }

        public void printSeedOnFailure()
        {
            System.err.println("Randomized test failed. To rerun test use -Dcassandra.test.random.seed=" + seed);
        }

        public Random getRandom()
        {
            return random;
        }

        public int nextInt()
        {
            return random.nextInt();
        }

        public int nextIntBetween(int minValue, int maxValue)
        {
            return RandomInts.randomIntBetween(random, minValue, maxValue);
        }

        public float nextFloatBetween(int minValue, int maxValue)
        {
            return random.nextFloat() * (maxValue - minValue) + minValue;
        }

        public long nextLong()
        {
            return random.nextLong();
        }

        public short nextShort()
        {
            return (short)random.nextInt(Short.MAX_VALUE + 1);
        }

        public byte nextByte()
        {
            return (byte)random.nextInt(Byte.MAX_VALUE + 1);
        }

        public BigInteger nextBigInteger(int minNumBits, int maxNumBits)
        {
            return new BigInteger(RandomInts.randomIntBetween(random, minNumBits, maxNumBits), random);
        }

        public BigDecimal nextBigDecimal(int minUnscaledValue, int maxUnscaledValue, int minScale, int maxScale)
        {
            return BigDecimal.valueOf(RandomInts.randomIntBetween(random, minUnscaledValue, maxUnscaledValue),
                                      RandomInts.randomIntBetween(random, minScale, maxScale));
        }

        public float nextFloat()
        {
            return random.nextFloat();
        }

        public double nextDouble()
        {
            return random.nextDouble();
        }

        public String nextAsciiString(int minLength, int maxLength)
        {
            return RandomStrings.randomAsciiOfLengthBetween(random, minLength, maxLength);
        }

        public String nextTextString(int minLength, int maxLength)
        {
            return RandomStrings.randomRealisticUnicodeOfLengthBetween(random, minLength, maxLength);
        }

        public boolean nextBoolean()
        {
            return random.nextBoolean();
        }

        public void nextBytes(byte[] bytes)
        {
            random.nextBytes(bytes);
        }
    }

    @Ignore // Check TinySegmentFlushingFailureTest for details why this annotation is needed here despite this is not a test
    public static class FailureWatcher extends TestWatcher
    {
        @Override
        protected void failed(Throwable e, Description description)
        {
            if (random != null)
                random.printSeedOnFailure();
        }
    }

    private static class User
    {
        /**
         * The user name
         */
        public final String username;

        /**
         * The user password
         */
        public final String password;

        public User(String username, String password)
        {
            this.username = username;
            this.password = password;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(username, password);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;

            if (!(o instanceof User))
                return false;

            User u = (User) o;

            return Objects.equal(username, u.username)
                   && Objects.equal(password, u.password);
        }

        public String toString()
        {
            return username;
        }
    }

    protected PlanSelectionAssertion assertThatIndexQueryPlanFor(String query, Object[]... expectedRows)
    {
        // First execute the query capturing warnings and check the query results
        disablePreparedReuseForTest();
        ClientWarn.instance.captureWarnings();
        assertRowsIgnoringOrder(execute(query), expectedRows);
        List<String> warnings = ClientWarn.instance.getWarnings();
        ClientWarn.instance.resetWarnings();

        ReadCommand command = parseReadCommand(query);
        Index.QueryPlan queryPlan = command.indexQueryPlan();
        if (queryPlan == null)
            return new PlanSelectionAssertion(null, warnings);

        Set<String> indexes = queryPlan.getIndexes().stream().map(i -> i.getIndexMetadata().name).collect(Collectors.toSet());
        return new PlanSelectionAssertion(indexes, warnings);
    }

    protected static class PlanSelectionAssertion
    {
        private final List<String> warnings;
        private final Set<String> selectedIndexes;

        protected PlanSelectionAssertion(@Nullable Set<String> selectedIndexes, @Nullable List<String> warnings)
        {
            this.selectedIndexes = selectedIndexes;
            this.warnings = warnings;
        }

        public PlanSelectionAssertion selects(String... indexes)
        {
            Assertions.assertThat(selectedIndexes)
                      .isNotNull()
                      .as("Expected to select only %s, but got: %s", indexes, selectedIndexes)
                      .isEqualTo(Set.of(indexes));
            return this;
        }

        public PlanSelectionAssertion selectsAnyOf(String index1, String index2, String... otherIndexes)
        {
            Set<String> expectedIndexes = new HashSet<>(otherIndexes.length + 1);
            expectedIndexes.add(index1);
            expectedIndexes.add(index2);
            expectedIndexes.addAll(Arrays.asList(otherIndexes));

            Assertions.assertThat(selectedIndexes)
                      .isNotNull()
                      .as("Expected to select any of %s, but got: %s", otherIndexes, selectedIndexes)
                      .containsAnyElementsOf(expectedIndexes);
            return this;
        }

        public PlanSelectionAssertion selectsNone()
        {
            Assertions.assertThat(selectedIndexes).isNull();
            return this;
        }

        public void doesntWarn()
        {
            Assert.assertNull(warnings);
        }

        public void warns(String expectedWarning)
        {
            Assert.assertNotNull(warnings);
            Assert.assertEquals(1, warnings.size());
            Assert.assertEquals(expectedWarning, warnings.get(0));
        }
    }
}
