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

package org.apache.cassandra.distributed.test.dsecompat;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.google.common.collect.ImmutableSet;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.impl.InstanceConfig;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.distributed.upgrade.UpgradeTestBase;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.dsecompat.DseFixtures.DseVersion;
import org.apache.cassandra.net.MessagingService;

/**
 * In-JVM harness for the DSE 6 compatibility mode without any DSE binary.
 * <p>
 * HCD 1.2 (the 4.0 line, branch {@code hcd-1.2.3}) still speaks the pre-4.0 internode dialect DSE uses
 * with non-DSE peers: started with {@code -Dds.current_messaging_version=11} it sends the plain pre-4.0
 * handshake at version 11 and serializes every message in the Cassandra 3.0.14 layout, and started with
 * {@code -Dcassandra.trie_index_format_version=bb} it writes DSE's own sstable version. A cluster of such
 * nodes stands in for a DSE cluster for the handshake at version 11, the 3.0-layout messages and the on-disk
 * formats. It does not stand in for DSE's gossip: HCD 1.2 exchanges its own 4.0 application-state ordinals at
 * version 11 without translation (DSE's differ from ordinal 15 on), and it has none of DSE's handshake flags,
 * gossip JSON or schema-compatibility rules. Those are covered by unit tests against captured DSE bytes.
 * <p>
 * Tests extend this class directly, declared as {@code public class <name> extends DseDialectTestBase}: that is how
 * {@code .build/run-tests.sh} keeps them out of the plain jvm-dtest split. They build a {@link TestCase} with
 * {@link #dse6TestCase(int)}: every node starts as HCD 1.2 at version 11, and the nodes selected for upgrade are
 * restarted as this software with {@code internode_compatibility_mode: DSE_6}. The HCD 1.2 dtest jar is built by
 * {@code test/bin/build-hcd12-dtest-jar.sh} into {@code build/} (the version {@link #HCD12_DTEST_JAR_VERSION}), and
 * the upgraded nodes run from the dtest jar of this build ({@code ant dtest-jar}, rebuilt by the
 * {@code test-jvm-dtest-dse} target; with {@code test-jvm-dtest-some} rebuild it by hand after every product change,
 * or the test runs stale code).
 * Without the HCD 1.2 jar the tests are skipped; without the jar of this build they fail with "No … versions found".
 * The old nodes run the highest 4.0 dtest jar in {@code build/}: when an upstream 4.0 dtest jar (from the
 * {@code jvm-dtest-upgrade} flow) outranks the HCD 1.2 one, the instance initializer refuses to start it.
 * <p>
 * Where this software follows DSE rather than HCD 1.2 at version 11, HCD 1.2 is not an oracle and the tests must not
 * exercise the difference: GROUP BY reads (DSE writes its own DataLimits kind ordinals at 11, 2 and 3 for GROUP BY,
 * which HCD 1.2 writes only at 10) and the FETCH_ALL_STATICS bit of the column filter (sent by HCD 1.2 at 11, absent
 * from the 3.0.14 layout DSE reads).
 */
public abstract class DseDialectTestBase extends UpgradeTestBase
{
    /** The messaging version DSE uses with non-DSE peers, and HCD 1.2 is forced to here. */
    public static final int DSE_DIALECT = MessagingService.VERSION_3014;

    private static final String MESSAGING_SERVICE = "org.apache.cassandra.net.MessagingService";
    private static final String HCD12_TRIE_FORMAT = "org.apache.cassandra.io.sstable.format.trieindex.TrieIndexFormat";
    /** Present only in HCD 1.2, not in upstream 4.0: how the initializer recognises an HCD 1.2 instance. */
    private static final String HCD12_MARKER_FIELD = "SUPPORTED_DSE_VERSION";
    /** Present only in this software, with the DSE_6 mode: how the initializer recognises an upgraded instance. */
    private static final String THIS_SOFTWARE_MARKER_CLASS = InternodeCompatibilityMode.class.getName();
    /** Present on the 5.0 line, not on the 4.0 one: how the initializer recognises a 5.0 jar without the DSE_6 mode. */
    private static final String V50_MARKER_FIELD = "VERSION_50";

    /** The version of the HCD 1.2 dtest jar; must match {@code dse.compat.hcd12.dtest.jar.version} in build.xml. */
    public static final String HCD12_DTEST_JAR_VERSION = "4.0.11.0";
    /**
     * Where the dtest API looks for the dtest jars: the default of its cassandra.test.dtest_jar_path, which no build
     * target overrides. When it is overridden by hand, this check still looks in build/.
     */
    private static final String DTEST_JAR_DIRECTORY = "build";

    private static final String HCD12_BATCH = "org.apache.cassandra.batchlog.Batch";

    /**
     * The class loader of the current incarnation of every instance, by node number, recorded by the instance
     * initializer.
     */
    private static final Map<Integer, ClassLoader> INSTANCE_CLASS_LOADERS = new ConcurrentHashMap<>();

    /**
     * The native-library classes are shared between the instance class loaders of this build (one JNA registration
     * for the JVM), but HCD 1.2's {@code NativeLibrary} facade expects the 4.0-line shape of that interface: each
     * instance loads its own copies here.
     */
    private static final Set<String> ISOLATED_CLASSES = ImmutableSet.of("org.apache.cassandra.utils.NativeLibraryWrapper",
                                                                        "org.apache.cassandra.utils.NativeLibraryLinux",
                                                                        "org.apache.cassandra.utils.NativeLibraryDarwin",
                                                                        "org.apache.cassandra.utils.NativeLibraryWindows");
    /**
     * Built lazily: touching {@code AbstractCluster} from a static initializer changes the class-initialisation order of
     * the test JVM.
     */
    private static Predicate<String> sharedClasses()
    {
        Predicate<String> defaults = AbstractCluster.SHARED_PREDICATE;
        return name -> !ISOLATED_CLASSES.contains(name) && defaults.test(name);
    }

    /** The SAI on-disk version pin of the class, see {@link #pinSaiVersion()}. */
    private static WithProperties saiVersionPin;

    /** Skips the tests of a class when the HCD 1.2 dtest jar has not been built (test-jvm-dtest-some, IDE runs). */
    @BeforeClass
    public static void assumeHcd12DtestJar()
    {
        Path jar = Paths.get(DTEST_JAR_DIRECTORY, "dtest-" + HCD12_DTEST_JAR_VERSION + ".jar");
        Assume.assumeTrue(jar + " is missing: build it with test/bin/build-hcd12-dtest-jar.sh", Files.isRegularFile(jar));
    }

    /**
     * A node in {@code DSE_6} mode refuses to start unless the SAI on-disk version is pinned. The HCD 1.2
     * stand-ins store no DSE release, so the release is undeterminable and the pin is taken as set: {@code aa}, what
     * DSE 6.8 writes, for the whole class. The instances share the system properties of the test JVM, and HCD 1.2 reads
     * the same property: the pin also makes the HCD 1.2 nodes write {@code aa} instead of their default {@code dc},
     * as DSE 6.8 nodes would. A test that stores a DSE 6.9 release sets {@code ca} for its cluster.
     */
    @BeforeClass
    public static void pinSaiVersion()
    {
        saiVersionPin = new WithProperties().set(CassandraRelevantProperties.SAI_CURRENT_VERSION, "aa");
    }

    @AfterClass
    public static void unpinSaiVersion()
    {
        if (saiVersionPin != null)
            saiVersionPin.close();
        saiVersionPin = null;
    }

    /**
     * A test case whose nodes start as HCD 1.2 at version 11 and are upgraded to this software in {@code DSE_6} mode.
     * The configuration, builder and before-restart hooks a test adds run after the harness ones instead of replacing them.
     */
    protected static TestCase dse6TestCase(int nodes)
    {
        return new Dse6TestCase().nodes(nodes).singleUpgradeToCurrentFrom(v40);
    }

    /**
     * As {@link #dse6TestCase(int)}, with extra configuration applied to every node after the harness configuration,
     * whichever version it runs. Keys must be understood by both versions (the 4.0 names, which this software still
     * accepts), or be ignored by HCD 1.2.
     */
    protected static TestCase dse6TestCase(int nodes, Consumer<IInstanceConfig> extraConfig)
    {
        return dse6TestCase(nodes).withConfig(extraConfig);
    }

    /**
     * The in-JVM defaults of this build include keys HCD 1.2 does not know; its yaml loader must ignore them (with the
     * check on, the node exits with the "wrong configuration" code before logging anything). The check stays off after
     * the upgrade too, so a misspelt key in {@link #configureDse6} is not rejected at startup: only the compatstatus
     * assertions of the tests catch it. HCD 1.2 names sstables with UUIDs by default; DSE, and this software in
     * {@code DSE_6} mode, use numeric generations. HCD 1.2 knows that setting only as
     * {@code enable_uuid_sstable_identifiers}, which this software reads as a deprecated alias of
     * {@code uuid_sstable_identifiers_enabled}; {@link #harnessBeforeNodeRestart} removes it before
     * {@link #configureDse6} sets the new name, since a configuration with both is refused whatever the check says.
     */
    private static void harnessConfig(IInstanceConfig config)
    {
        config.set(Constants.KEY_DTEST_API_CONFIG_CHECK, false)
              .set("enable_uuid_sstable_identifiers", false);
    }

    private static void harnessBuilder(UpgradeableCluster.Builder builder)
    {
        builder.withInstanceInitializer(DseDialectTestBase::initializeInstance)
               .withSharedClasses(sharedClasses());
    }

    private static void harnessBeforeNodeRestart(UpgradeableCluster cluster, int node)
    {
        ((InstanceConfig) cluster.get(node).config()).remove("enable_uuid_sstable_identifiers");
        configureDse6(cluster.get(node).config());
    }

    /**
     * {@link TestCase} keeps one consumer of each kind: this one chains the one a test adds after the harness one. A test
     * adds at most one of each kind (a second one would silently replace the first, so it is refused): combine them in a
     * single consumer. The instance class loaders recorded for the cluster are forgotten once it is closed.
     */
    private static final class Dse6TestCase extends TestCase
    {
        private boolean hasConfig;
        private boolean hasBuilder;
        private boolean hasBeforeNodeRestart;

        private Dse6TestCase()
        {
            super.withConfig(DseDialectTestBase::harnessConfig);
            super.withBuilder(DseDialectTestBase::harnessBuilder);
            super.runBeforeNodeRestart(DseDialectTestBase::harnessBeforeNodeRestart);
        }

        @Override
        public TestCase withConfig(Consumer<IInstanceConfig> config)
        {
            hasConfig = checkOnce(hasConfig, "withConfig");
            Consumer<IInstanceConfig> harness = DseDialectTestBase::harnessConfig;
            return super.withConfig(harness.andThen(config));
        }

        @Override
        public TestCase withBuilder(Consumer<UpgradeableCluster.Builder> builder)
        {
            hasBuilder = checkOnce(hasBuilder, "withBuilder");
            Consumer<UpgradeableCluster.Builder> harness = DseDialectTestBase::harnessBuilder;
            return super.withBuilder(harness.andThen(builder));
        }

        @Override
        public TestCase runBeforeNodeRestart(RunOnClusterAndNode hook)
        {
            hasBeforeNodeRestart = checkOnce(hasBeforeNodeRestart, "runBeforeNodeRestart");
            return super.runBeforeNodeRestart((cluster, node) -> {
                harnessBeforeNodeRestart(cluster, node);
                hook.run(cluster, node);
            });
        }

        @Override
        public void run() throws Throwable
        {
            INSTANCE_CLASS_LOADERS.clear();
            try
            {
                super.run();
            }
            finally
            {
                // the cluster is closed: its class loaders must not be kept alive, nor used by the next test case
                INSTANCE_CLASS_LOADERS.clear();
            }
        }

        private static boolean checkOnce(boolean alreadySet, String method)
        {
            if (alreadySet)
                throw new IllegalStateException(method + " was already called on this DSE 6 test case: it keeps a single consumer, "
                                                + "so combine the two in one call");
            return true;
        }
    }

    /** The configuration a former DSE node is started with. */
    protected static void configureDse6(IInstanceConfig config)
    {
        config.set("internode_compatibility_mode", InternodeCompatibilityMode.DSE_6.name())
              .set("storage_compatibility_mode", "HCD_1")
              .set("uuid_sstable_identifiers_enabled", false)
              .set("paxos_variant", "v1");
    }

    /**
     * Runs in every instance class loader before the instance starts. For an HCD 1.2 instance it pins the
     * messaging version to 11 and the trie sstable version to {@code bb} by initialising the two classes that
     * read those properties while the properties are set (the same trick as {@code StreamingTest}); for an
     * instance of this software it does nothing, the {@code DSE_6} mode sets the trie version itself. Any other
     * instance is refused: a 5.0 jar without the mode is a stale dtest jar of this version (from another branch), a
     * 4.0 one comes from an upstream 4.0 dtest jar that outranks the HCD 1.2 one in {@code build/}.
     * <p>
     * Synchronized because the two properties are JVM-wide: HCD 1.2 instances started in parallel (a full cluster
     * restart) must not clear them while another one initialises its classes. The lock does not cover an instance of
     * this software, which reads the messaging-version property when its own {@code MessagingService} initialises,
     * later in its startup: a mixed cluster must not start HCD 1.2 nodes and upgraded ones in parallel, or an
     * upgraded node may silently come up at version 11.
     */
    static synchronized void initializeInstance(ClassLoader classLoader, int node)
    {
        INSTANCE_CLASS_LOADERS.put(node, classLoader);
        try
        {
            if (isLoadable(THIS_SOFTWARE_MARKER_CLASS, classLoader))
                return;
            Class<?> messagingService = Class.forName(MESSAGING_SERVICE, false, classLoader);
            if (hasDeclaredField(messagingService, V50_MARKER_FIELD))
                throw new IllegalStateException("Instance " + node + " runs a 5.0 dtest jar without " + THIS_SOFTWARE_MARKER_CLASS +
                                                ": build/ holds a stale dtest jar of this version, from another branch; run ant dtest-jar");
            if (!hasDeclaredField(messagingService, HCD12_MARKER_FIELD))
                throw new IllegalStateException("build/ holds a 4.0 dtest jar that is not HCD 1.2 and outranks dtest-" + HCD12_DTEST_JAR_VERSION +
                                                ".jar (instance " + node + " has no " + MESSAGING_SERVICE + '.' + HCD12_MARKER_FIELD +
                                                "): remove the upstream 4.0 dtest jars from build/ before running the DSE 6 compatibility tests");
            // HCD 1.2's in-JVM instance re-serializes every message it receives for the message filters, even with none
            // installed, and for a batchlog write from a peer at 11 it does so without its 4.0 workaround for 'remote'
            // batches: a received batch holds encoded mutations, and Batch.Serializer.serialize and serializedSize assert
            // batch.isLocal() ("attempted to serialize a 'remote' batch"). The bytes only feed the filters; with the
            // assertion off the batch is still received and stored as usual. The assertion status can only be set per
            // top-level class, so it is off for the whole of Batch in HCD 1.2 instances, and nowhere else; those two
            // assertions are the only ones of that class in hcd-1.2.3, so nothing else is switched off. It cannot be narrowed
            // to the filters' re-serialization: that code is in HCD 1.2's dtest jar, which the harness does not change.
            classLoader.setClassAssertionStatus(HCD12_BATCH, false);
            try (WithProperties properties = new WithProperties().set(CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION, DSE_DIALECT)
                                                                 .set(CassandraRelevantProperties.TRIE_INDEX_FORMAT_VERSION,
                                                                      InternodeCompatibilityMode.DSE_6_TRIE_INDEX_FORMAT_VERSION))
            {
                int version = Class.forName(MESSAGING_SERVICE, true, classLoader).getField("current_version").getInt(null);
                if (version != DSE_DIALECT)
                    throw new IllegalStateException("HCD 1.2 instance " + node + " initialised with messaging version " + version + " instead of " + DSE_DIALECT);
                Object trieVersion = Class.forName(HCD12_TRIE_FORMAT, true, classLoader).getField("latestVersion").get(null);
                if (!InternodeCompatibilityMode.DSE_6_TRIE_INDEX_FORMAT_VERSION.equals(String.valueOf(trieVersion)))
                    throw new IllegalStateException("HCD 1.2 instance " + node + " initialised with trie sstable version " + trieVersion +
                                                    " instead of " + InternodeCompatibilityMode.DSE_6_TRIE_INDEX_FORMAT_VERSION);
            }
        }
        catch (ReflectiveOperationException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static boolean isLoadable(String className, ClassLoader classLoader)
    {
        try
        {
            Class.forName(className, false, classLoader);
            return true;
        }
        catch (ClassNotFoundException e)
        {
            return false;
        }
    }

    private static boolean hasDeclaredField(Class<?> clazz, String name)
    {
        for (Field field : clazz.getDeclaredFields())
            if (field.getName().equals(name))
                return true;
        return false;
    }

    /**
     * Asserts that a node of this software knows exactly {@code hcdPeers} peers at the dialect: every message it exchanges
     * with them goes through the pre-4.0 codec, which is what the mixed-mode tests exercise.
     */
    protected static void assertPeersAtDialect(UpgradeableCluster cluster, int node, int hcdPeers)
    {
        String status = compatStatus(cluster, node);
        if (!status.contains("(" + hcdPeers + " speaking the pre-4.0 dialect)"))
            throw new AssertionError("node " + node + " should speak the pre-4.0 dialect with " + hcdPeers + " peers:\n" + status);
    }

    /** Waits (bounded) until the local query returns the expected rows on every node: writes at ONE reach the others asynchronously. */
    protected static void assertOnEveryReplica(UpgradeableCluster cluster, String query, Object[]... expected)
    {
        for (int node = 1; node <= cluster.size(); node++)
        {
            int n = node;
            awaitAsserted(() -> AssertUtils.assertRows(cluster.get(n).executeInternal(query), expected));
        }
    }

    /** Runs the assertion until it passes, for up to 30 seconds. */
    protected static void awaitAsserted(Runnable assertion)
    {
        awaitAsserted(30, assertion);
    }

    /** Runs the assertion until it passes, for up to the given number of seconds, then fails with its last error. */
    protected static void awaitAsserted(int seconds, Runnable assertion)
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (true)
        {
            try
            {
                assertion.run();
                return;
            }
            catch (AssertionError e)
            {
                if (System.nanoTime() > deadline)
                    throw e;
                sleepMillis(100);
            }
        }
    }

    protected static void sleepMillis(long millis)
    {
        try
        {
            Thread.sleep(millis);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    /**
     * Starts the hints dispatcher of a node. The in-JVM instances run without gossip, and only the gossip startup path
     * ({@code StorageService.initServer}) starts it. Test code cannot run inside an HCD 1.2 instance (its class loader
     * has no test classes), so the call is made by reflection through the instance's class loader, for both versions.
     */
    protected static void startHintsDispatch(int node)
    {
        ClassLoader classLoader = instanceClassLoader(node);
        try
        {
            Class<?> hintsService = Class.forName("org.apache.cassandra.hints.HintsService", true, classLoader);
            Object instance = hintsService.getField("instance").get(null);
            hintsService.getMethod("startDispatch").invoke(instance);
        }
        catch (ReflectiveOperationException e)
        {
            throw new AssertionError("cannot start the hints dispatch of node " + node, e);
        }
    }

    /**
     * The class loader of the current instance of {@code node}, through which test code calls into the instance by
     * reflection (see {@link #startHintsDispatch}).
     */
    protected static ClassLoader instanceClassLoader(int node)
    {
        ClassLoader classLoader = INSTANCE_CLASS_LOADERS.get(node);
        if (classLoader == null)
            throw new IllegalStateException("no class loader recorded for node " + node);
        return classLoader;
    }

    /**
     * The schema of the DSE fixtures ({@link DseFixtures#fixtureSchema}) with the fixture keyspace replicated
     * {@code replicationFactor} times instead of once.
     */
    protected static List<String> fixtureSchema(DseVersion version, int replicationFactor)
    {
        String once = "'replication_factor': 1";
        List<String> statements = new ArrayList<>();
        for (String statement : DseFixtures.fixtureSchema(version))
        {
            if (statement.startsWith("CREATE KEYSPACE "))
            {
                if (!statement.contains(once))
                    throw new IllegalStateException("the fixture keyspace is no longer created with " + once + ": " + statement);
                statement = statement.replace(once, "'replication_factor': " + replicationFactor);
            }
            statements.add(statement);
        }
        return statements;
    }

    /** {@code nodetool compatstatus} of a node of this software, asserted successful. */
    protected static String compatStatus(ICluster<? extends IInstance> cluster, int node)
    {
        NodeToolResult result = cluster.get(node).nodetoolResult("compatstatus");
        result.asserts().success();
        return result.getStdout();
    }
}
