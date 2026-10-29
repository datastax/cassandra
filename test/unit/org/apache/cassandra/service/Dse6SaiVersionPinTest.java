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

package org.apache.cassandra.service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.config.StartupChecksOptions;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.dsecompat.DseFixtures.DseVersion;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.nodes.CC4UpgradeNodesPersistence;
import org.apache.cassandra.nodes.Nodes;
import org.apache.cassandra.nodes.PeerInfo;
import org.apache.cassandra.service.Dse6SaiVersionPin.Settings;
import org.apache.cassandra.service.Dse6SaiVersionPin.StoredRelease;
import org.apache.cassandra.utils.CassandraVersion;

import static org.apache.cassandra.config.CassandraRelevantProperties.INTERNODE_COMPATIBILITY_MODE_IGNORE_SAI_VERSION_PIN;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_CURRENT_VERSION;
import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_VERSION_SELECTOR_CLASS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The startup check of {@code internode_compatibility_mode: DSE_6} that requires the SAI on-disk version to be pinned to
 * the one the cluster's DSE release writes ({@link Dse6SaiVersionPin}): each rule on the DSE 6 releases and the settings
 * the check is given, then the whole check, which reads the system properties and the releases of {@code nodes/local}
 * (the DSE fixtures), {@code system.local} and the saved peers.
 */
public class Dse6SaiVersionPinTest
{
    private static final StartupChecksOptions OPTIONS = new StartupChecksOptions();
    private static final String DSE_68 = DseVersion.DSE_6_8_65.releaseVersion;
    private static final String DSE_69 = DseVersion.DSE_6_9_26.releaseVersion;
    private static final String KEY = SAI_CURRENT_VERSION.getKey();
    private static final String DEFAULT = SAI_CURRENT_VERSION.getDefaultValue();
    private static final String OVERRIDE_KEY = INTERNODE_COMPATIBILITY_MODE_IGNORE_SAI_VERSION_PIN.getKey();
    private static final String NODES_LOCAL = Dse6SaiVersionPin.THIS_NODE_IN_NODES_LOCAL;
    private static final String NOTHING_STORED = "no DSE 6 release (4.0.0.<build>) is stored for this node or its saved peers";

    private InternodeCompatibilityMode modeBefore;
    private File metadataBefore;
    private Path root;

    @BeforeClass
    public static void init()
    {
        SchemaLoader.prepareServer();
    }

    @Before
    public void setUp() throws IOException
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        metadataBefore = DatabaseDescriptor.getMetadataDirectory();
        root = Files.createTempDirectory("dse6saipin");
        DatabaseDescriptor.setMetadataDirectory(new File(Files.createDirectories(root.resolve("metadata"))));
        Dse6SaiVersionPin.resetStatus();
    }

    @After
    public void tearDown()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
        DatabaseDescriptor.setMetadataDirectory(metadataBefore);
        new File(root).deleteRecursive();
        Dse6SaiVersionPin.resetStatus();
    }

    private static StoredRelease release(String holder, String release)
    {
        return new StoredRelease(holder, new CassandraVersion(release), holder.equals(NODES_LOCAL));
    }

    private static List<StoredRelease> releases(StoredRelease... releases)
    {
        return List.of(releases);
    }

    /** The settings of a node that sets the property to {@code pinned} (null: not set), without selector or override. */
    private static Settings pinned(String pinned)
    {
        return new Settings(pinned, DEFAULT, "", false);
    }

    private static Settings overridden(String pinned)
    {
        return new Settings(pinned, DEFAULT, "", true);
    }

    private interface CheckedRunnable
    {
        void run() throws Exception;
    }

    /** Runs {@code action} and returns the messages {@link Dse6SaiVersionPin} logged at {@code level} or above meanwhile. */
    private static List<String> logsOf(Level level, CheckedRunnable action) throws Exception
    {
        return eventsOf(level, action).stream().map(ILoggingEvent::getFormattedMessage).collect(Collectors.toList());
    }

    /** Runs {@code action} and returns the events {@link Dse6SaiVersionPin} logged at {@code level} or above meanwhile. */
    private static List<ILoggingEvent> eventsOf(Level level, CheckedRunnable action) throws Exception
    {
        Logger logger = (Logger) LoggerFactory.getLogger(Dse6SaiVersionPin.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        // whatever the test logging configuration lets through, every level is captured here
        Level levelBefore = logger.getLevel();
        logger.setLevel(Level.TRACE);
        logger.addAppender(appender);
        try
        {
            action.run();
        }
        finally
        {
            logger.detachAppender(appender);
            logger.setLevel(levelBefore);
        }
        return appender.list.stream()
                            .filter(e -> e.getLevel().isGreaterOrEqual(level))
                            .collect(Collectors.toList());
    }

    private static void corruptNodesLocal() throws IOException
    {
        Path nodes = Files.createDirectories(DatabaseDescriptor.getMetadataDirectory().toPath().resolve("nodes"));
        Files.write(nodes.resolve("local"), new byte[]{ (byte) 0x85, 0x01 });
    }

    @Test
    public void runsAmongTheDefaultChecksAfterTheNodeMetadataIsLoaded()
    {
        List<StartupCheck> checks = new StartupChecks().withDefaultTests().getPreFlightChecks();
        // checkSystemKeyspaceState initialises the node metadata (and imports DSE's on the first start)
        assertThat(checks.indexOf(StartupChecks.checkSystemKeyspaceState)).isNotNegative();
        assertThat(checks.indexOf(StartupChecks.checkDse6SaiVersionPin)).isGreaterThan(checks.indexOf(StartupChecks.checkSystemKeyspaceState));
    }

    @Test
    public void dseVersionFromTheBuildNumber()
    {
        assertThat(Dse6SaiVersionPin.dseVersion(new CassandraVersion(DSE_68))).isEqualTo(Dse6SaiVersionPin.DseVersion.DSE_6_8);
        assertThat(Dse6SaiVersionPin.dseVersion(new CassandraVersion(DSE_69))).isEqualTo(Dse6SaiVersionPin.DseVersion.DSE_6_9);
        assertThat(Dse6SaiVersionPin.dseVersion(new CassandraVersion("4.0.0.6810"))).isEqualTo(Dse6SaiVersionPin.DseVersion.DSE_6_8);
        assertThat(Dse6SaiVersionPin.dseVersion(new CassandraVersion("4.0.0.685"))).isEqualTo(Dse6SaiVersionPin.DseVersion.DSE_6_8);
        assertThat(Dse6SaiVersionPin.dseVersion(new CassandraVersion("4.0.0.690"))).isEqualTo(Dse6SaiVersionPin.DseVersion.DSE_6_9);
        // 6.0.12, 6.7.8, and builds of no known form
        assertThat(Dse6SaiVersionPin.dseVersion(new CassandraVersion("4.0.0.6012"))).isNull();
        assertThat(Dse6SaiVersionPin.dseVersion(new CassandraVersion("4.0.0.678"))).isNull();
        assertThat(Dse6SaiVersionPin.dseVersion(new CassandraVersion("4.0.0.68"))).isNull();
        assertThat(Dse6SaiVersionPin.dseVersion(new CassandraVersion("4.0.0.68650"))).isNull();
        assertThat(Dse6SaiVersionPin.dseVersion(new CassandraVersion("4.0.0.7065"))).isNull();
        // no DSE 6 release at all
        assertThat(Dse6SaiVersionPin.dseVersion(null)).isNull();
        assertThat(Dse6SaiVersionPin.dseVersion(new CassandraVersion("4.0.11.0"))).isNull();
        assertThat(Dse6SaiVersionPin.dseVersion(new CassandraVersion("5.0.4"))).isNull();
    }

    /** The SAI version each DSE release is pinned to names the index files that release wrote (the DSE fixtures). */
    @Test
    public void saiVersionOfEachDseReleaseNamesTheIndexFilesItWrote() throws IOException
    {
        assertThat(Dse6SaiVersionPin.saiVersionOf(new CassandraVersion(DSE_68))).isEqualTo("aa");
        assertThat(Dse6SaiVersionPin.saiVersionOf(new CassandraVersion(DSE_69))).isEqualTo("ca");
        assertThat(Dse6SaiVersionPin.saiVersionOf(new CassandraVersion("4.0.0.678"))).isNull();
        assertThat(Dse6SaiVersionPin.saiVersionOf(new CassandraVersion("4.0.11.0"))).isNull();
        assertThat(Dse6SaiVersionPin.saiVersionOf(null)).isNull();

        for (DseVersion fixture : DseVersion.values())
        {
            Version pinned = Version.parse(Dse6SaiVersionPin.saiVersionOf(new CassandraVersion(fixture.releaseVersion)));
            String groupComplete = pinned.fileNameFormatter().format(IndexComponentType.GROUP_COMPLETION_MARKER, (String) null, 0);
            List<String> written = DseFixtures.sstableComponents(fixture).stream()
                                              .map(p -> p.getFileName().toString())
                                              .collect(Collectors.toList());
            assertThat(written).as("index files of " + fixture).contains("bb-1-bti-" + groupComplete);
        }
    }

    @Test
    public void derivedAaPasses() throws Exception
    {
        List<String> logged = logsOf(Level.INFO, () -> Dse6SaiVersionPin.verify(releases(release(NODES_LOCAL, DSE_68),
                                                                                         release("peer 127.0.0.2:7000", DSE_68)),
                                                                                pinned("aa")));
        assertThat(logged).containsExactly("DSE 6 compatibility mode: SAI on-disk version pinned to aa (cassandra.sai.latest.version), " +
                                           "derived from DSE 6.8 (" + DSE_68 + " of this node in nodes/local, " + DSE_68 +
                                           " of peer 127.0.0.2:7000)");
        assertThat(Dse6SaiVersionPin.status()).isEqualTo("aa, derived from DSE 6.8 (" + DSE_68 + " of this node in nodes/local, " +
                                                         DSE_68 + " of peer 127.0.0.2:7000)");
    }

    @Test
    public void derivedCaPasses() throws Exception
    {
        List<String> logged = logsOf(Level.INFO, () -> Dse6SaiVersionPin.verify(releases(release("peer 127.0.0.2:7000", DSE_69)), pinned("ca")));
        assertThat(logged).singleElement().asString().startsWith("DSE 6 compatibility mode: SAI on-disk version pinned to ca");
        assertThat(Dse6SaiVersionPin.status()).isEqualTo("ca, derived from DSE 6.9 (" + DSE_69 + " of peer 127.0.0.2:7000)");
    }

    @Test
    public void missingPinIsRefused()
    {
        assertThatThrownBy(() -> Dse6SaiVersionPin.verify(releases(release(NODES_LOCAL, DSE_68)), pinned(null)))
        .isInstanceOf(StartupException.class)
        .hasMessage(Dse6SaiVersionPin.REFUSED + "the SAI on-disk version is not pinned (" + KEY + " is not set, so this node writes " +
                    "version " + DEFAULT + ") but this node ran DSE 6.8 (" + DSE_68 + " of this node in nodes/local), which writes SAI " +
                    "version aa. Add -D" + KEY + "=aa to conf/jvm-server.options (or JVM_EXTRA_OPTS) and restart; a node rolled back " +
                    "to DSE must read the SAI index files this node writes, and DSE ignores the index files of a version it does not " +
                    "know and rebuilds those indexes at startup; -D" + OVERRIDE_KEY + "=true starts the node despite this check " +
                    "(with a warning), for a node that will not be rolled back to DSE");
        assertThat(Dse6SaiVersionPin.status()).isNull();
        // with peers among the sources, the cluster ran it
        assertThatThrownBy(() -> Dse6SaiVersionPin.verify(releases(release(NODES_LOCAL, DSE_68), release("peer 127.0.0.2:7000", DSE_68)), pinned(null)))
        .isInstanceOf(StartupException.class)
        .hasMessageContaining(") but the cluster ran DSE 6.8 (");
    }

    @Test
    public void presenceNotValueCounts()
    {
        // a default value equal to the expected version does not count: only the presence of the property does
        Settings defaultIsAa = new Settings(null, "aa", "", false);
        assertThatThrownBy(() -> Dse6SaiVersionPin.verify(releases(release(NODES_LOCAL, DSE_68)), defaultIsAa))
        .isInstanceOf(StartupException.class)
        .hasMessageContaining("the SAI on-disk version is not pinned (" + KEY + " is not set, so this node writes version aa)")
        .hasMessageContaining("Add -D" + KEY + "=aa to conf/jvm-server.options");
        // the settings of the check are read from the system properties: set or not, whatever the value
        try (WithProperties unset = new WithProperties().clear(SAI_CURRENT_VERSION))
        {
            assertThat(Settings.fromSystemProperties().pinned).isNull();
        }
        try (WithProperties set = new WithProperties().set(SAI_CURRENT_VERSION, DEFAULT))
        {
            assertThat(Settings.fromSystemProperties().pinned).isEqualTo(DEFAULT);
        }
    }

    @Test
    public void wrongPinIsRefused()
    {
        assertThatThrownBy(() -> Dse6SaiVersionPin.verify(releases(release(NODES_LOCAL, DSE_68)), pinned("ca")))
        .isInstanceOf(StartupException.class)
        .hasMessageStartingWith(Dse6SaiVersionPin.REFUSED + KEY + " is ca but this node ran DSE 6.8 (" + DSE_68 + " of this node in " +
                                "nodes/local), which writes SAI version aa. Set -D" + KEY + "=aa in conf/jvm-server.options (or " +
                                "JVM_EXTRA_OPTS) and restart; ");
        assertThatThrownBy(() -> Dse6SaiVersionPin.verify(releases(release("peer 127.0.0.2:7000", DSE_69)), pinned("ec")))
        .isInstanceOf(StartupException.class)
        .hasMessageContaining(KEY + " is ec but the cluster ran DSE 6.9")
        .hasMessageContaining("which writes SAI version ca. Set -D" + KEY + "=ca in");
    }

    @Test
    public void customVersionSelectorIsRefused() throws Exception
    {
        Settings selector = new Settings("aa", DEFAULT, "com.example.Selector", false);
        assertThatThrownBy(() -> Dse6SaiVersionPin.verify(releases(release(NODES_LOCAL, DSE_68)), selector))
        .isInstanceOf(StartupException.class)
        .hasMessageStartingWith(Dse6SaiVersionPin.REFUSED + SAI_VERSION_SELECTOR_CLASS.getKey() + " is com.example.Selector: that class, " +
                                "not " + KEY + ", chooses the SAI on-disk version this node writes, so the version cannot be pinned. " +
                                "Remove -D" + SAI_VERSION_SELECTOR_CLASS.getKey() + " from conf/jvm-server.options (or JVM_EXTRA_OPTS) " +
                                "and restart; ");
        try (WithProperties set = new WithProperties().set(SAI_VERSION_SELECTOR_CLASS, "com.example.Selector"))
        {
            assertThat(Settings.fromSystemProperties().selectorClass).isEqualTo("com.example.Selector");
        }
        // with the override: a warning naming the selector as what chooses the version written
        Settings overriddenSelector = new Settings("aa", DEFAULT, "com.example.Selector", true);
        assertThat(logsOf(Level.WARN, () -> Dse6SaiVersionPin.verify(releases(release(NODES_LOCAL, DSE_68)), overriddenSelector)))
        .singleElement().asString().contains("this node writes the version chosen by com.example.Selector");
    }

    @Test
    public void customVersionSelectorIsRefusedFirst()
    {
        // the selector is refused before any rule on the releases or on the pin could be
        Settings selector = new Settings(null, DEFAULT, "com.example.Selector", false);
        for (List<StoredRelease> releases : List.of(releases(release(NODES_LOCAL, DSE_68), release("peer 127.0.0.2:7000", DSE_69)),
                                                    releases(release(NODES_LOCAL, DSE_68)),
                                                    releases()))
        {
            assertThatThrownBy(() -> Dse6SaiVersionPin.verify(releases, selector))
            .isInstanceOf(StartupException.class)
            .hasMessageStartingWith(Dse6SaiVersionPin.REFUSED + SAI_VERSION_SELECTOR_CLASS.getKey() + " is com.example.Selector: ");
        }
    }

    @Test
    public void disagreeingDseVersionsAreRefused()
    {
        assertThatThrownBy(() -> Dse6SaiVersionPin.verify(releases(release(NODES_LOCAL, DSE_68), release("peer 127.0.0.2:7000", DSE_69)),
                                                          pinned("aa")))
        .isInstanceOf(StartupException.class)
        .hasMessageStartingWith(Dse6SaiVersionPin.REFUSED + "the DSE releases stored for this node and its saved peers are not all " +
                                "of one DSE version (DSE 6.8: " + DSE_68 + " of this node in nodes/local; DSE 6.9: " + DSE_69 +
                                " of peer 127.0.0.2:7000), so the SAI on-disk version to pin cannot be chosen. The upgrade requires " +
                                "one DSE release across the cluster")
        .hasMessageEndingWith("A peer row that still holds the release of a node since upgraded, and down since, can cause this: once " +
                              "the release of the cluster is checked, set the pin to it, start once with -D" + OVERRIDE_KEY + "=true " +
                              "(the pin still sets the version written, it is only not checked) and remove that option once the peer " +
                              "is back and its row is refreshed, so that later starts are checked again");
        // a DSE 6 release of no known SAI version next to a known one is a disagreement too
        assertThatThrownBy(() -> Dse6SaiVersionPin.verify(releases(release(NODES_LOCAL, DSE_68), release("peer 127.0.0.3:7000", "4.0.0.6012")),
                                                          pinned("aa")))
        .isInstanceOf(StartupException.class)
        .hasMessageContaining("(DSE 6.8: " + DSE_68 + " of this node in nodes/local; other DSE 6 releases: 4.0.0.6012 of peer 127.0.0.3:7000)");
    }

    @Test
    public void longListsOfHoldersAreCut()
    {
        List<StoredRelease> many = new ArrayList<>();
        for (int i = 1; i <= Dse6SaiVersionPin.LISTED_HOLDERS + 3; i++)
            many.add(release("peer 127.0.0." + i + ":7000", DSE_68));
        assertThatThrownBy(() -> Dse6SaiVersionPin.verify(many, pinned(null)))
        .isInstanceOf(StartupException.class)
        .hasMessageContaining(DSE_68 + " of peer 127.0.0." + Dse6SaiVersionPin.LISTED_HOLDERS + ":7000 and 3 more), which writes")
        .hasMessageNotContaining("127.0.0." + (Dse6SaiVersionPin.LISTED_HOLDERS + 1) + ":7000");
    }

    @Test
    public void undeterminableReleaseAcceptsAnExplicitPin() throws Exception
    {
        for (String value : new String[]{ "aa", "ca" })
        {
            List<String> logged = logsOf(Level.INFO, () -> Dse6SaiVersionPin.verify(releases(), pinned(value)));
            assertThat(logged).containsExactly("DSE 6 compatibility mode: SAI on-disk version pinned to " + value + " (cassandra.sai.latest.version), " +
                                               "set explicitly; the DSE release the cluster ran cannot be determined: " + NOTHING_STORED);
            assertThat(Dse6SaiVersionPin.status()).isEqualTo(value + ", set explicitly; the DSE release the cluster ran cannot be determined: " +
                                                             NOTHING_STORED);
        }
        // releases of DSE 6 versions without SAI determine nothing either
        List<String> logged = logsOf(Level.INFO, () -> Dse6SaiVersionPin.verify(releases(release("peer 127.0.0.3:7000", "4.0.0.678")), pinned("aa")));
        assertThat(logged).singleElement().asString().endsWith("the DSE 6 releases stored, 4.0.0.678 of peer 127.0.0.3:7000, are not of DSE 6.8 or 6.9");
    }

    @Test
    public void undeterminableReleaseRefusesAMissingOrOtherPin()
    {
        assertThatThrownBy(() -> Dse6SaiVersionPin.verify(releases(), pinned(null)))
        .isInstanceOf(StartupException.class)
        .hasMessageStartingWith(Dse6SaiVersionPin.REFUSED + "the SAI on-disk version is not pinned (" + KEY + " is not set, so this node " +
                                "writes version " + DEFAULT + ") and the DSE release the cluster ran cannot be determined (" + NOTHING_STORED +
                                "). Add -D" + KEY + "=aa for DSE 6.8 or -D" + KEY + "=ca for DSE 6.9 to conf/jvm-server.options (or " +
                                "JVM_EXTRA_OPTS), matching the DSE release of the cluster, and restart; a node rolled back to DSE must read");
        assertThatThrownBy(() -> Dse6SaiVersionPin.verify(releases(release("peer 127.0.0.3:7000", "4.0.0.678")), pinned(null)))
        .isInstanceOf(StartupException.class)
        .hasMessageContaining("cannot be determined (the DSE 6 releases stored, 4.0.0.678 of peer 127.0.0.3:7000, are not of DSE 6.8 or 6.9). Add -D");
        assertThatThrownBy(() -> Dse6SaiVersionPin.verify(releases(), pinned("ba")))
        .isInstanceOf(StartupException.class)
        .hasMessageStartingWith(Dse6SaiVersionPin.REFUSED + KEY + " is ba but only aa (DSE 6.8) or ca (DSE 6.9) can be pinned and the DSE " +
                                "release the cluster ran cannot be determined (")
        .hasMessageContaining("Set -D" + KEY + "=aa for DSE 6.8 or -D" + KEY + "=ca for DSE 6.9 in conf/jvm-server.options");
    }

    @Test
    public void overrideWarnsInsteadOfRefusing() throws Exception
    {
        List<String> warnings = logsOf(Level.WARN, () -> Dse6SaiVersionPin.verify(releases(release(NODES_LOCAL, DSE_68)), overridden(null)));
        assertThat(warnings).containsExactly("The SAI on-disk version pin is not enforced (" + OVERRIDE_KEY + "=true): the SAI on-disk " +
                                             "version is not pinned (" + KEY + " is not set, so this node writes version " + DEFAULT +
                                             ") but this node ran DSE 6.8 (" + DSE_68 + " of this node in nodes/local), which writes SAI " +
                                             "version aa; this node writes SAI version " + DEFAULT + ", and a node rolled back to DSE " +
                                             "ignores the index files of a version it does not know and rebuilds those indexes at startup");
        assertThat(Dse6SaiVersionPin.status()).startsWith("not enforced (" + OVERRIDE_KEY + "=true), this node writes SAI version " + DEFAULT +
                                                          ": the SAI on-disk version is not pinned");
        // the disagreement and the undeterminable release are not refused either
        assertThat(logsOf(Level.WARN, () -> Dse6SaiVersionPin.verify(releases(release(NODES_LOCAL, DSE_68), release("peer 127.0.0.2:7000", DSE_69)),
                                                                     overridden("aa"))))
        .singleElement().asString().contains("are not all of one DSE version");
        assertThat(logsOf(Level.WARN, () -> Dse6SaiVersionPin.verify(releases(), overridden(null)))).singleElement().asString()
                                                                                                   .contains("cannot be determined");
        // with a correct pin the override is still reported, in the log and in the status
        warnings = logsOf(Level.WARN, () -> Dse6SaiVersionPin.verify(releases(release(NODES_LOCAL, DSE_68)), overridden("aa")));
        assertThat(warnings).containsExactly(OVERRIDE_KEY + "=true is set: a missing or wrong SAI on-disk version pin would not refuse " +
                                             "the start of this node");
        assertThat(Dse6SaiVersionPin.status()).isEqualTo("aa, derived from DSE 6.8 (" + DSE_68 + " of this node in nodes/local); " +
                                                         OVERRIDE_KEY + "=true is set");
    }

    @Test
    public void noneModeChecksNothing() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        // a nodes/local that cannot be parsed and no pin: in DSE_6 the check would refuse; NONE neither reads nor logs
        corruptNodesLocal();
        try (WithProperties pin = new WithProperties().clear(SAI_CURRENT_VERSION).clear(INTERNODE_COMPATIBILITY_MODE_IGNORE_SAI_VERSION_PIN))
        {
            assertThat(logsOf(Level.TRACE, () -> StartupChecks.checkDse6SaiVersionPin.execute(OPTIONS))).isEmpty();
            assertThat(Dse6SaiVersionPin.status()).isNull();
        }
    }

    @Test
    public void unreadableNodesLocalIsRefused() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        corruptNodesLocal();
        try (WithProperties pin = new WithProperties().set(SAI_CURRENT_VERSION, "aa").clear(INTERNODE_COMPATIBILITY_MODE_IGNORE_SAI_VERSION_PIN))
        {
            assertThatThrownBy(() -> StartupChecks.checkDse6SaiVersionPin.execute(OPTIONS))
            .isInstanceOf(StartupException.class)
            .hasMessageStartingWith(Dse6SaiVersionPin.REFUSED + "the DSE release stored for this node or its saved peers, from which the SAI " +
                                    "on-disk version to pin is chosen, cannot be read (java.lang.RuntimeException: Failed to read the " +
                                    "release version from the node metadata file ")
            .hasMessageEndingWith("-D" + OVERRIDE_KEY + "=true starts the node despite this check (with a warning), for a node that " +
                                  "will not be rolled back to DSE")
            .satisfies(e -> assertThat(((StartupException) e).returnCode).isEqualTo(StartupException.ERR_WRONG_DISK_STATE));
        }
        try (WithProperties pin = new WithProperties().set(SAI_CURRENT_VERSION, "aa").set(INTERNODE_COMPATIBILITY_MODE_IGNORE_SAI_VERSION_PIN, true))
        {
            assertThat(logsOf(Level.WARN, () -> StartupChecks.checkDse6SaiVersionPin.execute(OPTIONS)))
            .singleElement().asString().startsWith("The SAI on-disk version pin is not enforced (" + OVERRIDE_KEY + "=true): the DSE release " +
                                                   "stored for this node or its saved peers, from which the SAI on-disk version to pin is " +
                                                   "chosen, cannot be read (");
            assertThat(Dse6SaiVersionPin.status()).startsWith("not enforced (" + OVERRIDE_KEY + "=true), this node writes SAI version aa: ");
            // the warning carries the failure, with the parse error as its cause
            assertThat(eventsOf(Level.WARN, () -> StartupChecks.checkDse6SaiVersionPin.execute(OPTIONS))).singleElement().satisfies(event -> {
                assertThat(event.getLevel()).isEqualTo(Level.WARN);
                assertThat(event.getThrowableProxy()).isNotNull();
                assertThat(event.getThrowableProxy().getMessage()).startsWith("Failed to read the release version from the node metadata file ");
                assertThat(event.getThrowableProxy().getCause()).isNotNull();
            });
        }
    }

    @Test
    public void storedReleasesKeepsTheDse6Ones() throws Exception
    {
        PeerInfo dsePeer = new PeerInfo().setPeerAddressAndPort(InetAddressAndPort.getByName("127.0.0.2:7000"));
        dsePeer.setReleaseVersion(new CassandraVersion(DSE_68));
        PeerInfo hcdPeer = new PeerInfo().setPeerAddressAndPort(InetAddressAndPort.getByName("127.0.0.3:7000"));
        hcdPeer.setReleaseVersion(new CassandraVersion("4.0.11.0"));
        PeerInfo upgradedPeer = new PeerInfo().setPeerAddressAndPort(InetAddressAndPort.getByName("127.0.0.4:7000"));
        upgradedPeer.setReleaseVersion(new CassandraVersion("5.0.7.0"));
        PeerInfo noRelease = new PeerInfo().setPeerAddressAndPort(InetAddressAndPort.getByName("127.0.0.5:7000"));
        PeerInfo noAddress = new PeerInfo();
        noAddress.setReleaseVersion(new CassandraVersion(DSE_68));

        List<String> found = Dse6SaiVersionPin.storedReleases(new CassandraVersion(DSE_68), new CassandraVersion("5.0.7.0"),
                                                              List.of(dsePeer, hcdPeer, upgradedPeer, noRelease, noAddress))
                                              .stream().map(StoredRelease::toString).collect(Collectors.toList());
        assertThat(found).containsExactly(DSE_68 + " of this node in nodes/local", DSE_68 + " of peer 127.0.0.2:7000",
                                          DSE_68 + " of peer (no address)");

        // the first start after the import: system.local still holds the imported release, listed once
        found = Dse6SaiVersionPin.storedReleases(new CassandraVersion(DSE_68), new CassandraVersion(DSE_68), List.of())
                                 .stream().map(StoredRelease::toString).collect(Collectors.toList());
        assertThat(found).containsExactly(DSE_68 + " of this node in nodes/local");

        // no nodes/local: only system.local counts
        found = Dse6SaiVersionPin.storedReleases(null, new CassandraVersion(DSE_69), List.of())
                                 .stream().map(StoredRelease::toString).collect(Collectors.toList());
        assertThat(found).containsExactly(DSE_69 + " of this node in system.local");
        assertThat(Dse6SaiVersionPin.storedReleases(null, null, List.of(hcdPeer))).isEmpty();
    }

    @Test
    public void releaseOfTheDseNodesFiles() throws Exception
    {
        assertThat(CC4UpgradeNodesPersistence.localReleaseVersionInNodesDirectory()).isNull();
        Path metadata = DatabaseDescriptor.getMetadataDirectory().toPath();
        DseFixtures.installNodesDirectory(metadata, DseVersion.DSE_6_8_65, false);
        assertThat(CC4UpgradeNodesPersistence.localReleaseVersionInNodesDirectory()).isEqualTo(new CassandraVersion(DSE_68));
        new File(metadata.resolve("nodes")).deleteRecursive();
        DseFixtures.installNodesDirectory(metadata, DseVersion.DSE_6_9_26, true);
        assertThat(CC4UpgradeNodesPersistence.localReleaseVersionInNodesDirectory()).isEqualTo(new CassandraVersion(DSE_69));
    }

    /** The whole check in DSE_6 mode: the release of DSE's nodes/local and of the saved peers, from the node metadata. */
    @Test
    public void startupCheckDerivesThePinFromTheNodeMetadata() throws Exception
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        Path metadata = DatabaseDescriptor.getMetadataDirectory().toPath();
        try (WithProperties pin = new WithProperties().clear(SAI_CURRENT_VERSION).clear(INTERNODE_COMPATIBILITY_MODE_IGNORE_SAI_VERSION_PIN))
        {
            // no DSE release stored anywhere (this JVM's node runs this software, it has no saved peer)
            assertThatThrownBy(() -> StartupChecks.checkDse6SaiVersionPin.execute(OPTIONS))
            .isInstanceOf(StartupException.class)
            .hasMessageContaining("cannot be determined (" + NOTHING_STORED + ')');

            DseFixtures.installNodesDirectory(metadata, DseVersion.DSE_6_9_26, true);
            assertThatThrownBy(() -> StartupChecks.checkDse6SaiVersionPin.execute(OPTIONS))
            .isInstanceOf(StartupException.class)
            .hasMessageContaining("this node ran DSE 6.9 (" + DSE_69 + " of this node in nodes/local)")
            .hasMessageContaining("Add -D" + KEY + "=ca to conf/jvm-server.options");
        }
        try (WithProperties pin = new WithProperties().set(SAI_CURRENT_VERSION, "ca").clear(INTERNODE_COMPATIBILITY_MODE_IGNORE_SAI_VERSION_PIN))
        {
            StartupChecks.checkDse6SaiVersionPin.execute(OPTIONS);
            assertThat(Dse6SaiVersionPin.status()).isEqualTo("ca, derived from DSE 6.9 (" + DSE_69 + " of this node in nodes/local)");
            assertThat(MessagingService.instance().getDseSaiVersionPin()).isEqualTo(Dse6SaiVersionPin.status());
            // outside DSE_6 the attribute is null, whatever an earlier check recorded
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
            assertThat(MessagingService.instance().getDseSaiVersionPin()).isNull();
            DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);

            // a saved peer stored with a DSE 6.8 release disagrees with the node's own 6.9
            InetAddressAndPort peer = InetAddressAndPort.getByName("127.0.0.62:7000");
            Nodes.peers().update(peer, p -> p.setReleaseVersion(new CassandraVersion(DSE_68)), true);
            try
            {
                assertThatThrownBy(() -> StartupChecks.checkDse6SaiVersionPin.execute(OPTIONS))
                .isInstanceOf(StartupException.class)
                .hasMessageContaining("(DSE 6.8: " + DSE_68 + " of peer 127.0.0.62:7000; DSE 6.9: " + DSE_69 + " of this node in nodes/local)");
            }
            finally
            {
                Nodes.peers().remove(peer, true, true);
            }
        }
    }
}
