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

package org.apache.cassandra.hints;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.google.common.collect.ImmutableMap;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.dsecompat.DseFixtures;
import org.apache.cassandra.dsecompat.DseFixtures.DseVersion;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.utils.Clock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What the hints catalog does with a hints file left behind by DataStax Enterprise 6 on a node upgraded in place:
 * nothing. DSE names its files {@code <host id>-<timestamp>-v<version>.hints}, which the catalog's file-name filter
 * does not match, so the catalog never opens, delivers or moves the file aside as corrupt, in either mode, and logs
 * nothing about it. Its descriptor has the 3.0 layout but DSE's own version (-1), which this software does not know.
 * In {@code internode_compatibility_mode: DSE_6} the file is converted when hints dispatch starts, not when the catalog
 * loads (see {@link DseHintsConversionTest}); with {@code NONE} it is left alone for good.
 */
public class DseHintsFileTest
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @BeforeClass
    public static void initialize()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void dseHintsFileIsNeitherLoadedNorTouchedWithNone() throws IOException
    {
        assertDseFileUntouched(InternodeCompatibilityMode.NONE, DseVersion.DSE_6_8_65);
    }

    @Test
    public void dseHintsFileIsNeitherLoadedNorTouchedByTheCatalogInDse6Mode() throws IOException
    {
        assertDseFileUntouched(InternodeCompatibilityMode.DSE_6, DseVersion.DSE_6_8_65);
    }

    @Test
    public void dse6926HintsFileIsNeitherLoadedNorTouchedWithNone() throws IOException
    {
        assertDseFileUntouched(InternodeCompatibilityMode.NONE, DseVersion.DSE_6_9_26);
    }

    @Test
    public void dse6926HintsFileIsNeitherLoadedNorTouchedByTheCatalogInDse6Mode() throws IOException
    {
        assertDseFileUntouched(InternodeCompatibilityMode.DSE_6, DseVersion.DSE_6_9_26);
    }

    @Test
    public void dseHintsDescriptorOf6865IsDsesOwn() throws IOException
    {
        assertDescriptorIsDsesOwn(DseVersion.DSE_6_8_65);
    }

    @Test
    public void dseHintsDescriptorOf6926IsDsesOwn() throws IOException
    {
        assertDescriptorIsDsesOwn(DseVersion.DSE_6_9_26);
    }

    /** The catalog load logs nothing about DSE files in either mode: in DSE_6 the dispatch start converts them. */
    @Test
    public void catalogLoadIsSilentInBothModes() throws IOException
    {
        for (InternodeCompatibilityMode mode : new InternodeCompatibilityMode[]{ InternodeCompatibilityMode.NONE, InternodeCompatibilityMode.DSE_6 })
        {
            File directory = new File(folder.newFolder());
            Path dseFile = install(directory, DseVersion.DSE_6_8_65);
            Path second = install(directory, DseVersion.DSE_6_9_26);

            assertThat(loadAndCollectEvents(directory, mode, 0)).as("log of the catalog load in " + mode).isEmpty();
            assertThat(list(directory)).containsExactlyInAnyOrder(dseFile.getFileName().toString(), second.getFileName().toString());
        }
    }

    @Test
    public void dse6ModeIgnoresDseCompanionFilesAndLoadsItsOwnHints() throws IOException
    {
        File directory = new File(folder.newFolder());
        // DSE's companion files of a hints file are not hints files
        String dseBase = UUID.randomUUID() + "-1790335633208-v1";
        Files.createFile(directory.toPath().resolve(dseBase + "-Statistics.hints"));
        Files.createFile(directory.toPath().resolve(dseBase + ".crc32"));
        // a hints file of this software, version 2 as written with storage_compatibility_mode HCD_1
        HintsDescriptor own = new HintsDescriptor(UUID.randomUUID(), HintsDescriptor.VERSION_40, Clock.Global.currentTimeMillis(), ImmutableMap.of());
        HintsWriter.create(directory, own).close();

        assertThat(loadAndCollectWarnings(directory, InternodeCompatibilityMode.DSE_6, 1)).isEmpty();
    }

    private void assertDseFileUntouched(InternodeCompatibilityMode mode, DseVersion version) throws IOException
    {
        File directory = new File(folder.newFolder());
        Path dseFile = install(directory, version);
        byte[] before = Files.readAllBytes(dseFile);

        assertThat(HintsDescriptor.isHintFileName(dseFile)).isFalse();
        assertThat(HintsCatalog.dseHintsFiles(directory)).containsExactly(dseFile);

        loadAndCollectWarnings(directory, mode, 0);

        assertThat(Files.readAllBytes(dseFile)).isEqualTo(before);
        assertThat(list(directory)).containsExactly(dseFile.getFileName().toString());
    }

    private static void assertDescriptorIsDsesOwn(DseVersion version) throws IOException
    {
        Path dseFile = DseFixtures.hintsFile(version);
        // <host id>-<timestamp>-v1.hints
        String[] parts = dseFile.getFileName().toString().split("-");
        UUID hostId = UUID.fromString(String.join("-", parts[0], parts[1], parts[2], parts[3], parts[4]));
        long timestamp = Long.parseLong(parts[5]);

        HintsDescriptor descriptor;
        try (FileInputStreamPlus in = new FileInputStreamPlus(dseFile))
        {
            descriptor = HintsDescriptor.deserialize(in);
        }
        // the header layout is the 3.0 one (version, timestamp, host id, parameters, checksums), but DSE writes -1
        // as the version, which maps to no messaging version here
        assertThat(descriptor.hostId).isEqualTo(hostId);
        assertThat(descriptor.timestamp).isEqualTo(timestamp);
        assertThat(descriptor.version).isEqualTo(-1);
        assertThat(descriptor.parameters).isEmpty();
        assertThatThrownBy(descriptor::messagingVersion).isInstanceOf(AssertionError.class);
    }

    private static Path install(File directory, DseVersion version) throws IOException
    {
        Path source = DseFixtures.hintsFile(version);
        return Files.copy(source, directory.toPath().resolve(source.getFileName().toString()));
    }

    private static List<String> list(File directory) throws IOException
    {
        try (Stream<Path> files = Files.list(directory.toPath()))
        {
            return files.map(p -> p.getFileName().toString()).collect(Collectors.toList());
        }
    }

    /** Loads the catalog in the given mode, asserts the number of stores it found, returns its WARN messages. */
    private static List<String> loadAndCollectWarnings(File directory, InternodeCompatibilityMode mode, int expectedStores)
    {
        return messages(loadAndCollectEvents(directory, mode, expectedStores), Level.WARN);
    }

    private static List<String> messages(List<ILoggingEvent> events, Level level)
    {
        return events.stream()
                     .filter(event -> event.getLevel() == level)
                     .map(ILoggingEvent::getFormattedMessage)
                     .collect(Collectors.toList());
    }

    /** Loads the catalog in the given mode, asserts the number of stores it found, returns what it logged. */
    private static List<ILoggingEvent> loadAndCollectEvents(File directory, InternodeCompatibilityMode mode, int expectedStores)
    {
        Logger logger = (Logger) LoggerFactory.getLogger(HintsCatalog.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        InternodeCompatibilityMode previous = DatabaseDescriptor.getInternodeCompatibilityMode();
        try
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(mode);
            HintsCatalog catalog = HintsCatalog.load(directory, ImmutableMap.of());
            assertThat(catalog.stores().count()).isEqualTo(expectedStores);
            assertThat(catalog.hasFiles()).isEqualTo(expectedStores > 0);
            return new ArrayList<>(appender.list);
        }
        finally
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(previous);
            logger.detachAppender(appender);
            appender.stop();
        }
    }
}
