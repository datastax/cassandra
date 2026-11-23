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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.config.StartupChecksOptions;
import org.apache.cassandra.db.commitlog.CommitLogDescriptor;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.StorageCompatibilityMode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A start in {@code internode_compatibility_mode: NONE} that the DSE 6 upgrade marker refuses, run through
 * {@link CassandraDaemon#setup}: the refusal comes before the commit log starts, so the start leaves the node's
 * directories as they were (before the check, setup touches them only to migrate the local system data across several
 * data directories, which the start in {@code DSE_6} that wrote the marker already did), and the start in
 * {@code DSE_6} that the refusal names as its remedy is not refused by anything the refused start left.
 * <p>
 * A class of its own: the daemon's setup installs process-wide state (the security manager, the file system error
 * handler) that the other tests of the startup checks do not expect. It works in the directories of the test
 * configuration, emptied first, rather than in directories of its own: the construction of the commit log, which
 * this test must not reach, would set the directories back to the configured ones.
 */
public class Dse6StartupRefusalTest
{
    private static final StartupChecksOptions OPTIONS = new StartupChecksOptions();

    private static InternodeCompatibilityMode modeBefore;
    private static StorageCompatibilityMode storageModeBefore;

    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.daemonInitialization();
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        storageModeBefore = DatabaseDescriptor.getStorageCompatibilityMode();
    }

    @After
    public void tearDown() throws IOException
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
        DatabaseDescriptor.setStorageCompatibilityMode(storageModeBefore);
        // the test build does not empty the metadata directory after the tests
        DseUpgradeMarker.delete();
    }

    /** The node's directories: data, commit log, metadata, hints, saved caches and, when set, CDC. */
    private static List<File> directories()
    {
        List<File> directories = new ArrayList<>(Arrays.asList(DatabaseDescriptor.getAllDataFileLocations()));
        directories.add(DatabaseDescriptor.getCommitLogLocation());
        directories.add(DatabaseDescriptor.getMetadataDirectory());
        directories.add(DatabaseDescriptor.getHintsDirectory());
        directories.add(DatabaseDescriptor.getSavedCachesLocation());
        if (DatabaseDescriptor.getCDCLogLocation() != null)
            directories.add(DatabaseDescriptor.getCDCLogLocation());
        return directories;
    }

    /**
     * Every file and directory under the node's directories, by path, with its size (-1 for a directory) and its last
     * modification time.
     */
    private static Map<String, String> content() throws IOException
    {
        Map<String, String> content = new TreeMap<>();
        for (File directory : directories())
        {
            try (Stream<Path> paths = Files.walk(directory.toPath()))
            {
                for (Path path : (Iterable<Path>) paths::iterator)
                    content.put(path.toString(), (Files.isDirectory(path) ? -1 : Files.size(path)) + " " +
                                                 Files.getLastModifiedTime(path));
            }
        }
        return content;
    }

    @Test
    public void aStartInNoneRefusedByTheMarkerLeavesTheDirectoriesAsTheyWere() throws Exception
    {
        for (File directory : directories())
        {
            if (directory.exists())
                directory.deleteRecursive();
            directory.createDirectoriesIfNotExists();
        }

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        // a storage mode whose commit log segments the start in DSE_6 refuses: had the commit log started before the
        // refusal, its first segment would have been one of them
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.NONE);
        String segmentOfThisMode = new CommitLogDescriptor(1, null, null).fileName();
        assertThat(StartupChecks.isCommitLogSegmentWrittenWithoutDse6Mode(segmentOfThisMode)).isTrue();

        assertThat(DseUpgradeMarker.writeIfAbsent()).isTrue();
        Map<String, String> before = content();

        // the check itself refuses, so that setup below stops at it rather than going through a whole start
        assertThatThrownBy(() -> StartupChecks.verifyBeforeCommitLogStart(OPTIONS))
        .isInstanceOf(StartupException.class)
        .satisfies(e -> assertThat(((StartupException) e).returnCode).isEqualTo(StartupException.ERR_WRONG_CONFIG));

        assertThatThrownBy(() -> new CassandraDaemon(true).setup())
        .isExactlyInstanceOf(RuntimeException.class)
        .hasNoCause()
        .hasMessageStartingWith("internode_compatibility_mode is NONE but the DSE 6 upgrade marker " +
                                DseUpgradeMarker.path() + " is present");

        assertThat(content()).isEqualTo(before);
        assertThat(DatabaseDescriptor.getCommitLogLocation().toPath()).isEmptyDirectory();

        // the same in the operator's terms: the remedy the refusal names, DSE_6 with the storage mode it requires,
        // finds nothing to refuse
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        DatabaseDescriptor.setStorageCompatibilityMode(StorageCompatibilityMode.HCD_1);
        StartupChecks.checkDse6Artefacts.execute(OPTIONS);
    }
}
