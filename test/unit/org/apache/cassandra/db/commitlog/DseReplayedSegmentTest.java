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

package org.apache.cassandra.db.commitlog;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.io.util.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * When {@link CommitLog#moveAsideReplayedDseSegment} renames a replayed segment: only in {@code DSE_6} mode, only a
 * DSE segment (version 680), only when no mutation failed; whether mutations of unknown tables were skipped does not
 * matter (the reader flags only the first segment holding each unknown table, so it would depend on the replay order).
 * A failed rename leaves the segment to the usual handling instead of stopping the node, with a WARN telling what
 * that handling does with it. The replay of the real fixture segment through the start-up path is in
 * {@code DseCommitLogReplayTest}.
 */
public class DseReplayedSegmentTest
{
    private static final String DSE_SEGMENT = "CommitLog-680-1790248906231.log";
    private static final String SECOND_DSE_SEGMENT = "CommitLog-680-1790248906233.log";
    private static final String OWN_SEGMENT = "CommitLog-7-1790248906232.log";
    private static final byte[] CONTENT = "segment".getBytes(StandardCharsets.UTF_8);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private InternodeCompatibilityMode previous;
    private Path dir;

    @BeforeClass
    public static void initialize()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setUp() throws IOException
    {
        previous = DatabaseDescriptor.getInternodeCompatibilityMode();
        dir = folder.newFolder().toPath();
    }

    @After
    public void tearDown()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(previous);
    }

    @Test
    public void dse6ModeMovesAsideADseSegment() throws IOException
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        Path segment = segment(DSE_SEGMENT);

        List<String> warnings = warnings(() -> assertThat(CommitLog.moveAsideReplayedDseSegment(new File(segment), true, false)).isTrue());

        assertThat(Files.exists(segment)).isFalse();
        Path moved = dir.resolve(DSE_SEGMENT + CommitLog.REPLAYED_DSE_SEGMENT_SUFFIX);
        assertThat(Files.readAllBytes(moved)).isEqualTo(CONTENT);
        assertThat(CommitLogDescriptor.isValid(moved.getFileName().toString())).as("no longer a segment to replay").isFalse();
        // the line an operator needs before a rollback or when the mode is dropped
        assertThat(warnings).singleElement().asString()
                            .startsWith("Commit log segment " + segment + " was written by DataStax Enterprise 6 and read at the start")
                            .contains("Renamed to " + moved.getFileName() + " so that it is not replayed again");
    }

    @Test
    public void dse6ModeMovesAsideEveryDseSegmentOfAReplay() throws IOException
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        // a crashed DSE node leaves several segments; only the first holding each unknown table has skipped mutations
        Path first = segment(DSE_SEGMENT);
        Path second = segment(SECOND_DSE_SEGMENT);

        assertThat(CommitLog.moveAsideReplayedDseSegment(new File(first), true, false)).isTrue();
        assertThat(CommitLog.moveAsideReplayedDseSegment(new File(second), false, false)).isTrue();

        try (Stream<Path> files = Files.list(dir))
        {
            assertThat(files.map(p -> p.getFileName().toString()))
            .containsExactlyInAnyOrder(DSE_SEGMENT + CommitLog.REPLAYED_DSE_SEGMENT_SUFFIX,
                                       SECOND_DSE_SEGMENT + CommitLog.REPLAYED_DSE_SEGMENT_SUFFIX);
        }
    }

    @Test
    public void anEarlierMovedAsideCopyIsReplaced() throws IOException
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        // the operator removed the suffix of a copy to replay it again, and kept the moved-aside file
        Path earlier = Files.write(dir.resolve(DSE_SEGMENT + CommitLog.REPLAYED_DSE_SEGMENT_SUFFIX),
                                   "earlier".getBytes(StandardCharsets.UTF_8));
        Path segment = segment(DSE_SEGMENT);

        assertThat(CommitLog.moveAsideReplayedDseSegment(new File(segment), true, false)).isTrue();

        assertThat(Files.exists(segment)).isFalse();
        assertThat(Files.readAllBytes(earlier)).isEqualTo(CONTENT);
    }

    @Test
    public void noneModeNeverMovesASegment() throws IOException
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        assertKept(DSE_SEGMENT, false);
    }

    @Test
    public void dse6ModeKeepsItsOwnSegments() throws IOException
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        assertKept(OWN_SEGMENT, false);
    }

    @Test
    public void dse6ModeKeepsADseSegmentWithFailedMutations() throws IOException
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        assertKept(DSE_SEGMENT, true);
    }

    @Test
    public void aFailedRenameOfAFlaggedSegmentWarnsThatItIsReplayedAgain() throws IOException
    {
        assertThat(failedRenameWarning(true)).contains("which by default keeps it: it will be replayed again at the next start")
                                             .contains("brings back any row of it whose deletion was purged since");
    }

    @Test
    public void aFailedRenameOfAnUnflaggedSegmentWarnsThatItIsDeleted() throws IOException
    {
        assertThat(failedRenameWarning(false)).contains("which by default deletes it")
                                              .doesNotContain("replayed again");
    }

    /**
     * Makes the rename fail (a non-empty directory where the renamed file would go), asserts that the segment is left
     * to the usual handling, untouched, and returns the single WARN. What the usual handling then does through the
     * start-up path is in {@code DseCommitLogReplayTest}.
     */
    private String failedRenameWarning(boolean hasInvalidMutations) throws IOException
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        Path blocker = Files.createDirectories(dir.resolve(DSE_SEGMENT + CommitLog.REPLAYED_DSE_SEGMENT_SUFFIX));
        Files.write(blocker.resolve("x"), CONTENT);
        Path segment = segment(DSE_SEGMENT);

        List<String> warnings = warnings(() -> assertThat(CommitLog.moveAsideReplayedDseSegment(new File(segment), hasInvalidMutations, false)).isFalse());

        assertThat(Files.readAllBytes(segment)).isEqualTo(CONTENT);
        assertThat(warnings).hasSize(1);
        assertThat(warnings.get(0)).startsWith("Unable to move the DSE commit log segment " + segment + " aside after its replay");
        return warnings.get(0);
    }

    /** Runs the body and returns the WARN lines of {@link CommitLog} it logged. */
    private static List<String> warnings(Runnable body)
    {
        Logger logger = (Logger) LoggerFactory.getLogger(CommitLog.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try
        {
            body.run();
            return appender.list.stream()
                                .filter(event -> event.getLevel() == Level.WARN)
                                .map(ILoggingEvent::getFormattedMessage)
                                .collect(Collectors.toList());
        }
        finally
        {
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    private void assertKept(String name, boolean hasFailedMutations) throws IOException
    {
        Path segment = segment(name);
        assertThat(CommitLog.moveAsideReplayedDseSegment(new File(segment), true, hasFailedMutations)).isFalse();
        assertThat(Files.readAllBytes(segment)).isEqualTo(CONTENT);
    }

    private Path segment(String name) throws IOException
    {
        return Files.write(dir.resolve(name), CONTENT);
    }
}
