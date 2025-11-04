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

package org.apache.cassandra.db.commitlog;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.io.util.File;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CommitLogSegmentHandlerTest
{
    private CommitLogSegmentHandler handler;
    private Path tempDir;
    private File testFile;

    @Before
    public void setUp() throws IOException
    {
        handler = new CommitLogSegmentHandler();
        tempDir = Files.createTempDirectory("commitlog_test");
        testFile = new File(tempDir.resolve("test_segment.log"));
        testFile.createFileIfNotExists();
    }

    @After
    public void tearDown() throws IOException
    {
        if (testFile.exists())
            testFile.delete();
        Files.deleteIfExists(tempDir);
    }

    @Test
    public void testHandleReplayedSegmentDeletesFileWhenNoIssues()
    {
        assertTrue("Test file should exist before handling", testFile.exists());
        handler.handleReplayedSegment(testFile, false, false);
        assertFalse("File should be deleted when no invalid or failed mutations", testFile.exists());
    }

    @Test
    public void testHandleReplayedSegmentKeepsFileWhenHasInvalidMutations()
    {
        assertTrue("Test file should exist before handling", testFile.exists());
        handler.handleReplayedSegment(testFile, true, false);
        assertTrue("File should be kept when has invalid mutations", testFile.exists());
    }

    @Test
    public void testHandleReplayedSegmentKeepsFileWhenHasFailedMutations()
    {
        assertTrue("Test file should exist before handling", testFile.exists());
        handler.handleReplayedSegment(testFile, false, true);
        assertTrue("File should be kept when has failed mutations", testFile.exists());
    }

    @Test
    public void testHandleReplayedSegmentKeepsFileWhenHasBothIssues()
    {
        assertTrue("Test file should exist before handling", testFile.exists());
        handler.handleReplayedSegment(testFile, true, true);
        assertTrue("File should be kept when has both invalid and failed mutations", testFile.exists());
    }
}
