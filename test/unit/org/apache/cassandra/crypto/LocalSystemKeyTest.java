/*
 * Copyright DataStax, Inc.
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

package org.apache.cassandra.crypto;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class LocalSystemKeyTest
{
    @Test
    public void shouldCreateKey() throws Exception
    {
        // given
        Path tempDir = createTempDir();
        // when
        Path key = LocalSystemKey.createKey(tempDir, "test_key", "AES", 128);
        // then
        assertTrue(Files.exists(key));
    }

    @Test
    public void shouldCreateKeyInSubDir() throws Exception
    {
        // given
        Path tempDir = createTempDir();
        // when
        Path key = LocalSystemKey.createKey(tempDir, "sub_dir/test_key", "AES", 128);
        // then
        assertTrue(Files.exists(key));
    }

    @Test
    public void shouldThrowExceptionWhenKeyFileAlreadyExists() throws Exception
    {
        // given
        Path tempDir = createTempDir();
        Path testKeyPath = tempDir.resolve("test_key");
        assertFalse(Files.exists(testKeyPath));
        Files.createFile(testKeyPath);
        assertTrue(Files.exists(testKeyPath));
        // when
        Throwable throwable = catchThrowable(() -> LocalSystemKey.createKey(tempDir, "test_key", "AES", 128));
        // then
        assertThat(throwable).isInstanceOf(FileAlreadyExistsException.class);
    }

    private Path createTempDir() throws IOException
    {
        return Files.createTempDirectory("tmp_dir").toAbsolutePath();
    }
}
