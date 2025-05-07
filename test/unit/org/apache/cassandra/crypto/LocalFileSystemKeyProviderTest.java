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

package org.apache.cassandra.crypto;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import javax.crypto.SecretKey;

import org.junit.Test;

import org.apache.cassandra.io.util.FileUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class LocalFileSystemKeyProviderTest
{
    @Test
    public void testKeyGeneration() throws Exception
    {
        Map<String, String> options = new HashMap<>();
        Path secretKeyPath = createTempFile();
        options.put("secret_key_file", secretKeyPath.toAbsolutePath().toString());
        IKeyProvider keyProvider = new LocalFileSystemKeyProviderFactory().getKeyProvider(options);

        SecretKey key1a = keyProvider.getSecretKey("AES", 128);
        SecretKey key1b = keyProvider.getSecretKey("AES", 256);
        assertNotNull(key1a);
        assertNotNull(key1b);
        assertNotEquals(key1a, key1b);

        IKeyProvider keyProvider2 = new LocalFileSystemKeyProviderFactory().getKeyProvider(options);
        SecretKey key2 = keyProvider2.getSecretKey("AES", 128);

        assertNotNull(key2);
        assertEquals(key1a, key2);
        assertTrue(Files.deleteIfExists(secretKeyPath));
    }

    @Test
    public void checkFactoryCaching() throws IOException
    {
        LocalFileSystemKeyProviderFactory factory1 = new LocalFileSystemKeyProviderFactory();
        LocalFileSystemKeyProviderFactory factory2 = new LocalFileSystemKeyProviderFactory();
        Map<String, String> options = new HashMap<>();
        Path secretKeyPath = createTempFile();
        options.put("secret_key_file", secretKeyPath.toAbsolutePath().toString());
        IKeyProvider provider1 = factory1.getKeyProvider(options);
        IKeyProvider provider2 = factory2.getKeyProvider(options);
        assertSame(provider1, provider2);
        assertTrue(Files.deleteIfExists(secretKeyPath));
    }

    @Test
    public void shouldCreateFileAndItsParentDirectoryForSecretKeyIfItDoesNotExists() throws IOException
    {
        Path tempDir = createTempDir();
        Path nonExistingKey = tempDir.resolve("non_existing_dir/non_existing_file");
        assertFalse(Files.exists(nonExistingKey));
        assertFalse(Files.exists(nonExistingKey.getParent()));

        LocalFileSystemKeyProvider localFileSystemKeyProvider = new LocalFileSystemKeyProvider(nonExistingKey);

        assertEquals(nonExistingKey.toAbsolutePath().toString(), localFileSystemKeyProvider.getFileName());
        assertTrue(Files.exists(nonExistingKey));
    }

    @Test
    public void shouldCreateFileForSecretKeyIfItDoesNotExists() throws IOException
    {
        Path tempDir = createTempDir();
        Path nonExistingKey = tempDir.resolve("non_existing_file");
        assertFalse(Files.exists(nonExistingKey));
        assertTrue(Files.exists(nonExistingKey.getParent()));

        LocalFileSystemKeyProvider localFileSystemKeyProvider = new LocalFileSystemKeyProvider(nonExistingKey);

        assertEquals(nonExistingKey.toAbsolutePath().toString(), localFileSystemKeyProvider.getFileName());
        assertTrue(Files.exists(nonExistingKey));
    }

    private Path createTempFile()
    {
        return FileUtils.createTempFile("secret_key_file_", ".txt").toPath();
    }

    private Path createTempDir() throws IOException
    {
        return Files.createTempDirectory("tmp_dir").toAbsolutePath();
    }
}
