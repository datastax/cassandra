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
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.EnumSet;
import java.util.Set;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

import java.util.Base64;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;

public class LocalSystemKey
{
    static final FileAttribute<Set<PosixFilePermission>> KEY_DEFAULT_PERMISSIONS = PosixFilePermissions.asFileAttribute(EnumSet.of(OWNER_READ, OWNER_WRITE));
    private static final SecureRandom RANDOM = new SecureRandom();

    public static Path createKey(String path, String cipherName, int keyStrength) throws IOException, NoSuchAlgorithmException, NoSuchPaddingException {
        return createKey(null, path, cipherName, keyStrength);
    }

    public static Path createKey(Path directory, String keyPath, String cipherName, int keyStrength) throws IOException, NoSuchAlgorithmException, NoSuchPaddingException
    {
        Path targetDirectory = directory != null ? directory : new File(TDEConfigurationProvider.getConfiguration().systemKeyDirectory).toPath();
        Path fullKeyPath = targetDirectory.resolve(keyPath);

        KeyGenerator keyGen = KeyGenerator.getInstance(getKeyType(cipherName));
        keyGen.init(keyStrength, RANDOM);
        SecretKey key = keyGen.generateKey();

        return storeKey(fullKeyPath, cipherName, keyStrength, key);
    }

    static Path storeKey(Path keyPath, String cipherName, int keyStrength, SecretKey key) throws NoSuchAlgorithmException, NoSuchPaddingException, IOException
    {
        // validate the ciphername
        Cipher.getInstance(cipherName);
        Files.createDirectories(keyPath.getParent());
        Path createdKeyPath = Files.createFile(keyPath, KEY_DEFAULT_PERMISSIONS);

        PrintStream ps = null;
        try
        {
            ps = new PrintStream(Files.newOutputStream(keyPath, APPEND));
            ps.println(cipherName + ":" + keyStrength + ":" + Base64.getEncoder().encodeToString(key.getEncoded()));
        }
        finally
        {
            FileUtils.closeQuietly(ps);
        }
        return createdKeyPath;
    }

    protected static String getKeyType(String cipherName)
    {
        return cipherName.replaceAll("/.*", "");
    }
}
