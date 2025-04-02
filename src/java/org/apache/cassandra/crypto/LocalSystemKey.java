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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;

import org.apache.cassandra.io.util.FileUtils;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;

public class LocalSystemKey extends SystemKey
{
    static final FileAttribute<Set<PosixFilePermission>> KEY_DEFAULT_PERMISSIONS = PosixFilePermissions.asFileAttribute(EnumSet.of(OWNER_READ, OWNER_WRITE));
    private static final String NAME_PROPERTY_KEY = "name";
    private static final SecureRandom RANDOM = new SecureRandom();

    private final Path keyPath;

    private final String cipherName;
    private final int keyStrength;
    private final int ivLength;
    private final SecretKey key;

    public LocalSystemKey(Path keyPath) throws IOException
    {
        assert keyPath != null;
        this.keyPath = keyPath;

        // load system key
        BufferedReader is = null;
        try
        {
            is = Files.newBufferedReader(keyPath);
            String line;
            line = is.readLine();
            if (line == null)
                throw new IOException("Key file: " + keyPath + " is empty");
            String[] fields = line.split(":");
            if (fields.length != 3)
                throw new IOException("Malformed key file");
            cipherName = fields[0];
            keyStrength = Integer.parseInt(fields[1]);
            byte[] keyBytes = Base64.decodeBase64(fields[2].getBytes());
            key = new SecretKeySpec(keyBytes, getKeyType(cipherName));
            ivLength = getIvLength(cipherName);
        }
        finally
        {
            FileUtils.closeQuietly(is);
        }
    }

    @Override
    protected SecretKey getKey()
    {
        return key;
    }

    @Override
    protected String getCipherName()
    {
        return cipherName;
    }

    @Override
    protected int getKeyStrength()
    {
        return keyStrength;
    }

    @Override
    protected int getIvLength()
    {
        return ivLength;
    }

    public static Path createKey(String path, String cipherName, int keyStrength) throws IOException, NoSuchAlgorithmException, NoSuchPaddingException {
        return createKey(null, path, cipherName, keyStrength);
    }

    public static Path createKey(Path directory, String keyPath, String cipherName, int keyStrength) throws IOException, NoSuchAlgorithmException, NoSuchPaddingException
    {
        Path targetDirectory = directory != null ? directory : Paths.get(TDEConfigurationProvider.getConfiguration().systemKeyDirectory);
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
            ps.println(cipherName + ":" + keyStrength + ":" + Base64.encodeBase64String(key.getEncoded()));
        }
        finally
        {
            FileUtils.closeQuietly(ps);
        }
        return createdKeyPath;
    }

    public static LocalSystemKey getKey(String path) throws IOException
    {
        Path systemKeyPath = getKeyFile(path);
        if (!Files.exists(systemKeyPath))
            throw new IOException(String.format("Master key file '%s' does not exist", systemKeyPath.toAbsolutePath()));

        return new LocalSystemKey(systemKeyPath);
    }

    private static Path getKeyFile(String path)
    {
        return Paths.get(TDEConfigurationProvider.getConfiguration().systemKeyDirectory, path);
    }

    public String getName()
    {
        return keyPath.getFileName().toString();
    }

    public String getAbsolutePath()
    {
        return keyPath.toAbsolutePath().toString();
    }

    @Override
    public EncryptionKeyBackup asEncryptionKeyBackup()
    {
        throw new UnsupportedOperationException();
    }
}
