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
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.concurrent.ConcurrentMap;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import com.google.common.collect.Maps;
import java.util.Base64;

import org.apache.cassandra.io.util.FileUtils;

import static java.nio.file.StandardOpenOption.APPEND;
import static org.apache.cassandra.crypto.LocalSystemKey.KEY_DEFAULT_PERMISSIONS;

public class LocalFileSystemKeyProvider implements IKeyProvider
{
    private static final SecureRandom RANDOM = new SecureRandom();

    private final Path keyPath;
    private final ConcurrentMap<String, SecretKey> keys = Maps.newConcurrentMap();

    public LocalFileSystemKeyProvider(Path keyPath) throws IOException
    {
        if (keyPath.getParent() == null)
        {
            throw new IllegalArgumentException("The key path must be absolute");
        }

        if (Files.exists(keyPath))
        {
            this.keyPath = keyPath;
        }
        else
        {
            Files.createDirectories(keyPath.getParent());
            this.keyPath = Files.createFile(keyPath, KEY_DEFAULT_PERMISSIONS);
        }

        loadKeys();
    }

    @Override
    public SecretKey getSecretKey(String cipherName, int keyStrength) throws KeyGenerationException
    {
        try
        {
            String mapKey = getMapKey(cipherName, keyStrength);
            SecretKey secretKey = keys.get(mapKey);
            if (secretKey == null)
            {
                secretKey = generateNewKey(cipherName, keyStrength);
                checkKey(cipherName, secretKey);
                SecretKey previous = keys.putIfAbsent(mapKey, secretKey);
                if (previous == null)
                    appendKey(cipherName, keyStrength, secretKey);
                else
                    secretKey = previous;
            }
            return secretKey;
        }
        catch (IOException e)
        {
            throw new KeyGenerationException("Could not write secret key: " + e.getMessage(), e);
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new KeyGenerationException("Failed to generate secret key: " + e.getMessage(), e);
        }
    }

    private void checkKey(String cipherName, SecretKey secretKey) throws KeyGenerationException
    {
        try
        {
            Cipher cipher = Cipher.getInstance(cipherName);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, RANDOM);
        }
        catch (NoSuchAlgorithmException | InvalidKeyException | NoSuchPaddingException e)
        {
            throw new KeyGenerationException("Error generating secret key: " + e.getMessage(), e);
        }
    }

    private SecretKey generateNewKey(String cipherName, int keyStrength) throws NoSuchAlgorithmException
    {
        KeyGenerator kgen = KeyGenerator.getInstance(getKeyType(cipherName));
        kgen.init(keyStrength, RANDOM);
        return kgen.generateKey();
    }

    private String getMapKey(String cipherName, int keyStrength)
    {
        return cipherName + ":" + keyStrength;
    }

    private synchronized void appendKey(String cipherName, int keyStrength, SecretKey key) throws IOException
    {
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
    }

    private synchronized void loadKeys() throws IOException
    {
        keys.clear();
        BufferedReader is = null;
        try
        {
            is = Files.newBufferedReader(keyPath);
            String line;
            while ((line = is.readLine()) != null)
            {
                String[] fields = line.split(":");
                String cipherName = fields[0];
                int keyStrength = Integer.parseInt(fields[1]);
                byte[] key = Base64.getDecoder().decode(fields[2]);
                keys.put(getMapKey(cipherName, keyStrength), new SecretKeySpec(key, getKeyType(cipherName)));
            }
        }
        finally
        {
            FileUtils.closeQuietly(is);
        }
    }

    private String getKeyType(String cipherName)
    {
        return cipherName.replaceAll("/.*", "");
    }

    String getFileName()
    {
        return keyPath.toAbsolutePath().toString();
    }
}
