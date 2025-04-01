/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.commons.codec.binary.Base64;

import org.apache.cassandra.io.util.FileUtils;

import static java.lang.String.format;
import static java.nio.file.StandardOpenOption.APPEND;
import static org.apache.cassandra.crypto.LocalSystemKey.KEY_DEFAULT_PERMISSIONS;

public class LocalFileSystemKeyProvider implements IKeyProvider
{
    private static final SecureRandom RANDOM = new SecureRandom();

    private final Path keyPath;
    private final ConcurrentMap<String, SecretKey> keys = Maps.newConcurrentMap();

    public LocalFileSystemKeyProvider(Path keyPath) throws IOException
    {
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
            ps.println(cipherName + ":" + keyStrength + ":" + Base64.encodeBase64String(key.getEncoded()));
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
                byte[] key = Base64.decodeBase64(fields[2].getBytes());
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

    private void addKeyIfItDoesNotExist(String cipherName, int keyStrength, SecretKey secretKey) throws IOException
    {
        SecretKey previousKey = keys.putIfAbsent(getMapKey(cipherName, keyStrength), secretKey);

        if (previousKey != null)
        {
            if (!secretKey.equals(previousKey))
                throw new RuntimeException(format("Unable to store a local encryption key (%s) as there is an different local key with the same cipher '%s' and key strength %d", getFileName(), cipherName, keyStrength));

            if (!Files.exists(keyPath))
                throw new RuntimeException(format("Unable to store a local encryption key with cipher '%s' and key strength %d as the file '%s' doesn't exist", cipherName, keyStrength, getFileName()));

            return;
        }

        appendKey(cipherName, keyStrength, secretKey);
    }

    @Override
    public List<EncryptionKeyBackup> getEncryptionKeyBackups(String cipherName, int keyStrength)
    {
        throw new UnsupportedOperationException();
    }
}
