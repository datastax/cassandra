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
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.ArrayUtils;

public abstract class SystemKey
{
    private static final Random random = new SecureRandom();

    private static final ConcurrentHashMap<String, SystemKey> keys = new ConcurrentHashMap<>();
    private static final byte[] NONE = new byte[]{};

    protected abstract SecretKey getKey() throws KeyAccessException;
    protected abstract String getCipherName();
    protected abstract int getKeyStrength();
    protected abstract int getIvLength();
    public abstract String getName();

    /**
     * Clear the system keys cache, to be used ONLY in testing code.
     * This method exists to increase testing speed, as otherwise to clear
     * the caches we should restart the node.
     */
    @VisibleForTesting
    public static void clearKeysCache()
    {
        keys.clear();
    }

    private static byte[] createIv(int size)
    {
        assert size >= 0;

        if (size == 0)
        {
            return NONE;
        }
        else
        {
            byte[] b = new byte[size];
            random.nextBytes(b);
            return b;
        }
    }

    protected static int getIvLength(String cipherName) throws IOException
    {
        if (cipherName.matches(".*/(CBC|CFB|OFB|PCBC)/.*"))
        {
            try
            {
                return Cipher.getInstance(cipherName).getBlockSize();
            }
            catch (NoSuchAlgorithmException | NoSuchPaddingException e)
            {
                throw new IOException(e);
            }
        }
        else
        {
            return 0;
        }
    }

    public byte[] encrypt(byte[] input) throws IOException
    {
        try
        {
            byte[] iv = createIv(getIvLength());
            Cipher cipher = Cipher.getInstance(getCipherName());
            if (iv.length > 0)
            {
                cipher.init(Cipher.ENCRYPT_MODE, getKey(), new IvParameterSpec(iv));
            }
            else
            {
                cipher.init(Cipher.ENCRYPT_MODE, getKey());
            }
            byte[] output = cipher.doFinal(input);
            return ArrayUtils.addAll(iv, output);
        }
        catch (NoSuchPaddingException | InvalidKeyException | NoSuchAlgorithmException | KeyAccessException |
                IllegalBlockSizeException | BadPaddingException | InvalidAlgorithmParameterException e)
        {
            throw new IOException("Couldn't encrypt input: " + e.getClass().getName() + ": " + e.getMessage(), e);
        }
    }

    public String encrypt(String input) throws IOException
    {
        return Base64.encodeBase64String(encrypt(input.getBytes()));
    }

    public byte[] decrypt(byte[] input) throws IOException
    {
        if (input == null)
            throw new IOException("input is null");
        try
        {
            byte[] iv = getIvLength() > 0 ? Arrays.copyOfRange(input, 0, getIvLength()): NONE;
            Cipher cipher = Cipher.getInstance(getCipherName());
            if (iv.length > 0)
            {
                cipher.init(Cipher.DECRYPT_MODE, getKey(), new IvParameterSpec(iv));
            }
            else
            {
                cipher.init(Cipher.DECRYPT_MODE, getKey());
            }

            return cipher.doFinal(input, getIvLength(), input.length - getIvLength());
        }
        catch (NoSuchPaddingException | IllegalBlockSizeException | BadPaddingException | InvalidKeyException |
                NoSuchAlgorithmException | KeyAccessException | InvalidAlgorithmParameterException e)
        {
            throw new IOException("Couldn't decrypt input: " + e.getClass().getName() + ": " + e.getMessage(), e);
        }
    }

    public String decrypt(String input) throws IOException
    {
        if (input == null)
            throw new IOException("input is null");
        return new String(decrypt(Base64.decodeBase64(input.getBytes())));
    }

    public static SystemKey getSystemKey(String path) throws IOException
    {
        SystemKey systemKey = keys.get(path);

        if (systemKey == null)
        {
            systemKey = LocalSystemKey.getKey(path);
            // put the key in the map, or use the existing one if it's already been loaded
            SystemKey previous = keys.putIfAbsent(path, systemKey);
            if (previous != null)
                systemKey = previous;
        }

        return systemKey;
    }

    protected static String getKeyType(String cipherName)
    {
        return cipherName.replaceAll("/.*", "");
    }

    /**
     * Returns a backup of this system key as an {@link EncryptionKeyBackup} that can be restored later on.
     * @return a backup of this system key.
     */
    public abstract EncryptionKeyBackup asEncryptionKeyBackup();
}
