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
package org.apache.cassandra.io.compress;

import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.IvParameterSpec;

import org.apache.cassandra.crypto.IKeyProvider;
import org.apache.cassandra.crypto.IMultiKeyProvider;
import org.apache.cassandra.crypto.KeyAccessException;
import org.apache.cassandra.crypto.KeyGenerationException;

/**
 * Decrypts blocks of data. Reuses the same Cipher instance and avoids needless initialization.
 * Not thread-safe!
 */
class StatefulDecryptor
{
    private final EncryptionConfig encryptionConfig;
    private final SecureRandom secureRandom;
    private final Cipher cipher;

    StatefulDecryptor(EncryptionConfig encryptionConfig, SecureRandom secureRandom) throws NoSuchPaddingException, NoSuchAlgorithmException
    {
        this.encryptionConfig = encryptionConfig;
        this.secureRandom = secureRandom;
        this.cipher = Cipher.getInstance(encryptionConfig.getCipherName());
    }

    void decrypt(ByteBuffer input, ByteBuffer output)
            throws InvalidAlgorithmParameterException, InvalidKeyException, BadPaddingException, ShortBufferException, IllegalBlockSizeException, KeyAccessException, KeyGenerationException
    {
        final IKeyProvider keyProvider = encryptionConfig.getKeyProvider();
        SecretKey key;
        if (keyProvider instanceof IMultiKeyProvider)
        {
            key = ((IMultiKeyProvider) keyProvider)
                    .readHeader(encryptionConfig.getCipherName(), encryptionConfig.getKeyStrength(), input);
        }
        else
        {
            key = keyProvider.getSecretKey(encryptionConfig.getCipherName(), encryptionConfig.getKeyStrength());
        }
        init(key, input);

        // The output size needed to decrypt is bigger than the resulting decrypted size
        // so we need to check that we have a large enough buffer and create a
        // temporary one if we haven't
        int requiredOutputSize = cipher.getOutputSize(input.limit() - input.position());
        int outputSize = output.limit() - output.position();
        if (outputSize >= requiredOutputSize)
        {
            cipher.doFinal(input, output);
        }
        else
        {
            ByteBuffer tempBuffer = ByteBuffer.allocate(requiredOutputSize);
            cipher.doFinal(input, tempBuffer);
            tempBuffer.flip();
            int actualOutputSize = tempBuffer.remaining();
            // If the output buffer size is still too small then we have to
            // throw
            if (actualOutputSize > outputSize)
            {
                throw new ShortBufferException("Need at least " + actualOutputSize + " bytes of space in output buffer");
            }
            output.put(tempBuffer);
        }
    }

    int decrypt(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset)
            throws InvalidAlgorithmParameterException, InvalidKeyException, BadPaddingException, ShortBufferException, IllegalBlockSizeException, KeyAccessException, KeyGenerationException
    {
        final IKeyProvider keyProvider = encryptionConfig.getKeyProvider();
        SecretKey key;
        int headerSize = 0;
        if (keyProvider instanceof IMultiKeyProvider)
        {
            ByteBuffer inputBuffer = ByteBuffer.wrap(input, inputOffset, inputLength - inputOffset);
            key = ((IMultiKeyProvider) keyProvider)
                    .readHeader(encryptionConfig.getCipherName(), encryptionConfig.getKeyStrength(), inputBuffer);

            // only update the offset if the header decryption was successful,
            // otherwise assume there is no header and default to the local key
            headerSize = inputBuffer.position() - inputOffset;
            inputLength -= headerSize;
            inputOffset = inputBuffer.position();
        }
        else
        {
            key = keyProvider.getSecretKey(encryptionConfig.getCipherName(), encryptionConfig.getKeyStrength());
        }
        init(key, input, inputOffset);
        int ivLength = encryptionConfig.getIvLength();
        return cipher.doFinal(
                input,
                inputOffset + ivLength,
                inputLength - ivLength,
                output,
                outputOffset);
    }

    private void init(SecretKey key, ByteBuffer input) throws InvalidKeyException, InvalidAlgorithmParameterException
    {
        if (encryptionConfig.isIvEnabled())
        {
            byte[] iv = new byte[encryptionConfig.getIvLength()];
            input.get(iv);
            IvParameterSpec ivParam = new IvParameterSpec(iv);
            cipher.init(Cipher.DECRYPT_MODE, key, ivParam, secureRandom);
        }
        else
        {
            cipher.init(Cipher.DECRYPT_MODE, key, secureRandom);
        }
    }

    private void init(SecretKey key, byte[] input, int inputOffset) throws InvalidKeyException, InvalidAlgorithmParameterException
    {
        if (encryptionConfig.isIvEnabled())
        {
            IvParameterSpec ivParam = new IvParameterSpec(input, inputOffset, encryptionConfig.getIvLength());
            cipher.init(Cipher.DECRYPT_MODE, key, ivParam, secureRandom);
        }
        else
        {
            cipher.init(Cipher.DECRYPT_MODE, key, secureRandom);
        }
    }
}
