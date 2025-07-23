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

import java.nio.ByteBuffer;
import javax.crypto.SecretKey;

/**
 * Interface for key providers which may use multiple keys to encrypt data and store some header data
 * in their encrypted chunks to identify which key was used.
 */
public interface IMultiKeyProvider extends IKeyProvider
{

    /**
     * Writes header data to the given buffer identifying the key it returns, which
     * needs to be used to encrypt the data.
     *
     * Caller should assume that the byte buffer passed in will be mutated, and is
     * responsible for updating any offset/length values it's using based on the state
     * of the output ByteBuffer after this call returns.
     *
     * @param cipherName name of the JCE cipher, optionally with mode and padding
     * @param keyStrength key length in bits
     * @param output byte buffer to write header data to. Limit must be >= size returned by headerLength
     * @return key instance identified by header data to be used for encryption
     * @throws KeyAccessException when the key exists but could not be retrieved, e.g. from the disk or external storage
     * @throws KeyGenerationException when invalid cipherName was given, or keyStrength does not match the algorithm
     */
    SecretKey writeHeader(String cipherName, int keyStrength, ByteBuffer output) throws KeyAccessException, KeyGenerationException;

    /**
     * Reads header data from the given buffer to determine which key to return, which
     * needs to be used to decrypt the data.
     *
     * Caller should assume that the byte buffer passed in will be mutated, and is
     * responsible for updating any offset/length values it's using based on the state
     * of the input ByteBuffer after this call returns.
     *
     * @param cipherName name of the JCE cipher, optionally with mode and padding
     * @param keyStrength key length in bits
     * @param input byte buffer to write header data to. Limit must be >= size returned by headerLength
     * @return key instance identified by header data to be used for encryption
     * @throws KeyAccessException when the key exists but could not be retrieved, e.g. from the disk or external storage
     * @throws KeyGenerationException when invalid cipherName was given, or keyStrength does not match the algorithm
     */
    SecretKey readHeader(String cipherName, int keyStrength, ByteBuffer input) throws KeyAccessException, KeyGenerationException;

    /**
     * @return size of header data to be written.
     * @throws KeyAccessException when the key exists but could not be retrieved, e.g. from the disk or external storage
     * @throws KeyGenerationException when invalid cipherName was given, or keyStrength does not match the algorithm
     */
    int headerLength();
}
