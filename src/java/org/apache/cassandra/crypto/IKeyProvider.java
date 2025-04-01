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


import java.util.List;
import javax.crypto.SecretKey;

/**
 * Interface for objects managing cryptographic secret keys
 * used for encryption and decryption of CFs.
 */
public interface IKeyProvider
{
    /**
     * Returns a key for the given cipher algorithm and key strength.
     * If the key for the given algorithm and length is requested for the first time, it should be created.
     * If the key is requested for the second time or more, always the same key should be returned.
     *
     * @param cipherName name of the JCE cipher, optionally with mode and padding
     * @param keyStrength key length in bits
     * @return a valid secret key, never returns null
     * @throws KeyAccessException when the key exists but could not be retrieved, e.g. from the disk or external storage
     * @throws KeyGenerationException when invalid cipherName was given, or keyStrength does not match the algorithm
     */
    SecretKey getSecretKey(String cipherName, int keyStrength) throws KeyAccessException, KeyGenerationException;

    /**
     * Returns a list of backup encryption keys for the given cipher algorithm and key strength.
     *
     * There are two reasons to return a List instead of just a single {@link EncryptionKeyBackup}:
     * <ul>
     *     <li>If the key has dependencies it would return a backup of those dependencies too, so it would be possible to restore the requested key.</li>
     *     <li>Some key providers might store multiple keys per cipher and keyStrength combination, so we should return all of those</li>
     * </ul>
     *
     *
     * @param cipherName name of the JCE cipher, optionally with mode and padding
     * @param keyStrength key length in bits
     * @return a list of encryption key backups.
     */
    List<EncryptionKeyBackup> getEncryptionKeyBackups(String cipherName, int keyStrength);
}
