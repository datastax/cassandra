/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.io.compress;


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
