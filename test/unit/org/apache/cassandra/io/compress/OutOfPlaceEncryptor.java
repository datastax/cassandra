/*
 * Copyright IBM Corp.
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

import java.util.Map;

/**
 * Test-only {@link Encryptor} that reports it cannot decrypt in place, to exercise the code paths of readers that
 * must use a separate input buffer for such encryptors.
 */
public class OutOfPlaceEncryptor extends Encryptor
{
    public static OutOfPlaceEncryptor create(Map<String, String> options)
    {
        EncryptionConfig encryptionConfig = EncryptionConfig.forClass(OutOfPlaceEncryptor.class).fromCompressionOptions(options).build();
        return new OutOfPlaceEncryptor(encryptionConfig);
    }

    OutOfPlaceEncryptor(EncryptionConfig encryptionConfig)
    {
        super(encryptionConfig);
    }

    @Override
    public boolean canDecompressInPlace()
    {
        return false;
    }
}
