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
 * An {@link Encryptor} that reports {@link #SHORTER_BY} bytes less room in a chunk than the cipher allows. Reading a
 * file written with a plain {@link Encryptor} of the same cipher and key with it simulates chunks that pass the
 * checksum and decrypt correctly, but hold more content than a chunk can.
 */
public class ShortPageEncryptor extends Encryptor
{
    public static final int SHORTER_BY = 32;

    public static ShortPageEncryptor create(Map<String, String> options)
    {
        EncryptionConfig encryptionConfig = EncryptionConfig.forClass(ShortPageEncryptor.class).fromCompressionOptions(options).build();
        return new ShortPageEncryptor(encryptionConfig);
    }

    ShortPageEncryptor(EncryptionConfig encryptionConfig)
    {
        super(encryptionConfig);
    }

    @Override
    public int findMaxBytesInChunk(int targetSize)
    {
        return super.findMaxBytesInChunk(targetSize) - SHORTER_BY;
    }
}
