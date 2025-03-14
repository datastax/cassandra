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
package org.apache.cassandra.io.compress;

import java.io.IOException;

public class CorruptBlockException extends IOException
{
    public CorruptBlockException(String filePath, CompressionMetadata.Chunk chunk)
    {
        this(filePath, chunk, null);
    }

    public CorruptBlockException(String filePath, CompressionMetadata.Chunk chunk, Throwable cause)
    {
        this(filePath, chunk.offset, chunk.length, cause);
    }

    public CorruptBlockException(String filePath, long offset, int length)
    {
        this(filePath, offset, length, null);
    }

    public CorruptBlockException(String filePath, long offset, int length, Throwable cause)
    {
        super(String.format("(%s): corruption detected, chunk at %d of length %d.", filePath, offset, length), cause);
    }

    public CorruptBlockException(String filePath, CompressionMetadata.Chunk chunk, int storedChecksum, int calculatedChecksum)
    {
        this(filePath, chunk.offset, chunk.length, storedChecksum, calculatedChecksum);
    }

    public CorruptBlockException(String filePath, long offset, int length, int storedChecksum, int calculatedChecksum)
    {
        super(String.format("(%s): corruption detected, chunk at %d of length %d has mismatched checksums. Expected %d, but calculated %d",
                            filePath, offset, length, storedChecksum, calculatedChecksum));
    }

}
