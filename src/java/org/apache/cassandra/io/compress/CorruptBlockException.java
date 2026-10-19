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

import org.apache.cassandra.io.util.File;

public class CorruptBlockException extends IOException
{
    private final File file;

    public CorruptBlockException(File file, CompressionMetadata.Chunk chunk)
    {
        this(file, chunk, null);
    }

    public CorruptBlockException(File file, CompressionMetadata.Chunk chunk, Throwable cause)
    {
        this(file, chunk.offset, chunk.length, cause);
    }

    public CorruptBlockException(File file, long offset, int length)
    {
        this(file, offset, length, null);
    }

    public CorruptBlockException(File file, long offset, int length, Throwable cause)
    {
        super(String.format("(%s): corruption detected, chunk at %d of length %d.", file.toString(), offset, length), cause);
        this.file = file;
    }

    public CorruptBlockException(File file, CompressionMetadata.Chunk chunk, int storedChecksum, int calculatedChecksum)
    {
        this(file, chunk.offset, chunk.length, storedChecksum, calculatedChecksum);
    }

    public CorruptBlockException(File file, long offset, int length, int storedChecksum, int calculatedChecksum)
    {
        super(String.format("(%s): corruption detected, chunk at %d of length %d has mismatched checksums. Expected %d, but calculated %d",
                            file.toString(), offset, length, storedChecksum, calculatedChecksum));
        this.file = file;
    }

    public File getFile()
    {
        return file;
    }
}
