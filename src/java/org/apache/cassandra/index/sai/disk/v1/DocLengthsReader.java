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
package org.apache.cassandra.index.sai.disk.v1;

import java.io.Closeable;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.io.IndexFileUtils;
import org.apache.cassandra.index.sai.disk.io.IndexInputReader;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;

/**
 * Reads the component written by {@link org.apache.cassandra.index.sai.disk.v1.trie.DocLengthsWriter}.
 */
@NotThreadSafe
public class DocLengthsReader implements Closeable
{
    private final IndexInputReader input;
    private final long offset;
    private final long upperBound;

    public DocLengthsReader(FileHandle fileHandle, SegmentMetadata.ComponentMetadata componentMetadata, Version version)
    {
        this.input = IndexFileUtils.instance().openInput(fileHandle);
        // Version EC skipped the header in the doc lengths component metadata.
        int headerAdjustment = Version.EC.equals(version) ? 0 : SAICodecUtils.headerSize();
        this.offset = componentMetadata.offset + headerAdjustment;
        // The offset + length get you the end of the file for all relevant versions.
        this.upperBound = componentMetadata.offset + componentMetadata.length;
    }

    public int get(int rowID) throws IOException
    {
        // Account for header size in offset calculation
        long position = offset + (long) rowID * Integer.BYTES;
        if (position >= upperBound)
            return 0;
        input.seek(position);
        return input.readInt();
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.close(input);
    }
}

