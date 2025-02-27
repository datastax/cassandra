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

import org.apache.cassandra.index.sai.disk.io.IndexFileUtils;
import org.apache.cassandra.index.sai.disk.io.IndexInputReader;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;

public class DocLengthsReader implements Closeable
{
    private final FileHandle fileHandle;
    private final IndexInputReader input;
    private final SegmentMetadata.ComponentMetadata componentMetadata;

    public DocLengthsReader(FileHandle fileHandle, SegmentMetadata.ComponentMetadata componentMetadata)
    {
        this.fileHandle = fileHandle;
        this.input = IndexFileUtils.instance().openInput(fileHandle);
        this.componentMetadata = componentMetadata;
    }

    public int get(int rowID) throws IOException
    {
        // Account for header size in offset calculation
        long position = componentMetadata.offset + (long) rowID * Integer.BYTES;
        if (position >= componentMetadata.offset + componentMetadata.length)
            return 0;
        input.seek(position);
        return input.readInt();
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.close(fileHandle, input);
    }
}

