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
package org.apache.cassandra.index.sai.disk.v1.trie;

import java.io.Closeable;
import java.io.IOException;

import org.agrona.collections.Int2IntHashMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;

/**
 * Writes document length information to disk for use in text scoring
 */
public class DocLengthsWriter implements Closeable
{
    private final IndexOutputWriter output;

    public DocLengthsWriter(IndexComponents.ForWrite components) throws IOException
    {
        this.output = components.addOrGet(IndexComponentType.DOC_LENGTHS).openOutput();
    }

    public void writeDocLengths(Int2IntHashMap lengths) throws IOException
    {
        if (lengths.isEmpty())
        {
            return;
        }

        // Calculate max row ID from doc lengths map
        int maxRowId = -1;
        var keyIterator = lengths.keySet().iterator();
        while (keyIterator.hasNext())
        {
            final int key = keyIterator.nextValue();
            if (key > maxRowId)
                maxRowId = key;
        }

        for (int rowId = 0; rowId <= maxRowId; rowId++)
        {
            final int length = lengths.get(rowId);
            output.writeInt(length == lengths.missingValue() ? 0 : length);
        }
    }

    public long getStartOffset()
    {
        return output.getFilePointer();
    }

    public long getFilePointer()
    {
        return output.getFilePointer();
    }

    @Override
    public void close() throws IOException
    {
        output.close();
    }
}
