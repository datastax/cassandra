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

package org.apache.cassandra.index.sai.disk.v2.lucene8xpostings;

import java.io.IOException;
import java.util.Arrays;

public class LuceneSkipReader extends MultiLevelSkipListReader
{
    private final long[] docPointer;
    private long lastDocPointer;

    public LuceneSkipReader(Lucene8xIndexInput skipStream, int maxSkipLevels, int blockSize)
    {
        super(skipStream, maxSkipLevels, blockSize, 8);
        docPointer = new long[maxSkipLevels];
    }

    public long getNextSkipDoc() {
        return skipDoc[0];
    }

    public long getDocPointer() {
        return lastDocPointer;
    }

    @Override
    protected void seekChild(int level) throws IOException
    {
        super.seekChild(level);
        docPointer[level] = lastDocPointer;
    }

    @Override
    protected void setLastSkipData(int level)
    {
        super.setLastSkipData(level);
        lastDocPointer = docPointer[level];
    }

    protected long trim(long df)
    {
        return df % 128 == 0 ? df - 1 : df;
    }

    public void init(long skipPointer, long docBaseFP, long df) throws IOException
    {
        super.init(skipPointer, trim(df));
        Arrays.fill(docPointer, docBaseFP);
    }

    @Override
    protected long readSkipData(int level, Lucene8xIndexInput skipStream) throws IOException
    {
        long delta = skipStream.readVLong();
        docPointer[level] += skipStream.readVLong();
        return delta;
    }
}
