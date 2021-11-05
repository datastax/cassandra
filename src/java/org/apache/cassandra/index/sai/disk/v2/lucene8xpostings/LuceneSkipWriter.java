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

import org.apache.lucene.store.IndexOutput;

public class LuceneSkipWriter extends MultiLevelSkipListWriter
{
    public static final int MAX_SKIP_LEVELS = 10;
    private boolean initialized = false;
    private long curDoc;
    private long curDocPointer;
    private long[] lastSkipDoc;
    private long[] lastSkipDocPointer;
    private long lastDocFP;

    private final IndexOutput postingsOut;

    public LuceneSkipWriter(int skipInterval,
                            int skipMultiplier,
                            int maxSkipLevels,
                            long df,
                            IndexOutput postingsOut)
    {
        super(skipInterval, skipMultiplier, maxSkipLevels, df);
        this.postingsOut = postingsOut;
        lastSkipDoc = new long[maxSkipLevels];
        lastSkipDocPointer = new long[maxSkipLevels];
    }

    @Override
    public void resetSkip()
    {
        lastDocFP = postingsOut.getFilePointer();
        initialized = false;
    }

    public void bufferSkip(long doc, long numDocs) throws IOException
    {
        initSkip();
        this.curDoc = doc;
        this.curDocPointer = postingsOut.getFilePointer();
        bufferSkip(numDocs);
    }

    public void initSkip()
    {
        if (!initialized)
        {
            super.resetSkip();
            Arrays.fill(lastSkipDoc, 0);
            Arrays.fill(lastSkipDocPointer, lastDocFP);
            initialized = true;
        }
    }

    @Override
    protected void writeSkipData(int level, IndexOutput skipBuffer) throws IOException
    {
        long delta = curDoc - lastSkipDoc[level];
        skipBuffer.writeVLong(delta);
        lastSkipDoc[level] = curDoc;

        long fpDelta = curDocPointer - lastSkipDocPointer[level];

        skipBuffer.writeVLong(fpDelta);
        lastSkipDocPointer[level] = curDocPointer;
    }
}
