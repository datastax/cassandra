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

package org.apache.cassandra.index.sai.disk.io;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Throwables;
import org.junit.Assert;

import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.lucene.store.IndexInput;

public class TrackingIndexFileUtils extends IndexFileUtils
{
    private final Map<TrackingIndexInput, String> openInputs = Collections.synchronizedMap(new HashMap<>());

    public TrackingIndexFileUtils(SequentialWriterOption writerOption)
    {
        super(writerOption);
    }

    @Override
    public IndexInput openInput(FileHandle handle)
    {
        TrackingIndexInput input = new TrackingIndexInput(super.openInput(handle));
        openInputs.put(input, Throwables.getStackTraceAsString(new RuntimeException("Input created")));
        return input;
    }

    @Override
    public IndexInput openBlockingInput(File file)
    {
        TrackingIndexInput input = new TrackingIndexInput(super.openBlockingInput(file));
        openInputs.put(input, Throwables.getStackTraceAsString(new RuntimeException("Blocking input created")));
        return input;
    }

    public Map<IndexInput, String> getOpenInputs()
    {
        return new HashMap<>(openInputs);
    }

    public class TrackingIndexInput extends FilterIndexInput
    {
        TrackingIndexInput(IndexInput delegate)
        {
            super(delegate);
        }

        @Override
        public void close() throws IOException
        {
            super.close();
            final String creationStackTrace = openInputs.remove(this);
            Assert.assertNotNull("Closed unregistered input: " + this, creationStackTrace);
        }
    }

}
