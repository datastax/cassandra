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

package org.apache.cassandra.index.sai.disk.v2;

import org.junit.Test;

import org.apache.cassandra.index.sai.utils.SharedIndexInput;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;

public class PrefixBytesTest
{
    @Test
    public void test() throws Exception
    {
        ByteBuffersDirectory dir = new ByteBuffersDirectory();

        IndexOutput out = dir.createOutput("test", IOContext.DEFAULT);
        PrefixBytesWriter writer = new PrefixBytesWriter(out);
        writer.add(new BytesRef("aaaaaa"));
        writer.add(new BytesRef("aaaabb"));
        writer.add(new BytesRef("aaaabc"));
        writer.add(new BytesRef("aaaabg"));
        writer.add(new BytesRef("aaaabggg"));

        PrefixBytesWriter.PrefixResult result = writer.finish();
        out.close();

        SharedIndexInput input = new SharedIndexInput(dir.openInput("test", IOContext.DEFAULT));

        PrefixBytesReader reader = new PrefixBytesReader(result.lengthsFP, input);

        BytesRef term = reader.next();

        input.close();
    }
}
