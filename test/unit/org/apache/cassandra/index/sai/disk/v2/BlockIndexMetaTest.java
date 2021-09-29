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

import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexMeta;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;

import static org.junit.Assert.assertEquals;

public class BlockIndexMetaTest
{
    @Test
    public void test() throws Exception
    {
        ByteBuffersDirectory dir = new ByteBuffersDirectory();

        try (IndexOutput out = dir.createOutput("meta", IOContext.DEFAULT))
        {
            BlockIndexMeta meta = new BlockIndexMeta(1000,
                                                     1001,
                                                     1002,
                                                     1003,
                                                     1004,
                                                     1005,
                                                     1006,
                                                     1007,
                                                     1008,
                                                     1009,
                                                     1010,
                                                     1011,
                                                     1012,
                                                     1013,
                                                     1014,
                                                     new BytesRef("ccc"),
                                                     new BytesRef("ddd"),
                                                     1015,
                                                     new BytesRef("zzz"),
                                                     1020);
            meta.write(out);
        }

        try (IndexInput input = dir.openInput("meta", IOContext.DEFAULT))
        {
            BlockIndexMeta meta = new BlockIndexMeta(input);
            assertEquals(meta.orderMapFP, 1000);
            assertEquals(meta.indexFP, 1001);
            assertEquals(meta.leafFilePointersFP, 1002);
            assertEquals(meta.numLeaves, 1003);
            assertEquals(meta.minTerm, new BytesRef("ccc"));
        }
    }
}
