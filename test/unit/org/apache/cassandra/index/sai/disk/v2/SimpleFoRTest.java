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

import org.apache.cassandra.index.sai.utils.NdiRandomizedTest;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

public class SimpleFoRTest extends NdiRandomizedTest
{
    @Test
    public void test() throws Exception
    {
        ByteBuffersDirectory dir = new ByteBuffersDirectory();

        IndexOutput out = dir.createOutput("test", IOContext.DEFAULT);
        int[] arr = new int[] {20, 80, 96, 1005, 9000};
        long fp = SimpleFoR.write(arr, out);
        out.close();

        IndexInput input = dir.openInput("test", IOContext.DEFAULT);
        int[] arr2 = new int[arr.length];
        SimpleFoR.read(arr2, arr.length, fp, input);
        input.close();

        assertArrayEquals(arr, arr2);
    }
}
