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

package org.apache.cassandra.index.sai.disk.vector.vectorcompression;

import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.index.sai.disk.vector.VectorSourceModel;

public class VectorCompressionOpenAiV3SmallTest extends AbstractVectorCompressionTest
{
    @Test
    public void testOpenAiV3Small() throws IOException
    {
        // V3_SMALL can be truncated
        for (int i = 1; i < 3; i++)
        {
            int D = 1536 / i;
            testOne(VectorSourceModel.OPENAI_V3_SMALL, D, VectorSourceModel.OPENAI_V3_SMALL.compressionProvider.apply(D));
        }
    }
}

// Made with Bob
