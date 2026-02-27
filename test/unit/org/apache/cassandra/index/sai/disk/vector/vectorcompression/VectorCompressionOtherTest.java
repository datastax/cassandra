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
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.index.sai.disk.vector.VectorSourceModel;

public class VectorCompressionOtherTest extends AbstractVectorCompressionTest
{
    @Test
    public void testOther() throws IOException
    {
        // 25..200 -> Glove dimensions
        // 1536 -> Ada002
        // 2000 -> something unknown and large
        for (int dimension : List.of(25, 50, 100, 200, 1536, 2000))
            testOne(VectorSourceModel.OTHER, dimension, VectorSourceModel.OTHER.compressionProvider.apply(dimension));
    }
}

// Made with Bob
