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

import org.apache.cassandra.index.sai.disk.vector.VectorCompression;
import org.apache.cassandra.index.sai.disk.vector.VectorSourceModel;

public class VectorCompressionFewRowsTest extends AbstractVectorCompressionTest
{
    @Test
    public void testFewRows() throws IOException
    {
        // with fewer than MIN_PQ_ROWS we expect to observe no compression no matter
        // what the source model would prefer
        testOne(1, VectorSourceModel.OTHER, 200, VectorCompression.NO_COMPRESSION);
    }
}

// Made with Bob
