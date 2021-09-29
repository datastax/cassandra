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

package org.apache.lucene.codecs.lucene84;


import java.io.IOException;

import org.apache.lucene.codecs.lucene84.ForDeltaUtil;
import org.apache.lucene.codecs.lucene84.ForUtil;
import org.apache.lucene.codecs.lucene84.PForUtil;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

public class PForEncoder
{
    final PForUtil pforUtil;
    final ForDeltaUtil forDeltaUtil;

    public PForEncoder()
    {
        final ForUtil forUtil = new ForUtil();
        forDeltaUtil = new ForDeltaUtil(forUtil);
        pforUtil = new PForUtil(forUtil);
    }

    public void encodeFoRDeltas(long[] longs, DataOutput out) throws IOException
    {
        forDeltaUtil.encodeDeltas(longs, out);
    }

    public void decodeFoRDeltaAndPrefixSum(long fp, long[] longs, DataInput in) throws IOException
    {
        forDeltaUtil.decodeAndPrefixSum(in, fp, longs);
    }

    public void encodePFoR(long[] longs, DataOutput out) throws IOException
    {
        pforUtil.encode(longs, out);
    }

    public void decodePFoR(long[] longs, DataInput in) throws IOException
    {
        pforUtil.decode(in, longs);
    }
}
