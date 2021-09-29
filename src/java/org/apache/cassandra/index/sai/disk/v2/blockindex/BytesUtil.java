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

package org.apache.cassandra.index.sai.disk.v2.blockindex;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FutureArrays;

public class BytesUtil
{
    public static ByteComparable fixedLength(BytesRef bytes)
    {
        return ByteComparable.fixedLength(bytes.bytes, bytes.offset, bytes.length);
    }

    public static ByteComparable nudge(ByteComparable value, int nudgeAt)
    {
        return version -> new ByteSource()
        {
            private final ByteSource v = value.asComparableBytes(version);
            private int cur = 0;

            @Override
            public int next()
            {
                int b = ByteSource.END_OF_STREAM;
                if (cur <= nudgeAt)
                {
                    b = v.next();
                    if (cur == nudgeAt)
                    {
                        if (b < 255)
                            ++b;
                        else
                            return b;  // can't nudge here, increase next instead (eventually will be -1)
                    }
                }
                ++cur;
                return b;
            }
        };
    }

    public static ByteComparable nudgeReverse(ByteComparable value, int nudgeAt)
    {
        return version -> new ByteSource()
        {
            private final ByteSource v = value.asComparableBytes(version);
            private int cur = 0;

            @Override
            public int next()
            {
                int b = ByteSource.END_OF_STREAM;
                if (cur <= nudgeAt)
                {
                    b = v.next();
                    if (cur == nudgeAt)
                    {
                        if (b > 0)
                            --b;
                        else
                            return ByteSource.END_OF_STREAM;  // can't nudge here, increase next instead (eventually will be -1)
                    }
                }
                ++cur;
                return b;
            }
        };
    }

    public static int gatherBytes(ByteComparable term, BytesRefBuilder builder)
    {
        return gatherBytes(term.asComparableBytes(ByteComparable.Version.OSS41), builder);
    }

    public static int gatherBytes(ByteSource byteSource, BytesRefBuilder builder)
    {
        int length = 0;
        // gather the term bytes from the byteSource
        while (true)
        {
            final int val = byteSource.next();
            if (val != ByteSource.END_OF_STREAM)
            {
                ++length;
                builder.append((byte) val);
            }
            else
            {
                break;
            }
        }
        return length;
    }

    public static int bytesDifference(BytesRef priorTerm, BytesRef currentTerm)
    {
        return FutureArrays.mismatch(priorTerm.bytes, priorTerm.offset, priorTerm.offset + priorTerm.length, currentTerm.bytes, currentTerm.offset, currentTerm.offset + currentTerm.length);
    }
}
