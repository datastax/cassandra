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

package org.apache.cassandra.index.sai.disk.hnsw;


import java.util.function.Function;

public class DiskBinarySearch
{
    public static int searchInt(int low, int high, int target, Function<Integer, Integer> f)
    {
        while (low < high)
        {
            int i = low + (high - low) / 2;
            int value = f.apply(i);
            if (target == value)
                return i;
            else if (target > value)
                low = i + 1;
            else
                high = i;
        }
        throw new IllegalStateException("Element " + target + " not found");
    }
}
