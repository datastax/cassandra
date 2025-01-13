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

package org.apache.cassandra.index.sai.utils;

import java.util.Collections;
import java.util.List;

public class PrimaryKeyListUtil
{

    /** Create a sublist of the keys within (inclusive) the segment's bounds */
    public static List<PrimaryKey> getKeysInRange(List<PrimaryKey> keys, PrimaryKey minKey, PrimaryKey maxKey)
    {
        int minIndex = PrimaryKeyListUtil.findBoundaryIndex(keys, minKey, true);
        int maxIndex = PrimaryKeyListUtil.findBoundaryIndex(keys, maxKey, false);
        return keys.subList(minIndex, maxIndex);
    }

    private static int findBoundaryIndex(List<PrimaryKey> keys, PrimaryKey key, boolean findMin)
    {
        // The minKey and maxKey are sometimes just partition keys (not primary keys), so binarySearch
        // may not return the index of the least/greatest match.
        int index = Collections.binarySearch(keys, key);
        if (index < 0)
            return -index - 1;
        if (findMin)
        {
            while (index > 0 && keys.get(index - 1).equals(key))
                index--;
        }
        else
        {
            while (index < keys.size() - 1 && keys.get(index + 1).equals(key))
                index++;
            // Because we use this for subList, we need to return the index of the next element
            index++;
        }
        return index;
    }
}
