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

package org.apache.cassandra.utils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Comparator;
import java.util.PriorityQueue;

public class PriorityQueueUtil
{
    static Method initFromCollection;

    static
    {
        try
        {
            initFromCollection = PriorityQueue.class.getDeclaredMethod("initFromCollection", Collection.class);
            initFromCollection.setAccessible(true);
        }
        catch (NoSuchMethodException e)
        {
            initFromCollection = null;
        }
    }

    /**
     * A hack to construct a queue from elements with given comparator in a more efficient way than by manually
     * calling {@code add} or {@code addAll}.
     * The internal {@code initFromCollection} method uses a fast O(n) algorithm instead of O(n log n).
     * Unfortunately it is not exposed to the end user in the cases when a custom comparator is needed,
     * so we have to use reflection.
     */
    public static <T> PriorityQueue<T> createQueue(Collection<T> elements, Comparator<T> comparator)
    {
        var pq = new PriorityQueue<>(comparator);
        try
        {
            if (initFromCollection != null)
                initFromCollection.invoke(pq, elements);
            else
                pq.addAll(elements);
        }
        catch (InvocationTargetException | IllegalAccessException e)
        {
            pq.addAll(elements);
        }
        return pq;
    }
}
