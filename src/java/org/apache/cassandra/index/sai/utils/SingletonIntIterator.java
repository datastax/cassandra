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

import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;

/**
 * Singleton int iterator used to prevent unnecessary object creation
 */
public class SingletonIntIterator implements PrimitiveIterator.OfInt
{
    private final int value;
    private boolean hasNext = true;

    public SingletonIntIterator(int value)
    {
        this.value = value;
    }

    @Override
    public boolean hasNext()
    {
        return hasNext;
    }

    @Override
    public int nextInt()
    {
        if (!hasNext)
            throw new NoSuchElementException();
        hasNext = false;
        return value;
    }
}
