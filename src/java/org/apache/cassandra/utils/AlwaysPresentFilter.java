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

import org.apache.cassandra.utils.concurrent.Ref;

public class AlwaysPresentFilter implements IFilter
{
    public boolean isPresent(FilterKey key)
    {
        return true;
    }

    public void add(FilterKey key) { }

    public void clear() { }

    public void close() { }

    public IFilter sharedCopy()
    {
        return this;
    }

    public Throwable close(Throwable accumulate)
    {
        return accumulate;
    }

    public void addTo(Ref.IdentityCollection identities)
    {
    }

    public long serializedSize() { return 0; }

    @Override
    public long offHeapSize()
    {
        return 0;
    }

    @Override
    public IFilterSerializer getSerializer()
    {
        throw new UnsupportedOperationException("AlwaysPresentFilter doesn't support serialization");
    }

    @Override
    public boolean isSerializable()
    {
        return false;
    }

    @Override
    public String toString()
    {
        return "AlwaysPresentFilter";
    }
}
