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

package org.apache.cassandra.index.sai.disk.format;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.index.sai.IndexContext;

/**
 * A unique (within this sstable) identifier for a column OR sstable index.  The instances
 * may be compared using object identity.
 *
 * Usage: create a Provider() and then call get() to get an identifier for a column or sstable.
 * The Provider takes care of ensuring instance uniqueness.
 */
public class IndexIdentifier
{
    public static final IndexIdentifier SSTABLE = new IndexIdentifier();

    public static class Provider
    {
        private final Map<String, IndexIdentifier> identifiers = new HashMap<>();

        public IndexIdentifier get(String indexName)
        {
            return identifiers.computeIfAbsent(indexName, __ -> new IndexIdentifier());
        }

        public IndexIdentifier get(IndexContext indexContext)
        {
            if (indexContext == null)
                return SSTABLE;
            return get(indexContext.getIndexName());
        }
    }
}
