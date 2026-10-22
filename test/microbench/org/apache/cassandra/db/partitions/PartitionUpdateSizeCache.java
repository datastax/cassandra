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

package org.apache.cassandra.db.partitions;

/**
 * Benchmark-only access to {@link TriePartitionUpdate}'s memoized serialized sizes.
 *
 * {@link TriePartitionUpdateSerializer#serializedSize} caches its result per messaging version on
 * the update itself, so re-sizing the same instance returns a field read rather than doing the work.
 * A benchmark that reuses one update would therefore measure nothing after its first invocation.
 * Production never sees that: every mutation carries a fresh update, sized at most twice.
 *
 * Lives in this package to reach the package-private fields without reflection.
 */
public class PartitionUpdateSizeCache
{
    /** Puts the update back in the state a freshly built one would be in. No-op for non-trie updates. */
    public static void invalidate(PartitionUpdate update)
    {
        if (update instanceof TriePartitionUpdate)
        {
            TriePartitionUpdate trieUpdate = (TriePartitionUpdate) update;
            trieUpdate.serializedSizeDS21 = -1;
            trieUpdate.serializedSizeDS20 = -1;
        }
    }

    private PartitionUpdateSizeCache()
    {
    }
}
