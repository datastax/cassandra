/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.index.sai.disk.v1;


import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.lucene.search.DisiPriorityQueue;

/**
 * Wrapper used in {@link DisiPriorityQueue}.
 *
 * Copied and modified from Lucene 7.5
 */
@NotThreadSafe
public class DisiWrapper
{
    final PostingList iterator;
    final long cost;
    long doc; // the current doc, used for comparison
    DisiWrapper next; // reference to a next element, see #topList

    public DisiWrapper(@Nonnull PostingList iterator)
    {
        this.iterator = iterator;
        this.cost = iterator.size();
        this.doc = -1;
    }
}

