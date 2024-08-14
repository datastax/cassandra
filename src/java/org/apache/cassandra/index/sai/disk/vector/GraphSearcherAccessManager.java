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

package org.apache.cassandra.index.sai.disk.vector;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.NotThreadSafe;

import io.github.jbellis.jvector.graph.GraphSearcher;

/**
 * Manages access to a {@link GraphSearcher} instance, validating that we respect the contract of GraphSearcher
 * to only use it in a single search at a time.
 */
@NotThreadSafe
public class GraphSearcherAccessManager
{
    private final GraphSearcher searcher;
    private final AtomicBoolean locked;

    public GraphSearcherAccessManager(GraphSearcher searcher)
    {
        this.searcher = searcher;
        this.locked = new AtomicBoolean(false);
    }

    /**
     * Get the {@link GraphSearcher} instance, locking it to the current in-progress search.
     */
    public GraphSearcher get()
    {
        if (!locked.compareAndSet(false, true))
            throw new IllegalStateException("GraphAccessManager is already locked");
        return searcher;
    }

    /**
     * Release the {@link GraphSearcher} instance, allowing it to be used in another search.
     */
    public void release()
    {
        if (!locked.compareAndSet(true, false))
            throw new IllegalStateException("GraphAccessManager is already unlocked");
    }

    /**
     * Close the {@link GraphSearcher} instance.  It cannot be used again after being closed.
     */
    public void close() throws IOException
    {
        searcher.close();
    }
}
