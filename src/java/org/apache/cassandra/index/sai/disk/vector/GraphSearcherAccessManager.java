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

import javax.annotation.concurrent.NotThreadSafe;

import io.github.jbellis.jvector.graph.GraphSearcher;

/**
 * Manages access to a {@link GraphSearcher} instance, ensuring that it is only locked by a single search at a time.
 * Because the {@link GraphSearcher} is not thread-safe, this class is not thread-safe either.
 */
@NotThreadSafe
public class GraphSearcherAccessManager
{
    private final GraphSearcher searcher;
    private boolean locked;

    public GraphSearcherAccessManager(GraphSearcher searcher)
    {
        this.searcher = searcher;
        this.locked = false;
    }

    public GraphSearcher getSearcher()
    {
        return searcher;
    }

    public void lock()
    {
        if (locked)
            throw new IllegalStateException("GraphAccessManager is already locked");
        locked = true;
    }

    public void release()
    {
        locked = false;
    }

    public void close() throws IOException
    {
        searcher.close();
    }
}
