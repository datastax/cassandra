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

package org.apache.cassandra.io.util;

/**
 * Interface for the classes that can be used to instantiate rebufferers over a given file.
 *
 * These are one of two types:
 *  - chunk sources (e.g. SimpleReadRebufferer) which instantiate a buffer managing rebufferer referencing
 *    themselves.
 *  - thread-safe shared rebufferers (e.g. MmapRebufferer) which directly return themselves.
 */
public interface RebuffererFactory extends ReaderFileProxy
{
    Rebufferer instantiateRebufferer();

    /**
     * If the {@link Rebufferer} created by this factory rebuffered in "chunks" of a fixed size, the size (> 0) of those
     * chunks. Otherwise, this should return a value <= 0.
     */
    int chunkSize();

    default void invalidateIfCached(long position)
    {
        // nothing in base class, to be overridden by CachingRebufferer
    }
}
