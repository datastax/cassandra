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
 * Base class for the RandomAccessReader components that implement reading.
 */
public interface ReaderFileProxy extends AutoCloseable
{
    void close();               // no checked exceptions

    ChannelProxy channel();

    long fileLength();

    /**
     * Called before rebuffering to allow for position adjustments.
     * This is used to enable files with holes (e.g. encryption data) where we still want to be able to write and read
     * sequences of bytes (e.g. keys) that span over a hole.
     */
    long adjustPosition(long position);

    /**
     * Needed for tests. Returns the table's CRC check chance, which is only set for compressed tables.
     */
    double getCrcCheckChance();
}
