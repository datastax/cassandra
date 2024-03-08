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
import java.util.function.Supplier;

import io.github.jbellis.jvector.util.BitSet;

public interface OrdinalsView extends AutoCloseable
{
    interface OrdinalConsumer
    {
        void accept(long rowId, int ordinal) throws IOException;
    }

    int getOrdinalForRowId(int rowId) throws IOException;

    /** iterates over all ordinals in the view.
     * return true if consumer was called at least once.
     * */
    boolean forEachOrdinalInRange(int startRowId, int endRowId, OrdinalConsumer consumer) throws IOException;

    BitSet buildOrdinalBitSet(int startRowId, int endRowId, Supplier<BitSet> bitsetSupplier) throws IOException;

    @Override
    void close();
}
