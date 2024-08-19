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

import com.google.common.annotations.VisibleForTesting;

import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.SparseBits;

public interface OrdinalsView extends AutoCloseable
{
    interface OrdinalConsumer
    {
        void accept(long rowId, int ordinal) throws IOException;
    }

    /** return the vector ordinal associated with the given row, or -1 if no vectors are associated with it */
    int getOrdinalForRowId(int rowId) throws IOException;

    /**
     * iterates over all ordinals in the view.  order of iteration is undefined.
     */
    @VisibleForTesting
    void forEachOrdinalInRange(int startRowId, int endRowId, OrdinalConsumer consumer) throws IOException;

    default Bits buildOrdinalBits(int startRowId, int endRowId, Supplier<SparseBits> bitsSupplier) throws IOException
    {
        var bits = bitsSupplier.get();
        this.forEachOrdinalInRange(startRowId, endRowId, (segmentRowId, ordinal) -> {
            bits.set(ordinal);
        });
        return bits;
    }

    @Override
    void close();
}
