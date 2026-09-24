/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.disk.vector;

import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.disk.OrdinalMapper;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import org.apache.cassandra.index.sai.disk.v5.V5VectorPostingsWriter;

/**
 * A simple wrapper that remaps the ordinals in the vector values to the new ordinals
 */
class RemappedVectorValues implements RandomAccessVectorValues
{
    final V5VectorPostingsWriter.RemappedPostings remapped;
    final int maxNewOrdinal;
    final RandomAccessVectorValues vectorValues;

    RemappedVectorValues(V5VectorPostingsWriter.RemappedPostings remapped, int maxNewOrdinal, RandomAccessVectorValues vectorValues)
    {
        this.remapped = remapped;
        this.maxNewOrdinal = maxNewOrdinal;
        this.vectorValues = vectorValues;
    }

    @Override
    public int size()
    {
        return maxNewOrdinal + 1;
    }

    @Override
    public int dimension()
    {
        return vectorValues.dimension();
    }

    @Override
    public VectorFloat<?> getVector(int i)
    {
        var oldOrdinal = remapped.ordinalMapper.newToOld(i);
        return oldOrdinal == OrdinalMapper.OMITTED ? null : vectorValues.getVector(oldOrdinal);
    }

    @Override
    public boolean isValueShared()
    {
        return vectorValues.isValueShared();
    }

    @Override
    public RandomAccessVectorValues copy()
    {
        return new RemappedVectorValues(remapped, maxNewOrdinal, vectorValues.copy());
    }
}
