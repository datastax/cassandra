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

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import org.apache.cassandra.exceptions.InvalidRequestException;

public class VectorValidation
{
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    // chosen to make sure dot products don't overflow
    public static final float MAX_FLOAT32_COMPONENT = 1E17f;

    public static void checkInBounds(VectorFloat<?> v)
    {
        for (int i = 0; i < v.length(); i++)
        {
            if (!Float.isFinite(v.get(i)))
            {
                throw new IllegalArgumentException("non-finite value at vector[" + i + "]=" + v.get(i));
            }

            if (Math.abs(v.get(i)) > MAX_FLOAT32_COMPONENT)
            {
                throw new IllegalArgumentException("Out-of-bounds value at vector[" + i + "]=" + v.get(i));
            }
        }
    }

    /** use with caution, it allocates a temporary VectorFloat */
    public static void validateIndexable(float[] raw, VectorSimilarityFunction similarityFunction)
    {
        validateIndexable(vts.createFloatVector(raw), similarityFunction);
    }

    public static void validateIndexable(VectorFloat<?> vector, VectorSimilarityFunction similarityFunction)
    {
        try
        {
            checkInBounds(vector);
        }
        catch (IllegalArgumentException e)
        {
            throw new InvalidRequestException(e.getMessage());
        }

        if (similarityFunction == VectorSimilarityFunction.COSINE)
        {
            if (isEffectivelyZero(vector))
                throw new InvalidRequestException("Zero and near-zero vectors cannot be indexed or queried with cosine similarity");
        }
    }

    public static boolean isEffectivelyZero(VectorFloat<?> vector)
    {
        for (int i = 0; i < vector.length(); i++)
        {
            if (vector.get(i) < -1E-6 || vector.get(i) > 1E-6)
                return false;
        }
        return true;
    }
}
