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

import io.github.jbellis.jvector.vector.VectorUtil;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;

import static io.github.jbellis.jvector.vector.VectorUtil.addInPlace;
import static io.github.jbellis.jvector.vector.VectorUtil.sub;
import static io.github.jbellis.jvector.vector.VectorUtil.subInPlace;

/**
 * Computes the sum of a series of floating-point vectors using the
 * <a href="https://en.wikipedia.org/wiki/Kahan_summation_algorithm">Kahan summation algorithm</a>
 * to minimize the numerical error introduced when adding sequences of floating-point numbers.
 */
public class KahanSum {
    private static final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    private long count;
    private VectorFloat<?> sum;  // Use a vector to store the running sum
    private VectorFloat<?> c;    // A running compensation vector for lost low-order bits

    /**
     * Initializes the KahanSum with the dimension of vectors it will be processing.
     */
    public KahanSum(int dimension) {
        this.sum = vts.createFloatVector(dimension);
        this.c = vts.createFloatVector(dimension);
    }

    /**
     * Accepts a vector of floats and adds it to the running total using the Kahan summation algorithm.
     */
    public void add(VectorFloat<?> value) {
        // Add the total compensation value that hasn't yet been reflected in `sum` to `y`
        // (Compensation is negated because of how it is computed)
        var y = sub(value, c);

        // Add the adjusted value to the sum
        var t = sum.copy();
        addInPlace(t, y);

        // extract the compensation = everything that got lost in the addition b/c of fp precision
        // c = (t - sum) - y
        c = sub(t, sum);
        subInPlace(c, y);
        // next time around, we'll add the compensation `c` to the new value until it gets large enough to affect `sum`

        sum = t;
        count++;
    }

    /**
     * @return the sum of the series of vectors
     */
    public VectorFloat<?> getSum() {
        return sum;
    }

    /**
     * @return the mean of the series of vectors
     */
    public VectorFloat<?> getMean()
    {
        var mean = sum.copy();
        VectorUtil.scale(mean, 1.0f / count);
        return mean;
    }
}


