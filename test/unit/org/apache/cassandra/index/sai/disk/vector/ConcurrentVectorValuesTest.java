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

import org.junit.Test;

import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;


public class ConcurrentVectorValuesTest
{
    private final VectorTypeSupport vts = VectorizationProvider.getInstance().getVectorTypeSupport();

    @Test(expected = IllegalStateException.class)
    public void testConcurrentVectorValuesFailsForAlreadySetOrdinal()
    {
        var vectorValues = new ConcurrentVectorValues(1);
        vectorValues.add(0, vts.createFloatVector(new float[]{ 1.0f }));
        vectorValues.add(0, vts.createFloatVector(new float[]{ 2.0f }));
    }

    @Test()
    public void testGet()
    {
        var vectorValues = new ConcurrentVectorValues(1);
        vectorValues.add(1, vts.createFloatVector(new float[]{ 1.0f }));
        vectorValues.add(2, vts.createFloatVector(new float[]{ 2.0f }));

        assertNull(vectorValues.getVector(0));
        assertArrayEquals(new float[]{ 1.0f }, (float[]) vectorValues.getVector(1).get(), 0f);
    }
}
