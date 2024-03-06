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
package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorUtil;
import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.NumberType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;

import static java.lang.String.format;

public abstract class VectorFcts
{
    public static void addFunctionsTo(NativeFunctions functions)
    {
        functions.add(similarity_function("similarity_cosine", VectorSimilarityFunction.COSINE));
        functions.add(similarity_function("similarity_euclidean", VectorSimilarityFunction.EUCLIDEAN));
        functions.add(similarity_function("similarity_dot_product", VectorSimilarityFunction.DOT_PRODUCT));
        functions.add(new RandomFloatVectorFunctionFactory());
        functions.add(new NormalizeL2FunctionFactory());
    }

    private static FunctionFactory similarity_function(String name, VectorSimilarityFunction f)
    {
        return new FunctionFactory(name,
                                   FunctionParameter.sameAs(1, FunctionParameter.vector(CQL3Type.Native.FLOAT)),
                                   FunctionParameter.sameAs(0, FunctionParameter.vector(CQL3Type.Native.FLOAT)))
        {
            @Override
            @SuppressWarnings("unchecked")
            protected NativeFunction doGetOrCreateFunction(List<? extends AssignmentTestable> args,
                                                           List<AbstractType<?>> argTypes,
                                                           AbstractType<?> receiverType)
            {
                // check that all arguments have the same vector dimensions
                VectorType<Float> firstArgType = (VectorType<Float>) argTypes.get(0);
                int dimensions = firstArgType.dimension;
                if (!argTypes.stream().allMatch(t -> ((VectorType<?>) t).dimension == dimensions))
                    throw new InvalidRequestException("All arguments must have the same vector dimensions");
                return createSimilarityFunction(name.name, firstArgType, f);
            }
        };
    }

    private static NativeFunction createSimilarityFunction(String name, VectorType<Float> type, VectorSimilarityFunction f)
    {
        return new NativeScalarFunction(name, FloatType.instance, type, type)
        {
            @Override
            public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> parameters) throws InvalidRequestException
            {
                var v1 = type.getSerializer().deserializeFloatArray(parameters.get(0));
                var v2 = type.getSerializer().deserializeFloatArray(parameters.get(1));
                return FloatType.instance.decompose(f.compare(v1, v2));
            }
        };
    }

    /**
     * CQL native function create a random float vector of a certain dimension.
     * All the components of the vector will be random floats between the specified min and max values.
     */
    private static class RandomFloatVectorFunctionFactory extends FunctionFactory
    {
        private static final String NAME = "random_float_vector";

        private RandomFloatVectorFunctionFactory()
        {
            super(NAME, FunctionParameter.literalInteger(), FunctionParameter.float32(), FunctionParameter.float32());
        }

        @Override
        protected NativeFunction doGetOrCreateFunction(List<? extends AssignmentTestable> args,
                                                       List<AbstractType<?>> argTypes,
                                                       AbstractType<?> receiverType)
        {
            // Get the vector type from the dimension argument. We need to do this here assuming that the argument is a
            // literal, so we know the dimension of the return type before actually executing the function.
            int dimension = Integer.parseInt(args.get(0).toString());
            VectorType<Float> type = VectorType.getInstance(FloatType.instance, dimension);

            final NumberType<?> minType = (NumberType<?>) argTypes.get(1);
            final NumberType<?> maxType = (NumberType<?>) argTypes.get(2);

            return new NativeScalarFunction(name.name, type, Int32Type.instance, minType, maxType)
            {
                private final Random random = new Random();

                @Override
                public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> args)
                {
                    // get the min argument
                    ByteBuffer arg1 = args.get(1);
                    if (arg1 == null || !arg1.hasRemaining())
                        throw new InvalidRequestException(format("Min argument of function %s must not be null",
                                                                 RandomFloatVectorFunctionFactory.this));
                    float min = minType.compose(arg1).floatValue();

                    // get the max argument
                    ByteBuffer arg2 = args.get(2);
                    if (arg2 == null || !arg2.hasRemaining())
                        throw new InvalidRequestException(format("Max argument of function %s must not be null",
                                                                 RandomFloatVectorFunctionFactory.this));
                    float max = maxType.compose(arg2).floatValue();
                    if (max <= min)
                        throw new InvalidRequestException("Max value must be greater than min value");

                    // generate the random vector within the range defined by min and max
                    float[] vector = new float[dimension];
                    for (int i = 0; i < dimension; i++)
                    {
                        vector[i] = min + random.nextFloat() * (max - min);
                    }

                    return type.getSerializer().serializeFloatArray(vector);
                }
            };
        }
    }

    /**
     * CQL native function to normalize a vector using L2 normalization.
     */
    private static class NormalizeL2FunctionFactory extends FunctionFactory
    {
        private static final String NAME = "normalize_l2";

        public NormalizeL2FunctionFactory()
        {
            super(NAME, FunctionParameter.vector(CQL3Type.Native.FLOAT));
        }

        @Override
        @SuppressWarnings("unchecked")
        protected NativeFunction doGetOrCreateFunction(List<? extends AssignmentTestable> args,
                                                       List<AbstractType<?>> argTypes,
                                                       AbstractType<?> receiverType)
        {
            VectorType<Float> vectorType = (VectorType<Float>) argTypes.get(0);

            return new NativeScalarFunction(name.name, vectorType, vectorType)
            {
                @Override
                public ByteBuffer execute(ProtocolVersion protocolVersion, List<ByteBuffer> args)
                {
                    // get the vector argument
                    ByteBuffer arg0 = args.get(0);
                    if (arg0 == null || !arg0.hasRemaining())
                        return null;
                    float[] vector = vectorType.getSerializer().deserializeFloatArray(arg0);

                    // normalize
                    float[] normalized = vector.clone();
                    VectorUtil.l2normalize(normalized);

                    // serialize the normalized vector
                    return vectorType.getSerializer().serializeFloatArray(normalized);
                }
            };
        }
    }
}
