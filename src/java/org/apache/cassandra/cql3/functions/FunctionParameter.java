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

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.selection.Selectable;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static java.lang.String.format;
import static org.apache.cassandra.cql3.AssignmentTestable.TestResult.NOT_ASSIGNABLE;

/**
 * Generic, loose definition of a function parameter, able to infer the specific data type of the parameter in the
 * function specifically built by a {@link FunctionFactory} for a particular function call.
 */
public interface FunctionParameter
{
    /**
     * Tries to infer the data type of the parameter for an argument in a call to the function.
     *
     * @param keyspace the current keyspace
     * @param arg a parameter value in a specific function call
     * @param receiverType the type of the object that will receive the result of the function call
     * @param inferredTypes the types that have been inferred for the other parameters
     * @return the inferred data type of the parameter, or {@link null} it isn't possible to infer it
     */
    @Nullable
    default AbstractType<?> inferType(String keyspace,
                                      AssignmentTestable arg,
                                      @Nullable AbstractType<?> receiverType,
                                      @Nullable List<AbstractType<?>> inferredTypes)
    {
        return arg.getCompatibleTypeIfKnown(keyspace);
    }

    void validateType(FunctionFactory factory, AssignmentTestable arg, AbstractType<?> argType);

    /**
     * @return a function parameter definition that accepts values of string-based data types (text, varchar and ascii)
     */
    static FunctionParameter string()
    {
        return fixed("string", CQL3Type.Native.TEXT, CQL3Type.Native.VARCHAR, CQL3Type.Native.ASCII);
    }

    /**
     * @return a function parameter definition that accepts values that can be interpreted as floats
     */
    static FunctionParameter float32()
    {
        return fixed("float", CQL3Type.Native.FLOAT, CQL3Type.Native.DOUBLE, CQL3Type.Native.INT, CQL3Type.Native.BIGINT);
    }

    /**
     * @param type the accepted data type
     * @return a function parameter definition that accepts values of a specific data type
     */
    static FunctionParameter fixed(CQL3Type type)
    {
        return fixed(type.toString(), type);
    }

    /**
     * @param name the name of the data type
     * @param types the accepted data types
     * @return a function parameter definition that accepts values of the specified data types
     */
    static FunctionParameter fixed(String name, CQL3Type... types)
    {
        assert types.length > 0;

        return new FunctionParameter()
        {
            @Override
            public AbstractType<?> inferType(String keyspace,
                                             AssignmentTestable arg,
                                             @Nullable AbstractType<?> receiverType,
                                             @Nullable List<AbstractType<?>> inferredTypes)
            {
                AbstractType<?> inferred = arg.getCompatibleTypeIfKnown(keyspace);
                return inferred != null ? inferred : types[0].getType();
            }

            @Override
            public void validateType(FunctionFactory factory, AssignmentTestable arg, AbstractType<?> argType)
            {
                if (Arrays.stream(types).allMatch(t -> argType.testAssignment(t.getType()) == NOT_ASSIGNABLE))
                    throw new InvalidRequestException(format("Function %s requires an argument of type %s, " +
                                                             "but found argument %s of type %s",
                                                             factory, this, arg, argType.asCQL3Type()));
            }

            @Override
            public String toString()
            {
                return name;
            }
        };
    }

    /**
     * @param inferFromReceiver whether the parameter should try to use the function receiver to infer its data type
     * @return a function parameter definition that accepts columns of any data type
     */
    static FunctionParameter anyType(boolean inferFromReceiver)
    {
        return new FunctionParameter()
        {
            @Override
            public AbstractType<?> inferType(String keyspace,
                                             AssignmentTestable arg,
                                             @Nullable AbstractType<?> receiverType,
                                             @Nullable List<AbstractType<?>> inferredTypes)
            {
                AbstractType<?> type = arg.getCompatibleTypeIfKnown(keyspace);
                return type == null && inferFromReceiver ? receiverType : type;
            }

            @Override
            public void validateType(FunctionFactory factory, AssignmentTestable arg, AbstractType<?> argType)
            {
                // nothing to do here, all types are accepted
            }

            @Override
            public String toString()
            {
                return "any";
            }
        };
    }

    /**
     * @return a function parameter definition that accepts values of any type, provided that it's the same type as all
     * the other parameters
     */
    static FunctionParameter sameAs(int index, FunctionParameter parameter)
    {
        return new FunctionParameter()
        {
            @Override
            public AbstractType<?> inferType(String keyspace,
                                             AssignmentTestable arg,
                                             @Nullable AbstractType<?> receiverType,
                                             @Nullable List<AbstractType<?>> inferredTypes)
            {
                AbstractType<?> type = inferredTypes == null ? null : inferredTypes.get(index);
                return type != null ? type : parameter.inferType(keyspace, arg, receiverType, inferredTypes);
            }

            @Override
            public void validateType(FunctionFactory factory, AssignmentTestable arg, AbstractType<?> argType)
            {
                parameter.validateType(factory, arg, argType);
            }

            @Override
            public String toString()
            {
                return parameter.toString();
            }
        };
    }

    /**
     * @return a function parameter definition that accepts values of type {@link VectorType}.
     */
    static FunctionParameter vector(CQL3Type type)
    {
        return new FunctionParameter()
        {
            @Override
            public AbstractType<?> inferType(String keyspace,
                                             AssignmentTestable arg,
                                             @Nullable AbstractType<?> receiverType,
                                             List<AbstractType<?>> inferredTypes)
            {
                AbstractType<?> inferred = arg.getCompatibleTypeIfKnown(keyspace);
                if (inferred != null && arg instanceof Selectable.WithList)
                {
                    return VectorType.getInstance(type.getType(), ((Selectable.WithList) arg).getSize());
                }

                return inferred == null ? receiverType : inferred;
            }

            @Override
            public void validateType(FunctionFactory factory, AssignmentTestable arg, AbstractType<?> argType)
            {
                if (argType.isVector())
                {
                    VectorType<?> vectorType = (VectorType<?>) argType;
                    if (vectorType.elementType.asCQL3Type() == type)
                        return;
                }
                else if (argType.isList()) // if it's terminal it will be a list
                {
                    ListType<?> listType = (ListType<?>) argType;
                    if (listType.getElementsType().testAssignment(type.getType()) == NOT_ASSIGNABLE)
                        return;
                }

                throw new InvalidRequestException(format("Function %s requires a %s vector argument, " +
                                                         "but found argument %s of type %s",
                                                         factory, type, arg, argType.asCQL3Type()));
            }

            @Override
            public String toString()
            {
                return format("vector<%s, n>", type);
            }
        };
    }

    /**
     * @param name the name of the function parameter
     * @param type the accepted type of literal
     * @param inferredType the inferred type of the literal
     * @return a function parameter definition that accepts a specific literal type
     */
    static FunctionParameter literal(String name, Constants.Type type, AbstractType<?> inferredType)
    {
        return new FunctionParameter()
        {
            @Override
            public AbstractType<?> inferType(String keyspace,
                                             AssignmentTestable arg,
                                             @Nullable AbstractType<?> receiverType,
                                             @Nullable List<AbstractType<?>> inferredTypes)
            {
                return inferredType;
            }

            @Override
            public void validateType(FunctionFactory factory, AssignmentTestable arg, AbstractType<?> argType)
            {
                if (arg instanceof Selectable.WithTerm)
                    arg = ((Selectable.WithTerm) arg).rawTerm;

                if (!(arg instanceof Constants.Literal))
                    throw invalidArgumentException(factory, arg);

                Constants.Literal literal = (Constants.Literal) arg;
                if (literal.type != type)
                    throw invalidArgumentException(factory, arg);
            }

            private InvalidRequestException invalidArgumentException(FunctionFactory factory, AssignmentTestable arg)
            {
                throw new InvalidRequestException(format("Function %s requires a %s argument, but found %s",
                                                         factory, this, arg));
            }

            @Override
            public String toString()
            {
                return name;
            }
        };
    }

    static FunctionParameter literalInteger()
    {
        return literal("literal_int", Constants.Type.INTEGER, Int32Type.instance);
    }
}
