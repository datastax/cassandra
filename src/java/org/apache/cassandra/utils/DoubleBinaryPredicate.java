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

package org.apache.cassandra.utils;

/**
 * Represents a predicate (boolean-valued function) of two {@code double} arguments.
 * This is the two-arity, primitive-specialised analogue of {@link java.util.function.BiPredicate}.
 *
 * <p>This is a functional interface whose functional method is {@link #test(double, double)}.
 */
@FunctionalInterface
public interface DoubleBinaryPredicate
{
    /**
     * Evaluates this predicate on the given arguments.
     *
     * @param left  the first input argument
     * @param right the second input argument
     * @return {@code true} if the input arguments match the predicate, otherwise {@code false}
     */
    boolean test(double left, double right);
}
