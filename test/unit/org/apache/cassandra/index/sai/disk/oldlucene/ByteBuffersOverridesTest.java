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

package org.apache.cassandra.index.sai.disk.oldlucene;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

/**
 * Tests for classes that implement big-endianness by overriding superclass methods. This is necessary to ensure that
 * new methods don't get introduced reintroducing the endianness of the superclass.
 */
public class ByteBuffersOverridesTest
{
    @Test
    public void testLegacyByteBuffersDataInputOverrides()
    {
        // manually checked methods that endianness doesn't impact
        Set<String> ignoredMethods = Set.of("readSetOfStrings", "readVInt", "readZInt", "readVLong", "readZLong",
                                            "readString", "readBytes", "clone", "readMapOfStrings");
        checkOverrides(LegacyByteBuffersDataInput.class, DataInput.class, ignoredMethods);
    }

    @Test
    public void testLegacyByteBuffersDataOutputOverrides()
    {
        // manually checked methods that endianness doesn't impact
        Set<String> ignoredMethods = Set.of("writeZLong", "writeVLong", "writeVInt", "copyBytes", "writeZInt");
        checkOverrides(LegacyByteBuffersDataOutput.class, DataOutput.class, ignoredMethods);
    }

    @Test
    public void testLegacyByteBuffersDataOutputAdapterOverrides()
    {
        Set<String> ignoredMethods = Set.of("writeZLong", "writeVLong", "writeVInt", "copyBytes", "writeZInt");
        checkOverrides(LegacyByteBuffersDataOutputAdapter.class, DataOutput.class, ignoredMethods);
    }

    @Test
    public void testModernByteBuffersDataOutputAdapterOverrides()
    {
        Set<String> ignoredMethods = Set.of("writeZLong", "writeVLong", "writeVInt", "copyBytes", "writeZInt");
        checkOverrides(ModernByteBuffersDataOutputAdapter.class, DataOutput.class, ignoredMethods);
    }

    /**
     * Check if all methods declared by the parent class are overridden in the subclass.
     *
     * @param subclass    the subclass to be tested
     * @param parentClass the parent class from which methods are inherited
     */
    private void checkOverrides(Class<?> subclass, Class<?> parentClass, Set<String> ignoredMethods)
    {
        Set<Method> parentMethods = new HashSet<>(Arrays.asList(parentClass.getDeclaredMethods()));
        Set<Method> declaredMethodsInSubclass = new HashSet<>(Arrays.asList(subclass.getDeclaredMethods()));

        for (Method parentMethod : parentMethods)
        {
            if (ignoredMethods.contains(parentMethod.getName()))
                continue;

            if (Modifier.isPublic(parentMethod.getModifiers()) || Modifier.isProtected(parentMethod.getModifiers()))
            {
                boolean isOverridden = declaredMethodsInSubclass.stream().anyMatch(m ->
                                                                                   m.getName().equals(parentMethod.getName()) &&
                                                                                   Arrays.equals(m.getParameterTypes(), parentMethod.getParameterTypes()));
                Assert.assertTrue("Method " + parentMethod.getName() + " is not overridden in " + subclass.getName(), isOverridden);
            }
        }
    }
}
