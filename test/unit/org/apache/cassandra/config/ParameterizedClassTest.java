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

package org.apache.cassandra.config;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

import org.apache.cassandra.auth.AllowAllAuthorizer;
import org.apache.cassandra.auth.IAuthorizer;
import org.apache.cassandra.exceptions.ConfigurationException;

public class ParameterizedClassTest
{
    @Test
    public void newInstance_NonExistentClass_FailsWithConfigurationException()
    {
        ParameterizedClass nonExistentClass = new ParameterizedClass("NonExistentClass");

        ConfigurationException exception = assertThrows(ConfigurationException.class, () -> {
            ParameterizedClass.newInstance(nonExistentClass, List.of("org.apache.cassandra.config"));
        });

        String expectedError = "Unable to find class NonExistentClass in packages [\"org.apache.cassandra.config\"]";
        assertEquals(expectedError, exception.getMessage());
    }

    @Test
    public void newInstance_WithSingleEmptyConstructor_UsesEmptyConstructor()
    {
        ParameterizedClass parameterizedClass = new ParameterizedClass(AllowAllAuthorizer.class.getName());
        IAuthorizer instance = ParameterizedClass.newInstance(parameterizedClass, null);
        assertNotNull(instance);
    }

    @Test
    public void newInstance_SingleEmptyConstructorWithParameters_FailsWithConfigurationException()
    {
        Map<String, String> parameters = Map.of("key", "value");
        ParameterizedClass parameterizedClass = new ParameterizedClass(AllowAllAuthorizer.class.getName(), parameters);

        ConfigurationException exception = assertThrows(ConfigurationException.class, () -> {
            ParameterizedClass.newInstance(parameterizedClass, null);
        });

        assertThat(exception.getMessage(), startsWith("No valid constructor found for class"));
    }

    @Test
    public void newInstance_WithValidConstructors_FavorsMapConstructor()
    {
        ParameterizedClass parameterizedClass = new ParameterizedClass(ParameterizedClassExample.class.getName());
        ParameterizedClassExample instance = ParameterizedClass.newInstance(parameterizedClass, null);

        assertTrue(instance.calledMapConstructor);
    }

    @Test
    public void newInstance_WithConstructorException_PreservesOriginalFailure()
    {
        Map <String, String> parameters = Map.of("fail", "true");
        ParameterizedClass parameterizedClass = new ParameterizedClass(ParameterizedClassExample.class.getName(), parameters);

        ConfigurationException exception = assertThrows(ConfigurationException.class, () -> {
            ParameterizedClass.newInstance(parameterizedClass, null);
        });

        assertThat(exception.getMessage(), startsWith("Failed to instantiate class"));
        assertThat(exception.getMessage(), containsString("Simulated failure"));
    }
}
