/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
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

package org.apache.cassandra.db.guardrails;


import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;


public class GuardrailCounterTest extends GuardrailTester
{
    private static boolean counterEnabled;

    @BeforeClass
    public static void setup()
    {
        counterEnabled = DatabaseDescriptor.getGuardrailsConfig().getCounterEnabled();
    }

    @AfterClass
    public static void tearDown()
    {
        DatabaseDescriptor.getGuardrailsConfig().setCounterEnabled(counterEnabled);
    }

    private void setGuardrails(boolean counterEnabled)
    {
        DatabaseDescriptor.getGuardrailsConfig().setCounterEnabled(counterEnabled);
    }

    @Test
    public void testCounterEnabled() throws Throwable
    {
        setGuardrails(true);
        assertValid(String.format("CREATE TABLE %s (pk int PRIMARY KEY, c counter)", createTableName()));
        execute("UPDATE %s SET c = c + 1 WHERE pk = 10");
        assertRows(execute("SELECT c FROM %s WHERE pk = 10"), row(1L));
    }

    @Test
    public void testCounterDisabled() throws Throwable
    {
        setGuardrails(false);
        assertFails(String.format("CREATE TABLE %s (pk int PRIMARY KEY, c counter)", createTableName()),
                "Guardrail counter violated: Counter is not allowed");
    }
}