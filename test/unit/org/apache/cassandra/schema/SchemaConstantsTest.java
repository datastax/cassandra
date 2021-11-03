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

package org.apache.cassandra.schema;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.exceptions.ConfigurationException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SchemaConstantsTest
{
    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
    }

    @Test
    public void testIsKeyspaceWithLocalStrategy()
    {
        assertTrue(SchemaConstants.isKeyspaceWithLocalStrategy("system"));
        assertTrue(SchemaConstants.isKeyspaceWithLocalStrategy(SchemaManager.instance.getKeyspaceMetadata("system")));
        assertFalse(SchemaConstants.isKeyspaceWithLocalStrategy("non_existing"));

        SchemaLoader.createKeyspace("local_ks", KeyspaceParams.local());
        SchemaLoader.createKeyspace("simple_ks", KeyspaceParams.simple(3));

        assertTrue(SchemaConstants.isKeyspaceWithLocalStrategy("local_ks"));
        assertTrue(SchemaConstants.isKeyspaceWithLocalStrategy(SchemaManager.instance.getKeyspaceMetadata("local_ks")));
        assertFalse(SchemaConstants.isKeyspaceWithLocalStrategy("simple_ks"));
    }
}
