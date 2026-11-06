/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.distributed.shared;

import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;

import static org.apache.cassandra.config.CassandraRelevantProperties.UCS_SHARED_STORAGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * {@link WithProperties#clear} puts back on close what it cleared: the previous value of a property that was set, and
 * no value at all for one that was absent (not the property's default).
 */
public class WithPropertiesTest
{
    /** A property with a default ({@code false}), not set in the unit-test JVM configuration. */
    private static final CassandraRelevantProperties PROPERTY = UCS_SHARED_STORAGE;

    @Test
    public void clearLeavesAnAbsentPropertyAbsent()
    {
        assertFalse(PROPERTY.getKey() + " is set in this JVM", PROPERTY.isPresent());
        try (WithProperties ignore = new WithProperties().clear(PROPERTY))
        {
            assertFalse(PROPERTY.isPresent());
        }
        assertFalse(PROPERTY.getKey() + " was set to " + PROPERTY.getString() + " on close", PROPERTY.isPresent());
    }

    @Test
    public void clearRestoresTheValueOfASetProperty()
    {
        assertFalse(PROPERTY.getKey() + " is set in this JVM", PROPERTY.isPresent());
        try (WithProperties ignore1 = new WithProperties().set(PROPERTY, "true"))
        {
            try (WithProperties ignore2 = new WithProperties().clear(PROPERTY))
            {
                assertFalse(PROPERTY.isPresent());
            }
            assertTrue(PROPERTY.isPresent());
            assertEquals("true", PROPERTY.getString());
        }
        assertFalse(PROPERTY.isPresent());
    }

    @Test
    public void clearRestoresAPropertySetToItsDefault()
    {
        assertFalse(PROPERTY.getKey() + " is set in this JVM", PROPERTY.isPresent());
        try (WithProperties ignore1 = new WithProperties().set(PROPERTY, "false"))
        {
            try (WithProperties ignore2 = new WithProperties().clear(PROPERTY))
            {
                assertFalse(PROPERTY.isPresent());
            }
            assertTrue(PROPERTY.getKey() + " set to its default was left absent on close", PROPERTY.isPresent());
            assertEquals("false", PROPERTY.getString());
        }
        assertFalse(PROPERTY.isPresent());
    }
}
