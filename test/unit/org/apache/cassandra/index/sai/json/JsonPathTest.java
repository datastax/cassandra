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

package org.apache.cassandra.index.sai.json;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class JsonPathTest
{
    /**
     * Verifies that various supported JSON path syntax forms are correctly parsed and normalized to canonical strings.
     */
    @Test
    public void normalizes()
    {
        assertEquals("$.profile.age", JsonPath.parse("$.profile.age").toString());
        assertEquals("$.profile.age", JsonPath.parse("$['profile']['age']").toString());
        assertEquals("$['first name'].x", JsonPath.parse("$['first name'].x").toString());
        assertEquals("$.items[2].sku", JsonPath.parse("$.items[2].sku").toString());
        assertEquals("$['it\\'s']", JsonPath.parse("$['it\\'s']").toString());
        assertEquals("$", JsonPath.parse("$").toString());
    }

    /**
     * Verifies that paths equivalent in meaning produce equal JsonPath instances and identical hash codes.
     */
    @Test
    public void equalityIsOnNormalizedForm()
    {
        assertEquals(JsonPath.parse("$.a.b"), JsonPath.parse("$['a'].b"));
        assertEquals(JsonPath.parse("$.a.b").hashCode(), JsonPath.parse("$['a'].b").hashCode());
    }

    /**
     * Verifies that path strings outside the supported subset or malformed paths are rejected with IllegalArgumentException.
     */
    @Test
    public void rejectsOutsideSubset()
    {
        for (String bad : new String[]{ "", "a.b", "$.", "$..a", "$[*]", "$.a[?(@.x)]", "$[-1]", "$['unterminated", "$.a b" })
            assertThrows(bad, IllegalArgumentException.class, () -> JsonPath.parse(bad));
    }
}
