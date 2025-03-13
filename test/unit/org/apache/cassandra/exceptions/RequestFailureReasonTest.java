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

package org.apache.cassandra.exceptions;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class RequestFailureReasonTest
{
    private static final RequestFailureReason[] REASONS = RequestFailureReason.values();
    private static final Object[][] EXPECTED_VALUES =
    {
    { 0, "UNKNOWN" },
    { 1, "READ_TOO_MANY_TOMBSTONES" },
    { 2, "TIMEOUT" },
    { 3, "INCOMPATIBLE_SCHEMA" },
    { 6, "INDEX_NOT_AVAILABLE" },
    { 500, "UNKNOWN_COLUMN" },
    { 501, "UNKNOWN_TABLE" },
    { 502, "REMOTE_STORAGE_FAILURE" },
    { 503, "INDEX_BUILD_IN_PROGRESS" }
    };
    @Test
    public void testEnumCodesAndNames()
    {
        for (int i = 0; i < REASONS.length; i++)
        {
            assertEquals("RequestFailureReason code mismatch for " +
                         REASONS[i].name(), EXPECTED_VALUES[i][0], REASONS[i].code);
            assertEquals("RequestFailureReason name mismatch for code " +
                         REASONS[i].code, EXPECTED_VALUES[i][1], REASONS[i].name());
        }
        assertEquals("Number of RequestFailureReason enum constants has changed. Update the test.",
                     EXPECTED_VALUES.length, REASONS.length);
    }

    @Test
    public void testFromCode()
    {
        // Test valid codes
        assertEquals(RequestFailureReason.UNKNOWN, RequestFailureReason.fromCode(0));
        assertEquals(RequestFailureReason.READ_TOO_MANY_TOMBSTONES, RequestFailureReason.fromCode(1));
        assertEquals(RequestFailureReason.TIMEOUT, RequestFailureReason.fromCode(2));
        assertEquals(RequestFailureReason.INCOMPATIBLE_SCHEMA, RequestFailureReason.fromCode(3));
        assertEquals(RequestFailureReason.INDEX_NOT_AVAILABLE, RequestFailureReason.fromCode(6));
        assertEquals(RequestFailureReason.UNKNOWN_COLUMN, RequestFailureReason.fromCode(500));
        assertEquals(RequestFailureReason.UNKNOWN_TABLE, RequestFailureReason.fromCode(501));
        assertEquals(RequestFailureReason.REMOTE_STORAGE_FAILURE, RequestFailureReason.fromCode(502));
        assertEquals(RequestFailureReason.INDEX_BUILD_IN_PROGRESS, RequestFailureReason.fromCode(503));

        // Test invalid codes
        assertEquals(RequestFailureReason.UNKNOWN, RequestFailureReason.fromCode(200));
        assertEquals(RequestFailureReason.UNKNOWN, RequestFailureReason.fromCode(999));
        assertThrows(IllegalArgumentException.class, () -> RequestFailureReason.fromCode(-1));
    }
}
