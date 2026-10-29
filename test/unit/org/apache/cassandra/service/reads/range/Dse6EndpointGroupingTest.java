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

package org.apache.cassandra.service.reads.range;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ReadCommand;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * A DSE 6 replica has no multi-range read verb: in DSE 6 compatibility mode a range read is never grouped
 * by endpoint into multi-range reads, whichever replicas would be chosen; with the mode off an SAI query is, as before.
 */
public class Dse6EndpointGroupingTest extends CQLTester
{
    private InternodeCompatibilityMode modeBefore;

    @Before
    public void saveMode()
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
    }

    @After
    public void restoreMode()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
    }

    @Test
    public void noEndpointGroupingInDse6Mode()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        ReadCommand command = parseReadCommand("SELECT * FROM %s WHERE v = 1");
        assertNotNull(command.indexQueryPlan());

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        assertTrue(RangeCommandIterator.supportsEndpointGrouping(command));

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        assertFalse(RangeCommandIterator.supportsEndpointGrouping(command));
    }
}
