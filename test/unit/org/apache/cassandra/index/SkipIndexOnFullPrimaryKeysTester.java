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

package org.apache.cassandra.index;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.net.MessagingService;
import org.assertj.core.api.Assertions;

/**
 * Verifies that queries specifying one or more full primary keys don't use indexes, unless index hints include them.
 * </p>
 * Queries with index-based ORDER BY will still use the index, since we don't have a way to run those without the index.
 * Queries in which the indexes change the semantics of the expression, such as those with analyzers,
 * will preserve the index semantics, even if the index itself is not read.
 */
@RunWith(Parameterized.class)
public abstract class SkipIndexOnFullPrimaryKeysTester extends SAITester
{
    @Parameterized.Parameter
    public boolean skip;

    @Parameterized.Parameters(name = "skip={0}")
    public static Collection<Object[]> data()
    {
        return Arrays.asList(new Object[] { true}, new Object[] { false});
    }

    @Before
    public void setSkip()
    {
        CassandraRelevantProperties.SKIP_INDEXES_ON_FULL_PRIMARY_KEYS.setBoolean(skip);
    }

    @BeforeClass
    public static void setUpClass()
    {
        // Set the messaging version that adds support for the new index hints before starting the server
        CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.setInt(MessagingService.VERSION_DS_12);
        CQLTester.setUpClass();
        CQLTester.enableCoordinatorExecution();
    }

    protected void assertHasQueryPlan(String query, Object[]... expectedRows)
    {
        ReadCommand command = parseReadCommand(formatQuery(query));
        Index.QueryPlan plan = command.indexQueryPlan();
        Assertions.assertThat(plan).isNotNull();
        assertRows(execute(query), expectedRows);
    }

    protected void assertHasNoQueryPlan(String query, Object[]... expectedRows)
    {
        ReadCommand command = parseReadCommand(formatQuery(query));
        Index.QueryPlan plan = command.indexQueryPlan();
        if (skip)
            Assertions.assertThat(plan).isNull();
        else
            Assertions.assertThat(plan).isNotNull();
        assertRows(execute(query), expectedRows);
    }

    protected void assertUnsupportedINOnPK(String query)
    {
        assertInvalidThrowMessage(StatementRestrictions.INDEX_WITH_IN_ON_PK_MESSAGE, InvalidRequestException.class, query);
    }
}
