/*
 * Copyright DataStax, Inc.
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

import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sasi.SASIIndex;

import static org.apache.cassandra.cql3.restrictions.StatementRestrictions.INDEX_DOES_NOT_SUPPORT_DISJUNCTION;
import static org.apache.cassandra.cql3.restrictions.StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE;

/**
 * Tests common functionality across all included index implementations.
 */
@RunWith(Parameterized.class)
public class AllIndexImplementationsTest extends CQLTester
{
    @Parameterized.Parameter
    public String alias;

    @Parameterized.Parameter(1)
    public Class<Index> indexClass;

    @Parameterized.Parameter(2)
    public String createIndexQuery;

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> parameters()
    {
        List<Object[]> parameters = new LinkedList<>();
        parameters.add(new Object[]{ "none", null, null });
        parameters.add(new Object[]{ "legacy", CassandraIndex.class, "CREATE INDEX ON %%s(%s)" });
        parameters.add(new Object[]{ "SASI", SASIIndex.class, "CREATE CUSTOM INDEX ON %%s(%s) USING 'org.apache.cassandra.index.sasi.SASIIndex'" });
        parameters.add(new Object[]{ "SAI", StorageAttachedIndex.class, "CREATE CUSTOM INDEX ON %%s(%s) USING 'StorageAttachedIndex'" });
        return parameters;
    }

    @Test
    public void testDisjunction()
    {
        createTable("CREATE TABLE %s (pk int, a int, b int, PRIMARY KEY(pk))");

        boolean indexSupportsDisjuntion = StorageAttachedIndex.class.equals(indexClass);
        boolean hasIndex = createIndexQuery != null;
        if (hasIndex)
            createIndex(String.format(createIndexQuery, 'a'));

        execute("INSERT INTO %s (pk, a, b) VALUES (?, ?, ?)", 1, 1, 1);
        execute("INSERT INTO %s (pk, a, b) VALUES (?, ?, ?)", 2, 2, 2);

        // query with disjunctions on both columns when only one of them is indexed
        assertDisjunction("a = 1 OR a = 2",
                          !indexSupportsDisjuntion,
                          hasIndex && indexSupportsDisjuntion,
                          hasIndex ? INDEX_DOES_NOT_SUPPORT_DISJUNCTION : REQUIRES_ALLOW_FILTERING_MESSAGE,
                          row(1), row(2));
        assertDisjunction("a = 1 OR b = 2", true, false, row(1), row(2));
        assertDisjunction("a = 1 AND (a = 1 OR b = 1)", true, hasIndex, row(1));
        assertDisjunction("a = 1 AND (a = 1 OR b = 2)", true, hasIndex, row(1));
        assertDisjunction("a = 1 AND (a = 2 OR b = 1)", true, hasIndex, row(1));
        assertDisjunction("a = 1 AND (a = 2 OR b = 2)", true, hasIndex);
        assertDisjunction("a = 2 AND (a = 1 OR b = 1)", true, hasIndex);
        assertDisjunction("a = 2 AND (a = 1 OR b = 2)", true, hasIndex, row(2));
        assertDisjunction("a = 2 AND (a = 2 OR b = 1)", true, hasIndex, row(2));
        assertDisjunction("a = 2 AND (a = 2 OR b = 2)", true, hasIndex, row(2));
        assertDisjunction("a = 1 OR (a = 1 AND b = 1)", true, indexSupportsDisjuntion, row(1));
        assertDisjunction("a = 1 OR (a = 1 AND b = 2)", true, indexSupportsDisjuntion, row(1));
        assertDisjunction("a = 1 OR (a = 2 AND b = 1)", true, indexSupportsDisjuntion, row(1));
        assertDisjunction("a = 1 OR (a = 2 AND b = 2)", true, indexSupportsDisjuntion, row(1), row(2));
        assertDisjunction("a = 2 OR (a = 1 AND b = 1)", true, indexSupportsDisjuntion, row(1), row(2));
        assertDisjunction("a = 2 OR (a = 1 AND b = 2)", true, indexSupportsDisjuntion, row(2));
        assertDisjunction("a = 2 OR (a = 2 AND b = 1)", true, indexSupportsDisjuntion, row(2));
        assertDisjunction("a = 2 OR (a = 2 AND b = 2)", true, indexSupportsDisjuntion, row(2));

        // create a second index in the remaining column, so all columns are indexed
        if (hasIndex)
            createIndex(String.format(createIndexQuery, 'b'));

        // test with all columns indexed
        assertDisjunction("a = 1 OR a = 2",
                          !indexSupportsDisjuntion,
                          hasIndex && indexSupportsDisjuntion,
                          hasIndex ? INDEX_DOES_NOT_SUPPORT_DISJUNCTION : REQUIRES_ALLOW_FILTERING_MESSAGE,
                          row(1), row(2));
        assertDisjunction("a = 1 OR b = 2", !indexSupportsDisjuntion, indexSupportsDisjuntion, row(1), row(2));
        assertDisjunction("a = 1 AND (a = 1 OR b = 1)", !indexSupportsDisjuntion, hasIndex, row(1));
        assertDisjunction("a = 1 AND (a = 1 OR b = 2)", !indexSupportsDisjuntion, hasIndex, row(1));
        assertDisjunction("a = 1 AND (a = 2 OR b = 1)", !indexSupportsDisjuntion, hasIndex, row(1));
        assertDisjunction("a = 1 AND (a = 2 OR b = 2)", !indexSupportsDisjuntion, hasIndex);
        assertDisjunction("a = 2 AND (a = 1 OR b = 1)", !indexSupportsDisjuntion, hasIndex);
        assertDisjunction("a = 2 AND (a = 1 OR b = 2)", !indexSupportsDisjuntion, hasIndex, row(2));
        assertDisjunction("a = 2 AND (a = 2 OR b = 1)", !indexSupportsDisjuntion, hasIndex, row(2));
        assertDisjunction("a = 2 AND (a = 2 OR b = 2)", !indexSupportsDisjuntion, hasIndex, row(2));
        assertDisjunction("a = 1 OR (a = 1 AND b = 1)", !indexSupportsDisjuntion, indexSupportsDisjuntion, row(1));
        assertDisjunction("a = 1 OR (a = 1 AND b = 2)", !indexSupportsDisjuntion, indexSupportsDisjuntion, row(1));
        assertDisjunction("a = 1 OR (a = 2 AND b = 1)", !indexSupportsDisjuntion, indexSupportsDisjuntion, row(1));
        assertDisjunction("a = 1 OR (a = 2 AND b = 2)", !indexSupportsDisjuntion, indexSupportsDisjuntion, row(1), row(2));
        assertDisjunction("a = 2 OR (a = 1 AND b = 1)", !indexSupportsDisjuntion, indexSupportsDisjuntion, row(1), row(2));
        assertDisjunction("a = 2 OR (a = 1 AND b = 2)", !indexSupportsDisjuntion, indexSupportsDisjuntion, row(2));
        assertDisjunction("a = 2 OR (a = 2 AND b = 1)", !indexSupportsDisjuntion, indexSupportsDisjuntion, row(2));
        assertDisjunction("a = 2 OR (a = 2 AND b = 2)", !indexSupportsDisjuntion, indexSupportsDisjuntion, row(2));
    }

    private void assertDisjunction(String restrictions,
                                   boolean requiresFiltering,
                                   boolean shouldUseIndexes,
                                   Object[]... rows)
    {
        assertDisjunction(restrictions, requiresFiltering, shouldUseIndexes, REQUIRES_ALLOW_FILTERING_MESSAGE, rows);
    }

    private void assertDisjunction(String restrictions,
                                   boolean requiresFiltering,
                                   boolean shouldUseIndexes,
                                   String error,
                                   Object[]... rows)
    {
        // without ALLOW FILTERING
        String query = "SELECT pk FROM %s WHERE " + restrictions;
        if (requiresFiltering)
            assertInvalidThrowMessage(error, InvalidRequestException.class, query);
        else
            assertRowsIgnoringOrder(execute(query), rows);

        // with ALLOW FILTERING
        query += " ALLOW FILTERING";
        assertRowsIgnoringOrder(execute(query), rows);

        // verify whether the indexes are used
        Index.QueryPlan plan = parseReadCommand(query).indexQueryPlan();
        Assert.assertEquals(shouldUseIndexes, plan != null);
    }
}
