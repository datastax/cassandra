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

package org.apache.cassandra.index.sai.cql.filtering;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.monitoring.runtime.instrumentation.common.collect.Sets;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.index.Index.QueryPlan;
import static org.junit.Assert.assertEquals;

/**
 * Parameterized {@link SAITester} for testing queries involving restrictions on multiple columns with different
 * combinations of indexes on those columns, to verify that:
 * <ul>
 *     <li>The query results are the same independently of what columns are indexed.</li>
 *     <li>The {@link QueryPlan} for each tested query uses the existing indexes (if it's possible).</li>
 *     <li>The query requires {@code ALLOW FILTERING} when it has restrictions that aren't fully supported by indexes.</li>
 * </ul>
 * See CNDB-10142 and CNDB-10233 for further details.
 */
@RunWith(Parameterized.class)
public abstract class FilteredQueryTester extends SAITester
{
    @Parameterized.Parameter
    public Set<Index> indexes;

    private Set<String> indexNames;

    @Before
    public void setup()
    {
        indexNames = indexes.stream().map(i -> i.name).collect(Collectors.toSet());
        createTable();
        indexes.forEach(i -> i.create(this));
        populateTable();
    }

    protected abstract void createTable();

    protected abstract void populateTable();

    protected static Collection<Object[]> parameters(Index... indexes)
    {
        Set<Index> indexesSet = ImmutableSet.copyOf(indexes);
        Set<Object[]> parameters = new HashSet<>();
        for (int i = 0; i <= indexesSet.size(); i++)
        {
            for (Set<Index> combination : Sets.combinations(indexesSet, i))
            {
                parameters.add(new Object[]{ combination });
            }
        }
        return parameters;
    }

    /**
     * Test the given query with the current {@link #indexes}.
     *
     * @param query the query to test
     * @param shouldFilter whether the query should require {@code ALLOW FILTERING} with the indexes
     * @param shouldUseIndexes whether the query should use any of the indexes
     * @param expectedRows the expected rows for the query result
     */
    protected void test(String query,
                        boolean shouldFilter,
                        boolean shouldUseIndexes,
                        Object[]... expectedRows)
    {
        // verify ALLOW FILTERING
        if (shouldFilter)
        {
            assertInvalidThrowMessage("ALLOW FILTERING", InvalidRequestException.class, query);
            query += " ALLOW FILTERING";
        }

        // verify query result
        assertRowsIgnoringOrder(execute(query), expectedRows);

        // verify whether indexes are used or skipped
        String formattedQuery = formatQuery(query);
        SelectStatement select = (SelectStatement) QueryProcessor.parseStatement(formattedQuery, ClientState.forInternalCalls());
        ReadCommand cmd = (ReadCommand) select.getQuery(QueryState.forInternalCalls(), QueryOptions.DEFAULT, FBUtilities.nowInSeconds());
        org.apache.cassandra.index.Index.QueryPlan plan = cmd.indexQueryPlan();
        assertEquals(shouldUseIndexes, plan != null);

        // if we are using indexes, verify that we are using the expected ones
        if (plan != null)
        {
            Set<String> selectedIndexes = plan.getIndexes()
                                              .stream()
                                              .map(i -> i.getIndexMetadata().name)
                                              .collect(Collectors.toSet());
            Assert.assertTrue(indexNames.containsAll(selectedIndexes));
        }
    }

    protected boolean hasIndex(String index)
    {
        return indexNames.contains(index);
    }

    protected boolean hasAllIndexes(String... indexes)
    {
        for (String index : indexes)
        {
            if (!indexNames.contains(index))
                return false;
        }
        return true;
    }

    protected boolean hasAnyIndexes(String... indexes)
    {
        for (String index : indexes)
        {
            if (indexNames.contains(index))
                return true;
        }
        return false;
    }

    protected static class Index
    {
        private final String name;
        private final String createQuery;

        protected Index(String column)
        {
            this.name = column;
            this.createQuery = String.format("CREATE CUSTOM INDEX %s ON %%s(%<s) USING 'StorageAttachedIndex'", column);
        }

        protected Index(String name, String createQuery)
        {
            this.name = name;
            this.createQuery = createQuery;
        }

        private void create(SAITester tester)
        {
            String name = tester.createIndex(createQuery);
            assert name.equals(this.name) : "Expected index name to be " + this.name + " but was " + name;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Index index = (Index) o;
            return Objects.equals(name, index.name);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name);
        }

        @Override
        public String toString()
        {
            return name;
        }
    }
}
