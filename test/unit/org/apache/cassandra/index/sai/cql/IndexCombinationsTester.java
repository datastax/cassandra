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

package org.apache.cassandra.index.sai.cql;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.SAITester;

/**
 * Utility class for testing different combinations of indexes on a table, to verify that the expected results are the
 * same independently of what columns are indexed.
 */
public class IndexCombinationsTester
{
    private static final Logger logger = LoggerFactory.getLogger(IndexCombinationsTester.class);

    private final SAITester tester;
    private final Set<Index> indexes = new HashSet<>();

    public IndexCombinationsTester(SAITester tester)
    {
        this.tester = tester;
    }

    public IndexCombinationsTester withIndexOn(String... columns)
    {
        IndexCombinationsTester tester = this;
        for (String column : columns)
        {
            tester = withIndexOn(column, "CREATE CUSTOM INDEX ON %s(" + column + ") USING 'StorageAttachedIndex'");
        }
        return tester;
    }

    public IndexCombinationsTester withIndexOn(String column, String indexCreationQuery)
    {
        indexes.add(new Index(column, indexCreationQuery));
        return this;
    }

    @SuppressWarnings("UnstableApiUsage")
    public void test(Consumer<Set<String>> test)
    {
        for (int i = 0; i <= indexes.size(); i++)
        {
            for (Set<Index> combination : Sets.combinations(indexes, i))
            {
                Set<String> indexedColumns = combination.stream().map(idx -> idx.column).collect(Collectors.toSet());
                logger.debug("Running test with indexes on: {}", indexedColumns);

                combination.forEach(Index::create);
                tester.waitForTableIndexesQueryable();
                test.accept(indexedColumns);
                combination.forEach(Index::drop);
            }
        }
    }

    private class Index
    {
        public final String column;
        public final String createQuery;
        public String name;

        public Index(String column, String createQuery)
        {
            this.column = column;
            this.createQuery = createQuery;
        }

        public void create()
        {
            name = tester.createIndex(createQuery);
        }

        public void drop()
        {
            assert name != null;
            tester.dropIndex("DROP INDEX %s." + name);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Index index = (Index) o;
            return Objects.equals(column, index.column);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(column);
        }
    }
}
