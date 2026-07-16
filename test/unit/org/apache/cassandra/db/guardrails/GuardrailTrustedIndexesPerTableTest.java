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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.StubIndex;
import org.apache.cassandra.schema.IndexMetadata;

import static java.lang.String.format;

/**
 * Tests the guardrail for the number of trusted custom indexes in a table, {@link Guardrails#trustedIndexesPerTable}.
 * The threshold is counted per implementation class.
 */
public class GuardrailTrustedIndexesPerTableTest extends ThresholdTester
{
    private static final int INDEXES_PER_TABLE_WARN_THRESHOLD = 1;
    private static final int INDEXES_PER_TABLE_FAIL_THRESHOLD = 3;

    public GuardrailTrustedIndexesPerTableTest()
    {
        super(INDEXES_PER_TABLE_WARN_THRESHOLD,
              INDEXES_PER_TABLE_FAIL_THRESHOLD,
              Guardrails.trustedIndexesPerTable,
              Guardrails::setTrustedIndexesPerTableThreshold,
              Guardrails::getTrustedIndexesPerTableWarnThreshold,
              Guardrails::getTrustedIndexesPerTableFailThreshold);
    }

    @Before
    public void setupTrustedIndexImplementations()
    {
        IndexMetadata.loadTrustedIndexImplementations(StubIndex.class.getName() + ',' + OtherStubIndex.class.getName());
    }

    @After
    public void restoreTrustedIndexImplementations()
    {
        IndexMetadata.loadTrustedIndexImplementations(null);
    }

    @Override
    protected long currentValue()
    {
        return getCurrentColumnFamilyStore().indexManager.listIndexes().stream()
                                            .filter(index -> index.getClass() == StubIndex.class)
                                            .count();
    }

    @Test
    public void testCreateIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int, v4 int, v5 int)");
        assertCreateIndexSucceeds("v1", "v1_idx");
        assertCurrentValue(1);

        assertCreateIndexWarns("v2", "");
        assertCreateIndexWarns("v3", "v3_idx");
        assertCreateIndexFails("v4", "");
        assertCreateIndexFails("v2", "v2_idx");
        assertCurrentValue(3);

        // the count is per implementation class: an index of another trusted class is not affected by the
        // indexes created above
        assertValid(format("CREATE CUSTOM INDEX other_idx ON %s.%s(v5) USING '%s'",
                           keyspace(), currentTable(), OtherStubIndex.class.getName()));

        // drop an index, we should be able to create new indexes again
        dropIndex(format("DROP INDEX %s.%s", keyspace(), "v3_idx"));
        assertCurrentValue(2);

        assertCreateIndexWarns("v3", "");
        assertCurrentValue(3);

        // previous guardrail should not apply to another base table
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int, v4 int)");
        assertCreateIndexSucceeds("v4", "");
        assertCreateIndexWarns("v3", "");
        assertCreateIndexWarns("v2", "");
        assertCreateIndexFails("v1", "");
        assertCurrentValue(3);
    }

    @Test
    public void testExcludedUsers() throws Throwable
    {
        createTable("CREATE TABLE %s (k int primary key, v1 int, v2 int)");
        testExcludedUsers(() -> "CREATE CUSTOM INDEX excluded_1 ON %s(v1) USING 'StubIndex'",
                          () -> "CREATE CUSTOM INDEX excluded_2 ON %s(v2) USING 'StubIndex'",
                          () -> "DROP INDEX excluded_1",
                          () -> "DROP INDEX excluded_2");
    }

    private void assertCreateIndexSucceeds(String column, String indexName) throws Throwable
    {
        assertMaxThresholdValid(format("CREATE CUSTOM INDEX %s ON %s.%s(%s) USING 'StubIndex'",
                                       indexName, keyspace(), currentTable(), column));
    }

    private void assertCreateIndexWarns(String column, String indexName) throws Throwable
    {
        assertThresholdWarns(format("CREATE CUSTOM INDEX %s ON %%s(%s) USING 'StubIndex'", indexName, column),
                             format("Creating trusted custom secondary index %son table %s, current number of indexes of the same class %s exceeds warning threshold of %s.",
                                    indexName.isEmpty() ? "" : indexName + ' ',
                                    currentTable(),
                                    currentValue() + 1,
                                    guardrails().getTrustedIndexesPerTableWarnThreshold())
        );
    }

    private void assertCreateIndexFails(String column, String indexName) throws Throwable
    {
        assertThresholdFails(format("CREATE CUSTOM INDEX %s ON %%s(%s) USING 'StubIndex'", indexName, column),
                             format("aborting the creation of secondary index %son table %s",
                                    indexName.isEmpty() ? "" : indexName + ' ', currentTable())
        );
    }

    /**
     * A second trusted implementation, used to verify that the guardrail is counted per implementation class.
     */
    public static final class OtherStubIndex extends StubIndex
    {
        public OtherStubIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
        }
    }
}
