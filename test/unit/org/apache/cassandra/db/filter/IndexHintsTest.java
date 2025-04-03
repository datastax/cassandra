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
package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.index.CustomIndexTest;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SingletonIndexGroup;
import org.apache.cassandra.index.TargetParser;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.Pair;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.db.filter.IndexHints.CONFLICTING_INDEXES_ERROR;
import static org.apache.cassandra.db.filter.IndexHints.MISSING_INDEX_ERROR;
import static org.apache.cassandra.db.filter.IndexHints.WRONG_KEYSPACE_ERROR;
import static org.apache.cassandra.index.SingletonIndexGroup.MULTIPLE_INDEXES_ERROR_MESSAGE;

/**
 * Tests for {@link IndexHints}, independent of the specific underlying index implementation.
 */
public class IndexHintsTest extends CQLTester
{
    @BeforeClass
    public static void setUpClass()
    {
        // Set the messaging version that adds support for the new index hints before starting the server
        CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.setInt(MessagingService.VERSION_DS_12);
        CQLTester.setUpClass();
    }

    /**
     * Test parsing and validation of index hints in {@code SELECT} queries.
     */
    @Test
    public void testParseAndValidate()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int, c int)");

        // valid queries without index hints
        execute("SELECT * FROM %s WHERE a = 1 ALLOW FILTERING");
        execute("SELECT * FROM %s WHERE a = 1 ALLOW FILTERING WITH included_indexes = {}");
        execute("SELECT * FROM %s WHERE a = 1 ALLOW FILTERING WITH excluded_indexes = {}");
        execute("SELECT * FROM %s WHERE a = 1 ALLOW FILTERING WITH included_indexes = {} AND excluded_indexes = {}");

        // index hints with unparseable properties
        assertInvalidThrowMessage("Invalid value for property 'included_indexes'. It should be a set of identifiers.",
                                  SyntaxException.class,
                                  "SELECT * FROM %s WHERE a = 1 ALLOW FILTERING WITH included_indexes = {'a': 'b'}");

        // invalid queries with unknown index (no index has been created yet)
        String missingIndexError = String.format(MISSING_INDEX_ERROR, currentTable(), "idx1");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  "SELECT * FROM %s WHERE a = 1 ALLOW FILTERING WITH included_indexes = {idx1}");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  "SELECT * FROM %s WHERE a = 1 ALLOW FILTERING WITH included_indexes = {idx1, idx2}");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  "SELECT * FROM %s WHERE a = 1 ALLOW FILTERING WITH excluded_indexes = {idx1}");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  "SELECT * FROM %s WHERE a = 1 ALLOW FILTERING WITH excluded_indexes = {idx1, idx2}");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  "SELECT * FROM %s WHERE a = 1 ALLOW FILTERING WITH included_indexes = {idx1} AND excluded_indexes = {idx2}");

        // create a single index and test queries with it
        createIndex(String.format("CREATE CUSTOM INDEX idx1 ON %%s(a) USING '%s'", GroupedIndex.class.getName()));
        execute("SELECT * FROM %s WHERE a = 1 WITH included_indexes = {}");
        execute("SELECT * FROM %s WHERE a = 1 WITH included_indexes = {idx1}");
        missingIndexError = String.format(MISSING_INDEX_ERROR, currentTable(), "idx2");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  "SELECT * FROM %s WHERE a = 1 WITH included_indexes = {idx2}");
        execute("SELECT * FROM %s WHERE a = 1 WITH excluded_indexes = {}");
        execute("SELECT * FROM %s WHERE a = 1 WITH excluded_indexes = {idx1}");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  "SELECT * FROM %s WHERE a = 1 WITH excluded_indexes = {idx2}");
        execute("SELECT * FROM %s WHERE a = 1 WITH included_indexes = {} AND excluded_indexes = {}");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  "SELECT * FROM %s WHERE a = 1 WITH included_indexes = {idx1} AND excluded_indexes = {idx2}");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  "SELECT * FROM %s WHERE a = 1 WITH included_indexes = {idx2} AND excluded_indexes = {idx1}");
        assertInvalidThrowMessage(CONFLICTING_INDEXES_ERROR + "idx1",
                                  InvalidRequestException.class,
                                  "SELECT * FROM %s WHERE a = 1 WITH included_indexes = {idx1} AND excluded_indexes = {idx1}");

        // create a second index and test queries with both indexes
        createIndex(String.format("CREATE CUSTOM INDEX idx2 ON %%s(b) USING '%s'", GroupedIndex.class.getName()));
        execute("SELECT * FROM %s WHERE a = 1 WITH included_indexes = {}");
        execute("SELECT * FROM %s WHERE a = 1 WITH included_indexes = {idx1}");
        execute("SELECT * FROM %s WHERE a = 1 WITH included_indexes = {idx2}");
        execute("SELECT * FROM %s WHERE a = 1 WITH included_indexes = {idx1, idx2}");
        execute("SELECT * FROM %s WHERE a = 1 WITH excluded_indexes = {}");
        execute("SELECT * FROM %s WHERE a = 1 WITH excluded_indexes = {idx1}");
        execute("SELECT * FROM %s WHERE a = 1 WITH excluded_indexes = {idx2}");
        execute("SELECT * FROM %s WHERE a = 1 WITH excluded_indexes = {idx1, idx2}");
        execute("SELECT * FROM %s WHERE a = 1 WITH included_indexes = {} AND excluded_indexes = {}");
        execute("SELECT * FROM %s WHERE a = 1 WITH included_indexes = {idx1} AND excluded_indexes = {idx2}");
        execute("SELECT * FROM %s WHERE a = 1 WITH included_indexes = {idx2} AND excluded_indexes = {idx1}");
        assertInvalidThrowMessage(CONFLICTING_INDEXES_ERROR + "idx1",
                                  InvalidRequestException.class,
                                  "SELECT * FROM %s WHERE a = 1 WITH included_indexes = {idx1} AND excluded_indexes = {idx1}");
        assertInvalidThrowMessage(CONFLICTING_INDEXES_ERROR + "idx2",
                                  InvalidRequestException.class,
                                  "SELECT * FROM %s WHERE a = 1 WITH included_indexes = {idx2} AND excluded_indexes = {idx2}");
        assertInvalidThrowMessage(CONFLICTING_INDEXES_ERROR + "idx1",
                                  InvalidRequestException.class,
                                  "SELECT * FROM %s WHERE a = 1 WITH included_indexes = {idx1, idx2} AND excluded_indexes = {idx1}");
        assertInvalidThrowMessage(CONFLICTING_INDEXES_ERROR + "idx1",
                                  InvalidRequestException.class,
                                  "SELECT * FROM %s WHERE a = 1 WITH included_indexes = {idx1} AND excluded_indexes = {idx1, idx2}");
        assertInvalidThrowMessage(CONFLICTING_INDEXES_ERROR + "idx1, idx2",
                                  InvalidRequestException.class,
                                  "SELECT * FROM %s WHERE a = 1 WITH included_indexes = {idx1, idx2} AND excluded_indexes = {idx1, idx2}");

        // invalid queries referencing other keyspaces
        String wrongKeyspaceError = String.format(WRONG_KEYSPACE_ERROR, "ks1.idx1");
        assertInvalidThrowMessage(wrongKeyspaceError,
                                  InvalidRequestException.class,
                                  "SELECT * FROM %s WHERE a = 1 ALLOW FILTERING WITH included_indexes = {ks1.idx1}");
        assertInvalidThrowMessage(wrongKeyspaceError,
                                  InvalidRequestException.class,
                                  "SELECT * FROM %s WHERE a = 1 ALLOW FILTERING WITH excluded_indexes = {ks1.idx1}");

        // valid queries with explicit keyspace
        String keyspace = keyspace();
        execute("SELECT * FROM %s WHERE a = 1 WITH included_indexes = {" + keyspace + ".idx1}");
        execute("SELECT * FROM %s WHERE a = 1 WITH included_indexes = {" + keyspace + ".idx1, idx2}");
        execute("SELECT * FROM %s WHERE a = 1 WITH included_indexes = {" + keyspace + ".idx1, " + keyspace + ".idx2}");
        execute("SELECT * FROM %s WHERE a = 1 WITH excluded_indexes = {" + keyspace + ".idx1}");
        execute("SELECT * FROM %s WHERE a = 1 WITH excluded_indexes = {" + keyspace + ".idx1, idx2}");
        execute("SELECT * FROM %s WHERE a = 1 WITH included_indexes = {" + keyspace + ".idx1} AND excluded_indexes = {idx2}");
        execute("SELECT * FROM %s WHERE a = 1 WITH included_indexes = {" + keyspace + ".idx1} AND excluded_indexes = {" + keyspace + ".idx2}");
        execute("SELECT * FROM %s WHERE a = 1 WITH excluded_indexes = {" + keyspace + ".idx1} AND included_indexes = {idx2}");
        execute("SELECT * FROM %s WHERE a = 1 WITH excluded_indexes = {" + keyspace + ".idx1} AND included_indexes = {" + keyspace + ".idx2}");

        // valid queries with quoted names
        execute("SELECT * FROM %s WHERE a = 1 WITH included_indexes = {\"idx1\"}");
        execute("SELECT * FROM %s WHERE a = 1 WITH excluded_indexes = {\"idx1\", idx2}");
        execute("SELECT * FROM %s WHERE a = 1 WITH excluded_indexes = {\"idx1\", \"idx2\"}");
        execute("SELECT * FROM %s WHERE a = 1 WITH excluded_indexes = {\"idx1\"}");
        execute("SELECT * FROM %s WHERE a = 1 WITH excluded_indexes = {\"idx1\", idx2}");
        execute("SELECT * FROM %s WHERE a = 1 WITH excluded_indexes = {\"idx1\", \"idx2\"}");
        execute("SELECT * FROM %s WHERE a = 1 WITH included_indexes = {\"idx1\"} AND excluded_indexes = {idx2}");
        execute("SELECT * FROM %s WHERE a = 1 WITH included_indexes = {\"idx1\"} AND excluded_indexes = {\"idx2\"}");
        execute("SELECT * FROM %s WHERE a = 1 WITH excluded_indexes = {\"idx1\"} AND included_indexes = {idx2}");
        execute("SELECT * FROM %s WHERE a = 1 WITH excluded_indexes = {\"idx1\"} AND included_indexes = {\"idx2\"}");
    }

    /**
     * Test that index hints are considered when generating the CQL string representation of a {@link ReadCommand}.
     */
    @Test
    public void testToCQLString()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int)");
        createIndex(String.format("CREATE CUSTOM INDEX idx1 ON %%s(a) USING '%s'", GroupedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX idx2 ON %%s(b) USING '%s'", GroupedIndex.class.getName()));

        // without index hints
        String formattedQuery = formatQuery("SELECT * FROM %%s WHERE a = 0");
        ReadCommand command = parseReadCommand(formattedQuery);
        Assertions.assertThat(command.toCQLString())
                  .doesNotContain("included_indexes")
                  .doesNotContain("excluded_indexes");

        // with empty hints
        formattedQuery = formatQuery("SELECT * FROM %%s WHERE a = 0 AND b = 0 WITH included_indexes = {} AND excluded_indexes = {}");
        command = parseReadCommand(formattedQuery);
        Assertions.assertThat(command.toCQLString())
                  .doesNotContain("included_indexes")
                  .doesNotContain("excluded_indexes");

        // with included indexes only
        formattedQuery = formatQuery("SELECT * FROM %%s WHERE a = 0 AND b = 0 WITH included_indexes = {idx1, idx2}");
        command = parseReadCommand(formattedQuery);
        Assertions.assertThat(command.toCQLString())
                  .contains(" WITH included_indexes = {idx1, idx2}")
                  .doesNotContain("excluded_indexes");

        // with excluded indexes only
        formattedQuery = formatQuery("SELECT * FROM %%s WHERE a = 0 AND b = 0 WITH excluded_indexes = {idx1, idx2}");
        command = parseReadCommand(formattedQuery);
        Assertions.assertThat(command.toCQLString())
                  .contains(" WITH excluded_indexes = {idx1, idx2}")
                  .doesNotContain("included_indexes");

        // with both included and excluded indexes
        formattedQuery = formatQuery("SELECT * FROM %%s WHERE a = 0 AND b = 0 WITH included_indexes = {idx1} AND excluded_indexes = {idx2}");
        command = parseReadCommand(formattedQuery);
        Assertions.assertThat(command.toCQLString())
                  .contains(" WITH included_indexes = {idx1} AND excluded_indexes = {idx2}");
    }

    /**
     * Verify that the index hints in a query get to the {@link ReadCommand} and the {@link Index},
     * even after serialization.
     */
    @Test
    public void testTransport()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int)");
        createIndex(String.format("CREATE CUSTOM INDEX idx1 ON %%s(a) USING '%s'", GroupedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX idx2 ON %%s(b) USING '%s'", GroupedIndex.class.getName()));
        Index idx1 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx1");
        Index idx2 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx2");

        // unespecified hints should be mapped to NONE
        testTransport("SELECT * FROM %s WHERE a = 1", IndexHints.NONE);
        testTransport("SELECT * FROM %s WHERE a = 1 WITH included_indexes = {}", IndexHints.NONE);
        testTransport("SELECT * FROM %s WHERE a = 1 WITH excluded_indexes = {}", IndexHints.NONE);

        // hints with a single index
        testTransport("SELECT * FROM %s WHERE a = 1 WITH included_indexes = {idx1}", IndexHints.create(indexes(idx1), indexes()));
        testTransport("SELECT * FROM %s WHERE a = 1 WITH excluded_indexes = {idx1}", IndexHints.create(indexes(), indexes(idx1)));

        // hints with multiple indexes
        testTransport("SELECT * FROM %s WHERE a = 1 WITH included_indexes = {idx1, idx2}", IndexHints.create(indexes(idx1, idx2), indexes()));
        testTransport("SELECT * FROM %s WHERE a = 1 WITH excluded_indexes = {idx1, idx2}", IndexHints.create(indexes(), indexes(idx1, idx2)));
    }

    private void testTransport(String query, IndexHints expectedHints)
    {
        // verify that the hints arrive correctly at the index
        execute(query);
        Assertions.assertThat(GroupedIndex.lastQueryIndexHints).isEqualTo(expectedHints);

        // verify that the hints are correctly parsed and stored in the ReadCommand
        String formattedQuery = formatQuery(query);
        ReadCommand command = parseReadCommand(formattedQuery);
        IndexHints actualHints = command.rowFilter().indexHints();
        Assertions.assertThat(actualHints).isEqualTo(expectedHints);

        // serialize and deserialize the command to check if the hints are preserved...
        try
        {
            // ...with a version that supports index hints
            DataOutputBuffer out = new DataOutputBuffer();
            ReadCommand.serializer.serialize(command, out, MessagingService.VERSION_DS_12);
            Assertions.assertThat(ReadCommand.serializer.serializedSize(command, MessagingService.VERSION_DS_12))
                      .isEqualTo(out.buffer().remaining());
            DataInputBuffer in = new DataInputBuffer(out.buffer(), true);
            command = ReadCommand.serializer.deserialize(in, MessagingService.VERSION_DS_12);
            actualHints = command.rowFilter().indexHints();
            Assertions.assertThat(actualHints).isEqualTo(expectedHints);

            // ...with a version that doesn't support index hints
            out = new DataOutputBuffer();
            if (expectedHints != IndexHints.NONE) {
                try
                {
                    ReadCommand.serializer.serialize(command, out, MessagingService.VERSION_DS_11);
                }
                catch (IllegalStateException e)
                {
                    // expected
                    Assertions.assertThat(e)
                              .hasMessageContaining("Unable to serialize index hints with messaging version: " + MessagingService.VERSION_DS_11);
                }
            }
            else
            {
                ReadCommand.serializer.serialize(command, out, MessagingService.VERSION_DS_11);
                Assertions.assertThat(ReadCommand.serializer.serializedSize(command, MessagingService.VERSION_DS_11))
                          .isEqualTo(out.buffer().remaining());
                in = new DataInputBuffer(out.buffer(), true);
                command = ReadCommand.serializer.deserialize(in, MessagingService.VERSION_DS_11);
                actualHints = command.rowFilter().indexHints();
                Assertions.assertThat(actualHints).isEqualTo(IndexHints.NONE);
            }
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    @Test
    public void testLegacyIndex()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int)");
        createIndex("CREATE INDEX idx1 ON %s(v1)");
        createIndex("CREATE INDEX idx2 ON %s(v2)");
        Index idx1 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx1");
        Index idx2 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx2");

        // without any hints
        assertAcceptsHints("SELECT * FROM %s ALLOW FILTERING");
        assertAcceptsHints("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING", idx1);
        assertAcceptsHints("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING", idx1);
        assertAcceptsHints("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING", idx2);
        assertAcceptsHints("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING");
        assertAcceptsHints("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING", idx1);
        assertAcceptsHints("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING", idx1);
        assertAcceptsHints("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING", idx2);

        // with a single restriction and included indexes
        assertAcceptsHints("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH included_indexes = {idx1}", idx1);
        assertRejectsHints("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH included_indexes = {idx2}",
                           SingletonIndexGroup.INDEX_WITHOUT_EXPRESSION_ERROR_MESSAGE + "idx2");
        assertRejectsHints("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH included_indexes = {idx1}",
                           SingletonIndexGroup.INDEX_WITHOUT_EXPRESSION_ERROR_MESSAGE + "idx1");
        assertAcceptsHints("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH included_indexes = {idx2}", idx2);
        assertRejectsHints("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH included_indexes = {idx1}",
                           SingletonIndexGroup.INDEX_WITHOUT_EXPRESSION_ERROR_MESSAGE + "idx1");
        assertRejectsHints("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH included_indexes = {idx2}",
                           SingletonIndexGroup.INDEX_WITHOUT_EXPRESSION_ERROR_MESSAGE + "idx2");

        // with a single restriction and excluded indexes
        assertAcceptsHints("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes = {idx1}");
        assertAcceptsHints("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes = {idx2}", idx1);
        assertAcceptsHints("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes = {idx1}", idx2);
        assertAcceptsHints("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes = {idx2}");
        assertAcceptsHints("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes = {idx1}");
        assertAcceptsHints("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes = {idx2}");

        // with restrictions in two columns (v1 and v2) and included indexes
        assertAcceptsHints("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes = {idx1}", idx1);
        assertAcceptsHints("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes = {idx2}", idx2);
        assertRejectsHints("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes = {idx1, idx2}",
                           SingletonIndexGroup.MULTIPLE_INDEXES_ERROR_MESSAGE);

        // with restrictions in two columns (v1 and v2) and excluded indexes
        assertAcceptsHints("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes = {idx1}", idx2);
        assertAcceptsHints("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes = {idx2}", idx1);
        assertAcceptsHints("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes = {idx1, idx2}");

        // with restrictions in two columns (v1 and v3) and included indexes
        assertAcceptsHints("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH included_indexes = {idx1}", idx1);
        assertRejectsHints("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH included_indexes = {idx2}",
                           SingletonIndexGroup.INDEX_WITHOUT_EXPRESSION_ERROR_MESSAGE + "idx2");
        assertRejectsHints("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH included_indexes = {idx1, idx2}",
                           SingletonIndexGroup.MULTIPLE_INDEXES_ERROR_MESSAGE);

        // with restrictions in two columns (v1 and v3) and excluded indexes
        assertAcceptsHints("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes = {idx1}");
        assertAcceptsHints("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes = {idx2}", idx1);
        assertAcceptsHints("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes = {idx1, idx2}");

        // without restrictions
        assertAcceptsHints("SELECT * FROM %s WITH included_indexes = {idx1}"); // TODO: should be rejected
        assertAcceptsHints("SELECT * FROM %s WITH excluded_indexes = {idx1}");
    }

    private void assertRejectsHints(String query, String message)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .hasMessageContaining(message);
    }

    private void assertAcceptsHints(String query, Index... indexes)
    {
        Index.QueryPlan plan = parseReadCommand(query).indexQueryPlan();
        if (indexes.length == 0)
        {
            Assertions.assertThat(plan).isNull();
        }
        else
        {
            Assertions.assertThat(plan).isNotNull();
            Assertions.assertThat(plan.getIndexes()).hasSize(indexes.length).contains(indexes);
        }
    }

    private static Set<Index> indexes(Index... indexes)
    {
        Set<Index> set = new HashSet<>(indexes.length);
        set.addAll(Arrays.asList(indexes));
        return set;
    }

    /**
     * Mock index with a common index group support.
     */
    public static final class GroupedIndex extends CustomIndexTest.IndexWithSharedGroup
    {
        private final ColumnMetadata indexedColumn;
        public static volatile IndexHints lastQueryIndexHints;

        public GroupedIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
            Pair<ColumnMetadata, IndexTarget.Type> target = TargetParser.parse(baseCfs.metadata(), metadata);
            indexedColumn = target.left;
        }

        @Override
        public boolean supportsExpression(ColumnMetadata column, Operator operator)
        {
            return indexedColumn.name.equals(column.name);
        }

        @Override
        public Searcher searcherFor(ReadCommand command)
        {
            lastQueryIndexHints = command.rowFilter().indexHints();
            return super.searcherFor(command);
        }
    }
}
