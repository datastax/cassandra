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
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.index.CustomIndexTest;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.TargetParser;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.Pair;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ThrowableAssert;

import static org.apache.cassandra.db.filter.IndexHints.CONFLICTING_INDEXES_ERROR;
import static org.apache.cassandra.db.filter.IndexHints.MISSING_INDEX_ERROR;
import static org.apache.cassandra.db.filter.IndexHints.WRONG_KEYSPACE_ERROR;

/**
 * Tests the {@link IndexHints} independently of the specific underlying index implementation details.
 * </p>
 * Regarding its effects on the {@link Index.QueryPlan} that is generated for the query:
 * <ul>
 *    <li>Included indexes should be included in the {@link Index.QueryPlan} for the query or fail.</li>
 *    <li>Excluded indexes should not be included in the {@link Index.QueryPlan} for the query.</li>
 * </ul>
 */
public class IndexHintsTest extends CQLTester
{
    @BeforeClass
    public static void setUpClass()
    {
        // Set the messaging version that adds support for the new index hints before starting the server
        CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.setInt(MessagingService.VERSION_DS_12);
        CQLTester.setUpClass();
        CQLTester.enableCoordinatorExecution();
    }

    /**
     * Test parsing and validation of index hints in {@code SELECT} queries.
     */
    @Test
    public void testParseAndValidate()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int, c int)");
        String query = "SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = 3 ALLOW FILTERING ";

        // valid queries without index hints
        execute(query);
        execute(query + "WITH included_indexes={}");
        execute(query + "WITH excluded_indexes={}");
        execute(query + "WITH included_indexes={} AND excluded_indexes={}");

        // index hints with unparseable properties
        assertInvalidThrowMessage("Invalid value for property 'included_indexes'. It should be a set of identifiers.",
                                  SyntaxException.class,
                                  query + "WITH included_indexes={'a': 'b'}");

        // invalid queries with unknown index (no index has been created yet)
        String missingIndexError = String.format(MISSING_INDEX_ERROR, currentTable(), "idx1");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  query + "WITH included_indexes={idx1}");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  query + "WITH included_indexes={idx1,idx2}");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  query + "WITH excluded_indexes={idx1}");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  query + "WITH excluded_indexes={idx1,idx2}");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  query + "WITH included_indexes={idx1} AND excluded_indexes={idx2}");

        // create a single index and test queries with it
        createIndex(String.format("CREATE CUSTOM INDEX idx1 ON %%s(a) USING '%s'", GroupedIndex.class.getName()));
        execute(query + "WITH included_indexes={}");
        execute(query + "WITH included_indexes={idx1}");
        missingIndexError = String.format(MISSING_INDEX_ERROR, currentTable(), "idx2");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  query + "WITH included_indexes={idx2}");
        execute(query + "WITH excluded_indexes={}");
        execute(query + "WITH excluded_indexes={idx1}");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  query + "WITH excluded_indexes={idx2}");
        execute(query + "WITH included_indexes={} AND excluded_indexes={}");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  query + "WITH included_indexes={idx1} AND excluded_indexes={idx2}");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  query + "WITH included_indexes={idx2} AND excluded_indexes={idx1}");
        assertInvalidThrowMessage(CONFLICTING_INDEXES_ERROR + "idx1",
                                  InvalidRequestException.class,
                                  query + "WITH included_indexes={idx1} AND excluded_indexes={idx1}");

        // create a second index and test queries with both indexes
        createIndex(String.format("CREATE CUSTOM INDEX idx2 ON %%s(b) USING '%s'", GroupedIndex.class.getName()));
        execute(query + "WITH included_indexes={}");
        execute(query + "WITH included_indexes={idx1}");
        execute(query + "WITH included_indexes={idx2}");
        execute(query + "WITH included_indexes={idx1,idx2}");
        execute(query + "WITH excluded_indexes={}");
        execute(query + "WITH excluded_indexes={idx1}");
        execute(query + "WITH excluded_indexes={idx2}");
        execute(query + "WITH excluded_indexes={idx1,idx2}");
        execute(query + "WITH included_indexes={} AND excluded_indexes={}");
        execute(query + "WITH included_indexes={idx1} AND excluded_indexes={idx2}");
        execute(query + "WITH included_indexes={idx2} AND excluded_indexes={idx1}");
        assertInvalidThrowMessage(CONFLICTING_INDEXES_ERROR + "idx1",
                                  InvalidRequestException.class,
                                  query + "WITH included_indexes={idx1} AND excluded_indexes={idx1}");
        assertInvalidThrowMessage(CONFLICTING_INDEXES_ERROR + "idx2",
                                  InvalidRequestException.class,
                                  query + "WITH included_indexes={idx2} AND excluded_indexes={idx2}");
        assertInvalidThrowMessage(CONFLICTING_INDEXES_ERROR + "idx1",
                                  InvalidRequestException.class,
                                  query + "WITH included_indexes={idx1,idx2} AND excluded_indexes={idx1}");
        assertInvalidThrowMessage(CONFLICTING_INDEXES_ERROR + "idx1",
                                  InvalidRequestException.class,
                                  query + "WITH included_indexes={idx1} AND excluded_indexes={idx1,idx2}");
        assertInvalidThrowMessage(CONFLICTING_INDEXES_ERROR + "idx1,idx2",
                                  InvalidRequestException.class,
                                  query + "WITH included_indexes={idx1,idx2} AND excluded_indexes={idx1,idx2}");

        // invalid queries referencing other keyspaces
        String wrongKeyspaceError = String.format(WRONG_KEYSPACE_ERROR, "ks1.idx1");
        assertInvalidThrowMessage(wrongKeyspaceError,
                                  InvalidRequestException.class,
                                  query + "WITH included_indexes={ks1.idx1}");
        assertInvalidThrowMessage(wrongKeyspaceError,
                                  InvalidRequestException.class,
                                  query + "WITH excluded_indexes={ks1.idx1}");

        // valid queries with explicit keyspace
        String keyspace = keyspace();
        execute(query + "WITH included_indexes={" + keyspace + ".idx1}");
        execute(query + "WITH included_indexes={" + keyspace + ".idx1, idx2}");
        execute(query + "WITH included_indexes={" + keyspace + ".idx1, " + keyspace + ".idx2}");
        execute(query + "WITH excluded_indexes={" + keyspace + ".idx1}");
        execute(query + "WITH excluded_indexes={" + keyspace + ".idx1, idx2}");
        execute(query + "WITH included_indexes={" + keyspace + ".idx1} AND excluded_indexes={idx2}");
        execute(query + "WITH included_indexes={" + keyspace + ".idx1} AND excluded_indexes={" + keyspace + ".idx2}");
        execute(query + "WITH excluded_indexes={" + keyspace + ".idx1} AND included_indexes={idx2}");
        execute(query + "WITH excluded_indexes={" + keyspace + ".idx1} AND included_indexes={" + keyspace + ".idx2}");

        // valid queries with quoted names
        execute(query + "WITH included_indexes={\"idx1\"}");
        execute(query + "WITH excluded_indexes={\"idx1\", idx2}");
        execute(query + "WITH excluded_indexes={\"idx1\", \"idx2\"}");
        execute(query + "WITH excluded_indexes={\"idx1\"}");
        execute(query + "WITH excluded_indexes={\"idx1\", idx2}");
        execute(query + "WITH excluded_indexes={\"idx1\", \"idx2\"}");
        execute(query + "WITH included_indexes={\"idx1\"} AND excluded_indexes={idx2}");
        execute(query + "WITH included_indexes={\"idx1\"} AND excluded_indexes={\"idx2\"}");
        execute(query + "WITH excluded_indexes={\"idx1\"} AND included_indexes={idx2}");
        execute(query + "WITH excluded_indexes={\"idx1\"} AND included_indexes={\"idx2\"}");
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
        formattedQuery = formatQuery("SELECT * FROM %%s WHERE a = 0 AND b = 0 " +
                                     "WITH included_indexes={} AND excluded_indexes={}");
        command = parseReadCommand(formattedQuery);
        Assertions.assertThat(command.toCQLString())
                  .doesNotContain("included_indexes")
                  .doesNotContain("excluded_indexes");

        // with included indexes only
        formattedQuery = formatQuery("SELECT * FROM %%s WHERE a = 0 AND b = 0 " +
                                     "WITH included_indexes={idx1,idx2}");
        command = parseReadCommand(formattedQuery);
        Assertions.assertThat(command.toCQLString())
                  .contains(" WITH included_indexes = {idx1, idx2}")
                  .doesNotContain("excluded_indexes");

        // with excluded indexes only
        formattedQuery = formatQuery("SELECT * FROM %%s WHERE a = 0 AND b = 0 ALLOW FILTERING " +
                                     "WITH excluded_indexes={idx1,idx2}");
        command = parseReadCommand(formattedQuery);
        Assertions.assertThat(command.toCQLString())
                  .contains(" WITH excluded_indexes = {idx1, idx2}")
                  .doesNotContain("included_indexes");

        // with both included and excluded indexes
        formattedQuery = formatQuery("SELECT * FROM %%s WHERE a = 0 AND b = 0 ALLOW FILTERING " +
                                     "WITH included_indexes={idx1} AND excluded_indexes={idx2}");
        command = parseReadCommand(formattedQuery);
        Assertions.assertThat(command.toCQLString())
                  .contains(" WITH included_indexes = {idx1} AND excluded_indexes = {idx2}");

        // with a single-partition read command
        formattedQuery = formatQuery("SELECT * FROM %%s WHERE k=1 AND a = 0 AND b = 0 ALLOW FILTERING " +
                                     "WITH included_indexes={idx1} AND excluded_indexes={idx2}");
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
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int, c int, d int)");
        createIndex(String.format("CREATE CUSTOM INDEX idx1 ON %%s(a) USING '%s'", GroupedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX idx2 ON %%s(b) USING '%s'", GroupedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX idx3 ON %%s(c) USING '%s'", GroupedIndex.class.getName()));
        createIndex(String.format("CREATE CUSTOM INDEX idx4 ON %%s(d) USING '%s'", GroupedIndex.class.getName()));
        IndexMetadata idx1 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx1").getIndexMetadata();
        IndexMetadata idx2 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx2").getIndexMetadata();
        IndexMetadata idx3 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx3").getIndexMetadata();
        IndexMetadata idx4 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx4").getIndexMetadata();
        String query = "SELECT * FROM %s WHERE a = 1 AND b = 2 AND c = 3 AND d = 4 ALLOW FILTERING ";

        // unspecified hints should be mapped to NONE
        testTransport(query, IndexHints.NONE);
        testTransport(query + "WITH included_indexes={}", IndexHints.NONE);
        testTransport(query + "WITH excluded_indexes={}", IndexHints.NONE);

        // hints with a single index
        testTransport(query + "WITH included_indexes={idx1}", IndexHints.create(indexes(idx1), indexes()));
        testTransport(query + "WITH excluded_indexes={idx1}", IndexHints.create(indexes(), indexes(idx1)));
        testTransport(query + "WITH included_indexes={idx1} AND excluded_indexes={idx2}", IndexHints.create(indexes(idx1), indexes(idx2)));

        // hints with multiple indexes
        testTransport(query + "WITH included_indexes={idx1,idx2}", IndexHints.create(indexes(idx1, idx2), indexes()));
        testTransport(query + "WITH excluded_indexes={idx1,idx2}", IndexHints.create(indexes(), indexes(idx1, idx2)));
        testTransport(query + "WITH included_indexes={idx1,idx2} AND excluded_indexes={idx3,idx4}", IndexHints.create(indexes(idx1, idx2), indexes(idx3, idx4)));
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
            if (expectedHints != IndexHints.NONE)
            {
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
    public void testLegacyIndexWithAllowFiltering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int)");
        String idx1 = createIndex("CREATE INDEX idx1 ON %s(v1)");
        String idx2 = createIndex("CREATE INDEX idx2 ON %s(v2)");

        String insert = "INSERT INTO %s (k, v1, v2, v3) VALUES (?, ?, ?, ?)";
        Object[] row1 = new Object[]{ 1, 0, 1, 1 };
        Object[] row2 = new Object[]{ 2, 1, 0, 1 };
        Object[] row3 = new Object[]{ 3, 1, 1, 0 };
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);

        // without any hints
        assertThatIndexQueryPlanFor("SELECT * FROM %s ALLOW FILTERING", row1, row2, row3).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING", row1).selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING", row2).selects(idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING", row3).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING").selectsAnyOf(idx1, idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING").selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING").selects(idx2);

        // with a single restriction and included indexes
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH included_indexes={idx1}", row1).selects(idx1);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH included_indexes={idx2}", idx2);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH included_indexes={idx1}", idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH included_indexes={idx2}", row2).selects(idx2);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH included_indexes={idx1}", idx1);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH included_indexes={idx2}", idx2);

        // with a single restriction and excluded indexes
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx1}", row1).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx2}", row1).selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx1}", row2).selects(idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx2}", row2).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx1}", row3).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx2}", row3).selectsNone();

        // with restrictions in two columns (v1 and v2) and included indexes
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes={idx1}").selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes={idx2}").selects(idx2);
        assertAnyUnselectedIndexError("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes={idx1,idx2}", idx1, idx2);

        // with restrictions in two columns (v1 and v2) and excluded indexes
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx1}").selects(idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx2}").selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx2}").selectsNone();

        // with restrictions in two columns (v1 and v3) and included indexes
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx1}").selects(idx1);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx2}", idx2);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx1,idx2}", idx2);

        // with restrictions in two columns (v1 and v3) and excluded indexes
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx1}").selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx2}").selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx2}").selectsNone();

        // without restrictions
        assertUnselectedIndexError("SELECT * FROM %s ALLOW FILTERING WITH included_indexes={idx1}", idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s ALLOW FILTERING WITH excluded_indexes={idx1}", row1, row2, row3).selectsNone();

        // prepared statements
        prepare("SELECT * FROM %s WHERE v1=? ALLOW FILTERING WITH included_indexes={idx1}");
        prepare("SELECT * FROM %s WHERE v1=? ALLOW FILTERING WITH excluded_indexes={idx1}");
    }

    @Test
    public void testLegacyIndexWithoutAllowFiltering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int)");
        String idx1 = createIndex("CREATE INDEX idx1 ON %s(v1)");
        String idx2 = createIndex("CREATE INDEX idx2 ON %s(v2)");

        String insert = "INSERT INTO %s (k, v1, v2, v3) VALUES (?, ?, ?, ?)";
        Object[] row1 = new Object[]{ 1, 0, 1, 1 };
        Object[] row2 = new Object[]{ 2, 1, 0, 1 };
        Object[] row3 = new Object[]{ 3, 1, 1, 0 };
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);

        // without any hints
        assertThatIndexQueryPlanFor("SELECT * FROM %s", row1, row2, row3).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0", row1).selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0", row2).selects(idx2);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v2=0 AND v3=0");

        // with a single restriction and included indexes
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 WITH included_indexes={idx1}", row1).selects(idx1);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v1=0 WITH included_indexes={idx2}", idx2);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v2=0 WITH included_indexes={idx1}", idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 WITH included_indexes={idx2}", row2).selects(idx2);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0 WITH included_indexes={idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0 WITH included_indexes={idx2}");

        // with a single restriction and excluded indexes
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 WITH excluded_indexes={idx1}");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 WITH excluded_indexes={idx2}", row1).selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 WITH excluded_indexes={idx1}", row2).selects(idx2);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v2=0 WITH excluded_indexes={idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0 WITH excluded_indexes={idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0 WITH excluded_indexes={idx2}");

        // with restrictions in two columns (v1 and v2) and included indexes
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH included_indexes={idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH included_indexes={idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH included_indexes={idx1,idx2}");

        // with restrictions in two columns (v1 and v2) and excluded indexes
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH excluded_indexes={idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH excluded_indexes={idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH excluded_indexes={idx1,idx2}");

        // with restrictions in two columns (v1 and v3) and included indexes
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH included_indexes={idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH included_indexes={idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH included_indexes={idx1,idx2}");

        // with restrictions in two columns (v1 and v3) and excluded indexes
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH excluded_indexes={idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH excluded_indexes={idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH excluded_indexes={idx1,idx2}");

        // without restrictions
        assertUnselectedIndexError("SELECT * FROM %s WITH included_indexes={idx1}", idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WITH excluded_indexes={idx1}", row1, row2, row3).selectsNone();

        // prepared statements
        prepare("SELECT * FROM %s WHERE v1=?");
        prepare("SELECT * FROM %s WHERE v1=? WITH included_indexes={idx1}");
        assertNeedsAllowFiltering(() -> prepare("SELECT * FROM %s WHERE v1=0 WITH excluded_indexes={idx1}"));
    }

    @Test
    public void testSAIWithAllowFiltering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int)");
        String idx1 = createIndex("CREATE CUSTOM INDEX idx1 ON %s(v1) USING 'StorageAttachedIndex'");
        String idx2 = createIndex("CREATE CUSTOM INDEX idx2 ON %s(v2) USING 'StorageAttachedIndex'");

        String insert = "INSERT INTO %s (k, v1, v2, v3) VALUES (?, ?, ?, ?)";
        Object[] row1 = new Object[]{ 1, 0, 1, 1 };
        Object[] row2 = new Object[]{ 2, 1, 0, 1 };
        Object[] row3 = new Object[]{ 3, 1, 1, 0 };
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);

        // without any hints
        assertThatIndexQueryPlanFor("SELECT * FROM %s ALLOW FILTERING", row1, row2, row3).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING", row1).selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING", row2).selects(idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING", row3).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING").selects(idx1, idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING").selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING").selects(idx2);

        // with a single restriction and included indexes
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH included_indexes={idx1}", row1).selects(idx1);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH included_indexes={idx2}", idx2);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH included_indexes={idx1}", idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH included_indexes={idx2}", row2).selects(idx2);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH included_indexes={idx1}", idx1);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH included_indexes={idx2}", idx2);

        // with a single restriction and excluded indexes
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx1}", row1).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx2}", row1).selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx1}", row2).selects(idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx2}", row2).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx1}", row3).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx2}", row3).selectsNone();

        // with restrictions in two columns (v1 and v2) and included indexes
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes={idx1}").selects(idx1, idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes={idx2}").selects(idx1, idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes={idx1,idx2}").selects(idx1, idx2);

        // with restrictions in two columns (v1 and v2) and excluded indexes
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx1}").selects(idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx2}").selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx2}").selectsNone();

        // with restrictions in two columns (v1 and v3) and included indexes
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx1}").selects(idx1);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx2}", idx2);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx1,idx2}", idx2);

        // with restrictions in two columns (v1 and v3) and excluded indexes
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx1}").selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx2}").selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx2}").selectsNone();

        // with mixed included and excluded indexes
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes={idx1} AND excluded_indexes={idx2}").selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes={idx2} AND excluded_indexes={idx1}").selects(idx2);

        // without restrictions
        assertUnselectedIndexError("SELECT * FROM %s ALLOW FILTERING WITH included_indexes={idx1}", idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s ALLOW FILTERING WITH excluded_indexes={idx1}", row1, row2, row3).selectsNone();

        // prepared statements
        prepare("SELECT * FROM %s WHERE v1=? ALLOW FILTERING WITH included_indexes={idx1}");
        prepare("SELECT * FROM %s WHERE v1=? ALLOW FILTERING WITH excluded_indexes={idx1}");
    }

    @Test
    public void testSAIWithoutAllowFiltering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int)");
        String idx1 = createIndex("CREATE CUSTOM INDEX idx1 ON %s(v1) USING 'StorageAttachedIndex'");
        String idx2 = createIndex("CREATE CUSTOM INDEX idx2 ON %s(v2) USING 'StorageAttachedIndex'");

        String insert = "INSERT INTO %s (k, v1, v2, v3) VALUES (?, ?, ?, ?)";
        Object[] row1 = new Object[]{ 1, 0, 1, 1 };
        Object[] row2 = new Object[]{ 2, 1, 0, 1 };
        Object[] row3 = new Object[]{ 3, 1, 1, 0 };
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);

        // without any hints
        assertThatIndexQueryPlanFor("SELECT * FROM %s", row1, row2, row3).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0", row1).selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0", row2).selects(idx2);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0").selectsAnyOf(idx1, idx2);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v2=0 AND v3=0");

        // with a single restriction and included indexes
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 WITH included_indexes={idx1}", row1).selects(idx1);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v1=0 WITH included_indexes={idx2}", idx2);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v2=0 WITH included_indexes={idx1}", idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 WITH included_indexes={idx2}", row2).selects(idx2);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0 WITH included_indexes={idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0 WITH included_indexes={idx2}");

        // with a single restriction and excluded indexes
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 WITH excluded_indexes={idx1}");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 WITH excluded_indexes={idx2}", row1).selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 WITH excluded_indexes={idx1}", row2).selects(idx2);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v2=0 WITH excluded_indexes={idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0 WITH excluded_indexes={idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0 WITH excluded_indexes={idx2}");

        // with restrictions in two columns (v1 and v2) and included indexes
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH included_indexes={idx1}").selects(idx1, idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH included_indexes={idx2}").selects(idx1, idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH included_indexes={idx1,idx2}").selects(idx1, idx2);

        // with restrictions in two columns (v1 and v2) and excluded indexes
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH excluded_indexes={idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH excluded_indexes={idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH excluded_indexes={idx1,idx2}");

        // with restrictions in two columns (v1 and v3) and included indexes
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH included_indexes={idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH included_indexes={idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH included_indexes={idx1,idx2}");

        // with restrictions in two columns (v1 and v3) and excluded indexes
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH excluded_indexes={idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH excluded_indexes={idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH excluded_indexes={idx1,idx2}");

        // with mixed included and excluded indexes
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH included_indexes={idx1} AND excluded_indexes={idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH included_indexes={idx2} AND excluded_indexes={idx1}");

        // without restrictions
        assertUnselectedIndexError("SELECT * FROM %s WITH included_indexes={idx1}", idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WITH excluded_indexes={idx1}", row1, row2, row3).selectsNone();

        // prepared statements
        prepare("SELECT * FROM %s WHERE v1=? WITH included_indexes={idx1}");
        assertNeedsAllowFiltering(() -> prepare("SELECT * FROM %s WHERE v1=? WITH excluded_indexes={idx1}"));
    }

    @Test
    public void testMixedIndexImplementations()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int)");
        String idx1 = createIndex("CREATE CUSTOM INDEX idx1 ON %s(v1) USING 'StorageAttachedIndex'");
        String idx2 = createIndex("CREATE CUSTOM INDEX idx2 ON %s(v2) USING 'StorageAttachedIndex'");
        String idx3 = createIndex("CREATE INDEX idx3 ON %s(v3)");

        String insert = "INSERT INTO %s (k, v1, v2, v3) VALUES (?, ?, ?, ?)";
        Object[] row1 = new Object[]{ 1, 0, 1, 1 };
        Object[] row2 = new Object[]{ 2, 1, 0, 1 };
        Object[] row3 = new Object[]{ 3, 1, 1, 0 };
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);

        // without any hints
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING", row1).selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING", row2).selects(idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING", row3).selects(idx3);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING").selects(idx1, idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING").selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING").selects(idx2);

        // including idx1 (SAI)
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH included_indexes={idx1}", row1).selects(idx1);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH included_indexes={idx1}", idx1);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH included_indexes={idx1}", idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes={idx1}").selects(idx1, idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx1}").selects(idx1);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx1}", idx1);

        // including idx2 (SAI)
        assertUnselectedIndexError("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH included_indexes={idx2}", idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH included_indexes={idx2}", row2).selects(idx2);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH included_indexes={idx2}", idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes={idx2}").selects(idx1, idx2);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx2}", idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx2}").selects(idx2);

        // including idx3 (legacy)
        assertUnselectedIndexError("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH included_indexes={idx3}", idx3);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH included_indexes={idx3}", idx3);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH included_indexes={idx3}", row3).selects(idx3);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes={idx3}", idx3);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx3}").selects(idx3); // chooses legacy over SAI
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx3}").selects(idx3); // chooses legacy over SAI

        // including idx1 and idx2 (both SAI)
        assertUnselectedIndexError("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH included_indexes={idx1,idx2}", idx2);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH included_indexes={idx1,idx2}", idx1);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH included_indexes={idx1,idx2}", idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes={idx1,idx2}").selects(idx1, idx2);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx1,idx2}", idx2);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx1,idx2}", idx1);

        // including idx1 and idx3 (SAI and legacy)
        assertUnselectedIndexError("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH included_indexes={idx1,idx3}", idx3);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH included_indexes={idx1,idx3}", idx1);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH included_indexes={idx1,idx3}", idx1);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes={idx1,idx3}", idx3);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx1,idx3}", idx3);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx1,idx3}", idx1);

        // including idx2 and idx3 (SAI and legacy)
        assertUnselectedIndexError("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH included_indexes={idx2,idx3}", idx2);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH included_indexes={idx2,idx3}", idx3);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH included_indexes={idx2,idx3}", idx2);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH included_indexes={idx1,idx3}", idx3);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx2,idx3}", idx2);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH included_indexes={idx2,idx3}", idx3);

        // excluding idx1 (SAI)
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx1}", row1).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx1}", row2).selects(idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx1}", row3).selects(idx3);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx1}").selects(idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx1}").selects(idx3);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx1}").selects(idx2);

        // excluding idx2 (SAI)
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx2}", row1).selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx2}", row2).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx2}", row3).selects(idx3);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx2}").selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx2}").selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx2}").selects(idx3);

        // excluding idx3 (legacy)
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx3}", row1).selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx3}", row2).selects(idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx3}", row3).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx3}").selects(idx1, idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx3}").selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx3}").selects(idx2);

        // excluding idx1 and idx2 (both SAI)
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx2}", row1).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx2}", row2).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx2}", row3).selects(idx3);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx2}").selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx2}").selects(idx3);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx2}").selects(idx3);

        // excluding idx1 and idx3 (SAI and legacy)
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx3}", row1).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx3}", row2).selects(idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx3}", row3).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx3}").selects(idx2);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx3}").selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx3}").selects(idx2);

        // excluding idx2 and idx3 (SAI and legacy)
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx2,idx3}", row1).selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx2,idx3}", row2).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx2,idx3}", row3).selectsNone();
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx2,idx3}").selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx2,idx3}").selects(idx1);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx2,idx3}").selectsNone();
    }

    @Test
    public void testMultipleIndexesOnSameColumnSASI()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        String sasi = createIndex("CREATE CUSTOM INDEX sasi ON %s(v) USING 'org.apache.cassandra.index.sasi.SASIIndex'");
        String legacy = createIndex("CREATE INDEX legacy ON %s(v)");

        String insert = "INSERT INTO %s (k, v) VALUES (?, ?)";
        Object[] row1 = new Object[]{ 1, 1 };
        Object[] row2 = new Object[]{ 2, 2 };
        Object[] row3 = new Object[]{ 3, 3 };
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);

        // without any hints
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=0").selectsAnyOf(legacy, sasi);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=1", row1).selectsAnyOf(legacy, sasi);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v>1", row2, row3).selects(sasi); // legacy doesn't support >

        // including SASI
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=0 WITH included_indexes={sasi}").selects(sasi);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=1 WITH included_indexes={sasi}", row1).selects(sasi);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v>1 WITH included_indexes={sasi}", row2, row3).selects(sasi);

        // including legacy
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=0 WITH included_indexes={legacy}").selects(legacy);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=1 WITH included_indexes={legacy}", row1).selects(legacy);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v>1 WITH included_indexes={legacy}", legacy); // legacy doesn't support >

        // excluding SASI
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=0 WITH excluded_indexes={sasi}").selects(legacy);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=1 WITH excluded_indexes={sasi}", row1).selects(legacy);
        assertIndexDoesNotSupportOperator("SELECT * FROM %s WHERE v>1 WITH excluded_indexes={sasi}", "v"); // legacy doesn't support >
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v>1 ALLOW FILTERING WITH excluded_indexes={sasi}", row2, row3).selectsNone();

        // excluding legacy
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=0 WITH excluded_indexes={legacy}").selects(sasi);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=1 WITH excluded_indexes={legacy}", row1).selects(sasi);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v>1 WITH excluded_indexes={legacy}", row2, row3).selects(sasi); // legacy doesn't support >
    }

    @Test
    public void testMultipleIndexesOnSameColumnSAI()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        String sai = createIndex("CREATE CUSTOM INDEX sai ON %s(v) USING 'StorageAttachedIndex'");
        String legacy = createIndex("CREATE INDEX legacy ON %s(v)");

        String insert = "INSERT INTO %s (k, v) VALUES (?, ?)";
        Object[] row1 = new Object[]{ 1, 0 };
        Object[] row2 = new Object[]{ 2, 1 };
        execute(insert, row1);
        execute(insert, row2);

        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=0", row1).selects(sai);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=0 WITH included_indexes={sai}", row1).selects(sai);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=0 WITH included_indexes={legacy}", row1).selects(legacy);
        assertUnselectedIndexError("SELECT * FROM %s WHERE v=0 WITH included_indexes={sai,legacy}", legacy);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=0 WITH excluded_indexes={sai}", row1).selects(legacy);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=0 WITH excluded_indexes={legacy}", row1).selects(sai);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v=0 WITH excluded_indexes={sai,legacy}");
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v=0 ALLOW FILTERING WITH excluded_indexes={sai,legacy}", row1).selectsNone();
    }

    @Test
    public void testMultipleIndexesPerColumnAndUnsupportedEqOnAnalyzer()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        String legacy = createIndex("CREATE INDEX legacy ON %s(v)");
        String literal = createIndex("CREATE CUSTOM INDEX literal ON %s(v) USING 'StorageAttachedIndex'");
        String analyzed = createIndex("CREATE CUSTOM INDEX analyzed ON %s(v) USING 'StorageAttachedIndex' " +
                                      "WITH OPTIONS = { 'equals_behaviour_when_analyzed': 'UNSUPPORTED', 'index_analyzer': 'standard' }");

        String insert = "INSERT INTO %s (k, v) VALUES (?, ?)";
        Object[] row1 = new Object[]{ 1, "Richard Strauss" };
        Object[] row2 = new Object[]{ 2, "Johann Strauss" };
        Object[] row3 = new Object[]{ 3, "Strauss" };
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);

        // test index selection with EQ query
        String query = "SELECT * FROM %s WHERE v = 'Strauss' ";
        assertThatIndexQueryPlanFor(query, row3).selects(literal);
        assertThatIndexQueryPlanFor(query + "WITH included_indexes={legacy}", row3).selects(legacy);
        assertThatIndexQueryPlanFor(query + "WITH included_indexes={literal}", row3).selects(literal);
        assertUnselectedIndexError(query + "WITH included_indexes={analyzed}", analyzed); // analyzed is not applicable to =
        assertUnselectedIndexError(query + "WITH included_indexes={legacy,literal}", legacy); // literal is selected over idx1
        assertUnselectedIndexError(query + "WITH included_indexes={legacy,analyzed}", analyzed); // analyzed is not applicable to =
        assertUnselectedIndexError(query + "WITH included_indexes={literal,analyzed}", analyzed); // analyzed is not applicable to =
        assertUnselectedIndexError(query + "WITH included_indexes={legacy,literal,analyzed}", analyzed, legacy); // analyzed is not applicable to =, literal is selected over idx1
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={legacy}", row3).selects(literal);
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={literal}", row3).selects(legacy);
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={analyzed}", row3).selects(literal);
        assertIndexDoesNotSupportOperator(query + "WITH excluded_indexes={legacy,literal}", "v");
        assertThatIndexQueryPlanFor(query + "ALLOW FILTERING WITH excluded_indexes={legacy,literal}", row3).selectsNone();
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={legacy,analyzed}", row3).selects(literal);
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={literal,analyzed}", row3).selects(legacy);
        assertNeedsAllowFiltering(query + "WITH excluded_indexes={legacy,literal,analyzed}");
        assertThatIndexQueryPlanFor(query + "ALLOW FILTERING WITH excluded_indexes={legacy,literal,analyzed}", row3).selectsNone();

        // test the same EQ query with a different value
        query = "SELECT * FROM %s WHERE v = 'Richard Strauss' ";
        assertThatIndexQueryPlanFor(query, row1).selects(literal);
        assertThatIndexQueryPlanFor(query + "WITH included_indexes={legacy}", row1).selects(legacy);
        assertThatIndexQueryPlanFor(query + "WITH included_indexes={literal}", row1).selects(literal);
        assertUnselectedIndexError(query + "WITH included_indexes={analyzed}", analyzed); // analyzed is not applicable to =
        assertUnselectedIndexError(query + "WITH included_indexes={legacy,literal}", legacy); // literal is selected over legacy
        assertUnselectedIndexError(query + "WITH included_indexes={legacy,analyzed}", analyzed);
        assertUnselectedIndexError(query + "WITH included_indexes={literal,analyzed}", analyzed);
        assertUnselectedIndexError(query + "WITH included_indexes={legacy,literal,analyzed}", analyzed, legacy);
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={legacy}", row1).selects(literal);
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={literal}", row1).selects(legacy);
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={analyzed}", row1).selects(literal);
        assertIndexDoesNotSupportOperator(query + "WITH excluded_indexes={legacy,literal}", "v");
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={legacy,analyzed}", row1).selects(literal);
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={literal,analyzed}", row1).selects(legacy);
        assertNeedsAllowFiltering(query + "WITH excluded_indexes={legacy,literal,analyzed}");
        assertThatIndexQueryPlanFor(query + "ALLOW FILTERING WITH excluded_indexes={legacy,literal,analyzed}", row1).selectsNone();

        // test index selection with MATCH query
        query = "SELECT * FROM %s WHERE v : 'strauss' ";
        assertThatIndexQueryPlanFor(query, row1, row2, row3).selects(analyzed);
        assertUnselectedIndexError(query + "WITH included_indexes={legacy}", legacy); // legacy is not applicable to :
        assertUnselectedIndexError(query + "WITH included_indexes={literal}", literal); // literal is not applicable to :
        assertThatIndexQueryPlanFor(query + "WITH included_indexes={analyzed}", row1, row2, row3).selects(analyzed);
        assertUnselectedIndexError(query + "WITH included_indexes={legacy,literal}", legacy, literal); // legacy and literal are not applicable to :
        assertUnselectedIndexError(query + "WITH included_indexes={legacy,analyzed}", legacy);
        assertUnselectedIndexError(query + "WITH included_indexes={literal,analyzed}", literal);
        assertUnselectedIndexError(query + "WITH included_indexes={legacy,literal,analyzed}", legacy, literal);
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={legacy}", row1, row2, row3).selects(analyzed);
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={literal}", row1, row2, row3).selects(analyzed);
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes={analyzed}", "v");
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={legacy,literal}", row1, row2, row3).selects(analyzed);
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes={legacy,analyzed}", "v");
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes={literal,analyzed}", "v");
        assertMatchNeedsIndex(query + "WITH excluded_indexes={legacy,literal,analyzed}", "v", "strauss");

        // test the same MATCH query with a different value
        query = "SELECT * FROM %s WHERE v : 'Richard Strauss' ";
        assertThatIndexQueryPlanFor(query, row1).selects(analyzed);
        assertUnselectedIndexError(query + "WITH included_indexes={legacy}", legacy); // legacy is not applicable to :
        assertUnselectedIndexError(query + "WITH included_indexes={literal}", literal); // literal is not applicable to :
        assertThatIndexQueryPlanFor(query + "WITH included_indexes={analyzed}", row1).selects(analyzed);
        assertUnselectedIndexError(query + "WITH included_indexes={legacy,literal}", legacy, literal); // legacy and literal are not applicable to :
        assertUnselectedIndexError(query + "WITH included_indexes={legacy,analyzed}", legacy);
        assertUnselectedIndexError(query + "WITH included_indexes={literal,analyzed}", literal);
        assertUnselectedIndexError(query + "WITH included_indexes={legacy,literal,analyzed}", legacy, literal);
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={legacy}", row1).selects(analyzed);
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={literal}", row1).selects(analyzed);
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes={analyzed}", "v");
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={legacy,literal}", row1).selects(analyzed);
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes={legacy,analyzed}", "v");
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes={literal,analyzed}", "v");
        assertMatchNeedsIndex(query + "WITH excluded_indexes={legacy,literal,analyzed}", "v", "Richard Strauss");
    }

    @Test
    public void testMultipleIndexesPerColumnAndMatchEqOnAnalyzer()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        String legacy = createIndex("CREATE INDEX legacy ON %s(v)");
        String literal = createIndex("CREATE CUSTOM INDEX literal ON %s(v) USING 'StorageAttachedIndex'");
        String analyzed = createIndex("CREATE CUSTOM INDEX analyzed ON %s(v) USING 'StorageAttachedIndex' " +
                                  "WITH OPTIONS = { 'equals_behaviour_when_analyzed': 'MATCH', 'index_analyzer': 'standard' }");

        String insert = "INSERT INTO %s (k, v) VALUES (?, ?)";
        Object[] row1 = new Object[]{ 1, "Richard Strauss" };
        Object[] row2 = new Object[]{ 2, "Johann Strauss" };
        Object[] row3 = new Object[]{ 3, "Strauss" };
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);

        // test index selection with EQ query, the hints will disambiguate the query and will produce different results
        // depending on the selected index
        String query = "SELECT * FROM %s WHERE v = 'Strauss' ";
        assertEqualityPredicateIsAmbiguous(query);
        assertThatIndexQueryPlanFor(query + "WITH included_indexes={legacy}", row3).selects(legacy);
        assertThatIndexQueryPlanFor(query + "WITH included_indexes={literal}", row3).selects(literal);
        assertThatIndexQueryPlanFor(query + "WITH included_indexes={analyzed}", row1, row2, row3).selects(analyzed);
        assertUnselectedIndexError(query + "WITH included_indexes={legacy,literal}", legacy);
        assertEqualityPredicateIsAmbiguous(query + "WITH included_indexes={legacy,analyzed}");
        assertEqualityPredicateIsAmbiguous(query + "WITH included_indexes={literal,analyzed}");
        assertEqualityPredicateIsAmbiguous(query + "WITH included_indexes={legacy,literal,analyzed}");
        assertEqualityPredicateIsAmbiguous(query + "WITH excluded_indexes={legacy}");
        assertEqualityPredicateIsAmbiguous(query + "WITH excluded_indexes={literal}");
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={analyzed}", row3).selects(literal);
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={legacy,literal}", row1, row2, row3).selects(analyzed);
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={legacy,analyzed}", row3).selects(literal);
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={literal,analyzed}", row3).selects(legacy);
        assertNeedsAllowFiltering(query + "WITH excluded_indexes={legacy,literal,analyzed}");
        assertThatIndexQueryPlanFor(query + "ALLOW FILTERING WITH excluded_indexes={legacy,literal,analyzed}", row3).selectsNone();

        // test the same EQ query with a different value
        query = "SELECT * FROM %s WHERE v = 'Richard Strauss' ";
        assertEqualityPredicateIsAmbiguous(query);
        assertThatIndexQueryPlanFor(query + "WITH included_indexes={legacy}", row1).selects(legacy);
        assertThatIndexQueryPlanFor(query + "WITH included_indexes={literal}", row1).selects(literal);
        assertThatIndexQueryPlanFor(query + "WITH included_indexes={analyzed}", row1).selects(analyzed);
        assertUnselectedIndexError(query + "WITH included_indexes={legacy,literal}", legacy);
        assertEqualityPredicateIsAmbiguous(query + "WITH included_indexes={legacy,analyzed}");
        assertEqualityPredicateIsAmbiguous(query + "WITH included_indexes={literal,analyzed}");
        assertEqualityPredicateIsAmbiguous(query + "WITH included_indexes={legacy,literal,analyzed}");
        assertEqualityPredicateIsAmbiguous(query + "WITH excluded_indexes={legacy}");
        assertEqualityPredicateIsAmbiguous(query + "WITH excluded_indexes={literal}");
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={analyzed}", row1).selects(literal);
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={legacy,literal}", row1).selects(analyzed);
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={legacy,analyzed}", row1).selects(literal);
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={literal,analyzed}", row1).selects(legacy);
        assertNeedsAllowFiltering(query + "WITH excluded_indexes={legacy,literal,analyzed}");
        assertThatIndexQueryPlanFor(query + "ALLOW FILTERING WITH excluded_indexes={legacy,literal,analyzed}", row1).selectsNone();

        // test index selection with MATCH query
        query = "SELECT * FROM %s WHERE v : 'strauss' ";
        assertThatIndexQueryPlanFor(query, row1, row2, row3).selects(analyzed);
        assertUnselectedIndexError(query + "WITH included_indexes={legacy}", legacy); // legacy is not applicable to :
        assertUnselectedIndexError(query + "WITH included_indexes={literal}", literal); // literal is not applicable to :
        assertThatIndexQueryPlanFor(query + "WITH included_indexes={analyzed}", row1, row2, row3).selects(analyzed);
        assertUnselectedIndexError(query + "WITH included_indexes={legacy,literal}", legacy, literal); // legacy and literal are not applicable to :
        assertUnselectedIndexError(query + "WITH included_indexes={legacy,analyzed}", legacy);
        assertUnselectedIndexError(query + "WITH included_indexes={literal,analyzed}", literal);
        assertUnselectedIndexError(query + "WITH included_indexes={legacy,literal,analyzed}", legacy, literal);
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={legacy}", row1, row2, row3).selects(analyzed);
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={literal}", row1, row2, row3).selects(analyzed);
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes={analyzed}", "v");
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={legacy,literal}", row1, row2, row3).selects(analyzed);
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes={legacy,analyzed}", "v");
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes={literal,analyzed}", "v");
        assertMatchNeedsIndex(query + "WITH excluded_indexes={legacy,literal,analyzed}", "v", "strauss");

        // test the same MATCH query with a different value
        query = "SELECT * FROM %s WHERE v : 'Richard Strauss' ";
        assertThatIndexQueryPlanFor(query, row1).selects(analyzed);
        assertUnselectedIndexError(query + "WITH included_indexes={legacy}", legacy); // legacy is not applicable to :
        assertUnselectedIndexError(query + "WITH included_indexes={literal}", literal); // literal is not applicable to :
        assertThatIndexQueryPlanFor(query + "WITH included_indexes={analyzed}", row1).selects(analyzed);
        assertUnselectedIndexError(query + "WITH included_indexes={legacy,literal}", legacy, literal); // legacy and literal are not applicable to :
        assertUnselectedIndexError(query + "WITH included_indexes={legacy,analyzed}", legacy);
        assertUnselectedIndexError(query + "WITH included_indexes={literal,analyzed}", literal);
        assertUnselectedIndexError(query + "WITH included_indexes={legacy,literal,analyzed}", legacy, literal);
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={legacy}", row1).selects(analyzed);
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={literal}", row1).selects(analyzed);
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes={analyzed}", "v");
        assertThatIndexQueryPlanFor(query + "WITH excluded_indexes={legacy,literal}", row1).selects(analyzed);
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes={legacy,analyzed}", "v");
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes={literal,analyzed}", "v");
        assertMatchNeedsIndex(query + "WITH excluded_indexes={legacy,literal,analyzed}", "v", "Richard Strauss");
    }

    @Test
    public void testMultipleIndexesPerColumnAndContains()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v list<text>)");
        String legacy = createIndex("CREATE INDEX legacy ON %s(v)");
        String literal = createIndex("CREATE CUSTOM INDEX literal ON %s(v) USING 'StorageAttachedIndex'");
        String analyzed = createIndex("CREATE CUSTOM INDEX analyzed ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");

        String insert = "INSERT INTO %s (k, v) VALUES (?, ?)";
        Object[] row1 = new Object[]{ 1, list("Johann Strauss") };
        Object[] row2 = new Object[]{ 2, list("Richard Strauss", "Johann Sebastian Bach") };
        execute(insert, row1);
        execute(insert, row2);

        // CONTAINS and NOT CONTAINS with hints including the legacy index.
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard Strauss' WITH included_indexes={legacy}", row2).selects(legacy);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Strauss' WITH included_indexes={legacy}").selects(legacy);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Johann' WITH included_indexes={legacy}").selects(legacy);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard' WITH included_indexes={legacy}").selects(legacy);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Debussy' WITH included_indexes={legacy}").selects(legacy);
        // TODO CNDB-13925: NOT CONTAINS isn't supported by the legacy index, so the hint will be ignored and the query
        //  will be executed by either of the SAI indexes. The query will return different results depending on what
        //  index gets selected by the query planner, which is based on estimated about selectivity rather than semantics.
        //  This is the same that happens when there are no hints. This is a bug introduced when BM25 added support for
        //  multiple indexes in the same column, and it's independent of index hints.
        // assertNotContainsPredicateIsAmbiguous("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard Strauss' WITH included_indexes={legacy}");
        // assertNotContainsPredicateIsAmbiguous("SELECT * FROM %s WHERE v NOT CONTAINS 'Strauss' WITH included_indexes={legacy}");
        // assertNotContainsPredicateIsAmbiguous("SELECT * FROM %s WHERE v NOT CONTAINS 'Johann' WITH included_indexes={legacy}");
        // assertNotContainsPredicateIsAmbiguous("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard' WITH included_indexes={legacy}");
        // assertNotContainsPredicateIsAmbiguous("SELECT * FROM %s WHERE v NOT CONTAINS 'Debussy' WITH included_indexes={legacy}");

        // CONTAINS and NOT CONTAINS with hints including the non-analyzed index.
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard Strauss' WITH included_indexes={literal}", row2).selects(literal);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Strauss' WITH included_indexes={literal}").selects(literal);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Johann' WITH included_indexes={literal}").selects(literal);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard' WITH included_indexes={literal}").selects(literal);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Debussy' WITH included_indexes={literal}").selects(literal);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard Strauss' WITH included_indexes={literal}", row1).selects(literal);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Strauss' WITH included_indexes={literal}").selects(literal);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Johann' WITH included_indexes={literal}").selects(literal);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard' WITH included_indexes={literal}", row1).selects(literal);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Debussy' WITH included_indexes={literal}", row1, row2).selects(literal);

        // CONTAINS and NOT CONTAINS with hints including the analyzed index.
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard Strauss' WITH included_indexes={analyzed}", row2).selects(analyzed);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Strauss' WITH included_indexes={analyzed}", row1, row2).selects(analyzed);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Johann' WITH included_indexes={analyzed}", row1, row2).selects(analyzed);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard' WITH included_indexes={analyzed}", row2).selects(analyzed);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Debussy' WITH included_indexes={analyzed}").selects(analyzed);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard Strauss' WITH included_indexes={analyzed}").selects(analyzed);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Strauss' WITH included_indexes={analyzed}").selects(analyzed);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Johann' WITH included_indexes={analyzed}").selects(analyzed);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard' WITH included_indexes={analyzed}", row1).selects(analyzed);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Debussy' WITH included_indexes={analyzed}", row1, row2).selects(analyzed);

        // CONTAINS and NOT CONTAINS with hints excluding the legacy index.
        // TODO CNDB-13925: We are not specifying what SAI index should be used, so the query will return different
        //  results depending on what index gets selected by the query planner, which is based on estimated about
        //  selectivity rather than semantics. This is the same that happens when there are no hints. This is a bug
        //  introduced when BM25 added support for multiple indexes in the same column, and it's independent of index
        //  hints.
        // assertNotContainsPredicateIsAmbiguous("SELECT * FROM %s WHERE v CONTAINS 'Richard Strauss' WITH excluded_indexes={legacy}");
        // assertNotContainsPredicateIsAmbiguous("SELECT * FROM %s WHERE v CONTAINS 'Strauss' WITH excluded_indexes={legacy}");
        // assertNotContainsPredicateIsAmbiguous("SELECT * FROM %s WHERE v CONTAINS 'Johann' WITH excluded_indexes={legacy}");
        // assertNotContainsPredicateIsAmbiguous("SELECT * FROM %s WHERE v CONTAINS 'Richard' WITH excluded_indexes={legacy}");
        // assertNotContainsPredicateIsAmbiguous("SELECT * FROM %s WHERE v CONTAINS 'Debussy' WITH excluded_indexes={legacy}");
        // assertNotContainsPredicateIsAmbiguous("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard Strauss' WITH excluded_indexes={legacy}");
        // assertNotContainsPredicateIsAmbiguous("SELECT * FROM %s WHERE v NOT CONTAINS 'Strauss' WITH excluded_indexes={legacy}");
        // assertNotContainsPredicateIsAmbiguous("SELECT * FROM %s WHERE v NOT CONTAINS 'Johann' WITH excluded_indexes={legacy}");
        // assertNotContainsPredicateIsAmbiguous("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard' WITH excluded_indexes={legacy}");
        // assertNotContainsPredicateIsAmbiguous("SELECT * FROM %s WHERE v NOT CONTAINS 'Debussy' WITH excluded_indexes={legacy}");

        // CONTAINS and NOT CONTAINS with hints excluding the non-analyzed index.
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard Strauss' WITH excluded_indexes={literal}", row2).selects(analyzed);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Strauss' WITH excluded_indexes={literal}", row1, row2).selects(analyzed);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Johann' WITH excluded_indexes={literal}", row1, row2).selects(analyzed);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard' WITH excluded_indexes={literal}", row2).selects(analyzed);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Debussy' WITH excluded_indexes={literal}").selects(analyzed);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard Strauss' WITH excluded_indexes={literal}").selects(analyzed);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Strauss' WITH excluded_indexes={literal}").selects(analyzed);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Johann' WITH excluded_indexes={literal}").selects(analyzed);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard' WITH excluded_indexes={literal}", row1).selects(analyzed);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Debussy' WITH excluded_indexes={literal}", row1, row2).selects(analyzed);

        // CONTAINS and NOT CONTAINS with hints excluding the analyzed index.
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard Strauss' WITH excluded_indexes={analyzed}", row2).selects(literal);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Strauss' WITH excluded_indexes={analyzed}").selects(literal);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Johann' WITH excluded_indexes={analyzed}").selects(literal);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Richard' WITH excluded_indexes={analyzed}").selects(literal);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v CONTAINS 'Debussy' WITH excluded_indexes={analyzed}").selects(literal);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard Strauss' WITH excluded_indexes={analyzed}", row1).selects(literal);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Strauss' WITH excluded_indexes={analyzed}", row1, row2).selects(literal);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Johann' WITH excluded_indexes={analyzed}", row1, row2).selects(literal);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Richard' WITH excluded_indexes={analyzed}", row1, row2).selects(literal);
        assertThatIndexQueryPlanFor("SELECT * FROM %s WHERE v NOT CONTAINS 'Debussy' WITH excluded_indexes={analyzed}", row1, row2).selects(literal);
    }

    private void assertUnselectedIndexError(String query, String... indexes)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(IndexHints.NON_INCLUDABLE_INDEX_ERROR, String.join(",", indexes)));
    }

    private void assertAnyUnselectedIndexError(String query, String one, String other)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .matches(e -> e.getMessage().equals(String.format(IndexHints.NON_INCLUDABLE_INDEX_ERROR, one))
                             || e.getMessage().equals(String.format(IndexHints.NON_INCLUDABLE_INDEX_ERROR, other)));
    }

    private void assertNeedsAllowFiltering(String query)
    {
        assertNeedsAllowFiltering(() -> execute(query));
    }

    private void assertNeedsAllowFiltering(ThrowableAssert.ThrowingCallable callable)
    {
        Assertions.assertThatThrownBy(callable)
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE);
    }

    private void assertIndexDoesNotSupportOperator(String query, String column)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(StatementRestrictions.HAS_UNSUPPORTED_INDEX_RESTRICTION_MESSAGE_SINGLE, column));
    }

    private void assertIndexDoesNotSupportAnalyzerMatches(String query, String column)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(StatementRestrictions.INDEX_DOES_NOT_SUPPORT_ANALYZER_MATCHES_MESSAGE, column));
    }

    private void assertMatchNeedsIndex(String query, String column, String value)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(String.format(StatementRestrictions.RESTRICTION_REQUIRES_INDEX_MESSAGE,
                                                      ':',
                                                      String.format("%s : '%s'", column, value)));
    }

    private void assertEqualityPredicateIsAmbiguous(String query)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining("equality predicate is ambiguous");
    }

    private static Set<IndexMetadata> indexes(IndexMetadata... indexes)
    {
        Set<IndexMetadata> set = new HashSet<>(indexes.length);
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
