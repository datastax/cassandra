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

import javax.annotation.Nullable;

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
 *    <li>Preferred indexes should be included in the {@link Index.QueryPlan} for the query when possible.</li>
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
        String query = "SELECT * FROM %s WHERE a = 1 ALLOW FILTERING ";

        // valid queries without index hints
        execute(query);
        execute(query + "WITH preferred_indexes = {}");
        execute(query + "WITH excluded_indexes = {}");
        execute(query + "WITH preferred_indexes = {} AND excluded_indexes = {}");

        // index hints with unparseable properties
        assertInvalidThrowMessage("Invalid value for property 'preferred_indexes'. It should be a set of identifiers.",
                                  SyntaxException.class,
                                  query + "WITH preferred_indexes = {'a': 'b'}");

        // invalid queries with unknown index (no index has been created yet)
        String missingIndexError = String.format(MISSING_INDEX_ERROR, currentTable(), "idx1");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  query + "WITH preferred_indexes = {idx1}");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  query + "WITH preferred_indexes = {idx1, idx2}");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  query + "WITH excluded_indexes = {idx1}");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  query + "WITH excluded_indexes = {idx1, idx2}");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  query + "WITH preferred_indexes = {idx1} AND excluded_indexes = {idx2}");

        // create a single index and test queries with it
        createIndex(String.format("CREATE CUSTOM INDEX idx1 ON %%s(a) USING '%s'", GroupedIndex.class.getName()));
        execute(query + "WITH preferred_indexes = {}");
        execute(query + "WITH preferred_indexes = {idx1}");
        missingIndexError = String.format(MISSING_INDEX_ERROR, currentTable(), "idx2");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  "SELECT * FROM %s WHERE a = 1 WITH preferred_indexes = {idx2}");
        execute(query + "WITH excluded_indexes = {}");
        execute(query + "WITH excluded_indexes = {idx1}");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  query + "WITH excluded_indexes = {idx2}");
        execute("SELECT * FROM %s WHERE a = 1 WITH preferred_indexes = {} AND excluded_indexes = {}");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  query + "WITH preferred_indexes = {idx1} AND excluded_indexes = {idx2}");
        assertInvalidThrowMessage(missingIndexError,
                                  InvalidRequestException.class,
                                  query + "WITH preferred_indexes = {idx2} AND excluded_indexes = {idx1}");
        assertInvalidThrowMessage(CONFLICTING_INDEXES_ERROR + "idx1",
                                  InvalidRequestException.class,
                                  query + "WITH preferred_indexes = {idx1} AND excluded_indexes = {idx1}");

        // create a second index and test queries with both indexes
        createIndex(String.format("CREATE CUSTOM INDEX idx2 ON %%s(b) USING '%s'", GroupedIndex.class.getName()));
        execute(query + "WITH preferred_indexes = {}");
        execute(query + "WITH preferred_indexes = {idx1}");
        execute(query + "WITH preferred_indexes = {idx2}");
        execute(query + "WITH preferred_indexes = {idx1, idx2}");
        execute(query + "WITH excluded_indexes = {}");
        execute(query + "WITH excluded_indexes = {idx1}");
        execute(query + "WITH excluded_indexes = {idx2}");
        execute(query + "WITH excluded_indexes = {idx1, idx2}");
        execute(query + "WITH preferred_indexes = {} AND excluded_indexes = {}");
        execute(query + "WITH preferred_indexes = {idx1} AND excluded_indexes = {idx2}");
        execute(query + "WITH preferred_indexes = {idx2} AND excluded_indexes = {idx1}");
        assertInvalidThrowMessage(CONFLICTING_INDEXES_ERROR + "idx1",
                                  InvalidRequestException.class,
                                  query + "WITH preferred_indexes = {idx1} AND excluded_indexes = {idx1}");
        assertInvalidThrowMessage(CONFLICTING_INDEXES_ERROR + "idx2",
                                  InvalidRequestException.class,
                                  query + "WITH preferred_indexes = {idx2} AND excluded_indexes = {idx2}");
        assertInvalidThrowMessage(CONFLICTING_INDEXES_ERROR + "idx1",
                                  InvalidRequestException.class,
                                  query + "WITH preferred_indexes = {idx1, idx2} AND excluded_indexes = {idx1}");
        assertInvalidThrowMessage(CONFLICTING_INDEXES_ERROR + "idx1",
                                  InvalidRequestException.class,
                                  query + "WITH preferred_indexes = {idx1} AND excluded_indexes = {idx1, idx2}");
        assertInvalidThrowMessage(CONFLICTING_INDEXES_ERROR + "idx1,idx2",
                                  InvalidRequestException.class,
                                  query + "WITH preferred_indexes = {idx1, idx2} AND excluded_indexes = {idx1, idx2}");

        // invalid queries referencing other keyspaces
        String wrongKeyspaceError = String.format(WRONG_KEYSPACE_ERROR, "ks1.idx1");
        assertInvalidThrowMessage(wrongKeyspaceError,
                                  InvalidRequestException.class,
                                  query + "WITH preferred_indexes = {ks1.idx1}");
        assertInvalidThrowMessage(wrongKeyspaceError,
                                  InvalidRequestException.class,
                                  query + "WITH excluded_indexes = {ks1.idx1}");

        // valid queries with explicit keyspace
        String keyspace = keyspace();
        execute(query + "WITH preferred_indexes = {" + keyspace + ".idx1}");
        execute(query + "WITH preferred_indexes = {" + keyspace + ".idx1, idx2}");
        execute(query + "WITH preferred_indexes = {" + keyspace + ".idx1, " + keyspace + ".idx2}");
        execute(query + "WITH excluded_indexes = {" + keyspace + ".idx1}");
        execute(query + "WITH excluded_indexes = {" + keyspace + ".idx1, idx2}");
        execute(query + "WITH preferred_indexes = {" + keyspace + ".idx1} AND excluded_indexes = {idx2}");
        execute(query + "WITH preferred_indexes = {" + keyspace + ".idx1} AND excluded_indexes = {" + keyspace + ".idx2}");
        execute(query + "WITH excluded_indexes = {" + keyspace + ".idx1} AND preferred_indexes = {idx2}");
        execute(query + "WITH excluded_indexes = {" + keyspace + ".idx1} AND preferred_indexes = {" + keyspace + ".idx2}");

        // valid queries with quoted names
        execute(query + "WITH preferred_indexes = {\"idx1\"}");
        execute(query + "WITH excluded_indexes = {\"idx1\", idx2}");
        execute(query + "WITH excluded_indexes = {\"idx1\", \"idx2\"}");
        execute(query + "WITH excluded_indexes = {\"idx1\"}");
        execute(query + "WITH excluded_indexes = {\"idx1\", idx2}");
        execute(query + "WITH excluded_indexes = {\"idx1\", \"idx2\"}");
        execute(query + "WITH preferred_indexes = {\"idx1\"} AND excluded_indexes = {idx2}");
        execute(query + "WITH preferred_indexes = {\"idx1\"} AND excluded_indexes = {\"idx2\"}");
        execute(query + "WITH excluded_indexes = {\"idx1\"} AND preferred_indexes = {idx2}");
        execute(query + "WITH excluded_indexes = {\"idx1\"} AND preferred_indexes = {\"idx2\"}");
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
                  .doesNotContain("preferred_indexes")
                  .doesNotContain("excluded_indexes");

        // with empty hints
        formattedQuery = formatQuery("SELECT * FROM %%s WHERE a = 0 AND b = 0 WITH preferred_indexes = {} AND excluded_indexes = {}");
        command = parseReadCommand(formattedQuery);
        Assertions.assertThat(command.toCQLString())
                  .doesNotContain("preferred_indexes")
                  .doesNotContain("excluded_indexes");

        // with preferred indexes only
        formattedQuery = formatQuery("SELECT * FROM %%s WHERE a = 0 AND b = 0 WITH preferred_indexes = {idx1, idx2}");
        command = parseReadCommand(formattedQuery);
        Assertions.assertThat(command.toCQLString())
                  .contains(" WITH preferred_indexes = {idx1, idx2}")
                  .doesNotContain("excluded_indexes");

        // with excluded indexes only
        formattedQuery = formatQuery("SELECT * FROM %%s WHERE a = 0 AND b = 0 ALLOW FILTERING WITH excluded_indexes = {idx1, idx2}");
        command = parseReadCommand(formattedQuery);
        Assertions.assertThat(command.toCQLString())
                  .contains(" WITH excluded_indexes = {idx1, idx2}")
                  .doesNotContain("preferred_indexes");

        // with both preferred and excluded indexes
        formattedQuery = formatQuery("SELECT * FROM %%s WHERE a = 0 AND b = 0 ALLOW FILTERING WITH preferred_indexes = {idx1} AND excluded_indexes = {idx2}");
        command = parseReadCommand(formattedQuery);
        Assertions.assertThat(command.toCQLString())
                  .contains(" WITH preferred_indexes = {idx1} AND excluded_indexes = {idx2}");
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
        IndexMetadata idx1 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx1").getIndexMetadata();
        IndexMetadata idx2 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx2").getIndexMetadata();
        String query = "SELECT * FROM %s WHERE a = 1 ALLOW FILTERING ";

        // unespecified hints should be mapped to NONE
        testTransport(query, IndexHints.NONE);
        testTransport(query + "WITH preferred_indexes = {}", IndexHints.NONE);
        testTransport(query + "WITH excluded_indexes = {}", IndexHints.NONE);

        // hints with a single index
        testTransport(query + "WITH preferred_indexes = {idx1}", IndexHints.create(indexes(idx1), indexes()));
        testTransport(query + "WITH excluded_indexes = {idx1}", IndexHints.create(indexes(), indexes(idx1)));

        // hints with multiple indexes
        testTransport(query + "WITH preferred_indexes = {idx1, idx2}", IndexHints.create(indexes(idx1, idx2), indexes()));
        testTransport(query + "WITH excluded_indexes = {idx1, idx2}", IndexHints.create(indexes(), indexes(idx1, idx2)));
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
        createIndex("CREATE INDEX idx1 ON %s(v1)");
        createIndex("CREATE INDEX idx2 ON %s(v2)");

        Index idx1 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx1");
        Index idx2 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx2");

        String insert = "INSERT INTO %s (k, v1, v2, v3) VALUES (?, ?, ?, ?)";
        Object[] row1 = new Object[]{ 1, 0, 1, 1 };
        Object[] row2 = new Object[]{ 2, 1, 0, 1 };
        Object[] row3 = new Object[]{ 3, 1, 1, 0 };
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);

        // without any hints
        assertThatPlan("SELECT * FROM %s ALLOW FILTERING", row1, row2, row3).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING", row1).selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING", row2).selects(idx2);
        assertThatPlan("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING", row3);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING").selectsAnyOf(idx1, idx2);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING").selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING").selects(idx2);

        // with a single restriction and preferred indexes
        assertThatPlan("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH preferred_indexes = {idx1}", row1).selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH preferred_indexes = {idx2}", row1).selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH preferred_indexes = {idx1}", row2).selects(idx2);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH preferred_indexes = {idx2}", row2).selects(idx2);
        assertThatPlan("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH preferred_indexes = {idx1}", row3).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH preferred_indexes = {idx2}", row3).selectsNone();

        // with a single restriction and excluded indexes
        assertThatPlan("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes = {idx1}", row1).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes = {idx2}", row1).selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes = {idx1}", row2).selects(idx2);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes = {idx2}", row2).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes = {idx1}", row3).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes = {idx2}", row3).selectsNone();

        // with restrictions in two columns (v1 and v2) and preferred indexes
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH preferred_indexes = {idx1}").selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH preferred_indexes = {idx2}").selects(idx2);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH preferred_indexes = {idx1, idx2}").selectsAnyOf(idx1, idx2);

        // with restrictions in two columns (v1 and v2) and excluded indexes
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes = {idx1}").selects(idx2);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes = {idx2}").selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes = {idx1, idx2}").selectsNone();

        // with restrictions in two columns (v1 and v3) and preferred indexes
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes = {idx1}").selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes = {idx2}").selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes = {idx1, idx2}").selects(idx1);

        // with restrictions in two columns (v1 and v3) and excluded indexes
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes = {idx1}").selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes = {idx2}").selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes = {idx1, idx2}").selectsNone();

        // without restrictions
        assertThatPlan("SELECT * FROM %s ALLOW FILTERING WITH preferred_indexes = {idx1}", row1, row2, row3).selectsNone();
        assertThatPlan("SELECT * FROM %s ALLOW FILTERING WITH excluded_indexes = {idx1}", row1, row2, row3).selectsNone();

        // prepared statements
        prepare("SELECT * FROM %s WHERE v1=? ALLOW FILTERING WITH preferred_indexes = {idx1}");
        prepare("SELECT * FROM %s WHERE v1=? ALLOW FILTERING WITH excluded_indexes = {idx1}");
    }

    @Test
    public void testLegacyIndexWithoutAllowFiltering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int)");
        createIndex("CREATE INDEX idx1 ON %s(v1)");
        createIndex("CREATE INDEX idx2 ON %s(v2)");

        Index idx1 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx1");
        Index idx2 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx2");

        String insert = "INSERT INTO %s (k, v1, v2, v3) VALUES (?, ?, ?, ?)";
        Object[] row1 = new Object[]{ 1, 0, 1, 1 };
        Object[] row2 = new Object[]{ 2, 1, 0, 1 };
        Object[] row3 = new Object[]{ 3, 1, 1, 0 };
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);

        // without any hints
        assertThatPlan("SELECT * FROM %s", row1, row2, row3).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v1=0", row1).selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v2=0", row2).selects(idx2);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v2=0 AND v3=0");

        // with a single restriction and preferred indexes
        assertThatPlan("SELECT * FROM %s WHERE v1=0 WITH preferred_indexes = {idx1}", row1).selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 WITH preferred_indexes = {idx2}", row1).selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 WITH preferred_indexes = {idx1}", row2).selects(idx2);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 WITH preferred_indexes = {idx2}", row2).selects(idx2);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0 WITH preferred_indexes = {idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0 WITH preferred_indexes = {idx2}");

        // with a single restriction and excluded indexes
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 WITH excluded_indexes = {idx1}");
        assertThatPlan("SELECT * FROM %s WHERE v1=0 WITH excluded_indexes = {idx2}", row1).selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 WITH excluded_indexes = {idx1}", row2).selects(idx2);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v2=0 WITH excluded_indexes = {idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0 WITH excluded_indexes = {idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0 WITH excluded_indexes = {idx2}");

        // with restrictions in two columns (v1 and v2) and preferred indexes
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH preferred_indexes = {idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH preferred_indexes = {idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH preferred_indexes = {idx1, idx2}");

        // with restrictions in two columns (v1 and v2) and excluded indexes
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH excluded_indexes = {idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH excluded_indexes = {idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH excluded_indexes = {idx1, idx2}");

        // with restrictions in two columns (v1 and v3) and preferred indexes
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH preferred_indexes = {idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH preferred_indexes = {idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH preferred_indexes = {idx1, idx2}");

        // with restrictions in two columns (v1 and v3) and excluded indexes
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH excluded_indexes = {idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH excluded_indexes = {idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH excluded_indexes = {idx1, idx2}");

        // without restrictions
        assertThatPlan("SELECT * FROM %s WITH preferred_indexes = {idx1}", row1, row2, row3).selectsNone();
        assertThatPlan("SELECT * FROM %s WITH excluded_indexes = {idx1}", row1, row2, row3).selectsNone();

        // prepared statements
        prepare("SELECT * FROM %s WHERE v1=?");
        prepare("SELECT * FROM %s WHERE v1=? WITH preferred_indexes = {idx1}");
        assertNeedsAllowFiltering(() -> prepare("SELECT * FROM %s WHERE v1=0 WITH excluded_indexes = {idx1}"));
    }

    @Test
    public void testSAIWithAllowFiltering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int)");
        createIndex("CREATE CUSTOM INDEX idx1 ON %s(v1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX idx2 ON %s(v2) USING 'StorageAttachedIndex'");

        Index idx1 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx1");
        Index idx2 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx2");

        String insert = "INSERT INTO %s (k, v1, v2, v3) VALUES (?, ?, ?, ?)";
        Object[] row1 = new Object[]{ 1, 0, 1, 1 };
        Object[] row2 = new Object[]{ 2, 1, 0, 1 };
        Object[] row3 = new Object[]{ 3, 1, 1, 0 };
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);

        // without any hints
        assertThatPlan("SELECT * FROM %s ALLOW FILTERING", row1, row2, row3).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING", row1).selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING", row2).selects(idx2);
        assertThatPlan("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING", row3).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING").selects(idx1, idx2);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING").selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING").selects(idx2);

        // with a single restriction and preferred indexes
        assertThatPlan("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH preferred_indexes = {idx1}", row1).selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH preferred_indexes = {idx2}", row1).selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH preferred_indexes = {idx1}", row2).selects(idx2);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH preferred_indexes = {idx2}", row2).selects(idx2);
        assertThatPlan("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH preferred_indexes = {idx1}", row3).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH preferred_indexes = {idx2}", row3).selectsNone();

        // with a single restriction and excluded indexes
        assertThatPlan("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes = {idx1}", row1).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes = {idx2}", row1).selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes = {idx1}", row2).selects(idx2);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes = {idx2}", row2).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes = {idx1}", row3).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes = {idx2}", row3).selectsNone();

        // with restrictions in two columns (v1 and v2) and preferred indexes
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH preferred_indexes = {idx1}").selects(idx1, idx2);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH preferred_indexes = {idx2}").selects(idx1, idx2);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH preferred_indexes = {idx1, idx2}").selects(idx1, idx2);

        // with restrictions in two columns (v1 and v2) and excluded indexes
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes = {idx1}").selects(idx2);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes = {idx2}").selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes = {idx1, idx2}").selectsNone();

        // with restrictions in two columns (v1 and v3) and preferred indexes
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes = {idx1}").selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes = {idx2}").selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes = {idx1, idx2}").selects(idx1);

        // with restrictions in two columns (v1 and v3) and excluded indexes
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes = {idx1}").selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes = {idx2}").selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes = {idx1, idx2}").selectsNone();

        // with mixed preferred and excluded indexes
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH preferred_indexes = {idx1} AND excluded_indexes = {idx2}").selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH preferred_indexes = {idx2} AND excluded_indexes = {idx1}").selects(idx2);

        // without restrictions
        assertThatPlan("SELECT * FROM %s ALLOW FILTERING WITH preferred_indexes = {idx1}", row1, row2, row3).selectsNone();
        assertThatPlan("SELECT * FROM %s ALLOW FILTERING WITH excluded_indexes = {idx1}", row1, row2, row3).selectsNone();

        // prepared statements
        prepare("SELECT * FROM %s WHERE v1=? ALLOW FILTERING WITH preferred_indexes = {idx1}");
        prepare("SELECT * FROM %s WHERE v1=? ALLOW FILTERING WITH excluded_indexes = {idx1}");
    }

    @Test
    public void testSAIWithoutAllowFiltering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int)");
        createIndex("CREATE CUSTOM INDEX idx1 ON %s(v1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX idx2 ON %s(v2) USING 'StorageAttachedIndex'");

        Index idx1 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx1");
        Index idx2 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx2");

        String insert = "INSERT INTO %s (k, v1, v2, v3) VALUES (?, ?, ?, ?)";
        Object[] row1 = new Object[]{ 1, 0, 1, 1 };
        Object[] row2 = new Object[]{ 2, 1, 0, 1 };
        Object[] row3 = new Object[]{ 3, 1, 1, 0 };
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);

        // without any hints
        assertThatPlan("SELECT * FROM %s", row1, row2, row3).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v1=0", row1).selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v2=0", row2).selects(idx2);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0");
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0").selectsAnyOf(idx1, idx2);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v2=0 AND v3=0");

        // with a single restriction and preferred indexes
        assertThatPlan("SELECT * FROM %s WHERE v1=0 WITH preferred_indexes = {idx1}", row1).selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 WITH preferred_indexes = {idx2}", row1).selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 WITH preferred_indexes = {idx1}", row2).selects(idx2);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 WITH preferred_indexes = {idx2}", row2).selects(idx2);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0 WITH preferred_indexes = {idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0 WITH preferred_indexes = {idx2}");

        // with a single restriction and excluded indexes
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 WITH excluded_indexes = {idx1}");
        assertThatPlan("SELECT * FROM %s WHERE v1=0 WITH excluded_indexes = {idx2}", row1).selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 WITH excluded_indexes = {idx1}", row2).selects(idx2);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v2=0 WITH excluded_indexes = {idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0 WITH excluded_indexes = {idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0 WITH excluded_indexes = {idx2}");

        // with restrictions in two columns (v1 and v2) and preferred indexes
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH preferred_indexes = {idx1}").selects(idx1, idx2);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH preferred_indexes = {idx2}").selects(idx1, idx2);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH preferred_indexes = {idx1, idx2}").selects(idx1, idx2);

        // with restrictions in two columns (v1 and v2) and excluded indexes
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH excluded_indexes = {idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH excluded_indexes = {idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH excluded_indexes = {idx1, idx2}");

        // with restrictions in two columns (v1 and v3) and preferred indexes
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH preferred_indexes = {idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH preferred_indexes = {idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH preferred_indexes = {idx1, idx2}");

        // with restrictions in two columns (v1 and v3) and excluded indexes
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH excluded_indexes = {idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH excluded_indexes = {idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0 WITH excluded_indexes = {idx1, idx2}");

        // with mixed preferred and excluded indexes
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH preferred_indexes = {idx1} AND excluded_indexes = {idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH preferred_indexes = {idx2} AND excluded_indexes = {idx1}");

        // without restrictions
        assertThatPlan("SELECT * FROM %s WITH preferred_indexes = {idx1}", row1, row2, row3).selectsNone();
        assertThatPlan("SELECT * FROM %s WITH excluded_indexes = {idx1}", row1, row2, row3).selectsNone();

        // prepared statements
        prepare("SELECT * FROM %s WHERE v1=? WITH preferred_indexes = {idx1}");
        assertNeedsAllowFiltering(() -> prepare("SELECT * FROM %s WHERE v1=? WITH excluded_indexes = {idx1}"));
    }

    @Test
    public void testMixedIndexImplementations()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int)");
        createIndex("CREATE CUSTOM INDEX idx1 ON %s(v1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX idx2 ON %s(v2) USING 'StorageAttachedIndex'");
        createIndex("CREATE INDEX idx3 ON %s(v3)");

        String insert = "INSERT INTO %s (k, v1, v2, v3) VALUES (?, ?, ?, ?)";
        Object[] row1 = new Object[]{ 1, 0, 1, 1 };
        Object[] row2 = new Object[]{ 2, 1, 0, 1 };
        Object[] row3 = new Object[]{ 3, 1, 1, 0 };
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);

        Index idx1 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx1");
        Index idx2 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx2");
        Index idx3 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx3");

        // without any hints
        assertThatPlan("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING", row1).selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING", row2).selects(idx2);
        assertThatPlan("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING", row3).selects(idx3);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING").selects(idx1, idx2);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING").selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING").selects(idx2);

        // preferring idx1 (SAI)
        assertThatPlan("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH preferred_indexes={idx1}", row1).selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH preferred_indexes={idx1}", row2).selects(idx2);
        assertThatPlan("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH preferred_indexes={idx1}", row3).selects(idx3);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH preferred_indexes={idx1}").selects(idx1, idx2);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes={idx1}").selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes={idx1}").selects(idx2);

        // preferring idx2 (SAI)
        assertThatPlan("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH preferred_indexes={idx2}", row1).selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH preferred_indexes={idx2}", row2).selects(idx2);
        assertThatPlan("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH preferred_indexes={idx2}", row3).selects(idx3);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH preferred_indexes={idx2}").selects(idx1, idx2);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes={idx2}").selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes={idx2}").selects(idx2);

        // preferring idx3 (legacy)
        assertThatPlan("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH preferred_indexes={idx3}", row1).selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH preferred_indexes={idx3}", row2).selects(idx2);
        assertThatPlan("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH preferred_indexes={idx3}", row3).selects(idx3);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH preferred_indexes={idx3}").selects(idx1, idx2);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes={idx3}").selects(idx3); // prefers legacy over SAI
        assertThatPlan("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes={idx3}").selects(idx3); // prefers legacy over SAI

        // preferring idx1 and idx2 (both SAI)
        assertThatPlan("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH preferred_indexes={idx1, idx2}", row1).selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH preferred_indexes={idx1, idx2}", row2).selects(idx2);
        assertThatPlan("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH preferred_indexes={idx1, idx2}", row3).selects(idx3);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH preferred_indexes={idx1, idx2}").selects(idx1, idx2);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes={idx1, idx2}").selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes={idx1, idx2}").selects(idx2);

        // preferring idx1 and idx3 (SAI and legacy)
        assertThatPlan("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH preferred_indexes={idx1, idx3}", row1).selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH preferred_indexes={idx1, idx3}", row2).selects(idx2);
        assertThatPlan("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH preferred_indexes={idx1, idx3}", row3).selects(idx3);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH preferred_indexes={idx1, idx3}").selects(idx1, idx2);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes={idx1, idx3}").selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes={idx1, idx3}").selects(idx3); // prefers legacy over SAI

        // preferring idx2 and idx3 (SAI and legacy)
        assertThatPlan("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH preferred_indexes={idx2, idx3}", row1).selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH preferred_indexes={idx2, idx3}", row2).selects(idx2);
        assertThatPlan("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH preferred_indexes={idx2, idx3}", row3).selects(idx3);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH preferred_indexes={idx1, idx3}").selects(idx1, idx2);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes={idx2, idx3}").selects(idx3); // prefers legacy over SAI
        assertThatPlan("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes={idx2, idx3}").selects(idx2);

        // excluding idx1 (SAI)
        assertThatPlan("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx1}", row1).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx1}", row2).selects(idx2);
        assertThatPlan("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx1}", row3).selects(idx3);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx1}").selects(idx2);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx1}").selects(idx3);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx1}").selects(idx2);

        // excluding idx2 (SAI)
        assertThatPlan("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx2}", row1).selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx2}", row2).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx2}", row3).selects(idx3);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx2}").selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx2}").selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx2}").selects(idx3);

        // excluding idx3 (legacy)
        assertThatPlan("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx3}", row1).selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx3}", row2).selects(idx2);
        assertThatPlan("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx3}", row3).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx3}").selects(idx1, idx2);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx3}").selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx3}").selects(idx2);

        // excluding idx1 and idx2 (both SAI)
        assertThatPlan("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx1, idx2}", row1).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx1, idx2}", row2).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx1, idx2}", row3).selects(idx3);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx1, idx2}").selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx1, idx2}").selects(idx3);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx1, idx2}").selects(idx3);

        // excluding idx1 and idx3 (SAI and legacy)
        assertThatPlan("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx1, idx3}", row1).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx1, idx3}", row2).selects(idx2);
        assertThatPlan("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx1, idx3}", row3).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx1, idx3}").selects(idx2);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx1, idx3}").selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx1, idx3}").selects(idx2);

        // excluding idx2 and idx3 (SAI and legacy)
        assertThatPlan("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx2, idx3}", row1).selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx2, idx3}", row2).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx2, idx3}", row3).selectsNone();
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx2, idx3}").selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx2, idx3}").selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx2, idx3}").selectsNone();
    }

    @Test
    public void testMultipleIndexesOnSameColumn()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        createIndex("CREATE CUSTOM INDEX idx1 ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE INDEX idx2 ON %s(v)");

        String insert = "INSERT INTO %s (k, v) VALUES (?, ?)";
        Object[] row1 = new Object[]{ 1, 0 };
        Object[] row2 = new Object[]{ 2, 1 };
        execute(insert, row1);
        execute(insert, row2);

        Index idx1 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx1");
        Index idx2 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx2");

        assertThatPlan("SELECT * FROM %s WHERE v=0", row1).selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v=0 WITH preferred_indexes={idx1}", row1).selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v=0 WITH preferred_indexes={idx2}", row1).selects(idx2);
        assertThatPlan("SELECT * FROM %s WHERE v=0 WITH preferred_indexes={idx1,idx2}", row1).selects(idx1);
        assertThatPlan("SELECT * FROM %s WHERE v=0 WITH excluded_indexes={idx1}", row1).selects(idx2);
        assertThatPlan("SELECT * FROM %s WHERE v=0 WITH excluded_indexes={idx2}", row1).selects(idx1);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v=0 WITH excluded_indexes={idx1,idx2}");
        assertThatPlan("SELECT * FROM %s WHERE v=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx2}", row1).selectsNone();
    }

    @Test
    public void testSAIWithMultipleIndexesPerColumnAndUnsupportedEqOnAnalyzer()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        createIndex("CREATE INDEX idx1 ON %s(v)");
        createIndex("CREATE CUSTOM INDEX idx2 ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX idx3 ON %s(v) USING 'StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'equals_behaviour_when_analyzed': 'UNSUPPORTED', 'index_analyzer': 'standard' }");

        Index idx1 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx1");
        Index idx2 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx2");
        Index idx3 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx3");

        String insert = "INSERT INTO %s (k, v) VALUES (?, ?)";
        Object[] row1 = new Object[]{ 1, "Richard Strauss" };
        Object[] row2 = new Object[]{ 2, "Johann Strauss" };
        execute(insert, row1);
        execute(insert, row2);

        // test index selection with EQ query
        String query = "SELECT * FROM %s WHERE v = 'Strauss' ";
        assertThatPlan(query).selects(idx2);
        assertThatPlan(query + "WITH preferred_indexes = {idx1}").selects(idx1);
        assertThatPlan(query + "WITH preferred_indexes = {idx2}").selects(idx2);
        assertThatPlan(query + "WITH preferred_indexes = {idx3}").selects(idx2); // preferred index is not mandatory, idx3 is not applicable to =
        assertThatPlan(query + "WITH preferred_indexes = {idx1, idx2}").selects(idx2);
        assertThatPlan(query + "WITH preferred_indexes = {idx1, idx3}").selects(idx1);
        assertThatPlan(query + "WITH preferred_indexes = {idx2, idx3}").selects(idx2);
        assertThatPlan(query + "WITH preferred_indexes = {idx1, idx2, idx3}").selects(idx2);
        assertThatPlan(query + "WITH excluded_indexes = {idx1}").selects(idx2);
        assertThatPlan(query + "WITH excluded_indexes = {idx2}").selects(idx1);
        assertThatPlan(query + "WITH excluded_indexes = {idx3}").selects(idx2);
        assertIndexDoesNotSupportOperator(query + "WITH excluded_indexes = {idx1, idx2}", "v");
        assertThatPlan(query + "WITH excluded_indexes = {idx1, idx3}").selects(idx2);
        assertThatPlan(query + "WITH excluded_indexes = {idx2, idx3}").selects(idx1);
        assertNeedsAllowFiltering(query + "WITH excluded_indexes = {idx1, idx2, idx3}");
        assertThatPlan(query + "ALLOW FILTERING WITH excluded_indexes = {idx1, idx2, idx3}").selectsNone();

        // test the same EQ query with a different value
        query = "SELECT * FROM %s WHERE v = 'Richard Strauss' ";
        assertThatPlan(query, row1).selects(idx2);
        assertThatPlan(query + "WITH preferred_indexes = {idx1}", row1).selects(idx1);
        assertThatPlan(query + "WITH preferred_indexes = {idx2}", row1).selects(idx2);
        assertThatPlan(query + "WITH preferred_indexes = {idx3}", row1).selects(idx2); // preferred index is not mandatory, idx3 is not applicable to =
        assertThatPlan(query + "WITH preferred_indexes = {idx1, idx2}", row1).selects(idx2);
        assertThatPlan(query + "WITH preferred_indexes = {idx1, idx3}", row1).selects(idx1);
        assertThatPlan(query + "WITH preferred_indexes = {idx2, idx3}", row1).selects(idx2);
        assertThatPlan(query + "WITH preferred_indexes = {idx1, idx2, idx3}", row1).selects(idx2);
        assertThatPlan(query + "WITH excluded_indexes = {idx1}", row1).selects(idx2);
        assertThatPlan(query + "WITH excluded_indexes = {idx2}", row1).selects(idx1);
        assertThatPlan(query + "WITH excluded_indexes = {idx3}", row1).selects(idx2);
        assertIndexDoesNotSupportOperator(query + "WITH excluded_indexes = {idx1, idx2}", "v");
        assertThatPlan(query + "WITH excluded_indexes = {idx1, idx3}", row1).selects(idx2);
        assertThatPlan(query + "WITH excluded_indexes = {idx2, idx3}", row1).selects(idx1);
        assertNeedsAllowFiltering(query + "WITH excluded_indexes = {idx1, idx2, idx3}");
        assertThatPlan(query + "ALLOW FILTERING WITH excluded_indexes = {idx1, idx2, idx3}", row1).selectsNone();

        // test index selection with MATCH query
        query = "SELECT * FROM %s WHERE v : 'strauss' ";
        assertThatPlan(query, row1, row2).selects(idx3);
        assertThatPlan(query + "WITH preferred_indexes = {idx1}", row1, row2).selects(idx3); // idx1 is not applicable to :
        assertThatPlan(query + "WITH preferred_indexes = {idx2}", row1, row2).selects(idx3); // idx2 is not applicable to :
        assertThatPlan(query + "WITH preferred_indexes = {idx3}", row1, row2).selects(idx3);
        assertThatPlan(query + "WITH preferred_indexes = {idx1, idx2}", row1, row2).selects(idx3); // idx1 and idx2 are not applicable to :
        assertThatPlan(query + "WITH preferred_indexes = {idx1, idx3}", row1, row2).selects(idx3);
        assertThatPlan(query + "WITH preferred_indexes = {idx2, idx3}", row1, row2).selects(idx3);
        assertThatPlan(query + "WITH preferred_indexes = {idx1, idx2, idx3}", row1, row2).selects(idx3);
        assertThatPlan(query + "WITH excluded_indexes = {idx1}", row1, row2).selects(idx3);
        assertThatPlan(query + "WITH excluded_indexes = {idx2}", row1, row2).selects(idx3);
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes = {idx3}", "v");
        assertThatPlan(query + "WITH excluded_indexes = {idx1, idx2}", row1, row2).selects(idx3);
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes = {idx1, idx3}", "v");
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes = {idx2, idx3}", "v");
        assertMatchNeedsIndex(query + "WITH excluded_indexes = {idx1, idx2, idx3}", "v", "strauss");

        // test the same MATCH query with a different value
        query = "SELECT * FROM %s WHERE v : 'Richard Strauss' ";
        assertThatPlan(query, row1).selects(idx3);
        assertThatPlan(query + "WITH preferred_indexes = {idx1}", row1).selects(idx3); // idx1 is not applicable to :
        assertThatPlan(query + "WITH preferred_indexes = {idx2}", row1).selects(idx3); // idx2 is not applicable to :
        assertThatPlan(query + "WITH preferred_indexes = {idx3}", row1).selects(idx3);
        assertThatPlan(query + "WITH preferred_indexes = {idx1, idx2}", row1).selects(idx3); // idx1 and idx2 are not applicable to :
        assertThatPlan(query + "WITH preferred_indexes = {idx1, idx3}", row1).selects(idx3);
        assertThatPlan(query + "WITH preferred_indexes = {idx2, idx3}", row1).selects(idx3);
        assertThatPlan(query + "WITH preferred_indexes = {idx1, idx2, idx3}", row1).selects(idx3);
        assertThatPlan(query + "WITH excluded_indexes = {idx1}", row1).selects(idx3);
        assertThatPlan(query + "WITH excluded_indexes = {idx2}", row1).selects(idx3);
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes = {idx3}", "v");
        assertThatPlan(query + "WITH excluded_indexes = {idx1, idx2}", row1).selects(idx3);
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes = {idx1, idx3}", "v");
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes = {idx2, idx3}", "v");
        assertMatchNeedsIndex(query + "WITH excluded_indexes = {idx1, idx2, idx3}", "v", "Richard Strauss");
    }

    @Test
    public void testSAIWithMultipleIndexesPerColumnAndMatchEqOnAnalyzer()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        createIndex("CREATE INDEX idx1 ON %s(v)");
        createIndex("CREATE CUSTOM INDEX idx2 ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX idx3 ON %s(v) USING 'StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'equals_behaviour_when_analyzed': 'MATCH', 'index_analyzer': 'standard' }");

        Index idx1 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx1");
        Index idx2 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx2");
        Index idx3 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx3");

        String insert = "INSERT INTO %s (k, v) VALUES (?, ?)";
        Object[] row1 = new Object[]{ 1, "Richard Strauss" };
        Object[] row2 = new Object[]{ 2, "Johann Strauss" };
        execute(insert, row1);
        execute(insert, row2);

        // test index selection with EQ query, the hints will desambiguate the query and will produce different results
        // depending on the selected index
        String query = "SELECT * FROM %s WHERE v = 'Strauss' ";
        assertEqualityPredicateIsAmbiguous(query);
        assertThatPlan(query + "WITH preferred_indexes = {idx1}").selects(idx1);
        assertThatPlan(query + "WITH preferred_indexes = {idx2}").selects(idx2);
        assertThatPlan(query + "WITH preferred_indexes = {idx3}", row1, row2).selects(idx3);
        assertThatPlan(query + "WITH preferred_indexes = {idx1, idx2}").selects(idx2);
        assertEqualityPredicateIsAmbiguous(query + "WITH preferred_indexes = {idx1, idx3}");
        assertEqualityPredicateIsAmbiguous(query + "WITH preferred_indexes = {idx2, idx3}");
        assertEqualityPredicateIsAmbiguous(query + "WITH preferred_indexes = {idx1, idx2, idx3}");
        assertEqualityPredicateIsAmbiguous(query + "WITH excluded_indexes = {idx1}");
        assertEqualityPredicateIsAmbiguous(query + "WITH excluded_indexes = {idx2}");
        assertThatPlan(query + "WITH excluded_indexes = {idx3}").selects(idx2);
        assertThatPlan(query + "WITH excluded_indexes = {idx1, idx2}", row1, row2).selects(idx3);
        assertThatPlan(query + "WITH excluded_indexes = {idx1, idx3}").selects(idx2);
        assertThatPlan(query + "WITH excluded_indexes = {idx2, idx3}").selects(idx1);
        assertNeedsAllowFiltering(query + "WITH excluded_indexes = {idx1, idx2, idx3}");
        assertThatPlan(query + "ALLOW FILTERING WITH excluded_indexes = {idx1, idx2, idx3}").selectsNone();

        // test the same EQ query with a different value
        query = "SELECT * FROM %s WHERE v = 'Richard Strauss' ";
        assertEqualityPredicateIsAmbiguous(query);
        assertThatPlan(query + "WITH preferred_indexes = {idx1}", row1).selects(idx1);
        assertThatPlan(query + "WITH preferred_indexes = {idx2}", row1).selects(idx2);
        assertThatPlan(query + "WITH preferred_indexes = {idx3}", row1).selects(idx3);
        assertThatPlan(query + "WITH preferred_indexes = {idx1, idx2}", row1).selects(idx2);
        assertEqualityPredicateIsAmbiguous(query + "WITH preferred_indexes = {idx1, idx3}");
        assertEqualityPredicateIsAmbiguous(query + "WITH preferred_indexes = {idx2, idx3}");
        assertEqualityPredicateIsAmbiguous(query + "WITH preferred_indexes = {idx1, idx2, idx3}");
        assertEqualityPredicateIsAmbiguous(query + "WITH excluded_indexes = {idx1}");
        assertEqualityPredicateIsAmbiguous(query + "WITH excluded_indexes = {idx2}");
        assertThatPlan(query + "WITH excluded_indexes = {idx3}", row1).selects(idx2);
        assertThatPlan(query + "WITH excluded_indexes = {idx1, idx2}", row1).selects(idx3);
        assertThatPlan(query + "WITH excluded_indexes = {idx1, idx3}", row1).selects(idx2);
        assertThatPlan(query + "WITH excluded_indexes = {idx2, idx3}", row1).selects(idx1);
        assertNeedsAllowFiltering(query + "WITH excluded_indexes = {idx1, idx2, idx3}");
        assertThatPlan(query + "ALLOW FILTERING WITH excluded_indexes = {idx1, idx2, idx3}", row1).selectsNone();

        // test index selection with MATCH query
        query = "SELECT * FROM %s WHERE v : 'strauss' ";
        assertThatPlan(query, row1, row2).selects(idx3);
        assertThatPlan(query + "WITH preferred_indexes = {idx1}", row1, row2).selects(idx3); // idx1 is not applicable to :
        assertThatPlan(query + "WITH preferred_indexes = {idx2}", row1, row2).selects(idx3); // idx2 is not applicable to :
        assertThatPlan(query + "WITH preferred_indexes = {idx3}", row1, row2).selects(idx3);
        assertThatPlan(query + "WITH preferred_indexes = {idx1, idx2}", row1, row2).selects(idx3); // idx1 and idx2 are not applicable to :
        assertThatPlan(query + "WITH preferred_indexes = {idx1, idx3}", row1, row2).selects(idx3);
        assertThatPlan(query + "WITH preferred_indexes = {idx2, idx3}", row1, row2).selects(idx3);
        assertThatPlan(query + "WITH preferred_indexes = {idx1, idx2, idx3}", row1, row2).selects(idx3);
        assertThatPlan(query + "WITH excluded_indexes = {idx1}", row1, row2).selects(idx3);
        assertThatPlan(query + "WITH excluded_indexes = {idx2}", row1, row2).selects(idx3);
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes = {idx3}", "v");
        assertThatPlan(query + "WITH excluded_indexes = {idx1, idx2}", row1, row2).selects(idx3);
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes = {idx1, idx3}", "v");
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes = {idx2, idx3}", "v");
        assertMatchNeedsIndex(query + "WITH excluded_indexes = {idx1, idx2, idx3}", "v", "strauss");

        // test the same MATCH query with a different value
        query = "SELECT * FROM %s WHERE v : 'Richard Strauss' ";
        assertThatPlan(query, row1).selects(idx3);
        assertThatPlan(query + "WITH preferred_indexes = {idx1}", row1).selects(idx3); // idx1 is not applicable to :
        assertThatPlan(query + "WITH preferred_indexes = {idx2}", row1).selects(idx3); // idx2 is not applicable to :
        assertThatPlan(query + "WITH preferred_indexes = {idx3}", row1).selects(idx3);
        assertThatPlan(query + "WITH preferred_indexes = {idx1, idx2}", row1).selects(idx3); // idx1 and idx2 are not applicable to :
        assertThatPlan(query + "WITH preferred_indexes = {idx1, idx3}", row1).selects(idx3);
        assertThatPlan(query + "WITH preferred_indexes = {idx2, idx3}", row1).selects(idx3);
        assertThatPlan(query + "WITH preferred_indexes = {idx1, idx2, idx3}", row1).selects(idx3);
        assertThatPlan(query + "WITH excluded_indexes = {idx1}", row1).selects(idx3);
        assertThatPlan(query + "WITH excluded_indexes = {idx2}", row1).selects(idx3);
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes = {idx3}", "v");
        assertThatPlan(query + "WITH excluded_indexes = {idx1, idx2}", row1).selects(idx3);
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes = {idx1, idx3}", "v");
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes = {idx2, idx3}", "v");
        assertMatchNeedsIndex(query + "WITH excluded_indexes = {idx1, idx2, idx3}", "v", "Richard Strauss");
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

    private PlanSelectionAssertion assertThatPlan(String query, Object[]... expectedRows)
    {
        // First execute the query and check returned rows
        assertRowsIgnoringOrder(execute(query), expectedRows);

        ReadCommand command = parseReadCommand(query);
        Index.QueryPlan queryPlan = command.indexQueryPlan();
        if (queryPlan == null)
            return new PlanSelectionAssertion(null);

        Set<Index> indexes = queryPlan.getIndexes();
        return new PlanSelectionAssertion(indexes);
    }

    private static class PlanSelectionAssertion
    {
        private final Set<Index> selectedIndexes;

        public PlanSelectionAssertion(@Nullable Set<Index> selectedIndexes)
        {
            this.selectedIndexes = selectedIndexes;
        }

        public void selects(Index... indexes)
        {
            Assertions.assertThat(selectedIndexes)
                      .isNotNull()
                      .as("Expected to select only %s, but got: %s", indexes, selectedIndexes)
                      .isEqualTo(Set.of(indexes));
        }

        public void selectsAnyOf(Index index1, Index index2, Index... otherIndexes)
        {
            Set<Index> expectedIndexes = new HashSet<>(otherIndexes.length + 1);
            expectedIndexes.add(index1);
            expectedIndexes.add(index2);
            expectedIndexes.addAll(Arrays.asList(otherIndexes));

            Assertions.assertThat(selectedIndexes)
                      .isNotNull()
                      .as("Expected to select any of %s, but got: %s", otherIndexes, selectedIndexes)
                      .containsAnyElementsOf(expectedIndexes);
        }

        public void selectsNone()
        {
            Assertions.assertThat(selectedIndexes).isNull();
        }
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
