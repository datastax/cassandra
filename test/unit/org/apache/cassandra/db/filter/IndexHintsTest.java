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

import com.google.common.collect.ImmutableSet;
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
        assertInvalidThrowMessage(CONFLICTING_INDEXES_ERROR + "idx1, idx2",
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

        // without any hints
        assertSelectsAny("SELECT * FROM %s ALLOW FILTERING");
        assertSelectsAny("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING", idx1);
        assertSelectsAny("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING", idx2);
        assertSelectsAny("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING");
        assertSelectsAny("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING", idx1, idx2);
        assertSelectsAny("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING", idx1);
        assertSelectsAny("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING", idx2);

        // with a single restriction and preferred indexes
        assertSelectsAny("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH preferred_indexes = {idx1}", idx1);
        assertSelectsAny("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH preferred_indexes = {idx2}", idx1);
        assertSelectsAny("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH preferred_indexes = {idx1}", idx2);
        assertSelectsAny("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH preferred_indexes = {idx2}", idx2);
        assertSelectsAny("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH preferred_indexes = {idx1}");
        assertSelectsAny("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH preferred_indexes = {idx2}");

        // with a single restriction and excluded indexes
        assertSelectsAny("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes = {idx1}");
        assertSelectsAny("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes = {idx2}", idx1);
        assertSelectsAny("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes = {idx1}", idx2);
        assertSelectsAny("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes = {idx2}");
        assertSelectsAny("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes = {idx1}");
        assertSelectsAny("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes = {idx2}");

        // with restrictions in two columns (v1 and v2) and preferred indexes
        assertSelectsAny("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH preferred_indexes = {idx1}", idx1);
        assertSelectsAny("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH preferred_indexes = {idx2}", idx2);
        assertSelectsAny("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH preferred_indexes = {idx1, idx2}", idx1, idx2);

        // with restrictions in two columns (v1 and v2) and excluded indexes
        assertSelectsAny("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes = {idx1}", idx2);
        assertSelectsAny("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes = {idx2}", idx1);
        assertSelectsAny("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes = {idx1, idx2}");

        // with restrictions in two columns (v1 and v3) and preferred indexes
        assertSelectsAny("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes = {idx1}", idx1);
        assertSelectsAny("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes = {idx2}", idx1);
        assertSelectsAny("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes = {idx1, idx2}", idx1);

        // with restrictions in two columns (v1 and v3) and excluded indexes
        assertSelectsAny("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes = {idx1}");
        assertSelectsAny("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes = {idx2}", idx1);
        assertSelectsAny("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes = {idx1, idx2}");

        // without restrictions
        assertSelectsAny("SELECT * FROM %s ALLOW FILTERING WITH preferred_indexes = {idx1}");
        assertSelectsAny("SELECT * FROM %s ALLOW FILTERING WITH excluded_indexes = {idx1}");

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

        // without any hints
        assertSelectsAny("SELECT * FROM %s");
        assertSelectsAny("SELECT * FROM %s WHERE v1=0", idx1);
        assertSelectsAny("SELECT * FROM %s WHERE v2=0", idx2);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v2=0");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v2=0 AND v3=0");

        // with a single restriction and preferred indexes
        assertSelectsAny("SELECT * FROM %s WHERE v1=0 WITH preferred_indexes = {idx1}", idx1);
        assertSelectsAny("SELECT * FROM %s WHERE v1=0 WITH preferred_indexes = {idx2}", idx1);
        assertSelectsAny("SELECT * FROM %s WHERE v2=0 WITH preferred_indexes = {idx1}", idx2);
        assertSelectsAny("SELECT * FROM %s WHERE v2=0 WITH preferred_indexes = {idx2}", idx2);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0 WITH preferred_indexes = {idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0 WITH preferred_indexes = {idx2}");

        // with a single restriction and excluded indexes
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 WITH excluded_indexes = {idx1}");
        assertSelectsAny("SELECT * FROM %s WHERE v1=0 WITH excluded_indexes = {idx2}", idx1);
        assertSelectsAny("SELECT * FROM %s WHERE v2=0 WITH excluded_indexes = {idx1}", idx2);
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
        assertSelectsAny("SELECT * FROM %s WITH preferred_indexes = {idx1}");
        assertSelectsAny("SELECT * FROM %s WITH excluded_indexes = {idx1}");

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

        // without any hints
        assertSelectsAll("SELECT * FROM %s ALLOW FILTERING");
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING", idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING");
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING", idx1, idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING", idx2);

        // with a single restriction and preferred indexes
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH preferred_indexes = {idx1}", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH preferred_indexes = {idx2}", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH preferred_indexes = {idx1}", idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH preferred_indexes = {idx2}", idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH preferred_indexes = {idx1}");
        assertSelectsAll("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH preferred_indexes = {idx2}");

        // with a single restriction and excluded indexes
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes = {idx1}");
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes = {idx2}", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes = {idx1}", idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes = {idx2}");
        assertSelectsAll("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes = {idx1}");
        assertSelectsAll("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes = {idx2}");

        // with restrictions in two columns (v1 and v2) and preferred indexes
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH preferred_indexes = {idx1}", idx1, idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH preferred_indexes = {idx2}", idx1, idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH preferred_indexes = {idx1, idx2}", idx1, idx2);

        // with restrictions in two columns (v1 and v2) and excluded indexes
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes = {idx1}", idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes = {idx2}", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes = {idx1, idx2}");

        // with restrictions in two columns (v1 and v3) and preferred indexes
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes = {idx1}", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes = {idx2}", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes = {idx1, idx2}", idx1);

        // with restrictions in two columns (v1 and v3) and excluded indexes
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes = {idx1}");
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes = {idx2}", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes = {idx1, idx2}");

        // with mixed preferred and excluded indexes
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH preferred_indexes = {idx1} AND excluded_indexes = {idx2}", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH preferred_indexes = {idx2} AND excluded_indexes = {idx1}", idx2);

        // without restrictions
        assertSelectsAll("SELECT * FROM %s ALLOW FILTERING WITH preferred_indexes = {idx1}");
        assertSelectsAll("SELECT * FROM %s ALLOW FILTERING WITH excluded_indexes = {idx1}");

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

        // without any hints
        assertSelectsAll("SELECT * FROM %s");
        assertSelectsAll("SELECT * FROM %s WHERE v1=0", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v2=0", idx2);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0");
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v2=0", idx1, idx2);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 AND v3=0");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v2=0 AND v3=0");

        // with a single restriction and preferred indexes
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 WITH preferred_indexes = {idx1}", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 WITH preferred_indexes = {idx2}", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 WITH preferred_indexes = {idx1}", idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 WITH preferred_indexes = {idx2}", idx2);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0 WITH preferred_indexes = {idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0 WITH preferred_indexes = {idx2}");

        // with a single restriction and excluded indexes
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1=0 WITH excluded_indexes = {idx1}");
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 WITH excluded_indexes = {idx2}", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 WITH excluded_indexes = {idx1}", idx2);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v2=0 WITH excluded_indexes = {idx2}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0 WITH excluded_indexes = {idx1}");
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v3=0 WITH excluded_indexes = {idx2}");

        // with restrictions in two columns (v1 and v2) and preferred indexes
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH preferred_indexes = {idx1}", idx1, idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH preferred_indexes = {idx2}", idx1, idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v2=0 WITH preferred_indexes = {idx1, idx2}", idx1, idx2);

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
        assertSelectsAll("SELECT * FROM %s WITH preferred_indexes = {idx1}");
        assertSelectsAll("SELECT * FROM %s WITH excluded_indexes = {idx1}");

        // prepared statements
        prepare("SELECT * FROM %s WHERE v1=? WITH preferred_indexes = {idx1}");
        assertNeedsAllowFiltering(() -> prepare("SELECT * FROM %s WHERE v1=? WITH excluded_indexes = {idx1}"));
    }

    @Test
    public void testSAIWithMultipleIndexesPerColumnAndUnsupportedEqOnAnalyzer()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX idx1 ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX idx2 ON %s(v) USING 'StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'equals_behaviour_when_analyzed': 'UNSUPPORTED', 'index_analyzer': 'standard' }");

        Index idx1 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx1");
        Index idx2 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx2");

        execute("INSERT INTO %s (k, v) VALUES (1, 'Richard Strauss')");
        execute("INSERT INTO %s (k, v) VALUES (2, 'Johann Strauss')");

        String query = "SELECT k FROM %s WHERE v = 'Strauss' ";
        assertSelectsAll(query + "WITH preferred_indexes = {idx1}", idx1);
        assertSelectsAll(query + "WITH preferred_indexes = {idx2}", idx1); // preferred index is not mandatory, idx2 is not applicable to =
        assertIndexDoesNotSupportOperator(query + "WITH excluded_indexes = {idx1}", "v");
        assertSelectsAll(query + "WITH excluded_indexes = {idx2}", idx1);
        assertRows(execute(query + "WITH preferred_indexes = {idx1}"));
        assertRows(execute(query + "WITH preferred_indexes = {idx2}"));
        assertRows(execute(query + "WITH excluded_indexes = {idx2}"));

        query = "SELECT k FROM %s WHERE v = 'Richard Strauss' ";
        assertSelectsAll(query + "WITH preferred_indexes = {idx1}", idx1);
        assertSelectsAll(query + "WITH preferred_indexes = {idx2}", idx1); // preferred index is not mandatory, idx2 is not applicable to =
        assertIndexDoesNotSupportOperator(query + "WITH excluded_indexes = {idx1}", "v");
        assertSelectsAll(query + "WITH excluded_indexes = {idx2}", idx1);
        assertRows(execute(query + "WITH preferred_indexes = {idx1}"), row(1));
        assertRows(execute(query + "WITH preferred_indexes = {idx2}"), row(1));
        assertRows(execute(query + "WITH excluded_indexes = {idx2}"), row(1));

        query = "SELECT k FROM %s WHERE v : 'strauss' ";
        assertSelectsAll(query + "WITH preferred_indexes = {idx1}", idx2); // preferred index is not mandatory, idx1 is not applicable to :
        assertSelectsAll(query + "WITH preferred_indexes = {idx2}", idx2);
        assertSelectsAll(query + "WITH excluded_indexes = {idx1}", idx2);
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes = {idx2}", "v"); // exclussions are mandatory
        assertRows(execute(query + "WITH preferred_indexes = {idx1}"), row(1), row(2));
        assertRows(execute(query + "WITH preferred_indexes = {idx2}"), row(1), row(2));
        assertRows(execute(query + "WITH excluded_indexes = {idx1}"), row(1), row(2));

        query = "SELECT k FROM %s WHERE v : 'Richard Strauss' ";
        assertSelectsAll(query + "WITH preferred_indexes = {idx1}", idx2); // preferred index is not mandatory, idx1 is not applicable to :
        assertSelectsAll(query + "WITH preferred_indexes = {idx2}", idx2);
        assertSelectsAll(query + "WITH excluded_indexes = {idx1}", idx2);
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes = {idx2}", "v"); // exclussions are mandatory
        assertRows(execute(query + "WITH preferred_indexes = {idx1}"), row(1));
        assertRows(execute(query + "WITH preferred_indexes = {idx2}"), row(1));
        assertRows(execute(query + "WITH excluded_indexes = {idx1}"), row(1));
    }

    @Test
    public void testSAIWithMultipleIndexesPerColumnAndMatchEqOnAnalyzer()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX idx1 ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX idx2 ON %s(v) USING 'StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'equals_behaviour_when_analyzed': 'MATCH', 'index_analyzer': 'standard' }");

        execute("INSERT INTO %s (k, v) VALUES (1, 'Richard Strauss')");
        execute("INSERT INTO %s (k, v) VALUES (2, 'Johann Strauss')");

        String query = "SELECT k FROM %s WHERE v = 'Strauss' ";
        assertEqualityPredicateIsAmbiguous(query + "WITH preferred_indexes = {idx1}");
        assertEqualityPredicateIsAmbiguous(query + "WITH preferred_indexes = {idx2}");
        assertEqualityPredicateIsAmbiguous(query + "WITH excluded_indexes = {idx1}");
        assertEqualityPredicateIsAmbiguous(query + "WITH excluded_indexes = {idx2}");

        query = "SELECT k FROM %s WHERE v = 'Richard Strauss' ";
        assertEqualityPredicateIsAmbiguous(query + "WITH preferred_indexes = {idx1}");
        assertEqualityPredicateIsAmbiguous(query + "WITH preferred_indexes = {idx2}");
        assertEqualityPredicateIsAmbiguous(query + "WITH excluded_indexes = {idx1}");
        assertEqualityPredicateIsAmbiguous(query + "WITH excluded_indexes = {idx2}");

        query = "SELECT k FROM %s WHERE v : 'strauss' ";
        assertRows(execute(query + "WITH preferred_indexes = {idx1}"), row(1), row(2)); // preferred, not mandatory
        assertRows(execute(query + "WITH preferred_indexes = {idx2}"), row(1), row(2));
        assertRows(execute(query + "WITH excluded_indexes = {idx1}"), row(1), row(2)); // it wouldn't have been selected anyway
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes = {idx2}", "v"); // exclussions are mandatory

        query = "SELECT k FROM %s WHERE v : 'Richard Strauss' ";
        assertRows(execute(query + "WITH preferred_indexes = {idx1}"), row(1)); // preferred, not mandatory
        assertRows(execute(query + "WITH preferred_indexes = {idx2}"), row(1));
        assertRows(execute(query + "WITH excluded_indexes = {idx1}"), row(1)); // it wouldn't have been selected anyway
        assertIndexDoesNotSupportAnalyzerMatches(query + "WITH excluded_indexes = {idx2}", "v"); // exclussions are mandatory
    }

    @Test
    public void testMixedIndexImplementations()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int)");
        createIndex("CREATE CUSTOM INDEX idx1 ON %s(v1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX idx2 ON %s(v2) USING 'StorageAttachedIndex'");
        createIndex("CREATE INDEX idx3 ON %s(v3)");
        execute("INSERT INTO %s (k, v1, v2, v3) VALUES (0, 1, 2, 3)");

        Index idx1 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx1");
        Index idx2 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx2");
        Index idx3 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx3");

        // without any hints
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING", idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING", idx3);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING", idx1, idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING", idx2);

        // preferring idx1 (SAI)
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH preferred_indexes={idx1}", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH preferred_indexes={idx1}", idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH preferred_indexes={idx1}", idx3);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH preferred_indexes={idx1}", idx1, idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes={idx1}", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes={idx1}", idx2);

        // preferring idx2 (SAI)
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH preferred_indexes={idx2}", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH preferred_indexes={idx2}", idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH preferred_indexes={idx2}", idx3);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH preferred_indexes={idx2}", idx1, idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes={idx2}", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes={idx2}", idx2);

        // preferring idx3 (legacy)
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH preferred_indexes={idx3}", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH preferred_indexes={idx3}", idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH preferred_indexes={idx3}", idx3);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH preferred_indexes={idx3}", idx1, idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes={idx3}", idx3); // prefers legacy over SAI
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes={idx3}", idx3); // prefers legacy over SAI

        // preferring idx1 and idx2 (both SAI)
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH preferred_indexes={idx1, idx2}", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH preferred_indexes={idx1, idx2}", idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH preferred_indexes={idx1, idx2}", idx3);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH preferred_indexes={idx1, idx2}", idx1, idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes={idx1, idx2}", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes={idx1, idx2}", idx2);

        // preferring idx1 and idx3 (SAI and legacy)
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH preferred_indexes={idx1, idx3}", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH preferred_indexes={idx1, idx3}", idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH preferred_indexes={idx1, idx3}", idx3);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH preferred_indexes={idx1, idx3}", idx1, idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes={idx1, idx3}", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes={idx1, idx3}", idx3); // prefers legacy over SAI

        // preferring idx2 and idx3 (SAI and legacy)
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH preferred_indexes={idx2, idx3}", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH preferred_indexes={idx2, idx3}", idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH preferred_indexes={idx2, idx3}", idx3);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH preferred_indexes={idx1, idx3}", idx1, idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes={idx2, idx3}", idx3); // prefers legacy over SAI
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH preferred_indexes={idx2, idx3}", idx2);

        // excluding idx1 (SAI)
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx1}");
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx1}", idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx1}", idx3);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx1}", idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx1}", idx3);
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx1}", idx2);

        // excluding idx2 (SAI)
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx2}", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx2}");
        assertSelectsAll("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx2}", idx3);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx2}", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx2}", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx2}", idx3);

        // excluding idx3 (legacy)
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx3}", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx3}", idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx3}");
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx3}", idx1, idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx3}", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx3}", idx2);

        // excluding idx1 and idx2 (both SAI)
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx1, idx2}");
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx1, idx2}");
        assertSelectsAll("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx1, idx2}", idx3);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx1, idx2}");
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx1, idx2}", idx3);
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx1, idx2}", idx3);

        // excluding idx1 and idx3 (SAI and legacy)
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx1, idx3}");
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx1, idx3}", idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx1, idx3}");
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx1, idx3}", idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx1, idx3}");
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx1, idx3}", idx2);

        // excluding idx2 and idx3 (SAI and legacy)
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes={idx2, idx3}", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes={idx2, idx3}");
        assertSelectsAll("SELECT * FROM %s WHERE v3=0 ALLOW FILTERING WITH excluded_indexes={idx2, idx3}");
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes={idx2, idx3}", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v1=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx2, idx3}", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v2=0 AND v3=0 ALLOW FILTERING WITH excluded_indexes={idx2, idx3}");
    }

    @Test
    public void testMultipleIndexesOnSameColumn()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        createIndex("CREATE CUSTOM INDEX idx1 ON %s(v) USING 'StorageAttachedIndex'");
        createIndex("CREATE INDEX idx2 ON %s(v)");
        execute("INSERT INTO %s (k, v) VALUES (0, 0)");

        Index idx1 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx1");
        Index idx2 = getCurrentColumnFamilyStore().indexManager.getIndexByName("idx2");

        assertSelectsAll("SELECT * FROM %s WHERE v=0", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v=0 WITH preferred_indexes={idx1}", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v=0 WITH preferred_indexes={idx2}", idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v=0 WITH preferred_indexes={idx1,idx2}", idx1);
        assertSelectsAll("SELECT * FROM %s WHERE v=0 WITH excluded_indexes={idx1}", idx2);
        assertSelectsAll("SELECT * FROM %s WHERE v=0 WITH excluded_indexes={idx2}", idx1);
        assertNeedsAllowFiltering("SELECT * FROM %s WHERE v=0 WITH excluded_indexes={idx1,idx2}");
        assertSelectsAll("SELECT * FROM %s WHERE v=0 ALLOW FILTERING WITH excluded_indexes={idx1,idx2}");
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

    private void assertEqualityPredicateIsAmbiguous(String query)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining("equality predicate is ambiguous");
    }

    private void assertSelectsAny(String query, Index... indexes)
    {
        Index.QueryPlan plan = parseReadCommand(query).indexQueryPlan();
        if (indexes.length == 0)
        {
            Assertions.assertThat(plan).isNull();
        }
        else
        {
            Assertions.assertThat(plan).isNotNull();
            Set<Index> selectedIndexes = plan.getIndexes();
            Assertions.assertThat(selectedIndexes).hasSize(1);
            Index selectedIndex = selectedIndexes.iterator().next();
            Set<Index> expectedIndexes = ImmutableSet.copyOf(indexes);
            Assertions.assertThat(expectedIndexes).contains(selectedIndex);
        }
    }

    private void assertSelectsAll(String query, Index... indexes)
    {
        ReadCommand command = parseReadCommand(query);
        Index.QueryPlan queryPlan = command.indexQueryPlan();
        if (indexes.length == 0)
        {
            Assertions.assertThat(queryPlan).isNull();
        }
        else
        {
            Assertions.assertThat(queryPlan).isNotNull();
            Set<Index> selectedIndexes = queryPlan.getIndexes();
            Assertions.assertThat(selectedIndexes).hasSize(indexes.length);
            Set<Index> expectedIndexes = ImmutableSet.copyOf(indexes);
            Assertions.assertThat(selectedIndexes).containsAll(expectedIndexes);
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
