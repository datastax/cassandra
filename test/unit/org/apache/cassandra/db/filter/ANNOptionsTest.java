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
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;

import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.statements.PropertyDefinitions;
import org.apache.cassandra.cql3.statements.SelectOptions;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.StubIndex;
import org.apache.cassandra.index.TargetParser;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.Pair;
import org.assertj.core.api.Assertions;
import org.quicktheories.QuickTheory;

import static org.quicktheories.generators.SourceDSL.integers;

/**
 * Tests for {@link ANNOptions}, independent of the specific underlying index implementation.
 */
public class ANNOptionsTest extends CQLTester
{
    @BeforeClass
    public static void setUpClass()
    {
        // Set the messaging version that adds support for the new ANN options before starting the server
        CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.setInt(MessagingService.VERSION_DS_11);
        CQLTester.setUpClass();
    }

    /**
     * Test parsing and validation of ANN options in {@code SELECT} queries.
     */
    @Test
    public void testParseAndValidate()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, n int, v vector<float, 2>)");

        // invalid queries with ALLOW FILTERING before creating the ANN index
        assertInvalidThrowMessage(StatementRestrictions.ANN_OPTIONS_WITHOUT_ORDER_BY_ANN,
                                  InvalidRequestException.class,
                                  "SELECT * FROM %s WHERE v = [1, 1] ALLOW FILTERING WITH ann_options = {}");
        assertInvalidThrowMessage(String.format(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE, 'v'),
                                  InvalidRequestException.class,
                                  "SELECT * FROM %s ORDER BY v ANN OF [1, 1] ALLOW FILTERING WITH ann_options = {}");

        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(v) USING '%s'", ANNIndex.class.getName()));

        // correct queries without specific ANN options
        execute("SELECT * FROM %s ORDER BY v ANN OF [1, 1] WITH ann_options = {}");
        execute("SELECT * FROM %s ORDER BY v ANN OF [1, 1] WITH ANN_OPTIONS = {}");
        execute("SELECT * FROM %s ORDER BY v ANN OF [1, 1] LIMIT 10 WITH ann_options = {}");
        execute("SELECT * FROM %s ORDER BY v ANN OF [1, 1] ALLOW FILTERING WITH ann_options = {}");
        execute("SELECT * FROM %s WHERE k=0 ORDER BY v ANN OF [1, 1] WITH ann_options = {}");

        // correct queries with specific ANN options - rerank_k
        execute("SELECT * FROM %s ORDER BY v ANN OF [1, 1] LIMIT 100 WITH ann_options = {'rerank_k': -1}");
        execute("SELECT * FROM %s ORDER BY v ANN OF [1, 1] LIMIT 100 WITH ann_options = {'rerank_k': 0}");
        execute("SELECT * FROM %s ORDER BY v ANN OF [1, 1] LIMIT 10 WITH ann_options = {'rerank_k': 10}");
        execute("SELECT * FROM %s ORDER BY v ANN OF [1, 1] LIMIT 10 WITH ann_options = {'rerank_k': 11}");
        execute("SELECT * FROM %s ORDER BY v ANN OF [1, 1] LIMIT 10 WITH ann_options = {'rerank_k': 1000}");
        execute("SELECT * FROM %s ORDER BY v ANN OF [1, 1] LIMIT 10 WITH ann_options = {'rerank_k': '1000'}");

        // correct queries with specific ANN options - use_pruning
        execute("SELECT * FROM %s ORDER BY v ANN OF [1, 1] WITH ann_options = {'use_pruning': true}");
        execute("SELECT * FROM %s ORDER BY v ANN OF [1, 1] WITH ann_options = {'use_pruning': false}");
        execute("SELECT * FROM %s ORDER BY v ANN OF [1, 1] WITH ann_options = {'use_pruning': 'true'}");
        execute("SELECT * FROM %s ORDER BY v ANN OF [1, 1] WITH ann_options = {'use_pruning': 'false'}");

        // correct queries with both options
        execute("SELECT * FROM %s ORDER BY v ANN OF [1, 1] LIMIT 10 WITH ann_options = {'rerank_k': 10, 'use_pruning': true}");
        execute("SELECT * FROM %s ORDER BY v ANN OF [1, 1] LIMIT 10 WITH ann_options = {'use_pruning': false, 'rerank_k': 20}");

        // Queries that exceed the failure threshold for the guardrail. Specifies a protocol version to trigger
        // validation in the coordinator.
        assertInvalidThrowMessage(Optional.of(ProtocolVersion.V5),
                                  "ANN options specifies rerank_k=5000, this exceeds the failure threshold of 4000.",
                                  InvalidQueryException.class,
                                  "SELECT * FROM %s ORDER BY v ANN OF [1, 1] LIMIT 10 WITH ann_options = {'rerank_k': 5000}");

        String baseQuery = "SELECT * FROM %s ORDER BY v ANN OF [1, 1]";

        // unknown SELECT options
        assertInvalidThrowMessage("Unknown property 'unknown_options'",
                                  SyntaxException.class,
                                  baseQuery + " WITH unknown_options = {}");

        // mixed known and unknown SELECT options
        assertInvalidThrowMessage("Unknown property 'unknown_options'",
                                  SyntaxException.class,
                                  baseQuery + " WITH ann_options = {'rerank_k': 0} AND unknown_options = {}");

        // duplicated SELECT options
        assertInvalidThrowMessage(String.format(PropertyDefinitions.MULTIPLE_DEFINITIONS_ERROR, SelectOptions.ANN_OPTIONS),
                                  SyntaxException.class,
                                  baseQuery + " WITH ann_options = {'rerank_k': 0} AND ann_options = {'rerank_k': 0}");

        // unknown ANN options
        assertInvalidThrowMessage("Unknown ANN option: unknown",
                                  InvalidRequestException.class,
                                  baseQuery + " WITH ann_options = {'unknown': 0}");

        // mixed known and unknown ANN options
        assertInvalidThrowMessage("Unknown ANN option: unknown",
                                  InvalidRequestException.class,
                                  baseQuery + " WITH ann_options = {'rerank_k': 0, 'unknown': 0}");

        // invalid ANN options (not a number)
        assertInvalidThrowMessage("Invalid 'rerank_k' ANN option. Expected a positive int but found: a",
                                  InvalidRequestException.class,
                                  baseQuery + " WITH ann_options = {'rerank_k': 'a'}");

        // ANN options with rerank lesser than limit
        assertInvalidThrowMessage("Invalid rerank_k value 10 greater than 0 and less than limit 100",
                                  InvalidRequestException.class,
                                  baseQuery + "LIMIT 100 WITH ann_options = {'rerank_k': 10}");

        // invalid use_pruning values
        assertInvalidThrowMessage("Invalid 'use_pruning' ANN option. Expected a boolean but found: notaboolean",
                                  InvalidRequestException.class,
                                  baseQuery + " WITH ann_options = {'use_pruning': 'notaboolean'}");
        assertInvalidThrowMessage("Invalid 'use_pruning' ANN option. Expected a boolean but found: 42",
                                  InvalidRequestException.class,
                                  baseQuery + " WITH ann_options = {'use_pruning': '42'}");

        // ANN options without ORDER BY ANN with empty options
        assertInvalidThrowMessage(StatementRestrictions.ANN_OPTIONS_WITHOUT_ORDER_BY_ANN,
                                  InvalidRequestException.class,
                                  "SELECT * FROM %s WITH ann_options = {}");

        // ANN options without ORDER BY ANN with non-empty options
        assertInvalidThrowMessage(StatementRestrictions.ANN_OPTIONS_WITHOUT_ORDER_BY_ANN,
                                  InvalidRequestException.class,
                                  "SELECT * FROM %s WITH ann_options = {'rerank_k': 10}");

        // ANN options without ORDER BY ANN with other expressions
        assertInvalidThrowMessage(StatementRestrictions.ANN_OPTIONS_WITHOUT_ORDER_BY_ANN,
                                  InvalidRequestException.class,
                                  "SELECT * FROM %s WHERE n = 0 ALLOW FILTERING WITH ann_options = {'rerank_k': 10}");
    }

    /**
     * Test that ANN options are considered when generating the CQL string representation of a {@link ReadCommand}.
     */
    @Test
    public void testToCQLString()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, n int, v vector<float, 2>)");
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(v) USING '%s'", ANNIndex.class.getName()));

        // without ANN options
        String formattedQuery = formatQuery("SELECT * FROM %%s ORDER BY v ANN OF [1, 1]");
        ReadCommand command = parseReadCommand(formattedQuery);
        Assertions.assertThat(command.toRedactedCQLString()).doesNotContain("WITH ann_options");

        // with rerank_k option
        formattedQuery = formatQuery("SELECT * FROM %%s ORDER BY v ANN OF [1, 1] LIMIT 1 WITH ann_options = {'rerank_k': 2}");
        command = parseReadCommand(formattedQuery);
        Assertions.assertThat(command.toRedactedCQLString()).contains("WITH ann_options = {'rerank_k': 2}");

        // with use_pruning option
        formattedQuery = formatQuery("SELECT * FROM %%s ORDER BY v ANN OF [1, 1] WITH ann_options = {'use_pruning': true}");
        command = parseReadCommand(formattedQuery);
        Assertions.assertThat(command.toRedactedCQLString()).contains("WITH ann_options = {'use_pruning': true}");

        // with both options
        formattedQuery = formatQuery("SELECT * FROM %%s ORDER BY v ANN OF [1, 1] LIMIT 1 WITH ann_options = {'rerank_k': 2, 'use_pruning': false}");
        command = parseReadCommand(formattedQuery);
        Assertions.assertThat(command.toRedactedCQLString()).contains("WITH ann_options = {'rerank_k': 2, 'use_pruning': false}");
    }

    /**
     * Verify that the ANN options in a query get to the {@link ReadCommand} and the {@link Index},
     * even after serialization.
     */
    @Test
    public void testTransport()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, n int, v vector<float, 2>)");
        createIndex(String.format("CREATE CUSTOM INDEX ON %%s(v) USING '%s'", ANNIndex.class.getName()));

        // unspecified ANN options, should be mapped to NONE
        testTransport("SELECT * FROM %s ORDER BY v ANN OF [1, 1]", ANNOptions.NONE);
        testTransport("SELECT * FROM %s ORDER BY v ANN OF [1, 1] WITH ann_options = {}", ANNOptions.NONE);

        // some random negative values, all should be accepted and not be mapped to NONE
        String negativeQuery = "SELECT * FROM %%s ORDER BY v ANN OF [1, 1] LIMIT 10 WITH ann_options = {'rerank_k': %d}";
        QuickTheory.qt()
                   .withExamples(100)
                   .forAll(integers().allPositive())
                   .checkAssert(i -> testTransport(String.format(negativeQuery, -i), ANNOptions.create(-i, null)));

        // rerankK = 0 must also work
        testTransport("SELECT * FROM %s ORDER BY v ANN OF [1, 1] LIMIT 10 WITH ann_options = {'rerank_k': 0}", ANNOptions.create(0, null));

        // test use_pruning values
        testTransport("SELECT * FROM %s ORDER BY v ANN OF [1, 1] WITH ann_options = {'use_pruning': true}",
                     ANNOptions.create(null, true));
        testTransport("SELECT * FROM %s ORDER BY v ANN OF [1, 1] WITH ann_options = {'use_pruning': false}",
                     ANNOptions.create(null, false));

        // test combinations of rerank_k and use_pruning
        String combinedQuery = "SELECT * FROM %%s ORDER BY v ANN OF [1, 1] LIMIT %d WITH ann_options = {'rerank_k': %<d, 'use_pruning': %b}";
        QuickTheory.qt()
                   .withExamples(100)
                   .forAll(integers().allPositive())
                   .checkAssert(i -> {
                       testTransport(String.format(combinedQuery, i, true), ANNOptions.create(i, true));
                       testTransport(String.format(combinedQuery, i, false), ANNOptions.create(i, false));
                   });
    }

    private void testTransport(String query, ANNOptions expectedOptions)
    {
        // verify that the options arrive correctly at the index
        execute(query);
        Assertions.assertThat(ANNIndex.lastQueryAnnOptions).isEqualTo(expectedOptions);

        // verify that the options are correctly parsed and stored in the ReadCommand
        String formattedQuery = formatQuery(query);
        ReadCommand command = parseReadCommand(formattedQuery);
        ANNOptions actualOptions = command.rowFilter().annOptions();
        Assertions.assertThat(actualOptions).isEqualTo(expectedOptions);

        // serialize and deserialize the command to check if the options are preserved...
        try
        {
            // ...with a version that supports ANN options
            DataOutputBuffer out = new DataOutputBuffer();
            ReadCommand.serializer.serialize(command, out, MessagingService.VERSION_DS_11);
            Assertions.assertThat(ReadCommand.serializer.serializedSize(command, MessagingService.VERSION_DS_11))
                      .isEqualTo(out.buffer().remaining());
            DataInputBuffer in = new DataInputBuffer(out.buffer(), true);
            command = ReadCommand.serializer.deserialize(in, MessagingService.VERSION_DS_11);
            actualOptions = command.rowFilter().annOptions();
            Assertions.assertThat(actualOptions).isEqualTo(expectedOptions);

            // ...with a version that doesn't support ANN options
            out = new DataOutputBuffer();
            if (expectedOptions != ANNOptions.NONE)
            {
                try
                {
                    ReadCommand.serializer.serialize(command, out, MessagingService.VERSION_DS_10);
                }
                catch (IllegalStateException e)
                {
                    // expected
                    Assertions.assertThat(e)
                              .hasMessageContaining("Unable to serialize ANN options with messaging version: " + MessagingService.VERSION_DS_10);
                }
            }
            else
            {
                ReadCommand.serializer.serialize(command, out, MessagingService.VERSION_DS_10);
                Assertions.assertThat(ReadCommand.serializer.serializedSize(command, MessagingService.VERSION_DS_10))
                          .isEqualTo(out.buffer().remaining());
                in = new DataInputBuffer(out.buffer(), true);
                command = ReadCommand.serializer.deserialize(in, MessagingService.VERSION_DS_10);
                actualOptions = command.rowFilter().annOptions();
                Assertions.assertThat(actualOptions).isEqualTo(ANNOptions.NONE);
            }
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    /**
     * Tests that the current version of {@link ANNOptions} can correctly serialize and deserialize all combinations
     * of current options.
     */
    @Test
    public void testCurrentVersionSerialization() throws IOException
    {
        // Test different combinations of options
        ANNOptions[] optionsToTest = {
            ANNOptions.NONE,
            ANNOptions.create(7, null),
            ANNOptions.create(null, true),
            ANNOptions.create(7, false)
        };

        for (ANNOptions options : optionsToTest)
        {
            DataOutputBuffer out = new DataOutputBuffer();
            ANNOptions.serializer.serialize(options, out, MessagingService.current_version);
            int serializedSize = out.buffer().remaining();
            Assertions.assertThat(ANNOptions.serializer.serializedSize(options, MessagingService.current_version))
                      .isEqualTo(serializedSize);

            DataInputBuffer in = new DataInputBuffer(out.buffer(), true);
            ANNOptions deserialized = ANNOptions.serializer.deserialize(in, MessagingService.current_version);
            Assertions.assertThat(deserialized).isEqualTo(options);
        }
    }

    /**
     * Tests that we will be able to deserialize future versions of {@link ANNOptions} with new properties if those new
     * properties are defaults.
     */
    @Test
    public void testDeserializationOfCompatibleFutureVersions() throws IOException
    {
        // a future new version of the ANN options with default new properties...
        FutureANNOptions sentOptions = new FutureANNOptions(7, FutureANNOptions.NEW_PROPERTY_DEFAULT);
        DataOutputBuffer out = new DataOutputBuffer();
        FutureANNOptions.serializer.serialize(sentOptions, out);
        int serializedSize = out.buffer().remaining();
        Assertions.assertThat(FutureANNOptions.serializer.serializedSize(sentOptions))
                  .isEqualTo(serializedSize);

        // ...should be readable with the current serializer
        DataInputBuffer in = new DataInputBuffer(out.buffer(), true);
        ANNOptions receivedOptions = ANNOptions.serializer.deserialize(in, MessagingService.current_version);
        Assertions.assertThat(receivedOptions).isEqualTo(ANNOptions.create(sentOptions.rerankK, null));
        Assertions.assertThat(ANNOptions.serializer.serializedSize(receivedOptions, MessagingService.current_version))
                  .isEqualTo(serializedSize);
    }

    /**
     * Tests that we won't be able to desrialize future versions of {@link ANNOptions} with new properties if those new
     * properties are not defaults.
     */
    @Test
    public void testDeserializationOfNonCompatibleFutureVersions() throws IOException
    {
        // a future new version of the ANN options with non-default new properties...
        FutureANNOptions sentOptions = new FutureANNOptions(7, "newProperty");
        DataOutputBuffer out = new DataOutputBuffer();
        FutureANNOptions.serializer.serialize(sentOptions, out);
        int serializedSize = out.buffer().remaining();
        Assertions.assertThat(FutureANNOptions.serializer.serializedSize(sentOptions))
                  .isEqualTo(serializedSize);

        // ...should fail in a controlled manner with the current serializer
        try (DataInputBuffer in = new DataInputBuffer(out.buffer(), true))
        {
            Assertions.assertThatThrownBy(() -> ANNOptions.serializer.deserialize(in, MessagingService.current_version))
                      .isInstanceOf(IOException.class)
                      .hasMessageContaining("Found unsupported ANN options");
        }
    }

    /**
     * Class simulating a future version of the ANN options, with an additional property.
     */
    private static class FutureANNOptions
    {
        public static final String NEW_PROPERTY_DEFAULT = "default";

        public static final Serializer serializer = new Serializer();

        @Nullable
        public final Integer rerankK;
        public final String newProperty;

        public FutureANNOptions(@Nullable Integer rerankK, String newProperty)
        {
            this.rerankK = rerankK;
            this.newProperty = newProperty;
        }

        public FutureANNOptions(ANNOptions options)
        {
            this.rerankK = options.rerankK;
            this.newProperty = NEW_PROPERTY_DEFAULT;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FutureANNOptions that = (FutureANNOptions) o;
            return Objects.equals(rerankK, that.rerankK) && Objects.equals(newProperty, that.newProperty);
        }

        public static class Serializer
        {
            private static final int RERANK_K_MASK = 1;
            private static final int NEW_PROPERTY_MASK = 1 << 31;
            private static final int UNKNOWN_OPTIONS_MASK = ~(RERANK_K_MASK | NEW_PROPERTY_MASK);

            public void serialize(FutureANNOptions options, DataOutputPlus out) throws IOException
            {
                int flags = flags(options);
                out.writeInt(flags);

                if (options.rerankK != null)
                    out.writeUnsignedVInt32(options.rerankK);

                if (hasNewProperty(flags))
                    out.writeUTF(options.newProperty);
            }

            public FutureANNOptions deserialize(DataInputPlus in) throws IOException
            {
                int flags = in.readInt();

                if ((flags & UNKNOWN_OPTIONS_MASK) != 0)
                    throw new IOException("Found unsupported ANN options");

                Integer rerankK = hasRerankK(flags) ? (int) in.readUnsignedVInt() : null;
                String newProperty = hasNewProperty(flags) ? in.readUTF() : NEW_PROPERTY_DEFAULT;

                return new FutureANNOptions(rerankK, newProperty);
            }

            public long serializedSize(FutureANNOptions options)
            {
                int flags = flags(options);
                long size = TypeSizes.sizeof(flags);

                if (options.rerankK != null)
                    size += TypeSizes.sizeofUnsignedVInt(options.rerankK);

                if (hasNewProperty(flags))
                    size += TypeSizes.sizeof(options.newProperty);

                return size;
            }

            private static int flags(FutureANNOptions options)
            {
                int flags = 0;

                if (options.rerankK != null)
                    flags |= RERANK_K_MASK;

                if (!Objects.equals(options.newProperty, NEW_PROPERTY_DEFAULT))
                    flags |= NEW_PROPERTY_MASK;

                return flags;
            }

            private static boolean hasRerankK(long flags)
            {
                return (flags & RERANK_K_MASK) == RERANK_K_MASK;
            }

            public static boolean hasNewProperty(long flags)
            {
                return (flags & NEW_PROPERTY_MASK) == NEW_PROPERTY_MASK;
            }
        }
    }

    /**
     * Mock index with dummy ANN support.
     */
    public static final class ANNIndex extends StubIndex
    {
        private final ColumnMetadata indexedColumn;
        public static volatile ANNOptions lastQueryAnnOptions;

        public ANNIndex(ColumnFamilyStore baseCfs, IndexMetadata metadata)
        {
            super(baseCfs, metadata);
            Pair<ColumnMetadata, IndexTarget.Type> target = TargetParser.parse(baseCfs.metadata(), metadata);
            indexedColumn = target.left;
        }

        @Override
        public boolean supportsExpression(ColumnMetadata column, Operator operator)
        {
            return indexedColumn.name.equals(column.name) && operator == Operator.ANN;
        }

        @Override
        public Searcher searcherFor(ReadCommand command)
        {
            lastQueryAnnOptions = command.rowFilter().annOptions();
            return super.searcherFor(command);
        }
    }
}
