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

package org.apache.cassandra.service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ANNOptions;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.IndexHints;
import org.apache.cassandra.db.filter.OptimizerOptions;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.net.DseCompatRefusals;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.OutboundConnections;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * The never-send guard of the DSE 6 compatibility mode: a read that only
 * this software can serve is refused at the coordinator while the mode is on, counted in {@code compatstatus}, and
 * untouched when the mode is off.
 */
public class Dse6ReadGuardTest extends CQLTester
{
    private static final String REFUSAL_SUFFIX = " is not available while the upgrade from DSE 6 is in progress; " +
                                                 "run the query once internode_compatibility_mode is NONE on every node";

    private InternodeCompatibilityMode modeBefore;

    @BeforeClass
    public static void setUpNetwork()
    {
        requireNetwork();
    }

    @Before
    public void saveMode()
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
        DseCompatRefusals.reset();
    }

    @After
    public void restoreMode()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
        DseCompatRefusals.reset();
    }

    @Test
    public void classifiesReadFeatures() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, vec vector<float, 2>, m map<text, int>, PRIMARY KEY (k, c))");
        String index = createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        TableMetadata metadata = currentTableMetadata();
        ColumnMetadata v = column(metadata, "v");
        ColumnMetadata vec = column(metadata, "vec");
        ByteBuffer text = UTF8Type.instance.decompose("x");
        ByteBuffer point = VectorType.getInstance(FloatType.instance, 2).decompose(Arrays.asList(1.0f, 2.0f));

        assertNull(DseReadGuard.unsupportedReadFeature(read(metadata, RowFilter.none())));
        assertNull(DseReadGuard.unsupportedReadFeature(read(metadata, filter(v, Operator.EQ, text))));
        assertNull(DseReadGuard.unsupportedReadFeature(read(metadata, filter(v, Operator.LIKE, text))));

        assertEquals("the " + Operator.NOT_IN + " operator", DseReadGuard.unsupportedReadFeature(read(metadata, filter(v, Operator.NOT_IN, text))));
        assertEquals("the " + Operator.ANN + " operator", DseReadGuard.unsupportedReadFeature(read(metadata, filter(vec, Operator.ANN, text))));
        assertEquals("the " + Operator.ANALYZER_MATCHES + " operator", DseReadGuard.unsupportedReadFeature(read(metadata, filter(v, Operator.ANALYZER_MATCHES, text))));

        RowFilter.Builder ann = RowFilter.builder(false);
        ann.addANNExpression(vec, text, ANNOptions.NONE);
        assertEquals("the " + Operator.ANN + " operator", DseReadGuard.unsupportedReadFeature(read(metadata, ann.build())));

        // a geo-distance search is expression kind 100, the first kind Cassandra 3.0 does not define
        RowFilter.Builder geo = RowFilter.builder(false);
        geo.addGeoDistanceExpression(vec, point, Operator.LT, FloatType.instance.decompose(1000.0f));
        RowFilter geoFilter = geo.build();
        assertEquals(DseReadGuard.FIRST_DS_ONLY_EXPRESSION_KIND, geoFilter.expressions().get(0).kindCode());
        assertEquals("a row-filter expression of kind 100", DseReadGuard.unsupportedReadFeature(read(metadata, geoFilter)));

        // a user-defined expression is a class of this node that a DSE 6 replica cannot load
        RowFilter.Builder user = RowFilter.builder(false);
        user.addUserExpression(new TestUserExpression(v, text));
        assertEquals("a user-defined row-filter expression", DseReadGuard.unsupportedReadFeature(read(metadata, user.build())));

        // a map entry is compared with = only in Cassandra 3.0 and DSE (kind 1, MAP_EQUALITY)
        ColumnMetadata m = column(metadata, "m");
        ByteBuffer one = Int32Type.instance.decompose(1);
        for (Operator operator : new Operator[]{ Operator.NEQ, Operator.GT, Operator.GTE, Operator.LT, Operator.LTE })
        {
            RowFilter.Builder mapComparison = RowFilter.builder(false);
            mapComparison.addMapComparison(m, text, operator, one);
            assertEquals("a map entry compared with " + operator, DseReadGuard.unsupportedReadFeature(read(metadata, mapComparison.build())));
        }
        RowFilter.Builder mapEquality = RowFilter.builder(false);
        mapEquality.addMapComparison(m, text, Operator.EQ, one);
        assertNull(DseReadGuard.unsupportedReadFeature(read(metadata, mapEquality.build())));

        // every filtered read above CL ONE needs reconciliation: accepted, the flag is left out at the dialect
        RowFilter.Builder reconciled = RowFilter.builder(true);
        reconciled.add(v, Operator.EQ, text);
        assertNull(DseReadGuard.unsupportedReadFeature(read(metadata, reconciled.build())));

        RowFilter disjunction = new RowFilter(new RowFilter.FilterElement(true, Collections.emptyList(), Collections.emptyList()), false, IndexHints.NONE, OptimizerOptions.NONE) {};
        assertEquals("a disjunction (OR) in the WHERE clause", DseReadGuard.unsupportedReadFeature(read(metadata, disjunction)));
        RowFilter.FilterElement nestedOr = new RowFilter.FilterElement(true, Collections.emptyList(), Collections.emptyList());
        RowFilter nestedDisjunction = new RowFilter(new RowFilter.FilterElement(false, Collections.emptyList(), Collections.singletonList(nestedOr)), false, IndexHints.NONE, OptimizerOptions.NONE) {};
        assertEquals("a disjunction (OR) in the WHERE clause", DseReadGuard.unsupportedReadFeature(read(metadata, nestedDisjunction)));
        // a nested conjunction has no OR, but the 3.0 layout cannot carry nesting either
        RowFilter.FilterElement nestedAnd = new RowFilter.FilterElement(false, filter(v, Operator.EQ, text).root().expressions(), Collections.emptyList());
        RowFilter nestedConjunction = new RowFilter(new RowFilter.FilterElement(false, Collections.emptyList(), Collections.singletonList(nestedAnd)), false, IndexHints.NONE, OptimizerOptions.NONE) {};
        assertEquals("a nested group of conditions in the WHERE clause", DseReadGuard.unsupportedReadFeature(read(metadata, nestedConjunction)));

        IndexMetadata indexMetadata = metadata.indexes.get(index).orElseThrow(AssertionError::new);
        RowFilter hinted = new RowFilter(filter(v, Operator.EQ, text).root(), false, IndexHints.create(Collections.singleton(indexMetadata), Collections.emptySet()), OptimizerOptions.NONE) {};
        assertEquals("an index hint", DseReadGuard.unsupportedReadFeature(read(metadata, hinted)));
        RowFilter excluded = new RowFilter(filter(v, Operator.EQ, text).root(), false, IndexHints.create(Collections.emptySet(), Collections.singleton(indexMetadata)), OptimizerOptions.NONE) {};
        assertEquals("an index hint", DseReadGuard.unsupportedReadFeature(read(metadata, excluded)));

        // the options of a WITH optimizer_options clause, one or several, are on the wire from VERSION_DS_20 only
        RowFilter optimized = new RowFilter(filter(v, Operator.EQ, text).root(), false, IndexHints.NONE, OptimizerOptions.create(0, null, null, null)) {};
        assertEquals("an SAI optimizer option", DseReadGuard.unsupportedReadFeature(read(metadata, optimized)));
        RowFilter optimizedMore = new RowFilter(filter(v, Operator.EQ, text).root(), false, IndexHints.NONE, OptimizerOptions.create(1, 3, true, null)) {};
        assertEquals("an SAI optimizer option", DseReadGuard.unsupportedReadFeature(read(metadata, optimizedMore)));
        OptimizerOptions sortOrderOnly = OptimizerOptions.create(null, null, null, OptimizerOptions.HybridSortOrder.SORT_THEN_FILTER);
        RowFilter sortOrdered = new RowFilter(filter(v, Operator.EQ, text).root(), false, IndexHints.NONE, sortOrderOnly) {};
        assertEquals("an SAI optimizer option", DseReadGuard.unsupportedReadFeature(read(metadata, sortOrdered)));
        // with an index hint as well, the hint is named first
        RowFilter hintedAndOptimized = new RowFilter(filter(v, Operator.EQ, text).root(), false,
                                                     IndexHints.create(Collections.singleton(indexMetadata), Collections.emptySet()),
                                                     OptimizerOptions.create(0, null, null, null)) {};
        assertEquals("an index hint", DseReadGuard.unsupportedReadFeature(read(metadata, hintedAndOptimized)));
        // no option given is NONE, for which the dialect writes nothing
        OptimizerOptions noOption = OptimizerOptions.create(null, null, null, null);
        assertSame(OptimizerOptions.NONE, noOption);
        RowFilter notOptimized = new RowFilter(filter(v, Operator.EQ, text).root(), false, IndexHints.NONE, noOption) {};
        assertNull(DseReadGuard.unsupportedReadFeature(read(metadata, notOptimized)));
    }

    @Test
    public void refusesOnlyInDse6Mode() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))");
        TableMetadata metadata = currentTableMetadata();
        ReadCommand notIn = read(metadata, filter(column(metadata, "v"), Operator.NOT_IN, UTF8Type.instance.decompose("x")));

        DseReadGuard.checkReadCommand(notIn); // NONE: nothing happens
        assertEquals(0, DseCompatRefusals.total());

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        InvalidRequestException refused = assertThrows(InvalidRequestException.class, () -> DseReadGuard.checkReadCommand(notIn));
        assertEquals("DSE 6 compatibility mode: the " + Operator.NOT_IN + " operator" + REFUSAL_SUFFIX, refused.getMessage());
        assertEquals(1, DseCompatRefusals.count("read with the " + Operator.NOT_IN + " operator"));
        DseReadGuard.checkReadCommand(read(metadata, RowFilter.none())); // a plain read is fine
        assertEquals(1, DseCompatRefusals.total());
    }

    /** The guard sits in StorageProxy.read, before the choice between a regular and a SERIAL (Paxos) read. */
    @Test
    public void refusesSinglePartitionReadsAtEveryConsistencyLevel() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))");
        TableMetadata metadata = currentTableMetadata();
        ReadCommand notIn = read(metadata, filter(column(metadata, "v"), Operator.NOT_IN, UTF8Type.instance.decompose("x")));
        SinglePartitionReadCommand.Group group = SinglePartitionReadCommand.Group.one((SinglePartitionReadCommand) notIn);

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        for (ConsistencyLevel cl : new ConsistencyLevel[]{ ConsistencyLevel.ONE, ConsistencyLevel.SERIAL, ConsistencyLevel.LOCAL_SERIAL })
        {
            InvalidRequestException refused = assertThrows(cl.toString(), InvalidRequestException.class,
                                                           () -> StorageProxy.read(group, cl, ClientState.forInternalCalls(), Dispatcher.RequestTime.forImmediateExecution()));
            assertEquals("DSE 6 compatibility mode: the " + Operator.NOT_IN + " operator" + REFUSAL_SUFFIX, refused.getMessage());
        }
        assertEquals(3, DseCompatRefusals.count("read with the " + Operator.NOT_IN + " operator"));
    }

    @Test
    public void refusesAtTheCoordinator() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        execute("INSERT INTO %s (k, v) VALUES (1, 'one')");
        // with the mode off the DS-only disjunction is served
        assertRowsNet(executeNet("SELECT k FROM %s WHERE v = 'one' OR v = 'two' ALLOW FILTERING"), row(1));

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        // a client read goes through the coordinator (StorageProxy), where the guard sits
        assertInvalidMessageNet("DSE 6 compatibility mode: a disjunction (OR) in the WHERE clause is not available",
                                "SELECT k FROM %s WHERE v = 'one' OR v = 'two' ALLOW FILTERING");
        // NOT IN is sent as NEQ expressions, which Cassandra 3.0 knows: it is not refused
        assertRowsNet(executeNet("SELECT k FROM %s WHERE v NOT IN ('two') ALLOW FILTERING"), row(1));
        assertRowsNet(executeNet("SELECT k FROM %s WHERE v = 'one' ALLOW FILTERING"), row(1));
        assertRowsNet(executeNet("SELECT v FROM %s WHERE k = 1"), row("one"));
        assertEquals(1, DseCompatRefusals.total());
    }

    @Test
    public void refusesMapEntryComparisonsAtTheCoordinator() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, m map<text, int>)");
        execute("INSERT INTO %s (k, m) VALUES (1, {'a': 2})");
        assertRowsNet(executeNet("SELECT k FROM %s WHERE m['a'] > 1 ALLOW FILTERING"), row(1));

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        assertInvalidMessageNet("DSE 6 compatibility mode: a map entry compared with > is not available", "SELECT k FROM %s WHERE m['a'] > 1 ALLOW FILTERING");
        assertRowsNet(executeNet("SELECT k FROM %s WHERE m['a'] = 2 ALLOW FILTERING"), row(1));
        assertEquals(1, DseCompatRefusals.count("read with a map entry compared with >"));
    }

    @Test
    public void refusesIndexHintsAtTheCoordinator() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        String index = createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        execute("INSERT INTO %s (k, v) VALUES (1, 'one')");
        String hinted = "SELECT k FROM %s WHERE v = 'one' WITH included_indexes = {" + index + '}';
        assertRowsNet(executeNet(hinted), row(1));

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        assertInvalidMessageNet("DSE 6 compatibility mode: an index hint is not available", hinted);
        assertRowsNet(executeNet("SELECT k FROM %s WHERE v = 'one'"), row(1));
        assertEquals(1, DseCompatRefusals.count("read with an index hint"));
    }

    /**
     * With no connection pool below {@code VERSION_DS_20}, as here, CNDB-19234's own check
     * ({@code OptimizerOptions.validate}, which {@code SelectStatement} runs before {@code StorageProxy}) lets the
     * clause through and the guard refuses it. With a connection pool to a DSE 6 peer, recorded at version 11, that
     * check refuses it first, with "SAI optimizer options are not supported in clusters below DS 20." and no count, as
     * {@link #optimizerOptionsCheckRefusesFirstWithAPoolToADsePeer} shows.
     */
    @Test
    public void refusesOptimizerOptionsAtTheCoordinator() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        execute("INSERT INTO %s (k, v) VALUES (1, 'one')");
        String optimized = "SELECT k FROM %s WHERE v = 'one' WITH optimizer_options = {'query_optimization_level': '0'}";
        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.NONE);
        assertRowsNet(executeNet(optimized), row(1));

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        assertInvalidMessageNet("DSE 6 compatibility mode: an SAI optimizer option" + REFUSAL_SUFFIX, optimized);
        assertRowsNet(executeNet("SELECT k FROM %s WHERE v = 'one'"), row(1));
        // an empty clause gives no option (NONE): served, not counted
        assertRowsNet(executeNet("SELECT k FROM %s WHERE v = 'one' WITH optimizer_options = {}"), row(1));
        assertEquals(1, DseCompatRefusals.count("read with an SAI optimizer option"));
        assertEquals(1, DseCompatRefusals.total());
    }

    /**
     * The common case in a mixed cluster: this node has a connection pool to a DSE 6 peer of the ring, recorded at
     * version 11 (here the pool's connections are never established, as nothing listens at the peer's address, port
     * 1, so their version is the recorded one). CNDB-19234's own check refuses the read first, in its generic words:
     * the guard is not reached, and nothing is counted. In {@code DSE_6} recording the peer at version 11 also enters
     * it in the record of DSE 6 peers ({@code DsePeersRecord}), and forgetting it leaves it; the test checks that it is
     * gone from the record and its file at the end.
     */
    @Test
    public void optimizerOptionsCheckRefusesFirstWithAPoolToADsePeer() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        execute("INSERT INTO %s (k, v) VALUES (1, 'one')");
        String optimized = "SELECT k FROM %s WHERE v = 'one' WITH optimizer_options = {'query_optimization_level': '0'}";

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        InetAddressAndPort dsePeer = InetAddressAndPort.getByName("127.0.0.77:1");
        MessagingService messaging = MessagingService.instance();
        TokenMetadata ring = StorageService.instance.getTokenMetadata();
        int minClusterVersionBefore = messaging.versions.minClusterVersion;
        try
        {
            messaging.versions.set(dsePeer, MessagingService.VERSION_3014);
            ring.updateNormalToken(DatabaseDescriptor.getPartitioner().getRandomToken(), dsePeer);
            ring.updateHostId(UUID.randomUUID(), dsePeer); // the ring's endpoints are those with a host id

            // an ECHO, a verb of the dialect, opens the pool; it expires soon, so that the pool can close
            messaging.send(Message.out(Verb.ECHO_REQ, NoPayload.noPayload, approxTime.now() + TimeUnit.SECONDS.toNanos(1)), dsePeer);
            OutboundConnections pool = messaging.channelManagers.get(dsePeer);
            assertNotNull(pool);
            assertFalse(pool.small.isConnected() || pool.large.isConnected() || pool.urgent.isConnected());
            assertTrue(messaging.endpointsWithConnectionsOnVersionBelow(KEYSPACE, MessagingService.VERSION_DS_20).contains(dsePeer));

            assertInvalidMessageNet("SAI optimizer options are not supported in clusters below DS 20.", optimized);
            assertEquals(0, DseCompatRefusals.total());
        }
        finally
        {
            try
            {
                messaging.closeOutboundNow(dsePeer);
                Util.spinAssertEquals(false, () -> messaging.channelManagers.containsKey(dsePeer), 30);
            }
            finally
            {
                try
                {
                    messaging.versions.reset(dsePeer);
                    DsePeersRecord.awaitPersisted();
                    assertFalse(DsePeersRecord.peers().contains(dsePeer));
                    assertFalse(DsePeersRecord.persistedPeers().contains(dsePeer.getHostAddressAndPort()));
                    assertEquals(minClusterVersionBefore, messaging.versions.minClusterVersion);
                }
                finally
                {
                    ring.removeEndpoint(dsePeer);
                }
            }
        }
    }

    /** GROUP BY a primary-key prefix is Cassandra 3.10's and passes; grouping by a function of a column is not DSE's. */
    @Test
    public void refusesGroupByWithASelectorFunction() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, time timestamp, v int, PRIMARY KEY (pk, time))");
        execute("INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27 16:10:00 UTC', 1)");
        execute("INSERT INTO %s (pk, time, v) VALUES (1, '2016-09-27 16:12:00 UTC', 2)");
        String byFunction = "SELECT pk, floor(time, 5m), count(v) FROM %s GROUP BY pk, floor(time, 5m)";
        assertRowsNet(executeNet(byFunction), row(1, new Date(1474992600000L), 2L));

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_6);
        assertInvalidMessageNet("DSE 6 compatibility mode: GROUP BY with a selector function is not available", byFunction);
        assertRowsNet(executeNet("SELECT pk, count(v) FROM %s GROUP BY pk"), row(1, 2L));
        assertEquals(1, DseCompatRefusals.count("read with GROUP BY with a selector function"));
    }

    private static ColumnMetadata column(TableMetadata metadata, String name)
    {
        return metadata.getColumn(UTF8Type.instance.decompose(name));
    }

    private static RowFilter filter(ColumnMetadata column, Operator operator, ByteBuffer value)
    {
        RowFilter.Builder builder = RowFilter.builder(false);
        builder.add(new RowFilter.SimpleExpression(column, operator, value, null, null));
        return builder.build();
    }

    private static ReadCommand read(TableMetadata metadata, RowFilter rowFilter)
    {
        DecoratedKey key = metadata.partitioner.decorateKey(Int32Type.instance.decompose(1));
        return SinglePartitionReadCommand.create(metadata, FBUtilities.nowInSeconds(), ColumnFilter.all(metadata), rowFilter, DataLimits.NONE, key,
                                                 new ClusteringIndexSliceFilter(Slices.ALL, false), null);
    }

    /** A user-defined expression, which the guard refuses by its kind; it is never evaluated nor serialized here. */
    private static final class TestUserExpression extends RowFilter.UserExpression
    {
        TestUserExpression(ColumnMetadata column, ByteBuffer value)
        {
            super(column, Operator.EQ, value);
        }

        @Override
        public boolean isSatisfiedBy(TableMetadata metadata, DecoratedKey partitionKey, Row row, long nowInSec)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toCQLString(boolean redact)
        {
            return column.name.toCQLString() + " = " + ByteBufferUtil.bytesToHex(value);
        }

        @Override
        protected void serialize(DataOutputPlus out, int version) throws IOException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected long serializedSize(int version)
        {
            throw new UnsupportedOperationException();
        }
    }
}
