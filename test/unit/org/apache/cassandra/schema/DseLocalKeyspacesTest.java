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
package org.apache.cassandra.schema;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.repair.consistent.admin.SchemaArgsParser;
import org.apache.cassandra.schema.SchemaTransformation.SchemaTransformationResult;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_ALLOW_LOCAL_STRATEGY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * The two keyspaces a DataStax Enterprise 6 node creates with {@code LocalStrategy}, {@code dse_system_local} and
 * {@code dse_insights_local}, are rows of {@code system_schema} on a node upgraded in place: {@link Schema} loads
 * them with the rest of the distributed schema, keeps them out of the keyspaces that own token ranges, and still
 * refuses {@code LocalStrategy} for any other distributed keyspace.
 */
public class DseLocalKeyspacesTest
{
    private static final String RING_KEYSPACE = "dse_local_keyspaces_ring";
    private static final String RING_TABLE = "ring_table";
    private static final String DSE_SYSTEM_LOCAL = "dse_system_local";
    private static final String DSE_INSIGHTS_LOCAL = "dse_insights_local";
    private static final String DSE_TABLE = "solr_resources";

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        ServerTestUtils.prepareServer();
        Schema.instance.loadFromDisk();
    }

    @Test
    public void theDseLocalKeyspacesLoadFromTheSchemaTablesButOwnNoTokenRanges() throws Exception
    {
        TokenMetadata tokenMetadata = StorageService.instance.getTokenMetadata();
        try (WithProperties properties = new WithProperties().clear(TEST_ALLOW_LOCAL_STRATEGY))
        {
            // the rows a DSE 6 node leaves in system_schema (the table as DSE 6.9.26 describes it), read back as the schema
            // of the node
            KeyspaceMetadata dseSystemLocal = KeyspaceMetadata.create(DSE_SYSTEM_LOCAL,
                                                                      KeyspaceParams.local(),
                                                                      Tables.of(TableMetadata.builder(DSE_SYSTEM_LOCAL, DSE_TABLE)
                                                                                             .addPartitionKeyColumn("core_name", UTF8Type.instance)
                                                                                             .addClusteringColumn("resource_name", UTF8Type.instance)
                                                                                             .addRegularColumn("resource_value", BytesType.instance)
                                                                                             .build()));
            saveKeyspaces(dseSystemLocal, KeyspaceMetadata.create(DSE_INSIGHTS_LOCAL, KeyspaceParams.local()));
            Schema.instance.reloadSchemaAndAnnounceVersion();
            assertNotNull(Schema.instance.getTableMetadata(DSE_SYSTEM_LOCAL, DSE_TABLE));

            // a keyspace that owns token ranges, loaded after the DSE ones, so that the first distributed keyspace is
            // a DSE local one: a call site that picks the first distributed keyspace as its default picks it
            KeyspaceMetadata ring = KeyspaceMetadata.create(RING_KEYSPACE,
                                                            KeyspaceParams.simple(1),
                                                            Tables.of(TableMetadata.builder(RING_KEYSPACE, RING_TABLE)
                                                                                   .addPartitionKeyColumn("k", UTF8Type.instance)
                                                                                   .addRegularColumn("v", UTF8Type.instance)
                                                                                   .build()));
            saveKeyspaces(ring);
            Schema.instance.reloadSchemaAndAnnounceVersion();
            assertThat(Schema.instance.distributedKeyspaces().iterator().next().name).isIn(SchemaConstants.DSE_LOCAL_STRATEGY_KEYSPACE_NAMES);

            assertThat(Schema.instance.distributedKeyspaces().names()).contains(RING_KEYSPACE, DSE_SYSTEM_LOCAL, DSE_INSIGHTS_LOCAL);
            assertThat(Schema.instance.getNonLocalStrategyKeyspaces().names()).contains(RING_KEYSPACE)
                                                                             .doesNotContain(DSE_SYSTEM_LOCAL, DSE_INSIGHTS_LOCAL);
            // nothing else is filtered out
            List<String> expected = new ArrayList<>(Schema.instance.distributedKeyspaces().names());
            expected.removeAll(SchemaConstants.DSE_LOCAL_STRATEGY_KEYSPACE_NAMES);
            assertThat(Schema.instance.getNonLocalStrategyKeyspaces().names()).containsExactlyInAnyOrderElementsOf(expected);

            assertThat(names(Keyspace.distributed())).contains(RING_KEYSPACE, DSE_SYSTEM_LOCAL, DSE_INSIGHTS_LOCAL);
            assertThat(names(Keyspace.nonLocalStrategy())).contains(RING_KEYSPACE)
                                                          .doesNotContain(DSE_SYSTEM_LOCAL, DSE_INSIGHTS_LOCAL);
            // the default keyspace list of nodetool repair and cleanup, and the non-system keyspaces it is not
            assertThat(StorageService.instance.getNonLocalStrategyKeyspaces()).contains(RING_KEYSPACE)
                                                                             .doesNotContain(DSE_SYSTEM_LOCAL, DSE_INSIGHTS_LOCAL);
            assertThat(StorageService.instance.getNonSystemKeyspaces()).contains(RING_KEYSPACE, DSE_SYSTEM_LOCAL, DSE_INSIGHTS_LOCAL);
            // the tables of nodetool repair_admin without a keyspace argument
            List<String> repairAdminTables = new ArrayList<>();
            for (ColumnFamilyStore cfs : SchemaArgsParser.parse(Collections.emptyList()))
                repairAdminTables.add(cfs.getKeyspaceName() + '.' + cfs.getTableName());
            assertThat(repairAdminTables).contains(RING_KEYSPACE + '.' + RING_TABLE)
                                         .doesNotContain(DSE_SYSTEM_LOCAL + '.' + DSE_TABLE);

            // two nodes with one token each: with SimpleStrategy and one replica each node owns one of the two
            // ranges, with LocalStrategy both ranges would map to this node
            InetAddressAndPort self = FBUtilities.getBroadcastAddressAndPort();
            InetAddressAndPort other = InetAddressAndPort.getByName("127.0.0.2");
            Token.TokenFactory tokenFactory = DatabaseDescriptor.getPartitioner().getTokenFactory();
            tokenMetadata.updateNormalToken(tokenFactory.fromString("10"), self);
            tokenMetadata.updateNormalToken(tokenFactory.fromString("80"), other);

            // the default keyspace of the range-to-endpoint map is one that owns token ranges
            List<String> rangeOwners = new ArrayList<>();
            StorageService.instance.getRangeToEndpointMap(null).values().forEach(rangeOwners::addAll);
            assertThat(rangeOwners).containsExactlyInAnyOrder(self.getHostAddress(false), other.getHostAddress(false));

            // nodetool status without a keyspace: the DSE local keyspaces do not make the ownership meaningless
            LinkedHashMap<InetAddress, Float> ownership = StorageService.instance.effectiveOwnership(null);
            assertThat(ownership).containsOnlyKeys(self.getAddress(), other.getAddress());
            assertEquals(1.0f, ownership.get(self.getAddress()) + ownership.get(other.getAddress()), 0.0001f);

            // without a LocalStrategy keyspace the filtered list is the distributed list itself
            Schema.instance.transform(keyspaces -> keyspaces.without(Arrays.asList(DSE_SYSTEM_LOCAL, DSE_INSIGHTS_LOCAL)));
            assertNull(Schema.instance.getKeyspaceMetadata(DSE_SYSTEM_LOCAL));
            assertNotNull(Schema.instance.getKeyspaceMetadata(RING_KEYSPACE));
            assertSame(Schema.instance.distributedKeyspaces(), Schema.instance.getNonLocalStrategyKeyspaces());
        }
        finally
        {
            tokenMetadata.clearUnsafe();
            Schema.instance.transform(keyspaces -> keyspaces.without(Arrays.asList(RING_KEYSPACE, DSE_SYSTEM_LOCAL, DSE_INSIGHTS_LOCAL)));
        }
    }

    @Test
    public void localStrategyIsRefusedForAnyOtherDistributedKeyspace()
    {
        boolean assertionsEnabled = false;
        assert assertionsEnabled = true;
        assertTrue("the LocalStrategy check of Schema is an assertion: run with -ea", assertionsEnabled);

        Keyspace.unsetInitialized();
        try (WithProperties properties = new WithProperties().clear(TEST_ALLOW_LOCAL_STRATEGY))
        {
            Schema schema = new Schema(false, Keyspaces.of(SchemaKeyspace.metadata(), SystemKeyspace.metadata()), mock(SchemaUpdateHandler.class));

            merge(schema, KeyspaceMetadata.create(RING_KEYSPACE, KeyspaceParams.simple(1)),
                  KeyspaceMetadata.create(DSE_SYSTEM_LOCAL, KeyspaceParams.local()),
                  KeyspaceMetadata.create(DSE_INSIGHTS_LOCAL, KeyspaceParams.local()));
            assertNotNull(schema.getKeyspaceMetadata(DSE_SYSTEM_LOCAL));
            assertNotNull(schema.getKeyspaceMetadata(DSE_INSIGHTS_LOCAL));
            assertThat(schema.getNonLocalStrategyKeyspaces().names()).containsExactly(RING_KEYSPACE);

            // any other name, the DSE names in another case included (keyspace names are case-sensitive)
            for (String name : Arrays.asList("other_local", "DSE_SYSTEM_LOCAL", "dse_system"))
            {
                assertThatThrownBy(() -> merge(schema, KeyspaceMetadata.create(name, KeyspaceParams.local())))
                .isInstanceOf(AssertionError.class)
                .hasMessage("LocalStrategy should not be used");
                assertNull(schema.getKeyspaceMetadata(name));
            }
            assertThat(schema.getNonLocalStrategyKeyspaces().names()).containsExactly(RING_KEYSPACE);
        }
        finally
        {
            Keyspace.setInitialized();
        }
    }

    @Test
    public void theDseLocalKeyspaceNamesAreComparedExactly()
    {
        assertTrue(SchemaConstants.isDseLocalStrategyKeyspace(DSE_SYSTEM_LOCAL));
        assertTrue(SchemaConstants.isDseLocalStrategyKeyspace(DSE_INSIGHTS_LOCAL));
        assertFalse(SchemaConstants.isDseLocalStrategyKeyspace("DSE_SYSTEM_LOCAL"));
        assertFalse(SchemaConstants.isDseLocalStrategyKeyspace("dse_system"));
        assertFalse(SchemaConstants.isDseLocalStrategyKeyspace(SchemaConstants.SYSTEM_KEYSPACE_NAME));
    }

    private static void saveKeyspaces(KeyspaceMetadata... keyspaces)
    {
        List<Mutation> mutations = new ArrayList<>();
        for (KeyspaceMetadata keyspace : keyspaces)
            mutations.add(SchemaKeyspace.makeCreateKeyspaceMutation(keyspace, FBUtilities.timestampMicros()).build());
        SchemaKeyspace.applyChanges(mutations);
    }

    private static void merge(Schema schema, KeyspaceMetadata... added)
    {
        DistributedSchema before = new DistributedSchema(schema.distributedKeyspaces(), schema.getVersion());
        DistributedSchema after = new DistributedSchema(schema.distributedKeyspaces().withAddedOrUpdated(Keyspaces.of(added)), UUID.randomUUID());
        schema.mergeAndUpdateVersion(new SchemaTransformationResult(before, after, Keyspaces.diff(before.getKeyspaces(), after.getKeyspaces())), true);
    }

    private static List<String> names(Iterable<Keyspace> keyspaces)
    {
        List<String> names = new ArrayList<>();
        for (Keyspace keyspace : keyspaces)
            names.add(keyspace.getName());
        return names;
    }
}
