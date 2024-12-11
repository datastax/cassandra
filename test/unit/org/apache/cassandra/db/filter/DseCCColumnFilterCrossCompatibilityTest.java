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

package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import com.google.common.collect.Iterators;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;

import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.gms.IClusterVersionProvider;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.CassandraVersion;

import static org.apache.cassandra.net.MessagingService.VERSION_30;
import static org.apache.cassandra.net.MessagingService.VERSION_3014;
import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.net.MessagingService.VERSION_41;
import static org.apache.cassandra.net.MessagingService.VERSION_DSE_68;
import static org.apache.cassandra.net.MessagingService.VERSION_SG_10;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

public class DseCCColumnFilterCrossCompatibilityTest
{
    static
    {
        System.setProperty("cassandra.cluster_version_provider.class_name", FixedClusterVersionProvider.class.getName());
    }

    public static class FixedClusterVersionProvider implements IClusterVersionProvider
    {
        public static final FixedClusterVersionProvider instance = new FixedClusterVersionProvider();
        public volatile CassandraVersion version = new CassandraVersion("3.0.0");

        @Override
        public CassandraVersion getMinClusterVersion()
        {
            return version;
        }

        @Override
        public void reset()
        {
            // no-op
        }

        @Override
        public boolean isUpgradeInProgress()
        {
            return !version.equals(SystemKeyspace.CURRENT_VERSION);
        }
    }

    public final String KS = "ks";
    public final String TAB = "tab";

    public final TableMetadata metadata = TableMetadata.builder(KS, TAB)
                                                       .addPartitionKeyColumn("k1", Int32Type.instance)
                                                       .addPartitionKeyColumn("k2", Int32Type.instance)
                                                       .addClusteringColumn("k3", Int32Type.instance)
                                                       .addRegularColumn("val1", Int32Type.instance)
                                                       .addStaticColumn("val2", Int32Type.instance)
                                                       .addStaticColumn("val3", UTF8Type.instance)
                                                       .addRegularColumn("val4", Int32Type.instance)
                                                       .build();

    @Test
    public void testAll()
    {
        ColumnFilter cc = ColumnFilter.all(metadata);
        DseColumnFilter dse = DseColumnFilter.all(metadata);
        assertFilters(cc, dse);
    }


    @Test
    public void testSelection() throws IOException
    {
        ColumnMetadata[] cols = Iterators.toArray(metadata.regularAndStaticColumns().iterator(), ColumnMetadata.class);

        testSelection(RegularAndStaticColumns.builder().addAll(metadata.regularColumns()).build());
        testSelection(RegularAndStaticColumns.builder().addAll(metadata.staticColumns()).build());
        testSelection(RegularAndStaticColumns.builder().addAll(metadata.regularAndStaticColumns()).build());
        testSelection(RegularAndStaticColumns.builder().add(cols[0]).build());
        testSelection(RegularAndStaticColumns.builder().add(cols[1]).build());
        testSelection(RegularAndStaticColumns.builder().add(cols[2]).build());
        testSelection(RegularAndStaticColumns.builder().add(cols[3]).build());
    }

    public void testSelection(RegularAndStaticColumns sel) throws IOException
    {
        for (CassandraVersion cassandraVersion : new CassandraVersion[]{ new CassandraVersion("3.0.0"), new CassandraVersion("3.11.0"), new CassandraVersion("4.0.0") })
        {
            for (int version : new int[]{ VERSION_30, VERSION_3014, VERSION_40, VERSION_41, VERSION_SG_10, VERSION_DSE_68 })
            {
                try {
                    ColumnFilter cc = ColumnFilter.allRegularColumnsBuilder(metadata, true).addAll(sel::selectOrderIterator).build();
                    DseColumnFilter dse = roundTrip(cc, version);
                    assertFilters(cc, dse);

                    dse = DseColumnFilter.allRegularColumnsBuilder(metadata).addAll(sel::selectOrderIterator).build();
                    cc = roundTrip(dse, version);
                    assertFilters(cc, dse);
                } catch (AssertionError e) {
                    System.out.println("Failed for version " + version + " and cassandra version " + cassandraVersion);
                    e.printStackTrace();
                }
            }
        }
    }

    private ColumnFilter roundTrip(DseColumnFilter dseColumnFilter, int version)
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            new DseColumnFilter.Serializer(version).serialize(dseColumnFilter, out);
            ByteBuffer buf = out.asNewBuffer();
            try (DataInputBuffer in = new DataInputBuffer(buf, true))
            {
                return ColumnFilter.serializer.deserialize(in, version, metadata);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private DseColumnFilter roundTrip(ColumnFilter ccColumnFilter, int version)
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            ColumnFilter.serializer.serialize(ccColumnFilter, out, version);
            ByteBuffer buf = out.asNewBuffer();
            try (DataInputBuffer in = new DataInputBuffer(buf, true))
            {
                return new DseColumnFilter.Serializer(version).deserialize(in, metadata);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private void assertSerialization(ColumnFilter cc, DseColumnFilter dse) throws IOException
    {
        for (int version : new int[]{ VERSION_30, VERSION_3014, VERSION_40, VERSION_41, VERSION_SG_10, VERSION_DSE_68 })
        {
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                ColumnFilter.serializer.serialize(cc, out, version);
                ByteBuffer buf = out.asNewBuffer();
                try (DataInputBuffer in = new DataInputBuffer(buf, true))
                {
                    DseColumnFilter ccToDSe = new DseColumnFilter.Serializer(version).deserialize(in, metadata);
                    assertFilters(cc, ccToDSe);
                }
            }


            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                new DseColumnFilter.Serializer(version).serialize(dse, out);
                ByteBuffer buf = out.asNewBuffer();
                try (DataInputBuffer in = new DataInputBuffer(buf, true))
                {
                    ColumnFilter dseToCC = ColumnFilter.serializer.deserialize(in, version, metadata);
                    assertFilters(dseToCC, dse);
                }
            }
        }
    }

    private void assertFilters(ColumnFilter cc, DseColumnFilter dse)
    {
        assertThat(cc.fetchedColumns()).isEqualTo(dse.fetchedColumns());
        assertThat(cc.queriedColumns()).isEqualTo(dse.queriedColumns());
         assertThat(cc.allFetchedColumnsAreQueried()).isEqualTo(dse.allFetchedColumnsAreQueried());
        assertThat(cc.fetchesAllColumns(true)).isEqualTo(dse.fetchesAllColumns(true));
        assertThat(cc.fetchesAllColumns(false)).isEqualTo(dse.fetchesAllColumns(false));

        for (ColumnMetadata c : metadata.columns())
        {
            assertThat(cc.fetchedColumnIsQueried(c)).describedAs(c.toString()).isEqualTo(dse.fetchedColumnIsQueried(c));
            assertThat(cc.fetches(c)).describedAs(c.toString()).isEqualTo(dse.fetches(c));
        }
    }
}
