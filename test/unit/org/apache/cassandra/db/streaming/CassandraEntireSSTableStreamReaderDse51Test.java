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

package org.apache.cassandra.db.streaming;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.InternodeCompatibilityMode;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.DseCompatRefusals;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.SessionInfo;
import org.apache.cassandra.streaming.StreamCoordinator;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamSummary;
import org.apache.cassandra.streaming.async.NettyStreamingConnectionFactory;
import org.apache.cassandra.streaming.messages.StreamMessageHeader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * In the DSE 5.1 compatibility mode an entire sstable stream, which keeps the version of the sstable, is taken only for
 * the big 3.x versions DSE 5.1 reads; outside the mode every version is taken, as before.
 */
public class CassandraEntireSSTableStreamReaderDse51Test
{
    private static final String KEYSPACE = "CassandraEntireSSTableStreamReaderDse51Test";
    private static final String TABLE = "Standard1";

    private static ColumnFamilyStore store;
    private static TableMetadata table;
    private static InetAddressAndPort peer;

    private InternodeCompatibilityMode modeBefore;

    @BeforeClass
    public static void init() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), SchemaLoader.standardCFMD(KEYSPACE, TABLE));
        store = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        table = store.metadata();
        peer = InetAddressAndPort.getByName("127.0.0.2:7000");
    }

    @Before
    public void saveMode()
    {
        modeBefore = DatabaseDescriptor.getInternodeCompatibilityMode();
    }

    @After
    public void restoreMode()
    {
        DatabaseDescriptor.setInternodeCompatibilityMode(modeBefore);
    }

    @Test
    public void onlyTheVersionsDse51ReadsAreStreamedEntireInTheMode()
    {
        Version[] refused = { BigFormat.getInstance().getVersion("nb"), BigFormat.getInstance().getVersion("oa"),
                              BtiFormat.getInstance().getVersion("bb"), BtiFormat.getInstance().getVersion("da") };
        Version[] taken = { BigFormat.getInstance().getVersion("me"), BigFormat.getInstance().getVersion("ma") };

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        for (Version version : refused)
            assertThat(CassandraEntireSSTableStreamReader.dse51Refusal(version, peer, table))
            .as("%s", version)
            .isEqualTo("internode_compatibility_mode DSE_5_1 refuses the entire sstable " +
                       version.toFormatAndVersionString() + " streamed from /127.0.0.2:7000 for " + table +
                       ": DSE 5.1 does not read that version, which an entire sstable stream keeps; stream " +
                       "it with stream_entire_sstables: false on the sender, so that it is written here at the " +
                       "version DSE 5.1 reads");
        for (Version version : taken)
            assertThat(CassandraEntireSSTableStreamReader.dse51Refusal(version, peer, table)).as("%s", version)
                                                                                              .isNull();

        for (InternodeCompatibilityMode other : new InternodeCompatibilityMode[]{ InternodeCompatibilityMode.NONE,
                                                                                  InternodeCompatibilityMode.DSE_6 })
        {
            DatabaseDescriptor.setInternodeCompatibilityMode(other);
            for (Version version : refused)
                assertThat(CassandraEntireSSTableStreamReader.dse51Refusal(version, peer, table))
                .as("%s %s", other, version).isNull();
        }
    }

    /**
     * The reader refuses an entire sstable of this software's version in the mode before it writes any component,
     * and counts the refusal.
     */
    @Test
    public void theReaderRefusesTheStreamBeforeWritingAnything() throws Exception
    {
        for (int i = 0; i < 10; i++)
            new RowUpdateBuilder(table, i, String.valueOf(i)).clustering("0")
                                                              .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
                                                              .build()
                                                              .applyUnsafe();
        Util.flush(store);
        SSTableReader sstable = store.getLiveSSTables().iterator().next();
        assertThat(sstable.descriptor.version.hasPre40Encoding()).isFalse();
        StreamSession session = session();
        session.prepareReceiving(new StreamSummary(table.id, 1, sstable.bytesOnDisk()));
        CassandraEntireSSTableStreamReader reader;
        try (ComponentContext context = ComponentContext.create(sstable))
        {
            CassandraStreamHeader header = CassandraStreamHeader.builder()
                                                                .withSSTableVersion(sstable.descriptor.version)
                                                                .withSSTableLevel(0)
                                                                .withEstimatedKeys(sstable.estimatedKeys())
                                                                .withSections(Collections.emptyList())
                                                                .withSerializationHeader(sstable.header.toComponent())
                                                                .withComponentManifest(context.manifest())
                                                                .isEntireSSTable(true)
                                                                .withFirstKey(sstable.getFirst())
                                                                .withTableId(table.id)
                                                                .build();
            reader = new CassandraEntireSSTableStreamReader(new StreamMessageHeader(table.id, peer, session.planId(),
                                                                                    false, 0, 0, 0, null),
                                                            header, session);
        }
        Set<Path> filesBefore = files();
        long before = DseCompatRefusals.count(CassandraEntireSSTableStreamReader.DSE51_REFUSED_STREAM);

        DatabaseDescriptor.setInternodeCompatibilityMode(InternodeCompatibilityMode.DSE_5_1);
        assertThatThrownBy(() -> reader.read(new DataInputBuffer(ByteBuffer.allocate(0), false)))
        .hasMessage(CassandraEntireSSTableStreamReader.dse51Refusal(sstable.descriptor.version, session.peer, table));
        assertThat(DseCompatRefusals.count(CassandraEntireSSTableStreamReader.DSE51_REFUSED_STREAM))
        .isEqualTo(before + 1);
        assertThat(files()).isEqualTo(filesBefore);
    }

    /** Every file of the table's directories, those of its subdirectories included. */
    private static Set<Path> files() throws IOException
    {
        Set<Path> files = new HashSet<>();
        for (File directory : store.getDirectories().getCFDirectories())
        {
            try (Stream<Path> paths = Files.walk(directory.toPath()))
            {
                paths.forEach(files::add);
            }
        }
        assertThat(files).isNotEmpty();
        return files;
    }

    private static StreamSession session()
    {
        StreamCoordinator coordinator = new StreamCoordinator(StreamOperation.BOOTSTRAP, 1,
                                                              new NettyStreamingConnectionFactory(), false, false,
                                                              null, PreviewKind.NONE);
        StreamResultFuture future = StreamResultFuture.createInitiator(nextTimeUUID(), StreamOperation.BOOTSTRAP,
                                                                       Collections.emptyList(), coordinator);
        InetAddressAndPort self = FBUtilities.getBroadcastAddressAndPort();
        coordinator.addSessionInfo(new SessionInfo(self, 0, self, Collections.emptyList(), Collections.emptyList(),
                                                   StreamSession.State.INITIALIZED, null));
        StreamSession session = coordinator.getOrCreateOutboundSession(self);
        session.init(future);
        return session;
    }
}
