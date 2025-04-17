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

package org.apache.cassandra.io.sstable;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.ClientState;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * SAI specific tests for {@link CQLSSTableWriter}. These tests have been moved from CQLSSTableWriterTest to isolate
 * the SAI tests from the rest of the CQLSSTableWriter tests because the SAI tests require the use of the
 * Murmur3Partitioner when executed with daemonInitialization in CQLSSTableWriterSATDaemonTest. See CNNDB-13401.
 * <p>
 * Please note: most tests here both create sstables and try to load them, so for the last part, we need to make sure
 * we have properly "loaded" the table (which we do with {@link SchemaLoader#load(String, String, String...)}). But
 * a small subtlety is that this <b>must</b> be called before we call {@link CQLSSTableWriter#builder} because
 * otherwise the guardrail validation in {@link CreateTableStatement#validate(ClientState)} ends up breaking because
 * the {@link ColumnFamilyStore} is not loaded yet. This would not be a problem in real usage of
 * {@link CQLSSTableWriter} because the later only calls {@link DatabaseDescriptor#clientInitialization}, not
 * {@link DatabaseDescriptor#daemonInitialization}, so said guardrail validation don't execute, but this test does
 * manually call {@link DatabaseDescriptor#daemonInitialization} so...
 */
@Ignore
public abstract class CQLSSTableWriterSAITest
{
    private static final AtomicInteger idGen = new AtomicInteger(0);

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private String keyspace;
    protected String table;
    protected String qualifiedTable;
    protected File dataDir;

    @Before
    public void perTestSetup() throws IOException
    {
        keyspace = "cql_keyspace" + idGen.incrementAndGet();
        table = "table" + idGen.incrementAndGet();
        qualifiedTable = keyspace + '.' + table;
        dataDir = new File(tempFolder.getRoot().getAbsolutePath() + File.pathSeparator() + keyspace + File.pathSeparator() + table);
        assert dataDir.tryCreateDirectories();
    }

    @Test
    public void testWriteWithSAI() throws Exception
    {
        writeWithSaiInternal();
        writeWithSaiInternal();
    }

    private void writeWithSaiInternal() throws Exception
    {
        String schema = "CREATE TABLE " + qualifiedTable + " ("
                        + "  k int PRIMARY KEY,"
                        + "  v1 text,"
                        + "  v2 int )";

        String v1Index = "CREATE INDEX idx1 ON " + qualifiedTable + " (v1) USING 'sai'";
        String v2Index = "CREATE INDEX idx2 ON " + qualifiedTable + " (v2) USING 'sai'";

        String insert = "INSERT INTO " + qualifiedTable + " (k, v1, v2) VALUES (?, ?, ?)";

        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .forTable(schema)
                                                  .using(insert)
                                                  .withIndexes(v1Index, v2Index)
                                                  .withBuildIndexes(true)
                                                  .withPartitioner(Murmur3Partitioner.instance)
                                                  .build();

        int rowCount = 30_000;
        for (int i = 0; i < rowCount; i++)
            writer.addRow(i, UUID.randomUUID().toString(), i);

        writer.close();

        File[] dataFiles = dataDir.list(f -> f.name().endsWith('-' + BigFormat.Components.DATA.type.repr));
        assertNotNull(dataFiles);

        IndexDescriptor indexDescriptor = IndexDescriptor.empty(Descriptor.fromFile(dataFiles[0]));

        IndexContext idx1 = createIndexContext("idx1", UTF8Type.instance);
        IndexContext idx2 = createIndexContext("idx2", UTF8Type.instance);
        HashSet<IndexContext> indices = new HashSet<>(Arrays.asList(idx1, idx2));
        SSTableReader sstable = SSTableReader.openNoValidation(null, indexDescriptor.descriptor, writer.getMetadata());
        indexDescriptor.reload(sstable, indices);

        assertTrue(indexDescriptor.perIndexComponents(idx1).isComplete());
        assertTrue(indexDescriptor.perIndexComponents(idx2).isComplete());

        if (PathUtils.isDirectory(dataDir.toPath()))
            PathUtils.forEach(dataDir.toPath(), PathUtils::deleteRecursive);
    }

    @Test
    public void testSkipBuildingIndexesWithSAI() throws Exception
    {
        String schema = "CREATE TABLE " + qualifiedTable + " ("
                        + "  k int PRIMARY KEY,"
                        + "  v1 text,"
                        + "  v2 int )";

        String v1Index = "CREATE INDEX idx1 ON " + qualifiedTable + " (v1) USING 'sai'";
        String v2Index = "CREATE INDEX idx2 ON " + qualifiedTable + " (v2) USING 'sai'";

        String insert = "INSERT INTO " + qualifiedTable + " (k, v1, v2) VALUES (?, ?, ?)";

        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .forTable(schema)
                                                  .using(insert)
                                                  .withIndexes(v1Index, v2Index)
                                                  // not building indexes here so no SAI components will be present
                                                  .withBuildIndexes(false)
                                                  .build();

        int rowCount = 30_000;
        for (int i = 0; i < rowCount; i++)
            writer.addRow(i, UUID.randomUUID().toString(), i);

        writer.close();

        File[] dataFiles = dataDir.list(f -> f.name().endsWith('-' + BigFormat.Components.DATA.type.repr));
        assertNotNull(dataFiles);

        // no indexes built due to withBuildIndexes set to false
        IndexDescriptor indexDescriptor = IndexDescriptor.empty(Descriptor.fromFile(dataFiles[0]));

        assertFalse(indexDescriptor.perIndexComponents(createIndexContext("idx1", UTF8Type.instance)).isComplete());
        assertFalse(indexDescriptor.perIndexComponents(createIndexContext("idx2", UTF8Type.instance)).isComplete());
    }

    public IndexContext createIndexContext(String name, AbstractType<?> validator)
    {
        return new IndexContext(keyspace,
                                table,
                                TableId.generate(),
                                UTF8Type.instance,
                                new ClusteringComparator(),
                                ColumnMetadata.regularColumn("sai", "internal", name, validator),
                                IndexTarget.Type.SIMPLE,
                                IndexMetadata.fromSchemaMetadata(name, IndexMetadata.Kind.CUSTOM, null),
                                MockSchema.newCFS(keyspace));
    }
}
