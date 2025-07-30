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
package org.apache.cassandra.io.sstable.metadata;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.common.primitives.Bytes;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.Encryptor;
import org.apache.cassandra.io.compress.EncryptorTest;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableIdTest;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.trieindex.TrieIndexFormat;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.io.compress.EncryptionConfig.CIPHER_ALGORITHM;
import static org.apache.cassandra.io.compress.EncryptionConfig.KEY_PROVIDER;
import static org.apache.cassandra.io.compress.EncryptionConfig.SECRET_KEY_STRENGTH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MetadataSerializerTest
{
    static final CompressionParams compressionParams;

    static
    {
        DatabaseDescriptor.daemonInitialization();
    }

    static
    {
        Map<String, String> opts = new HashMap<>();

        opts.put(CompressionParams.CLASS, Encryptor.class.getName());

        opts.put(CIPHER_ALGORITHM, "AES/CBC/PKCS5Padding");
        opts.put(SECRET_KEY_STRENGTH, Integer.toString(128));

        opts.put(KEY_PROVIDER, EncryptorTest.KeyProviderFactoryStub.class.getName());

        compressionParams = CompressionParams.fromMap(opts);
    }

    final static String sensitiveKey = "Key with sensitive information";

    @BeforeClass
    public static void initDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testSerialization() throws IOException
    {
        Map<MetadataType, MetadataComponent> originalMetadata = constructMetadata(false);

        MetadataSerializer serializer = new MetadataSerializer();
        File statsFile = serialize(originalMetadata, serializer, SSTableFormat.Type.current().info.getLatestVersion(), false);

        Descriptor desc = new Descriptor(statsFile.parent(), "", "", new SequenceBasedSSTableId(0), SSTableFormat.Type.current());
        try (RandomAccessReader in = RandomAccessReader.open(statsFile))
        {
            Map<MetadataType, MetadataComponent> deserialized = serializer.deserialize(desc, in, EnumSet.allOf(MetadataType.class));

            for (MetadataType type : MetadataType.values())
            {
                assertEquals(originalMetadata.get(type), deserialized.get(type));
            }
        }
    }

    @Test
    public void testHistogramSterilization() throws IOException
    {
        Map<MetadataType, MetadataComponent> originalMetadata = constructMetadata(false);

        // Modify the histograms to overflow:
        StatsMetadata originalStats = (StatsMetadata) originalMetadata.get(MetadataType.STATS);
        originalStats.estimatedCellPerPartitionCount.add(Long.MAX_VALUE);
        originalStats.estimatedPartitionSize.add(Long.MAX_VALUE);
        assertTrue(originalStats.estimatedCellPerPartitionCount.isOverflowed());
        assertTrue(originalStats.estimatedPartitionSize.isOverflowed());

        // Serialize w/ overflowed histograms:
        MetadataSerializer serializer = new MetadataSerializer();
        File statsFile = serialize(originalMetadata, serializer, BigFormat.latestVersion, false);
        Descriptor desc = new Descriptor(statsFile.parent(), "", "", new SequenceBasedSSTableId(0), SSTableFormat.Type.BIG);

        try (RandomAccessReader in = RandomAccessReader.open(statsFile))
        {
            // Deserialie and verify that the two histograms have had their overflow buckets cleared:
            Map<MetadataType, MetadataComponent> deserialized = serializer.deserialize(desc, in, EnumSet.allOf(MetadataType.class));
            StatsMetadata deserializedStats = (StatsMetadata) deserialized.get(MetadataType.STATS);
            assertFalse(deserializedStats.estimatedCellPerPartitionCount.isOverflowed());
            assertFalse(deserializedStats.estimatedPartitionSize.isOverflowed());
        }
    }

    @Test
    public void testSerializationWithEncryption() throws IOException
    {
        Map<MetadataType, MetadataComponent> originalMetadata = constructMetadata(false);

        MetadataSerializer serializer = new MetadataSerializer();
        File statsFile = serialize(originalMetadata, serializer, SSTableFormat.Type.current().info.getLatestVersion(), true);

        Descriptor desc = new Descriptor(statsFile.parent(), "", "", new SequenceBasedSSTableId(0), SSTableFormat.Type.current());
        try (RandomAccessReader in = RandomAccessReader.open(statsFile))
        {
            Map<MetadataType, MetadataComponent> deserialized = serializer.deserialize(desc, in, EnumSet.allOf(MetadataType.class));

            for (MetadataType type : MetadataType.values())
            {
                assertEquals(originalMetadata.get(type), deserialized.get(type));
            }
        }
    }

    @Test
    public void testEncryption() throws IOException
    {
        Map<MetadataType, MetadataComponent> originalMetadata = constructMetadata(false);

        for (Version version : ImmutableList.of(BigFormat.latestVersion, TrieIndexFormat.latestVersion))
        {
            MetadataSerializer serializer = new MetadataSerializer();
            File statsFile = serialize(originalMetadata, serializer, version, true);

            byte[] contents = Files.toByteArray(statsFile.toJavaIOFile());
            byte[] sought = sensitiveKey.getBytes(StandardCharsets.UTF_8);

            assertEquals(version.metadataAreEncrypted(), Bytes.indexOf(contents, sought) == -1);
        }
    }

    public File serialize(Map<MetadataType, MetadataComponent> metadata, MetadataSerializer serializer, Version version, boolean tryEncryption)
    throws IOException
    {
        Descriptor descriptor = new Descriptor(version, FileUtils.getTempDir(), "test", FileUtils.createTempFile("test", "").name(), new SequenceBasedSSTableId(0), version.getSSTableFormat().getType());

        // Serialize to tmp file
        File statsFile = descriptor.fileFor(Component.STATS);

        if (tryEncryption)
        {
            CompressionMetadata.Writer writer = CompressionMetadata.Writer.open(compressionParams, descriptor.fileFor(Component.COMPRESSION_INFO));
            writer.doPrepare();
        }

        try (DataOutputStreamPlus out = new FileOutputStreamPlus(statsFile))
        {
            serializer.serialize(metadata, out, descriptor);
        }
        return statsFile;
    }

    public Map<MetadataType, MetadataComponent> constructMetadata(boolean withNulls)
    {
        CommitLogPosition club = new CommitLogPosition(11L, 12);
        CommitLogPosition cllb = new CommitLogPosition(9L, 12);

        TableMetadata cfm = SchemaLoader.clusteringSASICFMD("ks1", "cf1").build();
        MetadataCollector collector = new MetadataCollector(cfm.comparator)
                                      .commitLogIntervals(new IntervalSet<>(cllb, club));

        String partitioner = RandomPartitioner.class.getCanonicalName();
        double bfFpChance = 0.1;
        collector.updateClusteringValues(Clustering.make(UTF8Type.instance.decompose("abc"), Int32Type.instance.decompose(123)));
        collector.updateClusteringValues(Clustering.make(UTF8Type.instance.decompose("cba"), withNulls ? null : Int32Type.instance.decompose(234)));
        collector.updateClusteringValues(Clustering.make(UTF8Type.instance.decompose(sensitiveKey)));
        return collector.finalizeMetadata(partitioner, bfFpChance, 0, null, false, SerializationHeader.make(cfm, Collections.emptyList()));
    }

    private void testVersions(String... versions) throws Throwable
    {
        Throwable t = null;
        for (int oldIdx = 0; oldIdx < versions.length; oldIdx++)
        {
            for (int newIdx = oldIdx; newIdx < versions.length; newIdx++)
            {
                try
                {
                    testOldReadsNew(versions[oldIdx], versions[newIdx]);
                }
                catch (Exception | AssertionError e)
                {
                    t = Throwables.merge(t, new AssertionError("Failed to test " + versions[oldIdx] + " -> " + versions[newIdx], e));
                }
            }
        }
        if (t != null)
        {
            throw t;
        }
    }

    @Test
    public void testMVersions() throws Throwable
    {
        Assume.assumeTrue(SSTableFormat.Type.current() == SSTableFormat.Type.BIG);
        testVersions("ma", "mb", "mc", "md", "me");
    }

    @Test
    public void testNVersions() throws Throwable
    {
        Assume.assumeTrue(SSTableFormat.Type.current() == SSTableFormat.Type.BIG);
        testVersions("na", "nb", "nc");
    }

    @Test
    public void testAVersions() throws Throwable
    {
        Assume.assumeTrue(SSTableFormat.Type.current() == SSTableFormat.Type.BTI);
        testVersions("aa", "ac", "ad");
    }

    @Test
    public void testBVersions() throws Throwable
    {
        Assume.assumeTrue(SSTableFormat.Type.current() == SSTableFormat.Type.BTI);
        testVersions("ba", "bb");
    }

    @Test
    public void testCVersions() throws Throwable
    {
        Assume.assumeTrue(SSTableFormat.Type.current() == SSTableFormat.Type.BTI);
        testVersions("ca", "cb");
    }

    public void testOldReadsNew(String oldV, String newV) throws IOException
    {
        Map<MetadataType, MetadataComponent> originalMetadata = constructMetadata(true);

        MetadataSerializer serializer = new MetadataSerializer();
        // Write metadata in two minor formats.
        File statsFileLb = serialize(originalMetadata, serializer, SSTableFormat.Type.current().info.getVersion(newV), false);
        File statsFileLa = serialize(originalMetadata, serializer, SSTableFormat.Type.current().info.getVersion(oldV), false);
        // Reading both as earlier version should yield identical results.
        SSTableFormat.Type stype = SSTableFormat.Type.current();
        Descriptor desc = new Descriptor(stype.info.getVersion(oldV), statsFileLb.parent(), "", "", new SequenceBasedSSTableId(0), stype);
        try (RandomAccessReader inLb = RandomAccessReader.open(statsFileLb);
             RandomAccessReader inLa = RandomAccessReader.open(statsFileLa))
        {
            Map<MetadataType, MetadataComponent> deserializedLb = serializer.deserialize(desc, inLb, EnumSet.allOf(MetadataType.class));
            Map<MetadataType, MetadataComponent> deserializedLa = serializer.deserialize(desc, inLa, EnumSet.allOf(MetadataType.class));

            for (MetadataType type : MetadataType.values())
            {
                assertEquals(deserializedLa.get(type), deserializedLb.get(type));

                if (MetadataType.STATS != type)
                    assertEquals(originalMetadata.get(type), deserializedLb.get(type));
            }
        }
    }

    @Test
    public void pendingRepairCompatibility()
    {
        Arrays.asList("ma", "mb", "mc", "md", "me").forEach(v -> assertFalse(BigFormat.instance.getVersion(v).hasPendingRepair()));
        Arrays.asList("na", "nb").forEach(v -> assertTrue(BigFormat.instance.getVersion(v).hasPendingRepair()));
    }

    @Test
    public void originatingHostCompatibility()
    {
        Arrays.asList("ma", "mb", "mc", "md", "na").forEach(v -> assertFalse(BigFormat.instance.getVersion(v).hasOriginatingHostId()));
        Arrays.asList("me", "nb").forEach(v -> assertTrue(BigFormat.instance.getVersion(v).hasOriginatingHostId()));
    }
}
