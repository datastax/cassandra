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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.ChecksumType;

import static java.lang.String.format;
import static org.apache.cassandra.config.CassandraRelevantProperties.SSTABLE_CHECKSUM_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

public class SSTableDigestTest extends TestBaseImpl
{
    public void testDigestFile(ChecksumType checksumType, Component digestComponent) throws IOException
    {
        try (WithProperties properties = new WithProperties())
        {
            if (checksumType != null)
                properties.set(SSTABLE_CHECKSUM_TYPE, checksumType.toString());

            try (Cluster cluster = init(Cluster.build(1)
                                               .withDataDirCount(1)
                                               .start()))
            {
                cluster.disableAutoCompaction(KEYSPACE);
                cluster.schemaChange(createTableStmt(KEYSPACE));
                cluster.get(1).executeInternal(format("INSERT INTO %s.%s (pk, ck, v) VALUES (?, ?, ?)", KEYSPACE, "tbl"), 1, 1, 1);
                cluster.get(1).flush(KEYSPACE);

                String digestFileName = digestComponent.type.repr;

                cluster.get(1).runOnInstance(() -> {
                    try
                    {
                        SSTableReader ssTable = new ArrayList<>(Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").getLiveSSTables()).get(0);

                        Path digestPath = ssTable.descriptor.pathFor(Component.parse(digestFileName, null));

                        assertThat(digestPath).exists();

                        long digest = Long.parseLong(Files.readString(digestPath));

                        Path data = ssTable.descriptor.pathFor(Components.DATA);
                        byte[] bytes = Files.readAllBytes(data);

                        ChecksumType effectiveChecksumType = checksumType == null ? ChecksumType.CRC32 : checksumType;
                        long checksum = effectiveChecksumType.of(bytes, 0, bytes.length);

                        assertThat(checksum).isEqualTo(digest);
                    }
                    catch (IOException e)
                    {
                        throw new RuntimeException(e);
                    }
                });
            }
        }
    }

    @Test
    public void testDigestFileDefault() throws IOException
    {
        testDigestFile(null, Components.DIGEST);
    }

    @Test
    public void testDigestFileCRC32() throws IOException
    {
        testDigestFile(ChecksumType.CRC32, Components.DIGEST);
    }

    @Test
    public void testDigestFileCRC32C() throws IOException
    {
        testDigestFile(ChecksumType.CRC32C, Components.DIGEST_CRC32C);
    }

    @Test
    public void testDigestFileCRC64NVME() throws IOException
    {
        testDigestFile(ChecksumType.CRC64NVME, Components.DIGEST_CRC64NVME);
    }

    private static String createTableStmt(String ks)
    {
        return format("CREATE TABLE %s.%s (pk int, ck int, v int, PRIMARY KEY (pk, ck)) " +
                      "WITH compaction = {'class':'SizeTieredCompactionStrategy', 'enabled':'false'}",
                      ks, "tbl");
    }
}
