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

package org.apache.cassandra.index.sai.functional;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.locator.InetAddressAndPort;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Parameterized.class)
public class SaiDiskSizeTest extends SAITester
{
    private static final Logger logger = LoggerFactory.getLogger(SaiDiskSizeTest.class);

    @Parameterized.Parameter
    public Version version;

    @Parameterized.Parameter(1)
    public int size;

    @Parameterized.Parameter(2)
    public String pkSuffix;

    @Parameterized.Parameter(3)
    public int rowsPerPartition;

    @Parameterized.Parameters(name = "{0}, size={1}, pk={2}, partitionSize={3}")
    public static Collection<Object[]> data()
    {
        return Version.ALL.stream()
                          .flatMap(v -> {
                              switch (v.toString())
                              {
                                  case "aa":
                                      return Stream.of(
                                      new Object[]{ v, 13014, "pk", 1 },
                                      new Object[]{ v, 13016, "pk, v_int", 2 },
                                      new Object[]{ v, 14266, "pk, v_int", 100 });
                                  case "ba":
                                  case "ca":
                                  case "db":
                                  case "dc":
                                      return Stream.of(
                                      new Object[]{ v, 65734, "pk", 1 },
                                      new Object[]{ v, 57627, "pk, v_int", 2 },
                                      new Object[]{ v, 28004, "pk, v_int", 100 });
                                  case "eb":
                                  case "ec":
                                      return Stream.of(
                                      new Object[]{ v, 67370, "pk", 1 },
                                      new Object[]{ v, 59263, "pk, v_int", 2 },
                                      new Object[]{ v, 29640, "pk, v_int", 100 });
                                  case "ed":
                                      return Stream.of(
                                      new Object[]{ v, 67378, "pk", 1 },
                                      new Object[]{ v, 59271, "pk, v_int", 2 },
                                      new Object[]{ v, 29648, "pk, v_int", 100 });
                                  case "fa":
                                      return Stream.of(
                                      new Object[]{ v, 17514, "pk", 1 },
                                      new Object[]{ v, 19707, "pk, v_int", 2 },
                                      new Object[]{ v, 16750, "pk, v_int", 100 });
                                  default:
                                      return // A new version assumes the latest size by default
                                      Stream.of(
                                      new Object[]{ v, 17514, "pk", 1 },
                                      new Object[]{ v, 19707, "pk, v_int", 2 },
                                      new Object[]{ v, 16750, "pk, v_int", 100 });
                              }
                          })
                          .collect(Collectors.toList());
    }

    @Before
    public void setVersion()
    {
        SAIUtil.setCurrentVersion(version);
    }

    @Test
    public void testIndexDiskSizeAcrossVersions() throws UnknownHostException
    {
        createTable("CREATE TABLE %s (" +
                    "pk int, " +
                    "v_ascii ascii, " +
                    "v_bigint bigint, " +
                    "v_blob blob, " +
                    "v_boolean boolean, " +
                    "v_decimal decimal, " +
                    "v_double double, " +
                    "v_float float, " +
                    "v_int int, " +
                    "v_text text, " +
                    "v_timestamp timestamp, " +
                    "v_uuid uuid, " +
                    "v_varchar varchar, " +
                    "v_varint varint, " +
                    "v_timeuuid timeuuid, " +
                    "v_inet inet, " +
                    "v_date date, " +
                    "v_time time, " +
                    "v_smallint smallint, " +
                    "v_tinyint tinyint, " +
                    "v_duration duration, " +
                    "PRIMARY KEY (" + pkSuffix + "))");

        verifyNoIndexFiles();
        createIndex("CREATE CUSTOM INDEX ON %s(v_int) USING 'StorageAttachedIndex'");

        waitForTableIndexesQueryable();

        // Split data into 2 sstable segments
        insertRows(0, 1000);
        flush();
        insertRows(1000, 1000);
        flush();

        long diskSize = indexDiskSpaceUse();
        logger.info("SAI Version: {}, Index Disk Size: {} bytes", version, diskSize);
        assertThat(diskSize)
        .as("Disk size for SAI version %s", version)
        .isLessThanOrEqualTo(size)
        .isGreaterThan((long) (size * 0.8));

        compact();

        diskSize = indexDiskSpaceUse();
        logger.info("SAI Version: {}, Index Disk Size: {} bytes", version, diskSize);
        assertThat(diskSize)
        .as("Disk size for SAI version %s", version)
        .isLessThanOrEqualTo(size)
        .isGreaterThan((long) (size * 0.8));
    }

    private void insertRows(int size, int start) throws UnknownHostException
    {
        for (int i = start; i < start + size; i++)
        {
            execute("INSERT INTO %s (pk, v_ascii, v_bigint, v_blob, v_boolean, v_decimal, " +
                    "v_double, v_float, v_int, v_text, v_timestamp, v_uuid, v_varchar, " +
                    "v_varint, v_timeuuid, v_inet, v_date, v_time, v_smallint, v_tinyint, v_duration) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    i % (size / rowsPerPartition), // Have 2 rows per partition
                    "ascii_" + i,
                    (long) i * 1000000,
                    ByteBuffer.wrap(("blob_" + i).getBytes()),
                    i % 2 == 0,
                    new BigDecimal(i + ".123"),
                    i * 1.5,
                    (float) (i * 2.5),
                    i * 2,
                    "text_value_" + i,
                    new Date(System.currentTimeMillis() + i * 1000L),
                    UUID.randomUUID(),
                    "varchar_" + i,
                    BigInteger.valueOf(i).multiply(BigInteger.valueOf(100)),
                    UUID.fromString("00000000-0000-1000-8000-" +
                                    String.format("%012d", i)),
                    InetAddressAndPort.getByName("127.0.0." + (i % 256)).address,
                    i + 1,
                    (long) i * 1000000000L,
                    (short) (i % 32767),
                    (byte) (i % 128),
                    org.apache.cassandra.cql3.Duration.newInstance(i % 12, i % 30, i * 1000000000L)
            );
        }
    }
}
