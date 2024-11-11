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
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.tools.SSTableExport;
import org.apache.cassandra.tools.ToolRunner;
import org.apache.cassandra.utils.Collectors3;
import org.assertj.core.api.Assertions;
import org.json.simple.JSONObject;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.util.Arrays.asList;
import static org.apache.cassandra.config.DatabaseDescriptor.getCommitLogLocation;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

public class DropUDTWithRestartTest extends TestBaseImpl
{
    private final static Logger logger = LoggerFactory.getLogger(DropUDTWithRestartTest.class);

    private final static Path TEST_DATA_UDT_PATH = Paths.get("test/data/udt");
    private final static Path DS_TRUNK_40_PRODUCT_PATH = TEST_DATA_UDT_PATH.resolve("ds-trunk-4.0");
    private final static Path DS_TRUNK_40_POST_UDT_PRODUCT_PATH = TEST_DATA_UDT_PATH.resolve("ds-trunk-4.0-post-udt");
    private final static Path VSEARCH_PRODUCT_PATH = TEST_DATA_UDT_PATH.resolve("vsearch");
    private final static Path DSE_PRODUCT_PATH = TEST_DATA_UDT_PATH.resolve("dse");
    private final static Path THIS_PRODUCT_PATH = VSEARCH_PRODUCT_PATH;
    private final static String COMMITLOG_DIR = "commitlog";
    private final static String KS = "ks";
    private final static String SCHEMA_TXT = "schema.txt";
    private final static String SCHEMA0_TXT = "schema0.txt";
    private final static String DATA_JSON = "data.json";

    private Cluster startCluster() throws IOException
    {
        Cluster cluster = Cluster.build(1).withConfig(config -> config.set("auto_snapshot", "false")
                                                                      .set("enable_uuid_sstable_identifiers", "false")
                                                                      .with(NATIVE_PROTOCOL)).start();
        cluster.setUncaughtExceptionsFilter(t -> {
            String cause = Optional.ofNullable(t.getCause()).map(c -> c.getClass().getName()).orElse("");
            return t.getClass().getName().equals(FSWriteError.class.getName()) && cause.equals(AccessDeniedException.class.getName());
        });
        return cluster;
    }

    @Test
    public void mergeDataFromSSTableAndCommitLogWithDroppedColumnTest() throws Throwable
    {
        try (Cluster cluster = startCluster())
        {
            // Create tables, populate them with the first dataset, drop complex column (which follows flushing),
            // and then populate with the second dataset. This way we will have data on disk and in the memtable.
            // Finally record query results.
            IInvokableInstance node = cluster.get(1);
            node.executeInternal("DROP KEYSPACE IF EXISTS " + KS);
            node.executeInternal("CREATE KEYSPACE " + KS + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            createTables(node);
            cluster.disableAutoCompaction(KS);

            // let's have some data in sstables
            insertData(node, 0, true);
            insertData(node, 256, true);

            // then drop the complex column
            dropComplexColumn(node);

            // insert some other data
            insertData(node, 128, false);
            insertData(node, 256 + 17, false);

            // and see what we have
            Map<String, List<List<Object>>> data0 = selectData(node);
            Map<String, List<List<Object>>> cqlData0 = selectCQLData(node);

            assertThat(cqlData0).isEqualTo(data0);

            // make sure we have the same after flushing and compacting
            node.flush(KS);
            assertThat(selectData(node)).isEqualTo(data0);
            for (String table : data0.keySet())
                node.forceCompact(KS, table);
            assertThat(selectData(node)).isEqualTo(data0);

            // Create tables, populate them with the first dataset, block data dir and drop complex column (prevent
            // flushing so that the data stays in the commit log), restart the node to replay the commit log, populate
            // the tables with the second data set, and finally record query results.
            node.executeInternal("DROP KEYSPACE " + KS);
            node.executeInternal("CREATE KEYSPACE " + KS + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            createTables(node);
            List<Path> dataDirs = getDataDirectories(node);

            cluster.disableAutoCompaction(KS);
            blockFlushing(dataDirs);
            insertData(node, 0, true);
            insertData(node, 256, true);
            try
            {
                dropComplexColumn(node);
            }
            finally
            {
                unblockFlushing(dataDirs);
            }
            node.shutdown(true).get(10, TimeUnit.SECONDS);

            node.startup();
            insertData(node, 128, false);
            insertData(node, 256 + 17, false);

            // eventually we expect that the result sets from both runs are the same
            assertThat(selectData(node)).isEqualTo(data0);

            // make sure we have the same after flushing and compacting
            node.flush(KS);
            node.shutdown(true).get(10, TimeUnit.SECONDS);
            node.startup();
            assertThat(selectData(node)).isEqualTo(data0);

            for (String table : data0.keySet())
                node.forceCompact(KS, table);
            assertThat(selectData(node)).isEqualTo(data0);
        }
    }

    private static List<Path> getDataDirectories(IInvokableInstance node)
    {
        return node.callOnInstance(() -> Keyspace.open(KS).getColumnFamilyStores().stream().map(cfs -> cfs.getDirectories().getDirectoryForNewSSTables().toPath()).collect(Collectors.toList()));
    }

    /**
     * This is actually not a test - it is used to generate data files to be used by {@code loadCommitLogAndSSTablesWithDroppedColumnTest*}.
     * Those files should be populated across different products between which we want to verify the compatibility.
     */
    @Test
    @Ignore
    public void storeCommitLogAndSSTablesWithDroppedColumn() throws Throwable
    {
        Files.createDirectories(THIS_PRODUCT_PATH);
        try (Cluster cluster = startCluster())
        {
            IInvokableInstance node = cluster.get(1);
            node.executeInternal("DROP KEYSPACE IF EXISTS " + KS);
            node.executeInternal("CREATE KEYSPACE " + KS + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            createTables(node);
            cluster.disableAutoCompaction(KS);

            List<Path> dataDirs = getDataDirectories(node);
            Path commitLogDir = node.callOnInstance(() -> getCommitLogLocation().toPath());

            Map<String, String> schema0 = getSchemaDesc(node);
            Files.writeString(THIS_PRODUCT_PATH.resolve(SCHEMA0_TXT),
                              String.join(";\n", schema0.values()).replaceAll(";;", ";"),
                              UTF_8,
                              CREATE, TRUNCATE_EXISTING);

            insertData(node, 0, true);
            insertData(node, 256, true);
            node.flush(KS);

            blockFlushing(dataDirs);
            try
            {
                dropComplexColumn(node);
                insertData(node, 128, false);
                insertData(node, 256 + 17, false);

                Map<String, String> schema1 = getSchemaDesc(node);
                Files.writeString(THIS_PRODUCT_PATH.resolve(SCHEMA_TXT),
                                  String.join(";\n", schema1.values()).replaceAll(";;", ";"),
                                  UTF_8,
                                  CREATE, TRUNCATE_EXISTING);

                node.shutdown(true).get(10, TimeUnit.SECONDS);

                Path clTargetPath = THIS_PRODUCT_PATH.resolve(COMMITLOG_DIR);
                Files.createDirectories(clTargetPath);
                PathUtils.deleteContent(clTargetPath);
                FileUtils.copyDirectory(commitLogDir.toFile(), clTargetPath.toFile());

                Path ksTargetPath = THIS_PRODUCT_PATH.resolve(KS);
                Files.createDirectories(ksTargetPath);
                PathUtils.deleteContent(ksTargetPath);
                for (Path dir : dataDirs)
                {
                    String name = dir.getFileName().toString();
                    Path targetDir = ksTargetPath.resolve(name);
                    Files.createDirectories(targetDir);
                    FileUtils.copyDirectory(dir.toFile(), targetDir.toFile(), pathname -> !pathname.toString().endsWith(".log"));
                }
            }
            finally
            {
                unblockFlushing(dataDirs);
            }

            node.startup();
            node.flush(KS);
            Map<String, List<List<Object>>> data = selectData(node);
            Files.writeString(THIS_PRODUCT_PATH.resolve(DATA_JSON), JSONObject.toJSONString(data), UTF_8, CREATE, TRUNCATE_EXISTING);
        }
    }

    @Test
    public void loadCommitLogAndSSTablesWithDroppedColumnTestVSearch() throws Exception
    {
        loadCommitLogAndSSTablesWithDroppedColumnTest(VSEARCH_PRODUCT_PATH);
    }

    @Test
    public void loadCommitLogAndSSTablesWithDroppedColumnTestDSTrunk40() throws Exception
    {
        loadCommitLogAndSSTablesWithDroppedColumnTest(DS_TRUNK_40_PRODUCT_PATH);
    }

    @Test
    public void loadCommitLogAndSSTablesWithDroppedColumnTestDSE() throws Exception
    {
        loadCommitLogAndSSTablesWithDroppedColumnTest(DSE_PRODUCT_PATH);
    }

    @Test
    public void loadCommitLogAndSSTablesWithDroppedColumnTestDSTrunkPreUDT() throws Exception
    {
        loadCommitLogAndSSTablesWithDroppedColumnTest(DS_TRUNK_40_POST_UDT_PRODUCT_PATH);
    }

    private void loadCommitLogAndSSTablesWithDroppedColumnTest(Path productPath) throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        try (Cluster cluster = startCluster())
        {
            IInvokableInstance node = cluster.get(1);
            node.executeInternal("DROP KEYSPACE IF EXISTS " + KS);
            node.executeInternal("CREATE KEYSPACE " + KS + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");

            for (String stmt : Files.readString(productPath.resolve(SCHEMA_TXT), UTF_8).split(";"))
            {
                if (!stmt.isBlank())
                {
                    logger.info("Executing: {}", stmt);
                    node.executeInternal(stmt);
                }
            }

            cluster.disableAutoCompaction(KS);

            List<Path> dataDirs = getDataDirectories(node);
            Path commitLogDir = node.callOnInstance(() -> getCommitLogLocation().toPath());

            node.shutdown(true).get(10, TimeUnit.SECONDS);

            Path commitLogSourcePath = productPath.resolve(COMMITLOG_DIR);
            FileUtils.copyDirectory(commitLogSourcePath.toFile(), commitLogDir.toFile());

            Path ksSourcePath = productPath.resolve(KS);
            for (Path dir : dataDirs)
            {
                String name = dir.getFileName().toString();
                Path sourceDir = ksSourcePath.resolve(name);
                FileUtils.copyDirectory(sourceDir.toFile(), dir.toFile());
            }

            logger.info("Restarting node");
            node.startup();
            Map<String, List<List<Object>>> data1 = selectData(node);

            String jsonData0 = Files.readString(productPath.resolve(DATA_JSON), UTF_8);
            String jsonData1 = JSONObject.toJSONString(data1);
            assertThat(jsonData0).isEqualTo(jsonData1);

            node.flush(KS);
            node.shutdown(true).get(10, TimeUnit.SECONDS);
            node.startup();

            assertThat(selectData(node)).isEqualTo(data1);

            for (String table : data1.keySet())
                node.forceCompact(KS, table);

            assertThat(selectData(node)).isEqualTo(data1);
        }
    }

    private Map<String, String> getSchemaDesc(IInvokableInstance node)
    {
        return Arrays.stream(node.executeInternal("DESCRIBE " + KS + " WITH INTERNALS"))
                     .filter(r -> r[1].equals("table") || r[1].equals("type"))
                     .collect(Collectors3.toImmutableMap(r -> r[2].toString(),
                                                         r -> Arrays.stream(r[3].toString().split("\\n"))
                                                                    .filter(s -> !s.strip().startsWith("AND") || s.contains("DROPPED COLUMN RECORD"))
                                                                    .collect(Collectors.joining("\n"))));
    }

    private static String udtValue(int i, List<Integer> bits, BiFunction<Integer, Integer, String> vals)
    {
        List<String> cols = asList("foo", "bar", "baz");
        ArrayList<String> udtVals = new ArrayList<>();
        for (int j = 0; j < bits.size(); j++)
        {
            if ((i & bits.get(j)) != 0)
                udtVals.add(cols.get(j) + ": " + vals.apply(i, j));
        }
        return '{' + String.join(", ", udtVals) + '}';
    }

    private static String tupleValue(int i, List<Integer> bits, BiFunction<Integer, Integer, String> vals)
    {
        ArrayList<String> tupleVals = new ArrayList<>();
        for (int j = 0; j < bits.size(); j++)
        {
            if ((i & bits.get(j)) != 0)
                tupleVals.add(vals.apply(i, j));
            else
                tupleVals.add("null");
        }
        return '(' + String.join(", ", tupleVals) + ')';
    }

    private static String genInsert(int pk, int i, List<Integer> bits, BiFunction<Integer, Integer, String> vals)
    {
        List<String> cols = asList("a_int", "b_complex", "c_int");
        ArrayList<String> c = new ArrayList<>();
        ArrayList<String> v = new ArrayList<>();
        for (int j = 0; j < bits.size(); j++)
        {
            if ((i & bits.get(j)) != 0)
            {
                c.add(cols.get(j));
                v.add(vals.apply(i, j));
            }
        }
        if (c.isEmpty())
            return String.format("(pk) VALUES (%d)", pk);
        else
            return String.format("(pk, %s) VALUES (%d, %s)", String.join(", ", c), pk, String.join(", ", v));
    }

    private static BiFunction<Integer, Integer, String> valsFunction(IntFunction<String> nonIntFunction)
    {
        return (i, j) -> {
            if (j == 0)
                return Integer.toString(i);
            if (j == 1)
                return nonIntFunction.apply(i);
            if (j == 2)
                return Integer.toString(i * 2);
            assert false;
            return null;
        };
    }

    private static BiFunction<Integer, Integer, String> valsFunction()
    {
        return (i, j) -> {
            if (j == 0)
                return Integer.toString(i);
            if (j == 1)
                return String.format("'bar%d'", i);
            if (j == 2)
                return Integer.toString(i * 2);
            assert false;
            return null;
        };
    }

    private void insertData(IInstance node, int offset, boolean withComplex)
    {
        for (int pk = offset; pk < offset + (1 << 5); pk++)
        {
            int i = withComplex ? (pk - offset) : (pk - offset) & ~(2 + 4 + 8);
            node.executeInternal("INSERT INTO " + KS + ".tab1_udt1 " + genInsert(pk, i, asList(1, 2 + 4 + 8, 16), valsFunction(j -> udtValue(j, asList(2, 4, 8), valsFunction()))));
            node.executeInternal("INSERT INTO " + KS + ".tab2_frozen_udt1 " + genInsert(pk, i, asList(1, 2 + 4 + 8, 16), valsFunction(j -> udtValue(j, asList(2, 4, 8), valsFunction()))));
            node.executeInternal("INSERT INTO " + KS + ".tab5_tuple " + genInsert(pk, i, asList(1, 2 + 4 + 8, 16), valsFunction(j -> tupleValue(j, asList(2, 4, 8), valsFunction()))));
            node.executeInternal("INSERT INTO " + KS + ".tab6_frozen_tuple " + genInsert(pk, i, asList(1, 2 + 4 + 8, 16), valsFunction(j -> tupleValue(j, asList(2, 4, 8), valsFunction()))));
        }

        for (int pk = offset; pk < offset + (1 << 7); pk++)
        {
            int i = withComplex ? (pk - offset) : (pk - offset) & ~(2 + 4 + 8 + 16 + 32);
            node.executeInternal("INSERT INTO " + KS + ".tab4_frozen_udt2 " + genInsert(pk, i, asList(1, 2 + 4 + 8 + 16 + 32, 64),
                                                                                        valsFunction(j -> udtValue(j, asList(2, 4 + 8 + 16, 32), valsFunction(k -> udtValue(k, asList(4, 8, 16), valsFunction()))))));
            node.executeInternal("INSERT INTO " + KS + ".tab7_tuple_with_udt " + genInsert(pk, i, asList(1, 2 + 4 + 8 + 16 + 32, 64),
                                                                                           valsFunction(j -> tupleValue(j, asList(2, 4 + 8 + 16, 32), valsFunction(k -> udtValue(k, asList(4, 8, 16), valsFunction()))))));
            node.executeInternal("INSERT INTO " + KS + ".tab8_frozen_tuple_with_udt " + genInsert(pk, i, asList(1, 2 + 4 + 8 + 16 + 32, 64),
                                                                                                  valsFunction(j -> tupleValue(j, asList(2, 4 + 8 + 16, 32), valsFunction(k -> udtValue(k, asList(4, 8, 16), valsFunction()))))));
            node.executeInternal("INSERT INTO " + KS + ".tab9_udt_with_tuple " + genInsert(pk, i, asList(1, 2 + 4 + 8 + 16 + 32, 64),
                                                                                           valsFunction(j -> udtValue(j, asList(2, 4 + 8 + 16, 32), valsFunction(k -> tupleValue(k, asList(4, 8, 16), valsFunction()))))));
            node.executeInternal("INSERT INTO " + KS + ".tab10_frozen_udt_with_tuple " + genInsert(pk, i, asList(1, 2 + 4 + 8 + 16 + 32, 64),
                                                                                                   valsFunction(j -> udtValue(j, asList(2, 4 + 8 + 16, 32), valsFunction(k -> tupleValue(k, asList(4, 8, 16), valsFunction()))))));
        }
    }

    private static void dropComplexColumn(IInvokableInstance node)
    {
        List<String> tables = node.callOnInstance(() -> Schema.instance.getKeyspaceMetadata(KS).tables.stream().map(t -> t.name).collect(Collectors.toList()));
        for (String table : tables)
            node.executeInternal("ALTER TABLE " + KS + "." + table + " DROP b_complex");
    }

    private Map<String, List<List<Object>>> selectData(IInvokableInstance node)
    {
        Map<String, List<List<Object>>> results = new HashMap<>();
        List<String> tables = node.callOnInstance(() -> Schema.instance.getKeyspaceMetadata(KS).tables.stream().map(t -> t.name).collect(Collectors.toList()));
        for (String table : tables)
        {
            Object[][] rows = node.executeInternal("SELECT * FROM " + KS + "." + table);
            Arrays.sort(rows, Comparator.comparing(a -> ((Integer) a[0])));
            results.put(table, Arrays.stream(rows).map(Arrays::asList).collect(Collectors.toList()));
        }
        return results;
    }

    private Map<String, List<List<Object>>> selectCQLData(IInvokableInstance node)
    {
        Map<String, List<List<Object>>> results = new HashMap<>();
        List<String> tables = node.callOnInstance(() -> Schema.instance.getKeyspaceMetadata(KS).tables.stream().map(t -> t.name).collect(Collectors.toList()));
        try (com.datastax.driver.core.Cluster cluster = com.datastax.driver.core.Cluster.builder().addContactPoint(node.broadcastAddress().getHostString()).build();
             Session session = cluster.connect())
        {
            for (String table : tables)
            {
                ResultSet rs = session.execute("SELECT * FROM " + KS + "." + table);
                assertThat(rs.getColumnDefinitions().contains("b_complex")).isFalse();
                List<List<Object>> rows = rs.all().stream().map(r -> Arrays.<Object>asList(r.get("pk", Integer.class), r.get("a_int", Integer.class), r.get("c_int", Integer.class)))
                                            .sorted(Comparator.comparing(a -> ((Integer) a.get(0))))
                                            .collect(Collectors.toList());
                results.put(table, rows);
            }
        }
        return results;
    }

    private static void createTables(IInvokableInstance node)
    {
        node.executeInternal("CREATE TYPE " + KS + ".udt1(foo int, bar text, baz int)");
        node.executeInternal("CREATE TYPE " + KS + ".udt2(foo int, bar udt1, baz int)");
        node.executeInternal("CREATE TYPE " + KS + ".udt3(foo int, bar tuple<int, text, int>, baz int)");

        node.executeInternal("CREATE TABLE " + KS + ".tab1_udt1                   (pk int PRIMARY KEY, a_int int, b_complex udt1, c_int int) WITH ID = 513f2627-9356-41c4-a379-7ad42be97432");
        node.executeInternal("CREATE TABLE " + KS + ".tab2_frozen_udt1            (pk int PRIMARY KEY, a_int int, b_complex frozen<udt1>, c_int int) WITH ID = 450f91fe-7c47-41c9-97bf-fdad854fa7e5");
        Assertions.assertThatExceptionOfType(RuntimeException.class).isThrownBy(
        () -> node.executeInternal("CREATE TABLE " + KS + ".tab3_udt2             (pk int PRIMARY KEY, a_int int, b_complex udt2, c_int int) WITH ID = b613aee8-645c-4384-90d2-fc9e82fb1a59"));
        node.executeInternal("CREATE TABLE " + KS + ".tab4_frozen_udt2            (pk int PRIMARY KEY, a_int int, b_complex frozen<udt2>, c_int int) WITH ID = 9c03c71c-6775-4357-9173-0f8808901afa");
        node.executeInternal("CREATE TABLE " + KS + ".tab5_tuple                  (pk int PRIMARY KEY, a_int int, b_complex tuple<int, text, int>, c_int int) WITH ID = 90826dd3-8437-4585-9de4-15908236687f");
        node.executeInternal("CREATE TABLE " + KS + ".tab6_frozen_tuple           (pk int PRIMARY KEY, a_int int, b_complex frozen<tuple<int, text, int>>, c_int int) WITH ID = 54185f9a-a6fd-487c-abc3-c01bd5835e48");
        node.executeInternal("CREATE TABLE " + KS + ".tab7_tuple_with_udt         (pk int PRIMARY KEY, a_int int, b_complex tuple<int, udt1, int>, c_int int) WITH ID = 4e78f403-7b63-4e0d-a231-42e42cba7cb5");
        node.executeInternal("CREATE TABLE " + KS + ".tab8_frozen_tuple_with_udt  (pk int PRIMARY KEY, a_int int, b_complex frozen<tuple<int, udt1, int>>, c_int int) WITH ID = 8660f235-0816-4019-9cc9-1798fa7beb17");
        node.executeInternal("CREATE TABLE " + KS + ".tab9_udt_with_tuple         (pk int PRIMARY KEY, a_int int, b_complex udt3, c_int int) WITH ID = f670fd5a-8145-4669-aceb-75667c000ea6");
        node.executeInternal("CREATE TABLE " + KS + ".tab10_frozen_udt_with_tuple (pk int PRIMARY KEY, a_int int, b_complex frozen<udt3>, c_int int) WITH ID = 6a5cff4e-2f94-4c8b-9aa2-0fbd65292caa");
    }

    private void blockFlushing(List<Path> dirs) throws IOException
    {
        for (Path dir : dirs)
        {
            Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(dir);
            permissions.remove(PosixFilePermission.OWNER_WRITE);
            permissions.remove(PosixFilePermission.GROUP_WRITE);
            permissions.remove(PosixFilePermission.OTHERS_WRITE);
            Files.setPosixFilePermissions(dir, permissions);
        }
    }

    private void unblockFlushing(List<Path> dirs) throws IOException
    {
        for (Path dir : dirs)
        {
            Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(dir);
            permissions.add(PosixFilePermission.OWNER_WRITE);
            Files.setPosixFilePermissions(dir, permissions);
        }
    }

    @Test
    public void testReadingValuesOfDroppedColumns() throws Throwable
    {
        // given there is a table with a UDT column and some additional non-UDT columns, and there are rows with
        // different combinations of values and nulls for all columns
        try (Cluster cluster = Cluster.build(1).withConfig(c -> c.with(GOSSIP, NATIVE_PROTOCOL)).start())
        {
            IInvokableInstance node = cluster.get(1);
            node.executeInternal("CREATE KEYSPACE " + KS + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            node.executeInternal("CREATE TYPE " + KS + ".udt (foo text, bar text)");
            node.executeInternal("CREATE TABLE " + KS + ".tab (pk int PRIMARY KEY, a_udt udt, b text, c text)");
            node.executeInternal("INSERT INTO " + KS + ".tab (pk, c) VALUES (1, 'c_value')");
            node.executeInternal("INSERT INTO " + KS + ".tab (pk, b) VALUES (2, 'b_value')");
            node.executeInternal("INSERT INTO " + KS + ".tab (pk, a_udt) VALUES (3, {foo: 'a_foo', bar: 'a_bar'})");

            File dataDir = new File(node.callOnInstance(() -> Keyspace.open(KS)
                                                                      .getColumnFamilyStore("tab")
                                                                      .getDirectories()
                                                                      .getDirectoryForNewSSTables()
                                                                      .absolutePath()));
            checkData(cluster);

            // when the UDT columns is dropped while the data cannot be flushed before drop and must remain in the commitlog

            // prevent flushing the data
            Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(dataDir.toPath());
            permissions.remove(PosixFilePermission.OWNER_WRITE);
            permissions.remove(PosixFilePermission.GROUP_WRITE);
            permissions.remove(PosixFilePermission.OTHERS_WRITE);
            Files.setPosixFilePermissions(dataDir.toPath(), permissions);

            node.executeInternal("ALTER TABLE " + KS + ".tab DROP a_udt");

            // and the node is restarted
            // restart is needed because this way we can simulate the situation where the commit log contains the data
            // of the dropped cell, while the schema is already altered (the column moved to dropped columns and transformed)
            node.shutdown(false).get(10, TimeUnit.SECONDS);

            // unlock the ability to flush data
            permissions = Files.getPosixFilePermissions(dataDir.toPath());
            permissions.add(PosixFilePermission.OWNER_WRITE);
            Files.setPosixFilePermissions(dataDir.toPath(), permissions);
            node.startup();

            // then, we should still be able to read the data of the remaining columns correctly
            checkData(cluster);

            // and even after flushing and restarting the node again
            // the next restart is needed to make sure that the sstable header is read from disk
            node.flush(KS);
            node.shutdown(false).get(10, TimeUnit.SECONDS);
            node.startup();

            checkData(cluster);

            // verify that the sstable can be read with sstabledump
            String sstable = node.callOnInstance(() -> Keyspace.open(KS).getColumnFamilyStore("tab")
                                                               .getDirectories().getCFDirectories()
                                                               .get(0).tryList()[0].toString());
            ToolRunner.ToolResult tool = ToolRunner.invokeClass(SSTableExport.class, sstable);
            tool.assertCleanStdErr();
            tool.assertOnExitCode();
            assertThat(tool.getStdout())
            .contains("\"key\" : [ \"1\" ],")
            .contains("\"key\" : [ \"2\" ],")
            .contains("{ \"name\" : \"c\", \"value\" : \"c_value\" }")
            .contains("{ \"name\" : \"b\", \"value\" : \"b_value\" }");
        }
    }

    private void checkData(Cluster cluster)
    {
        ICoordinator coordinator = cluster.coordinator(1);
        String query = "SELECT b, c FROM " + KS + ".tab WHERE pk = ?";
        assertRows(coordinator.execute(query, ConsistencyLevel.QUORUM, 1), row(null, "c_value"));
        assertRows(coordinator.execute(query, ConsistencyLevel.QUORUM, 2), row("b_value", null));
        assertRows(coordinator.execute(query, ConsistencyLevel.QUORUM, 3), row(null, null));
    }
}
