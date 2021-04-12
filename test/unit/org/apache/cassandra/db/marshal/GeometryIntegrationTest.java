/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.db.marshal;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.dse.framework.CassandraYamlBuilder;
import com.datastax.dse.framework.DseTestRunner;
import org.apache.cassandra.db.marshal.geometry.LineString;
import org.apache.cassandra.db.marshal.geometry.OgcGeometry;
import org.apache.cassandra.db.marshal.geometry.Point;
import org.apache.cassandra.db.marshal.geometry.Polygon;
import org.apache.cassandra.transport.ProtocolVersion;

import static org.apache.cassandra.db.marshal.GeometricTypeTests.lineString;
import static org.apache.cassandra.db.marshal.GeometricTypeTests.p;
import static org.apache.cassandra.db.marshal.GeometricTypeTests.polygon;

@RunWith(Parameterized.class)
public class GeometryIntegrationTest extends DseTestRunner
{
    private static boolean initialized = false;

    @Parameterized.Parameters(name = "memtable_allocation_type={0}")
    public static Collection<Object> generateData()
    {
        return Arrays.asList("heap_buffers", "offheap_buffers", "offheap_objects");
    }

    private String memtableAllocationType;

    public GeometryIntegrationTest(String memtableAllocationType)
    {
        this.memtableAllocationType = memtableAllocationType;
    }

    @Before
    public void setUp() throws Exception
    {
        super.setUp();
        if (!initialized)
        {
            CassandraYamlBuilder cassandraYamlBuilder = CassandraYamlBuilder.newInstance().withMemtableAllocationType(memtableAllocationType);

            DseTestRunner.startNode(1, cassandraYamlBuilder, false);


            CodecRegistry.DEFAULT_INSTANCE.register(GeometryCodec.pointCodec,
                                                    GeometryCodec.lineStringCodec,
                                                    GeometryCodec.polygonCodec);
            initialized = true;
        }
        sendCql3Native(1, null, "CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 1}");
    }

    @After
    public void teardown() throws Exception
    {
        sendCql3Native(1, null, "DROP KEYSPACE ks;");
    }

    private <T extends OgcGeometry> void testType(T expected, Class<T> klass, AbstractGeometricType<T> type, String tableName, String wkt, String columnType) throws Exception
    {
        sendCql3Native(1, null, String.format("CREATE TABLE ks.%s (k INT PRIMARY KEY, g '%s')", tableName, columnType));
        sendCql3Native(1, null, String.format("INSERT INTO ks.%s (k, g) VALUES (1, '%s')", tableName, wkt));

        ResultSet results = sendCql3Native(1, null, String.format("SELECT * FROM ks.%s", tableName));
        List<Row> rows = results.all();
        Assert.assertEquals(1, rows.size());
        Row row = rows.get(0);
        T actual = row.get("g", klass);
        Assert.assertEquals(expected, actual);
        results = sendCql3Native(1, null, String.format("SELECT toJson(g) FROM ks.%s", tableName));
        rows = results.all();
        Assert.assertEquals(1, rows.size());
        row = rows.get(0);
        String actualJson = row.getString("system.tojson(g)");
        String expectedJson = type.toJSONString(type.getSerializer().serialize(expected), ProtocolVersion.CURRENT);
        Assert.assertEquals(expectedJson, actualJson);
    }

    @Test
    public void pointTest() throws Exception
    {
        sendCql3Native(1, null, "CREATE TABLE ks.point (k INT PRIMARY KEY, g 'PointType')");
        String wkt = "POINT(1.1 2.2)";
        sendCql3Native(1, null, String.format("INSERT INTO ks.point (k, g) VALUES (1, '%s')", wkt));

        ResultSet results = sendCql3Native(1, null, "SELECT * FROM ks.point");
        List<Row> rows = results.all();
        Assert.assertEquals(1, rows.size());
        Row row = rows.get(0);
        Point point = row.get("g", Point.class);
        Assert.assertEquals(new Point(1.1, 2.2), point);
    }

    @Test
    public void lineStringTest() throws Exception
    {
        LineString expected = lineString(p(30, 10), p(10, 30), p(40, 40));
        String wkt = "linestring(30 10, 10 30, 40 40)";
        testType(expected, LineString.class, LineStringType.instance, "linestring", wkt, "LineStringType");
    }

    @Test
    public void polygonTest() throws Exception
    {
        Polygon expected = polygon(p(30, 10), p(10, 20), p(20, 40), p(40, 40));
        String wkt = "polygon((30 10, 40 40, 20 40, 10 20, 30 10))";
        testType(expected, Polygon.class, PolygonType.instance, "polygon", wkt, "PolygonType");
    }

    @Test
    public void primaryKeyTest() throws Exception
    {
        sendCql3Native(1, null, "CREATE TABLE ks.geo (k 'PointType', c 'LineStringType', g 'PointType', PRIMARY KEY (k, c))");
        sendCql3Native(1, null, "INSERT INTO ks.geo (k, c, g) VALUES ('POINT(1 2)', 'linestring(30 10, 10 30, 40 40)', 'POINT(1.1 2.2)')");
        ResultSet results = sendCql3Native(1, null, "SELECT * FROM ks.geo");
        List<Row> rows = results.all();
        Assert.assertEquals(1, rows.size());
        Row row = rows.get(0);

        Point point1 = row.get("k", Point.class);
        Assert.assertEquals(new Point(1, 2), point1);

        LineString lineString = row.get("c", LineString.class);
        Assert.assertEquals(lineString(p(30, 10), p(10, 30), p(40, 40)), lineString);

        Point point = row.get("g", Point.class);
        Assert.assertEquals(new Point(1.1, 2.2), point);
    }
}