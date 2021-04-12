/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import org.apache.cassandra.db.marshal.geometry.LineString;
import org.apache.cassandra.db.marshal.geometry.OgcGeometry;
import org.apache.cassandra.db.marshal.geometry.Point;
import org.apache.cassandra.db.marshal.geometry.Polygon;

public class GeometryCodec<T extends OgcGeometry> extends TypeCodec<T>
{
    public static TypeCodec<Point> pointCodec = new GeometryCodec<>(PointType.instance);
    public static TypeCodec<LineString> lineStringCodec = new GeometryCodec<>(LineStringType.instance);
    public static TypeCodec<Polygon> polygonCodec = new GeometryCodec<>(PolygonType.instance);

    private final OgcGeometry.Serializer<T> serializer;

    public GeometryCodec(AbstractGeometricType type)
    {
        super(DataType.custom(type.getClass().getName()), (Class<T>) type.getGeoType().getGeoClass());
        this.serializer = (OgcGeometry.Serializer<T>) type.getGeoType().getSerializer();
    }

    @Override
    public T deserialize(ByteBuffer bb, ProtocolVersion protocolVersion) throws InvalidTypeException
    {
        return bb == null || bb.remaining() == 0 ? null : serializer.fromWellKnownBinary(bb);
    }

    @Override
    public ByteBuffer serialize(T geometry, ProtocolVersion protocolVersion) throws InvalidTypeException
    {
        return geometry == null ? null : geometry.asWellKnownBinary();
    }

    @Override
    public T parse(String s) throws InvalidTypeException
    {
        if (s == null || s.isEmpty() || s.equalsIgnoreCase("NULL"))
            return null;
        return serializer.fromWellKnownText(s);
    }

    @Override
    public String format(T geometry) throws InvalidTypeException
    {
        return geometry == null ? "NULL" : geometry.asWellKnownText();
    }
}
