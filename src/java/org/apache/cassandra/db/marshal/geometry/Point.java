/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.db.marshal.geometry;

import java.nio.ByteBuffer;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCPoint;
import org.apache.cassandra.serializers.MarshalException;

public class Point extends OgcGeometry
{
    public static final Serializer<Point> serializer = new Serializer<Point>()
    {
        @Override
        public String toWellKnownText(Point geometry)
        {
            return geometry.point.asText();
        }

        @Override
        public ByteBuffer toWellKnownBinaryNativeOrder(Point geometry)
        {
            return geometry.point.asBinary();
        }

        @Override
        public String toGeoJson(Point geometry)
        {
            return geometry.point.asGeoJson();
        }

        @Override
        public Point fromWellKnownText(String source)
        {
            return new Point(fromOgcWellKnownText(source, OGCPoint.class));
        }

        @Override
        public Point fromWellKnownBinary(ByteBuffer source)
        {
            return new Point(fromOgcWellKnownBinary(source, OGCPoint.class));
        }

        @Override
        public Point fromGeoJson(String source)
        {
            return new Point(fromOgcGeoJson(source, OGCPoint.class));
        }
    };

    final OGCPoint point;

    public Point(double x, double y)
    {
        this(new OGCPoint(new com.esri.core.geometry.Point(x, y), OgcGeometry.SPATIAL_REFERENCE_4326));
    }

    private Point(OGCPoint point)
    {
        this.point = point;
        validate();
    }

    @Override
    public boolean contains(OgcGeometry geometry)
    {
        return false;
    }

    @Override
    public GeometricType getType()
    {
        return GeometricType.POINT;
    }

    @Override
    public void validate() throws MarshalException
    {
        validateOgcGeometry(point);
        if (point.isEmpty() || point.is3D())
            throw new MarshalException(getClass().getSimpleName() + " requires exactly 2 coordinate values");
    }

    @Override
    protected OGCGeometry getOgcGeometry()
    {
        return point;
    }

    @Override
    public Serializer getSerializer()
    {
        return serializer;
    }

    public OGCPoint getOgcPoint()
    {
        return point;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Point point1 = (Point) o;

        return !(point != null ? !point.equals(point1.point) : point1.point != null);

    }

    @Override
    public int hashCode()
    {
        return point != null ? point.hashCode() : 0;
    }

    @Override
    public String toString()
    {
        return asWellKnownText();
    }
}
