/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.db.marshal.geometry;

import java.nio.ByteBuffer;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCPolygon;
import org.apache.cassandra.serializers.MarshalException;

public class Polygon extends OgcGeometry
{
    public static final Serializer<Polygon> serializer = new Serializer<Polygon>()
    {
        @Override
        public String toWellKnownText(Polygon geometry)
        {
            return geometry.polygon.asText();
        }

        @Override
        public ByteBuffer toWellKnownBinaryNativeOrder(Polygon geometry)
        {
            return geometry.polygon.asBinary();
        }

        @Override
        public String toGeoJson(Polygon geometry)
        {
            return geometry.polygon.asGeoJson();
        }

        @Override
        public Polygon fromWellKnownText(String source)
        {
            return new Polygon(fromOgcWellKnownText(source, OGCPolygon.class));
        }

        @Override
        public Polygon fromWellKnownBinary(ByteBuffer source)
        {
            return new Polygon(fromOgcWellKnownBinary(source, OGCPolygon.class));
        }

        @Override
        public Polygon fromGeoJson(String source)
        {
            return new Polygon(fromOgcGeoJson(source, OGCPolygon.class));
        }
    };

    OGCPolygon polygon;

    public Polygon(OGCPolygon polygon)
    {
        this.polygon = polygon;
        validate();
    }

    @Override
    protected OGCGeometry getOgcGeometry()
    {
        return polygon;
    }

    @Override
    public GeometricType getType()
    {
        return GeometricType.POLYGON;
    }

    @Override
    public void validate() throws MarshalException
    {
        validateOgcGeometry(polygon);
    }

    @Override
    public Serializer getSerializer()
    {
        return serializer;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Polygon polygon1 = (Polygon) o;

        return !(polygon != null ? !polygon.equals(polygon1.polygon) : polygon1.polygon != null);

    }

    @Override
    public int hashCode()
    {
        return polygon != null ? polygon.hashCode() : 0;
    }

    @Override
    public String toString()
    {
        return asWellKnownText();
    }
}
