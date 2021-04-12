/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.db.marshal.geometry;

import java.nio.ByteBuffer;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCLineString;
import org.apache.cassandra.serializers.MarshalException;

public class LineString extends OgcGeometry
{
    public static final Serializer<LineString> serializer = new Serializer<LineString>()
    {
        @Override
        public String toWellKnownText(LineString geometry)
        {
            return geometry.lineString.asText();
        }

        @Override
        public ByteBuffer toWellKnownBinaryNativeOrder(LineString geometry)
        {
            return geometry.lineString.asBinary();
        }

        @Override
        public String toGeoJson(LineString geometry)
        {
            return geometry.lineString.asGeoJson();
        }

        @Override
        public LineString fromWellKnownText(String source)
        {
            return new LineString(fromOgcWellKnownText(source, OGCLineString.class));
        }

        @Override
        public LineString fromWellKnownBinary(ByteBuffer source)
        {
            return new LineString(fromOgcWellKnownBinary(source, OGCLineString.class));
        }

        @Override
        public LineString fromGeoJson(String source)
        {
            return new LineString(fromOgcGeoJson(source, OGCLineString.class));
        }
    };

    private final OGCLineString lineString;

    public LineString(OGCLineString lineString)
    {
        this.lineString = lineString;
        validate();
    }

    @Override
    public GeometricType getType()
    {
        return GeometricType.LINESTRING;
    }

    @Override
    public void validate() throws MarshalException
    {
        validateOgcGeometry(lineString);
    }

    @Override
    public Serializer getSerializer()
    {
        return serializer;
    }

    @Override
    protected OGCGeometry getOgcGeometry()
    {
        return lineString;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LineString that = (LineString) o;

        return !(lineString != null ? !lineString.equals(that.lineString) : that.lineString != null);

    }

    @Override
    public int hashCode()
    {
        return lineString != null ? lineString.hashCode() : 0;
    }

    @Override
    public String toString()
    {
        return asWellKnownText();
    }
}
