/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.db.marshal;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Json;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.db.marshal.geometry.GeometricType;
import org.apache.cassandra.db.marshal.geometry.OgcGeometry;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;

public abstract class AbstractGeometricType<T extends OgcGeometry> extends AbstractType<T>
{
    private final TypeSerializer<T> serializer = new TypeSerializer<T>()
    {
        @Override
        public ByteBuffer serialize(T geometry)
        {
            return geoSerializer.toWellKnownBinary(geometry);
        }

        @Override
        public T deserialize(ByteBuffer byteBuffer)
        {
            // OGCGeometry does not respect the current position of the buffer, so you need to use slice()
            return geoSerializer.fromWellKnownBinary(byteBuffer.slice());
        }

        @Override
        public void validate(ByteBuffer byteBuffer) throws MarshalException
        {
            int pos = byteBuffer.position();
            // OGCGeometry does not respect the current position of the buffer, so you need to use slice()
            geoSerializer.fromWellKnownBinary(byteBuffer.slice()).validate();
            byteBuffer.position(pos);
        }

        @Override
        public String toString(T geometry)
        {
            return geoSerializer.toWellKnownText(geometry);
        }

        @Override
        public Class<T> getType()
        {
            return klass;
        }
    };

    private final GeometricType type;
    private final Class<T> klass;
    private final OgcGeometry.Serializer<T> geoSerializer;

    public AbstractGeometricType(GeometricType type)
    {
        super(ComparisonType.BYTE_ORDER);
        this.type = type;
        this.klass = (Class<T>) type.getGeoClass();
        this.geoSerializer = type.getSerializer();
    }

    public GeometricType getGeoType()
    {
        return type;
    }

    @Override
    public ByteBuffer fromString(String s) throws MarshalException
    {
        try
        {
            T geometry = geoSerializer.fromWellKnownText(s);
            geometry.validate();
            return geoSerializer.toWellKnownBinary(geometry);
        }
        catch (Exception e)
        {
            String parentMsg = e.getMessage() != null ? " " + e.getMessage() : "";
            String msg = String.format("Unable to make %s from '%s'", getClass().getSimpleName(), s) + parentMsg;
            throw new MarshalException(msg, e);
        }
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        if (!(parsed instanceof String))
        {
            try
            {
                parsed = Json.JSON_OBJECT_MAPPER.writeValueAsString(parsed);
            }
            catch (IOException e)
            {
                throw new MarshalException(e.getMessage());
            }
        }

        T geometry;
        try
        {
            geometry = geoSerializer.fromGeoJson((String) parsed);
        }
        catch (MarshalException e)
        {
            try
            {
                geometry = geoSerializer.fromWellKnownText((String) parsed);
            }
            catch (MarshalException ignored)
            {
                throw new MarshalException(e.getMessage());
            }
        }
        geometry.validate();
        return new Constants.Value(geoSerializer.toWellKnownBinary(geometry));
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        // OGCGeometry does not respect the current position of the buffer, so you need to use slice()
        return geoSerializer.toGeoJson(geoSerializer.fromWellKnownBinary(buffer.slice()));
    }

    @Override
    public TypeSerializer<T> getSerializer()
    {
        return serializer;
    }
}
