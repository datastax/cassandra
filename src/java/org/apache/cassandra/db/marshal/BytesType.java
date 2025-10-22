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
package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.serializers.BytesSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Hex;

public class BytesType extends AbstractType<ByteBuffer>
{
    public static final BytesType instance = new BytesType();

    private static final ByteBuffer MASKED_VALUE = ByteBufferUtil.EMPTY_BYTE_BUFFER;

    BytesType() {super(ComparisonType.BYTE_ORDER);} // singleton

    @Override
    public boolean allowsEmpty()
    {
        return true;
    }

    public ByteBuffer fromString(String source)
    {
        try
        {
            return ByteBuffer.wrap(Hex.hexToBytes(source));
        }
        catch (NumberFormatException e)
        {
            throw new MarshalException(String.format("cannot parse '%s' as hex bytes", source), e);
        }
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        try
        {
            String parsedString = (String) parsed;
            if (!parsedString.startsWith("0x"))
                throw new MarshalException(String.format("String representation of blob is missing 0x prefix: %s", parsedString));

            return new Constants.Value(BytesType.instance.fromString(parsedString.substring(2)));
        }
        catch (ClassCastException | MarshalException exc)
        {
            throw new MarshalException(String.format("Value '%s' is not a valid blob representation: %s", parsed, exc.getMessage()));
        }
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        return "\"0x" + ByteBufferUtil.bytesToHex(buffer) + '"';
    }

    @Override
    public boolean isCompatibleWith(AbstractType<?> previous)
    {
        // TODO BytesType is actually compatible with all types which use BYTE_ORDER comparison type
        // Both asciiType and utf8Type really use bytes comparison and
        // bytesType validate everything, so it is compatible with the former.
        return this == previous || previous == AsciiType.instance || previous == UTF8Type.instance;
    }

    @Override
    protected boolean isValueCompatibleWithInternal(AbstractType<?> previous)
    {
        // BytesType can read anything
        return true;
    }

    @Override
    public boolean isSerializationCompatibleWith(AbstractType<?> previous)
    {
        // BytesType can read anything, but for serialization compatibility (i.e., when dropping
        // and re-adding a column), we need to be very restrictive. We cannot be serialization
        // compatible with geometric types or date ranges as they have different wire formats.
        if (previous instanceof AbstractGeometricType || previous instanceof DateRangeType)
            return false;

        // BytesType is serialization-compatible with itself and with a specific set of types
        // that have compatible serialization semantics, even though they may have different
        // fixed-length properties.
        if (this == previous)
            return true;

        if (isMultiCell() != previous.isMultiCell())
            return false;

        // BytesType is only compatible with simple scalar types, not complex
        // structured types, even if they're variable-length.
        return previous instanceof SimpleDateType
            || previous instanceof ByteType
            || previous instanceof AsciiType
            || previous instanceof TimeType
            || previous instanceof UTF8Type
            || previous instanceof InetAddressType
            || previous instanceof DurationType
            || previous instanceof ShortType
            || previous instanceof IntegerType
            || previous instanceof DecimalType;
    }

    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.BLOB;
    }

    public TypeSerializer<ByteBuffer> getSerializer()
    {
        return BytesSerializer.instance;
    }

    @Override
    public ArgumentDeserializer getArgumentDeserializer()
    {
        return ArgumentDeserializer.NOOP_DESERIALIZER;
    }

    @Override
    public ByteBuffer getMaskedValue()
    {
        return MASKED_VALUE;
    }
}
