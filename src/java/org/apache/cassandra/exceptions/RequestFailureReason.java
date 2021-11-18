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
package org.apache.cassandra.exceptions;

import java.io.IOException;
import java.util.function.ToIntFunction;

import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.vint.VIntCoding;

import static java.lang.Math.max;
import static org.apache.cassandra.net.MessagingService.VERSION_40;

public enum RequestFailureReason
{
    UNKNOWN(0, 0),
    READ_TOO_MANY_TOMBSTONES(1, 1),
    TIMEOUT(2, 2),
    INCOMPATIBLE_SCHEMA(3, 3),
    READ_SIZE(4, 4),
    NODE_DOWN(5, 8),
    INDEX_NOT_AVAILABLE(6, 9),
    UNKNOWN_COLUMN(7, 5),
    UNKNOWN_TABLE(8, 6),
    REMOTE_STORAGE_FAILURE(9, 7),
    ;

    public static final Serializer serializer = new Serializer();

    public final int code;
    public final int ccCode;

    RequestFailureReason(int code, int ccCode)
    {
        this.code = code;
        this.ccCode = ccCode;
    }

    public int codeForNativeProtocol()
    {
        // We explicitly indicated in the protocol spec that drivers should not error out on unknown code, and we
        // currently support a superset of the OSS codes, so we don't yet worry about the version.
        return code;
    }

    private static RequestFailureReason[] makeCodeMapping(ToIntFunction<RequestFailureReason> mappingFunction)
    {
        RequestFailureReason[] reasons = values();

        int max = -1;
        for (RequestFailureReason r : reasons)
            max = max(mappingFunction.applyAsInt(r), max);

        RequestFailureReason[] codeMap = new RequestFailureReason[max + 1];

        for (RequestFailureReason reason : reasons)
        {
            if (codeMap[mappingFunction.applyAsInt(reason)] != null)
                throw new RuntimeException("Two RequestFailureReason-s that map to the same code: " + reason.code);
            codeMap[reason.code] = reason;
        }

        return codeMap;
    }

    private static final RequestFailureReason[] codeToReasonMap = makeCodeMapping(r -> r.code);
    private static final RequestFailureReason[] ccCodeToReasonMap = makeCodeMapping(r -> r.ccCode);

    public static RequestFailureReason fromCode(int code, int version)
    {
        assert version >= VERSION_40;

        if (code < 0)
            throw new IllegalArgumentException("RequestFailureReason code must be non-negative (got " + code + ')');

        // be forgiving and return UNKNOWN if we aren't aware of the code - for forward compatibility
        if (version >= MessagingService.VERSION_SG_10)
            return code < ccCodeToReasonMap.length ? ccCodeToReasonMap[code] : UNKNOWN;
        else
            return code < codeToReasonMap.length ? codeToReasonMap[code] : UNKNOWN;
    }

    public static RequestFailureReason forException(Throwable t)
    {
        if (t instanceof TombstoneOverwhelmingException)
            return READ_TOO_MANY_TOMBSTONES;

        if (t instanceof IncompatibleSchemaException)
            return INCOMPATIBLE_SCHEMA;

        return UNKNOWN;
    }

    public static final class Serializer implements IVersionedSerializer<RequestFailureReason>
    {
        private Serializer()
        {
        }

        public void serialize(RequestFailureReason reason, DataOutputPlus out, int version) throws IOException
        {
            assert version >= VERSION_40;
            if (version >= MessagingService.VERSION_SG_10)
                out.writeUnsignedVInt32(reason.ccCode);
            else
                out.writeUnsignedVInt32(reason.code);
        }

        public RequestFailureReason deserialize(DataInputPlus in, int version) throws IOException
        {
            assert version >= VERSION_40;
            return fromCode(in.readUnsignedVInt32(), version);
        }

        public long serializedSize(RequestFailureReason reason, int version)
        {
            assert version >= VERSION_40;
            if (version >= MessagingService.VERSION_SG_10)
                return VIntCoding.computeVIntSize(reason.ccCode);
            else
                return VIntCoding.computeVIntSize(reason.code);
        }
    }
}
