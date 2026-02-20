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
import java.util.HashMap;
import java.util.Map;

import com.google.common.primitives.Ints;

import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.index.IndexBuildInProgressException;
import org.apache.cassandra.index.IndexNotAvailableException;
import org.apache.cassandra.index.FeatureNeedsIndexUpgradeException;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.net.MessagingService.VERSION_40;

public enum RequestFailureReason
{
    UNKNOWN                  (0),
    READ_TOO_MANY_TOMBSTONES (1),
    TIMEOUT                  (2),
    INCOMPATIBLE_SCHEMA      (3),
    INDEX_NOT_AVAILABLE      (6), // We match it to Apache Cassandra's INDEX_NOT_AVAILABLE code introduced in 5.0
    // The following codes are not present in Apache Cassandra's RequestFailureReason
    // We should add new codes in HCD (which do not exist in Apache Cassandra) only with big numbers, to avoid conflicts
    UNKNOWN_COLUMN           (500),
    UNKNOWN_TABLE            (501),
    REMOTE_STORAGE_FAILURE   (502),
    INDEX_BUILD_IN_PROGRESS  (503),
    FEATURE_NEEDS_INDEX_REBUILD(504); // The index uses an old version that doesn't support the requested feature

    public static final Serializer serializer = new Serializer();

    public final int code;

    RequestFailureReason(int code)
    {
        this.code = code;
    }

    public int codeForNativeProtocol()
    {
        // We explicitly indicated in the protocol spec that drivers should not error out on unknown code, and we
        // currently support a superset of the OSS codes, so we don't yet worry about the version.
        return code;
    }

    private static final Map<Integer, RequestFailureReason> codeToReasonMap = new HashMap<>();
    private static final Map<Class<? extends Throwable>, RequestFailureReason> exceptionToReasonMap = new HashMap<>();

    static
    {
        RequestFailureReason[] reasons = values();

        for (RequestFailureReason reason : reasons)
        {
            if (codeToReasonMap.put(reason.code, reason) != null)
                throw new RuntimeException("Two RequestFailureReason-s that map to the same code: " + reason.code);
        }

        exceptionToReasonMap.put(TombstoneOverwhelmingException.class, READ_TOO_MANY_TOMBSTONES);
        exceptionToReasonMap.put(IncompatibleSchemaException.class, INCOMPATIBLE_SCHEMA);
        exceptionToReasonMap.put(AbortedOperationException.class, TIMEOUT);
        exceptionToReasonMap.put(IndexNotAvailableException.class, INDEX_NOT_AVAILABLE);
        exceptionToReasonMap.put(UnknownColumnException.class, UNKNOWN_COLUMN);
        exceptionToReasonMap.put(UnknownTableException.class, UNKNOWN_TABLE);
        exceptionToReasonMap.put(IndexBuildInProgressException.class, INDEX_BUILD_IN_PROGRESS);
        exceptionToReasonMap.put(FeatureNeedsIndexUpgradeException.class, FEATURE_NEEDS_INDEX_REBUILD);

        if (exceptionToReasonMap.size() != reasons.length-2)
            throw new RuntimeException("A new RequestFailureReasons was probably added and you may need to update the exceptionToReasonMap");
    }

    public static RequestFailureReason fromCode(int code)
    {
        if (code < 0)
            throw new IllegalArgumentException("RequestFailureReason code must be non-negative (got " + code + ')');

        // be forgiving and return UNKNOWN if we aren't aware of the code - for forward compatibility
        return codeToReasonMap.getOrDefault(code, UNKNOWN);
    }

    public static RequestFailureReason forException(Throwable t)
    {
        RequestFailureReason r = exceptionToReasonMap.get(t.getClass());
        if (r != null)
            return r;

        for (Map.Entry<Class<? extends Throwable>, RequestFailureReason> entry : exceptionToReasonMap.entrySet())
            if (entry.getKey().isInstance(t))
                return entry.getValue();

        return UNKNOWN;
    }

    public static final class Serializer implements IVersionedSerializer<RequestFailureReason>
    {
        private Serializer()
        {
        }

        public void serialize(RequestFailureReason reason, DataOutputPlus out, int version) throws IOException
        {
            if (version < VERSION_40)
                out.writeShort(reason.code);
            else
                out.writeUnsignedVInt(reason.code);
        }

        public RequestFailureReason deserialize(DataInputPlus in, int version) throws IOException
        {
            return fromCode(version < VERSION_40 ? in.readUnsignedShort() : Ints.checkedCast(in.readUnsignedVInt()));
        }

        public long serializedSize(RequestFailureReason reason, int version)
        {
            return version < VERSION_40 ? 2 : VIntCoding.computeVIntSize(reason.code);
        }
    }
}
