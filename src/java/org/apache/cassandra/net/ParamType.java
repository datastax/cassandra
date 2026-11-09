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
package org.apache.cassandra.net;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.Int32Serializer;
import org.apache.cassandra.utils.Int64Serializer;
import org.apache.cassandra.utils.RangesSerializer;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.StringSerializer;

import static java.lang.Math.max;

import static org.apache.cassandra.locator.InetAddressAndPort.FwdFrmSerializer.fwdFrmSerializer;

/**
 * Type names and serializers for various parameters that can be put in {@link Message} params map.
 *
 * It should be safe to add new params without bumping messaging version - {@link Message} serializer
 * will skip over any params it doesn't recognise.
 *
 * Please don't add boolean params here. Extend and use {@link MessageFlag} instead.
 *
 * Do not re-use old, nor fill gaps in the sequence of, ids.  New IDs must be higher.
 *
 * Peers speaking the pre-4.0 dialect (DSE 6 compatibility mode, messaging version 11) name params by their
 * legacy alias string instead of the id; a param without an alias is not sent to such a peer and is unknown to it.
 * The deprecated ids 2, 3, 4 and 7 exist only for that dialect: pre-4.0 encoded the message flags and failure
 * responses as params.
 */
public enum ParamType
{
    FORWARD_TO                       (0,  "FWD_TO",        ForwardingInfo.serializer),
    RESPOND_TO                       (1,  "FWD_FRM",       fwdFrmSerializer),

    /** Pre-4.0 only: marks a failure response (DSE 6 compatibility). */
    FAILURE_RESPONSE                 (2,  "FAIL",          LegacyFlag.serializer),
    /** Pre-4.0 only: the reason of a failure response (DSE 6 compatibility). */
    FAILURE_REASON                   (3,  "FAIL_REASON",   RequestFailureReason.serializer),
    /** Pre-4.0 only: the {@link MessageFlag#CALL_BACK_ON_FAILURE} flag (DSE 6 compatibility). */
    FAILURE_CALLBACK                 (4,  "CAL_BAC",       LegacyFlag.serializer),

    TRACE_SESSION                    (5,  "TraceSession",  TimeUUID.Serializer.instance),
    TRACE_TYPE                       (6,  "TraceType",     Tracing.traceTypeSerializer),

    /** Pre-4.0 only: the {@link MessageFlag#TRACK_REPAIRED_DATA} flag (DSE 6 compatibility). */
    TRACK_REPAIRED_DATA              (7,  "TrackRepaired", LegacyFlag.serializer),

    TOMBSTONE_FAIL                   (8,  Int32Serializer.serializer),
    TOMBSTONE_WARNING                (9,  Int32Serializer.serializer),
    LOCAL_READ_SIZE_FAIL             (10, Int64Serializer.serializer),
    LOCAL_READ_SIZE_WARN             (11, Int64Serializer.serializer),
    ROW_INDEX_READ_SIZE_FAIL         (12, Int64Serializer.serializer),
    ROW_INDEX_READ_SIZE_WARN         (13, Int64Serializer.serializer),
    CUSTOM_MAP                       (14, "CUSTOM",        CustomParamsSerializer.serializer),
    SNAPSHOT_RANGES                  (15, RangesSerializer.serializer),
    TOO_MANY_REFERENCED_INDEXES_WARN (16, Int32Serializer.serializer),
    TOO_MANY_REFERENCED_INDEXES_FAIL (17, Int32Serializer.serializer),
    /**
     * Messages with tracing sessions are decorated with the traced keyspace.
     */
    TRACE_KEYSPACE                   (18, "TraceKeyspace", StringSerializer.serializer),
    /**
     * Failure response messages contain verb name of the incoming message.
     */
    REQUEST_VERB_NAME                (19, StringSerializer.serializer);

    final int id;
    /** The pre-4.0 name of the param, or null when the param does not exist in that dialect. */
    @Nullable
    final String legacyAlias;
    final IVersionedSerializer serializer;

    ParamType(int id, IVersionedSerializer serializer)
    {
        this(id, null, serializer);
    }

    ParamType(int id, String legacyAlias, IVersionedSerializer serializer)
    {
        if (id < 0)
            throw new IllegalArgumentException("ParamType id must be non-negative");

        this.id = id;
        this.legacyAlias = legacyAlias;
        this.serializer = serializer;
    }

    private static final ParamType[] idToTypeMap;
    private static final Map<String, ParamType> aliasToTypeMap;

    static
    {
        ParamType[] types = values();

        int max = -1;
        for (ParamType t : types)
            max = max(t.id, max);

        ParamType[] idMap = new ParamType[max + 1];
        Map<String, ParamType> aliasMap = new HashMap<>();

        for (ParamType type : types)
        {
            if (idMap[type.id] != null)
                throw new RuntimeException("Two ParamType-s that map to the same id: " + type.id);
            idMap[type.id] = type;

            if (type.legacyAlias != null && aliasMap.put(type.legacyAlias, type) != null)
                throw new RuntimeException("Two ParamType-s that map to the same legacy alias: " + type.legacyAlias);
        }

        idToTypeMap = idMap;
        aliasToTypeMap = aliasMap;
    }

    @Nullable
    static ParamType lookUpById(int id)
    {
        if (id < 0)
            throw new IllegalArgumentException("ParamType id must be non-negative (got " + id + ')');

        ParamType type = id < idToTypeMap.length ? idToTypeMap[id] : null;
        // the pre-4.0-only params are never sent from 4.0 on: a value under one of their ids is skipped, as before they existed
        return type == null || type.isPre40Only() ? null : type;
    }

    /** True for the params that exist only in the pre-4.0 dialect: the message flags and the failure responses of 3.0. */
    boolean isPre40Only()
    {
        return this == FAILURE_RESPONSE || this == FAILURE_REASON || this == FAILURE_CALLBACK || this == TRACK_REPAIRED_DATA;
    }

    /** The param a pre-4.0 peer named by this alias, or null when the alias is unknown (the value is skipped). */
    @Nullable
    static ParamType lookUpByAlias(String alias)
    {
        return aliasToTypeMap.get(alias);
    }
}
