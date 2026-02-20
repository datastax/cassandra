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
package org.apache.cassandra.gms;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.CassandraVersion;

/**
 * This abstraction represents both the HeartBeatState and the ApplicationState in an EndpointState
 * instance. Any state for a given endpoint can be retrieved from this instance.
 */


public class EndpointState
{
    protected static final Logger logger = LoggerFactory.getLogger(EndpointState.class);

    public final static IVersionedSerializer<EndpointState> serializer = new EndpointStateSerializer();

    private volatile HeartBeatState hbState;
    private final AtomicReference<Map<ApplicationState, VersionedValue>> applicationState;

    /* fields below do not get serialized */
    private volatile long updateTimestamp;
    private volatile boolean isAlive;

    public EndpointState(HeartBeatState initialHbState)
    {
        this(initialHbState, new EnumMap<ApplicationState, VersionedValue>(ApplicationState.class));
    }

    public EndpointState(EndpointState other)
    {
        this(new HeartBeatState(other.hbState), new EnumMap<>(other.applicationState.get()));
    }

    EndpointState(HeartBeatState initialHbState, Map<ApplicationState, VersionedValue> states)
    {
        hbState = initialHbState;
        applicationState = new AtomicReference<Map<ApplicationState, VersionedValue>>(new EnumMap<>(states));
        updateTimestamp = System.nanoTime();
        isAlive = true;
    }

    HeartBeatState getHeartBeatState()
    {
        return hbState;
    }

    void setHeartBeatState(HeartBeatState newHbState)
    {
        updateTimestamp();
        hbState = newHbState;
    }

    public VersionedValue getApplicationState(ApplicationState key)
    {
        return applicationState.get().get(key);
    }

    public boolean containsApplicationState(ApplicationState key)
    {
        return applicationState.get().containsKey(key);
    }

    public Set<Map.Entry<ApplicationState, VersionedValue>> states()
    {
        return applicationState.get().entrySet();
    }

    public void addApplicationState(ApplicationState key, VersionedValue value)
    {
        addApplicationStates(Collections.singletonMap(key, value));
    }

    public void addApplicationStates(Map<ApplicationState, VersionedValue> values)
    {
        addApplicationStates(values.entrySet());
    }

    public void addApplicationStates(Set<Map.Entry<ApplicationState, VersionedValue>> values)
    {
        while (true)
        {
            Map<ApplicationState, VersionedValue> orig = applicationState.get();
            Map<ApplicationState, VersionedValue> copy = new EnumMap<>(orig);

            for (Map.Entry<ApplicationState, VersionedValue> value : values)
                copy.put(value.getKey(), value.getValue());

            if (applicationState.compareAndSet(orig, copy))
                return;
        }
    }

    void removeMajorVersion3LegacyApplicationStates()
    {
        while (hasLegacyFields())
        {
            Map<ApplicationState, VersionedValue> orig = applicationState.get();
            Map<ApplicationState, VersionedValue> updatedStates = filterMajorVersion3LegacyApplicationStates(orig);
            // avoid updating if no state is removed
            if (orig.size() == updatedStates.size()
                || applicationState.compareAndSet(orig, updatedStates))
                return;
        }
    }

    private boolean hasLegacyFields()
    {
        Set<ApplicationState> statesPresent = applicationState.get().keySet();
        if (statesPresent.isEmpty())
            return false;
        return (statesPresent.contains(ApplicationState.STATUS) && statesPresent.contains(ApplicationState.STATUS_WITH_PORT))
               || (statesPresent.contains(ApplicationState.INTERNAL_IP) && statesPresent.contains(ApplicationState.INTERNAL_ADDRESS_AND_PORT))
               || (statesPresent.contains(ApplicationState.RPC_ADDRESS) && statesPresent.contains(ApplicationState.NATIVE_ADDRESS_AND_PORT));
    }

    private static Map<ApplicationState, VersionedValue> filterMajorVersion3LegacyApplicationStates(Map<ApplicationState, VersionedValue> states)
    {
        return states.entrySet().stream().filter(entry -> {
                // Filter out pre-4.0 versions of data for more complete 4.0 versions
                switch (entry.getKey())
                {
                    case INTERNAL_IP:
                        return !states.containsKey(ApplicationState.INTERNAL_ADDRESS_AND_PORT);
                    case STATUS:
                        return !states.containsKey(ApplicationState.STATUS_WITH_PORT);
                    case RPC_ADDRESS:
                        return !states.containsKey(ApplicationState.NATIVE_ADDRESS_AND_PORT);
                    default:
                        return true;
                }
            }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /* getters and setters */
    /**
     * @return System.nanoTime() when state was updated last time.
     */
    public long getUpdateTimestamp()
    {
        return updateTimestamp;
    }

    void updateTimestamp()
    {
        updateTimestamp = System.nanoTime();
    }

    public boolean isAlive()
    {
        return isAlive;
    }

    void markAlive()
    {
        isAlive = true;
    }

    void markDead()
    {
        isAlive = false;
    }

    /**
     * @return true if {@link HeartBeatState#isEmpty()} is true and no STATUS application state exists
     */
    public boolean isEmptyWithoutStatus()
    {
        Map<ApplicationState, VersionedValue> state = applicationState.get();
        return hbState.isEmpty() && !(state.containsKey(ApplicationState.STATUS_WITH_PORT) || state.containsKey(ApplicationState.STATUS));
    }

    public boolean isRpcReady()
    {
        VersionedValue rpcState = getApplicationState(ApplicationState.RPC_READY);
        return rpcState != null && Boolean.parseBoolean(rpcState.value);
    }

    public boolean isNormalState()
    {
        return getStatus().equals(VersionedValue.STATUS_NORMAL);
    }

    public String getStatus()
    {
        VersionedValue status = getApplicationState(ApplicationState.STATUS_WITH_PORT);
        if (status == null)
        {
            status = getApplicationState(ApplicationState.STATUS);
        }
        if (status == null)
        {
            return "";
        }

        String[] pieces = status.value.split(VersionedValue.DELIMITER_STR, -1);
        assert (pieces.length > 0);
        return pieces[0];
    }

    @Nullable
    public UUID getSchemaVersion()
    {
        VersionedValue applicationState = getApplicationState(ApplicationState.SCHEMA);
        return applicationState != null
               ? UUID.fromString(applicationState.value)
               : null;
    }

    @Nullable
    public CassandraVersion getReleaseVersion()
    {
        VersionedValue applicationState = getApplicationState(ApplicationState.RELEASE_VERSION);
        return applicationState != null
               ? new CassandraVersion(applicationState.value)
               : null;
    }

    public String toString()
    {
        return "EndpointState: HeartBeatState = " + hbState + ", AppStateMap = " + applicationState.get();
    }
}

class EndpointStateSerializer implements IVersionedSerializer<EndpointState>
{
    private static final Logger logger = LoggerFactory.getLogger(EndpointStateSerializer.class);

    public void serialize(EndpointState epState, DataOutputPlus out, int version) throws IOException
    {
        /* serialize the HeartBeatState */
        HeartBeatState hbState = epState.getHeartBeatState();
        HeartBeatState.serializer.serialize(hbState, out, version);

        /* serialize the map of ApplicationState objects */
        Set<Map.Entry<ApplicationState, VersionedValue>> states = filterOutgoingStates(epState.states(), version);
        out.writeInt(states.size());
        for (Map.Entry<ApplicationState, VersionedValue> state : states)
        {
            out.writeInt(state.getKey().ordinal());
            VersionedValue.serializer.serialize(state.getValue(), out, version);
        }
    }

    public EndpointState deserialize(DataInputPlus in, int version) throws IOException
    {
        HeartBeatState hbState = HeartBeatState.serializer.deserialize(in, version);

        int appStateSize = in.readInt();
        Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
        for (int i = 0; i < appStateSize; ++i)
        {
            int key = in.readInt();
            VersionedValue value = VersionedValue.serializer.deserialize(in, version);
            states.put(Gossiper.STATES[key], value);
        }

        return new EndpointState(hbState, filterIncomingStates(states, version));
    }

    public long serializedSize(EndpointState epState, int version)
    {
        long size = HeartBeatState.serializer.serializedSize(epState.getHeartBeatState(), version);
        Set<Map.Entry<ApplicationState, VersionedValue>> states = filterOutgoingStates(epState.states(), version);
        size += TypeSizes.sizeof(states.size());
        for (Map.Entry<ApplicationState, VersionedValue> state : states)
        {
            size += TypeSizes.sizeof(state.getKey().ordinal());
            size += VersionedValue.serializer.serializedSize(state.getValue(), version);
        }
        return size;
    }

    @VisibleForTesting
    static Set<Map.Entry<ApplicationState, VersionedValue>> filterOutgoingStates(Set<Map.Entry<ApplicationState, VersionedValue>> states, int version)
    {
        if (version < MessagingService.VERSION_40 && !MessagingService.current_version_override)
        {
            Set<Map.Entry<ApplicationState, VersionedValue>> filteredStates = new HashSet<>();
            for (Map.Entry<ApplicationState, VersionedValue> state : states)
                filteredStates.addAll(filterOutgoingState(state, version).entrySet());
            return filteredStates;
        }
        return states;
    }

    private static Map<ApplicationState, VersionedValue> filterOutgoingState(Map.Entry<ApplicationState, VersionedValue> state, int version)
    {
        assert version < MessagingService.VERSION_40 && !MessagingService.current_version_override;
        VersionedValue vv = state.getValue();
        if (logger.isTraceEnabled())
            logger.trace("Fetching the key from state {}({}) with value of {}", state.getKey(), state.getKey().ordinal(), vv);
        String[] values;
        switch (state.getKey())
        {
            case INTERNAL_ADDRESS_AND_PORT:
                values = vv.value.split(":");
                if (values.length > 1)
                    return Map.of(ApplicationState.values()[7], VersionedValue.unsafeMakeVersionedValue(values[0], vv.version),
                                  ApplicationState.values()[17], VersionedValue.unsafeMakeVersionedValue(values[1], vv.version));
                else
                    return Map.of(ApplicationState.values()[17], VersionedValue.unsafeMakeVersionedValue(vv.value, vv.version));
            case NATIVE_ADDRESS_AND_PORT:
                values = vv.value.split(":");
                if (values.length > 1)
                    return Map.of(ApplicationState.values()[15], VersionedValue.unsafeMakeVersionedValue(values[1], vv.version));
                else
                    return Map.of(ApplicationState.values()[15], VersionedValue.unsafeMakeVersionedValue(vv.value, vv.version));
            case STATUS_WITH_PORT:
                values = vv.value.split("[:,]");
                if (values.length > 1)
                    return Map.of(ApplicationState.values()[0], VersionedValue.unsafeMakeVersionedValue(values[0], vv.version));
                else
                    return Map.of(ApplicationState.values()[0], VersionedValue.unsafeMakeVersionedValue(vv.value, vv.version));
            case DISK_USAGE:
                return Map.of(ApplicationState.values()[21], state.getValue());
            case SSTABLE_VERSIONS:
                // has it as 18 but DSE doesn't have it at all (dse's 18 is STORAGE_PORT_SSL)
                //  as schema changes must not be performed during a major upgrade, just ignore sending this state to the legacy peers
                return Map.of();
            case INDEX_STATUS:
                return Map.of();
            case RELEASE_VERSION:
                // CC versions add a sha suffix that C* 3.x nodes cannot parse
                return Map.of(ApplicationState.RELEASE_VERSION, VersionedValue.unsafeMakeVersionedValue(vv.value.replaceFirst("-[0-9a-f]{7,40}$", ""), vv.version));
            default:
                return Map.of(state.getKey(), state.getValue());
        }
    }

    @VisibleForTesting
    static Map<ApplicationState, VersionedValue> filterIncomingStates(Map<ApplicationState, VersionedValue> states, int version)
    {
        if (version < MessagingService.VERSION_40 && !MessagingService.current_version_override)
        {
            Map<ApplicationState, VersionedValue> filteredStates = new EnumMap<>(ApplicationState.class);
            for (Map.Entry<ApplicationState, VersionedValue> state : states.entrySet())
            {
                VersionedValue vv = state.getValue();
                if (logger.isTraceEnabled())
                    logger.trace("Storing the key to state {}({}) with value of {}", state.getKey(), state.getKey().ordinal(), vv);
                switch (state.getKey().ordinal())
                {
                    case 15: // NATIVE_TRANSPORT_PORT --> NATIVE_ADDRESS_AND_PORT
                        filteredStates.put(ApplicationState.NATIVE_ADDRESS_AND_PORT, vv);
                        break;
                    case 16: // NATIVE_TRANSPORT_PORT_SSL
                        break;
                    case 17: // STORAGE_PORT --> INTERNAL_ADDRESS_AND_PORT
                        filteredStates.put(ApplicationState.INTERNAL_ADDRESS_AND_PORT, vv);
                        break;
                    case 18: // STORAGE_PORT_SSL
                    case 19: // JMX_PORT
                    case 20: // SCHEMA_COMPATIBILITY_VERSION
                        break;
                    case 21:
                        filteredStates.put(ApplicationState.DISK_USAGE, vv);
                        break;
                    default:
                        filteredStates.put(state.getKey(), vv);
                }
            }
            return filteredStates;
        }
        return states;
    }
}
