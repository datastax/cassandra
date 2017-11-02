/*
 * Copyright DataStax, Inc.
 */
package org.apache.cassandra.service;

import java.util.*;

import org.apache.cassandra.db.*;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

/**
 * Modes supported by the rebuild command.
 */
enum RebuildMode
{
    /**
     * Conventional behaviour, only streams ranges that are not already locally available.
     */
    NORMAL
    {
        @Override
        public void beforeStreaming(List<String> keyspaces)
        {
        }

        @Override
        public void beforeStreaming(Map<String, Collection<Range<Token>>> rangesPerKeyspaces)
        {
        }
    },

    /**
     * Resets the locally available ranges, streams all ranges but leaves current data untouched.
     */
    REFETCH
    {
        @Override
        public void beforeStreaming(List<String> keyspaces)
        {
            // reset the locally available ranges for the keyspaces
            keyspaces.forEach(SystemKeyspace::resetAvailableRanges);
        }

        @Override
        public void beforeStreaming(Map<String, Collection<Range<Token>>> rangesPerKeyspaces)
        {
            // reset the locally available ranges for the keyspaces
            rangesPerKeyspaces.forEach(SystemKeyspace::resetAvailableRanges);
        }
    },

    /**
     * Resets the locally available ranges, removes all locally present data (like a {@code TRUNCATE}), streams all ranges
     */
    RESET
    {
        @Override
        public void beforeStreaming(List<String> keyspaces)
        {
            for (String keyspaceName : keyspaces)
            {
                // reset the locally available ranges for the keyspaces
                SystemKeyspace.resetAvailableRanges(keyspaceName);

                // truncate the tables for the keyspaces (local, not cluster wide)
                Keyspace.open(keyspaceName).getColumnFamilyStores().forEach(ColumnFamilyStore::truncateBlocking);
            }
        }

        @Override
        public void beforeStreaming(Map<String, Collection<Range<Token>>> rangesPerKeyspaces)
        {
            throw new IllegalArgumentException("mode=reset is only supported for all ranges");
        }
    },

    /**
     * Resets the locally available ranges, removes all locally present data (like a {@code TRUNCATE}),
     * streams all ranges, but never creates a snapshot during truncate.
     */
    RESET_NO_SNAPSHOT
    {
        @Override
        public void beforeStreaming(List<String> keyspaces)
        {
            for (String keyspaceName : keyspaces)
            {
                // reset the locally available ranges for the keyspaces
                SystemKeyspace.resetAvailableRanges(keyspaceName);

                // truncate the tables for the keyspaces (local, not cluster wide)
                Keyspace.open(keyspaceName).getColumnFamilyStores().forEach(cfs -> cfs.truncateBlocking(false));
            }
        }

        @Override
        public void beforeStreaming(Map<String, Collection<Range<Token>>> rangesPerKeyspaces)
        {
            throw new IllegalArgumentException("mode=reset-no-snapshot is only supported for all ranges");
        }
    };

    /**
     * Called before streaming.
     * @param keyspaces the keyspaces that need to be streamed
     */
    public abstract void beforeStreaming(List<String> keyspaces);

    /**
     * Called before streaming.
     * @param rangesPerKeyspaces the keyspaces ranges that need to be streamed
     */
    public abstract void beforeStreaming(Map<String, Collection<Range<Token>>> rangesPerKeyspaces);

    /**
     * Returns the mode corresponding to the specified name or if the name is {@code null} the default mode.
     * @param name the name of the mode to retrieve
     * @return the mode corresponding to the specified name or if the name is {@code null} the default mode
     * @throws IllegalArgumentException if the specified name cannot be found.
     */
    public static RebuildMode getMode(String name)
    {
        if (name == null)
            return NORMAL;

        String modeName = name.toUpperCase(Locale.US).replaceAll("-", "_");
        for (RebuildMode mode : RebuildMode.values())
        {
            if (mode.name().equals(modeName))
                return mode;
        }
        throw new IllegalArgumentException("Unknown mode used for rebuild: " + name);
    }
}
