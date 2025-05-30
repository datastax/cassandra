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

package org.apache.cassandra.schema;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.db.Digest;

public final class SchemaConstants
{
    public static final Pattern PATTERN_WORD_CHARS = Pattern.compile("\\w+");
    public static final Pattern PATTERN_NON_WORD_CHAR = Pattern.compile("\\W");


    public static final String SYSTEM_KEYSPACE_NAME = "system";
    public static final String SCHEMA_KEYSPACE_NAME = "system_schema";

    public static final String TRACE_KEYSPACE_NAME = "system_traces";
    public static final String AUTH_KEYSPACE_NAME = "system_auth";
    public static final String DISTRIBUTED_KEYSPACE_NAME = "system_distributed";

    public static final String VIRTUAL_SCHEMA = "system_virtual_schema";

    public static final String VIRTUAL_VIEWS = "system_views";
    public static final String SCHEMA_VIRTUAL_KEYSPACE_NAME = "system_virtual_schema";
    public static final String SYSTEM_VIEWS_KEYSPACE_NAME = "system_views";

    public static final String DUMMY_KEYSPACE_OR_TABLE_NAME = "--dummy--";

    /* system keyspace names (the ones with LocalStrategy replication strategy) */
    public static final Set<String> LOCAL_SYSTEM_KEYSPACE_NAMES = ImmutableSet.of(SYSTEM_KEYSPACE_NAME, SCHEMA_KEYSPACE_NAME);

    /* replicate system keyspace names (the ones with a "true" replication strategy) */
    public static final Set<String> REPLICATED_SYSTEM_KEYSPACE_NAMES = ImmutableSet.of(TRACE_KEYSPACE_NAME, AUTH_KEYSPACE_NAME, DISTRIBUTED_KEYSPACE_NAME);

    /* virtual keyspace names */
    public static final Set<String> VIRTUAL_KEYSPACE_NAMES = ImmutableSet.of(SCHEMA_VIRTUAL_KEYSPACE_NAME, SYSTEM_VIEWS_KEYSPACE_NAME);

    /**
     * Longest acceptable file name. Longer names lead to file write or read errors.
     */
    public static final int FILENAME_LENGTH = 255;

    /**
     * longest permissible KS or CF name.  Our main concern is that filename not be more than 255 characters;
     * the filename will contain both the KS and CF names. Since non-schema-name components only take up
     * ~64 characters, we could allow longer names than this, but on Windows, the entire path should be not greater than
     * 255 characters, so a lower limit here helps avoid problems.  See CASSANDRA-4110.
     *
     * Note: This extended to 222 for CNDB tenant specific keyspaces. The windows restriction is not valid here
     * because CNDB does not support windows.
     * 222 is maximum filename length of 255 chars minus a separator char and
     * 32 chars for table UUID.
     */
    public static final int NAME_LENGTH = FILENAME_LENGTH - 32 - 1;

    /**
     * Longest permissible index name, so no index can fail on file name error.
     * It is based on the most restrictive requirement coming from SAI and calculated by
     * {@link org.apache.cassandra.index.sai.disk.format.Version#calculateIndexNameAllowedLength}.
     * The exact number is used here, since it will be in user's documentation.
     */
    public static final int INDEX_NAME_LENGTH = 182;

    // 59adb24e-f3cd-3e02-97f0-5b395827453f
    public static final UUID emptyVersion;

    public static final List<String> LEGACY_AUTH_TABLES = Arrays.asList("credentials", "users", "permissions");

    /**
     * Checks if the length of the given name will be suitable to be used
     * in constructed file names.
     *
     * @param name the name to check
     * @return true if the name is short enough to be safe to use, otherwise false
     */
    public static boolean isSafeLengthForFilename(String name)
    {
        return name.length() <= NAME_LENGTH;
    }

    /**
     * Names such as keyspace, table, index names are used in file paths and file names,
     * so, they need to be safe for the use there, i.e., short enough and
     * containing only alphanumeric characters and underscores.
     *
     * @param name the name to check
     * @return whether the name is safe for use in file paths and file names
     */
    public static boolean isValidName(String name)
    {
        return isValidName(name, false);
    }

    /**
     * Names such as keyspace, table, index names are used in file paths and file names,
     * so, they need to be safe for the use there, i.e., short enough and
     * containing only alphanumeric characters and underscores.
     * However, historically not all names were checked for their length.
     * Such legacy behaviour is supported through passing true for doNotCheckLength.
     *
     * @param name             the name to check
     * @param doNotCheckLength specifies if no check on the name length should be done
     *                         to support legacy behaviour
     * @return true if the name is valid, false otherwise
     */
    public static boolean isValidName(String name, boolean doNotCheckLength)
    {
        return name != null && !name.isEmpty() && PATTERN_WORD_CHARS.matcher(name).matches() &&
               (doNotCheckLength || isSafeLengthForFilename(name));
    }

    static
    {
        emptyVersion = UUID.nameUUIDFromBytes(Digest.forSchema().digest());
    }

    /**
     * @return whether or not the keyspace is a really system one (w/ LocalStrategy, unmodifiable, hardcoded)
     */
    public static boolean isLocalSystemKeyspace(String keyspaceName)
    {
        return LOCAL_SYSTEM_KEYSPACE_NAMES.contains(keyspaceName.toLowerCase());
    }

    /**
     * @return whether or not the keyspace is a replicated system ks (system_auth, system_traces, system_distributed)
     */
    public static boolean isReplicatedSystemKeyspace(String keyspaceName)
    {
        return REPLICATED_SYSTEM_KEYSPACE_NAMES.contains(keyspaceName.toLowerCase());
    }

    /**
     * Checks if the keyspace is a virtual system keyspace.
     * @return {@code true} if the keyspace is a virtual system keyspace, {@code false} otherwise.
     */
    public static boolean isVirtualSystemKeyspace(String keyspaceName)
    {
        return VIRTUAL_SCHEMA.equals(keyspaceName.toLowerCase()) || VIRTUAL_VIEWS.equals(keyspaceName.toLowerCase());
    }

    /**
     * Checks if the keyspace is a system keyspace (local replicated or virtual).
     * @return {@code true} if the keyspace is a system keyspace, {@code false} otherwise.
     */
    public static boolean isSystemKeyspace(String keyspaceName)
    {
        return isLocalSystemKeyspace(keyspaceName)
                || isReplicatedSystemKeyspace(keyspaceName)
                || isVirtualSystemKeyspace(keyspaceName);
    }
    
    /**
     * @return whether or not the keyspace is a virtual keyspace (system_virtual_schema, system_views)
     */
    public static boolean isVirtualKeyspace(String keyspaceName)
    {
        return VIRTUAL_KEYSPACE_NAMES.contains(keyspaceName.toLowerCase());
    }

    public static boolean isInternalKeyspace(String keyspaceName)
    {
        return isLocalSystemKeyspace(keyspaceName)
               || isReplicatedSystemKeyspace(keyspaceName)
               || isVirtualKeyspace(keyspaceName);
    }
    
    /**
     * @return whether or not the keyspace is a user keyspace
     */
    public static boolean isUserKeyspace(String keyspaceName)
    {
        return !isInternalKeyspace(keyspaceName);
    }
}
