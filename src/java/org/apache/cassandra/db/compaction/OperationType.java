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
package org.apache.cassandra.db.compaction;

import com.google.common.base.Predicate;

/**
 * The types of operations that can be observed with {@link AbstractTableOperation} and tracked by
 * {@link org.apache.cassandra.db.lifecycle.LifecycleTransaction}.
 * <p/>
 * Historically these operations have been broadly described as "compactions", even though they have
 * nothing to do with actual compactions. Any operation that can report progress and that normally
 * involves files, either for reading or writing, is a valid operation.
 */
public enum OperationType
{
    /** Each modification here should be also applied to {@link org.apache.cassandra.tools.nodetool.Stop#compactionType} */
    P0("Cancel all operations", false, 0),

    // Automation or operator-driven tasks
    CLEANUP("Cleanup", true, 1),
    SCRUB("Scrub", true, 1),
    UPGRADE_SSTABLES("Upgrade sstables", true, 1),
    VERIFY("Verify", false, 1),
    MAJOR_COMPACTION("Major compaction", true, 1),
    RELOCATE("Relocate sstables to correct disk", false, 1),
    GARBAGE_COLLECT("Remove deleted data", true, 1),

    // Internal SSTable writing
    FLUSH("Flush", true, 1),
    WRITE("Write", true, 1),

    ANTICOMPACTION("Anticompaction after repair", true, 2),
    VALIDATION("Validation", false, 3),

    INDEX_BUILD("Secondary index build", false, 4),
    VIEW_BUILD("View build", false, 4),

    COMPACTION("Compaction", true, 5),
    TOMBSTONE_COMPACTION("Tombstone Compaction", true, 5), // Compaction for tombstone removal
    UNKNOWN("Unknown compaction type", false, 5),

    STREAM("Stream", true, 6),
    KEY_CACHE_SAVE("Key cache save", false, 6),
    ROW_CACHE_SAVE("Row cache save", false, 6),
    COUNTER_CACHE_SAVE("Counter cache save", false, 6),
    INDEX_SUMMARY("Index summary redistribution", false, 6),
    // FIXME CNDB-11008: Review port of STAR-979 to review values of `writesData` and `priority` for the added operations below
    RESTORE("Restore", false, 6),
    // operations used for sstables on remote storage
    REMOTE_RELOAD("Remote reload", false, 6, true), // reload locally sstables that already exist remotely
    REMOTE_COMPACTION("Remote compaction", false, 6, true), // no longer used, kept for backward compatibility
    REMOTE_RELOAD_FOR_REPAIR("Remote reload for repair", false, 6, true, false), // reload locally sstables that already exist remotely for repair
    TRUNCATE_TABLE("Table truncated", false, 6),
    DROP_TABLE("Table dropped", false, 6),
    REMOVE_UNREADEABLE("Remove unreadable sstables", false, 6),
    REGION_BOOTSTRAP("Region Bootstrap", false, 6),
    REGION_DECOMMISSION("Region Decommission", false, 6),
    REGION_REPAIR("Region Repair", false, 6),
    SSTABLE_DISCARD("Local-only sstable discard", false, 6, true);

    public final String type;
    public final String fileName;
    /** true if the transaction of this type should NOT be uploaded remotely */
    public final boolean localOnly;
    /** true if the transaction should remove unfinished leftovers for CNDB */
    public final boolean removeTransactionLeftovers;

    /**
     * For purposes of calculating space for interim compactions in flight, whether or not this OperationType is expected
     * to write data to disk
     */
    public final boolean writesData;

    // As of now, priority takes part only for interrupting tasks to give way to operator-driven tasks.
    // Operation types that have a smaller number will be allowed to cancel ones that have larger numbers.
    //
    // Submitted tasks may be prioritised differently when forming a queue, if/when CASSANDRA-11218 is implemented.
    public final int priority;

    OperationType(String type, boolean writesData, int priority)
    {
        this(type, writesData, priority, false);
    }

    OperationType(String type, boolean writesData, int priority, boolean localOnly)
    {
        this(type, writesData, priority, localOnly, true);
    }

    OperationType(String type, boolean writesData, int priority, boolean localOnly, boolean removeTransactionLeftovers)
    {
        this.type = type;
        this.fileName = type.toLowerCase().replace(" ", "");
        this.writesData = writesData;
        this.priority = priority;
        this.localOnly = localOnly;
        this.removeTransactionLeftovers = removeTransactionLeftovers;
    }

    public static OperationType fromFileName(String fileName)
    {
        for (OperationType opType : OperationType.values())
            if (opType.fileName.equals(fileName))
                return opType;

        throw new IllegalArgumentException("Invalid fileName for operation type: " + fileName);
    }

    public boolean isCacheSave()
    {
        return this == COUNTER_CACHE_SAVE || this == KEY_CACHE_SAVE || this == ROW_CACHE_SAVE;
    }

    public String toString()
    {
        return type;
    }

    public static final Predicate<OperationType> EXCEPT_VALIDATIONS = o -> o != VALIDATION;
    public static final Predicate<OperationType> COMPACTIONS_ONLY = o -> o == COMPACTION || o == TOMBSTONE_COMPACTION;
    public static final Predicate<OperationType> REWRITES_SSTABLES = o -> o == COMPACTION || o == CLEANUP || o == SCRUB ||
                                                                          o == TOMBSTONE_COMPACTION || o == ANTICOMPACTION ||
                                                                          o == UPGRADE_SSTABLES || o == RELOCATE ||
                                                                          o == GARBAGE_COLLECT;
}
