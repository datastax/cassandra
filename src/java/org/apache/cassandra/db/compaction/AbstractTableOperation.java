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

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;

/**
 * This is a base abstract class for a table operation. It must be able to report the operation progress and to
 * optionally interrupt the operation when requested.
 * <p/>
 * Any operation defined by {@link OperationType} is valid, for example index building, view building, cache saving,
 * anti-compaction, compaction, scrubbing, verifying, tombstone collection and others.
 * <p/>
 * Basically any operation that can either read or write one or more files and that needs to reach a total number
 * of "items" to be processed. "items" are normally bytes but they could be ranges or keys as defined by {@link Unit}.
 * <p/>
 * This class implements serializable to allow structured info to be returned via JMX.
 * */
public abstract class AbstractTableOperation
{
    private volatile boolean stopRequested = false;
    private volatile StopTrigger trigger = StopTrigger.NONE;

    /**
     * @return the progress of the operation, see {@link Progress}.
     */
    public abstract Progress getProgress();

    /**
     * Interrupt the current operation if possible.
     */
    public void stop()
    {
        stopRequested = true;
    }

    /**
     * Interrupt the current operation if possible and if the predicate is true.
     *
     * @param trigger cause of compaction interruption
     */
    public void stop(StopTrigger trigger)
    {
        this.stopRequested = true;
        if (!this.trigger.isFinal())
            this.trigger = trigger;
    }

    /**
     * if this compaction involves several/all tables we can safely check globalCompactionsPaused
     * in isStopRequested() below
     */
    public abstract boolean isGlobal();

    /**
     * @return true if the operation has received a request to be interrupted.
     */
    public boolean isStopRequested()
    {
        return stopRequested || (isGlobal() && CompactionManager.instance.isGlobalCompactionPaused());
    }

    boolean shouldStop(Predicate<SSTableReader> sstablePredicate)
    {
        Progress progress = getProgress();
        if (progress.sstables.isEmpty())
        {
            return true;
        }
        return progress.sstables.stream().anyMatch(sstablePredicate);
    }

    /**
     * @return cause of compaction interruption.
     */
    public StopTrigger trigger()
    {
        return trigger;
    }

    public enum StopTrigger
    {
        NONE(false),
        TRUNCATE(true);

        private final boolean isFinal;

        StopTrigger(boolean isFinal)
        {
            this.isFinal = isFinal;
        }

        // A stop trigger marked as final should not be overwritten. So a table operation that is
        // marked with a final stop trigger cannot have it's stop trigger changed to another value.
        public boolean isFinal()
        {
            return isFinal;
        }
    }

    /**
     * The unit for the {@link Progress} report.
     */
    public enum Unit
    {
        BYTES("bytes"), RANGES("token range parts"), KEYS("keys");

        private final String name;

        Unit(String name)
        {
            this.name = name;
        }

        @Override
        public String toString()
        {
            return this.name;
        }

        public static boolean isFileSize(String unit)
        {
            return BYTES.toString().equals(unit);
        }
    }

    /**
     * The progress information for an operation, refer to the description of the class properties.
     */
    public static final class Progress implements Serializable
    {
        private static final long serialVersionUID = 3695381572726744816L;

        public static final String ID = "id";
        public static final String KEYSPACE = "keyspace";
        public static final String COLUMNFAMILY = "columnfamily";
        public static final String COMPLETED = "completed";
        public static final String TOTAL = "total";
        public static final String OPERATION_TYPE = "operationType";
        public static final String UNIT = "unit";
        public static final String OPERATION_ID = "operationId";

        /**
         * The table metadata
         */
        private final TableMetadata metadata;
        /**
         * The type of operation
         */
        private final OperationType operationType;
        /**
         * Normally the bytes processed so far by this operation, but depending on the unit it could mean something else, e.g. ranges or keys.
         */
        private final long completed;
        /**
         * The total bytes that need to be processed, for example the size of the input files. Depending on the unit it could mean something else, e.g. ranges or keys.
         */
        private final long total;
        /**
         * The unit for {@link this#completed} and for {@link this#total}.
         */
        private final Unit unit;
        /**
         * A unique ID for this operation
         */
        private final UUID operationId;

        /**
         * A set of SSTables participating in this operation
         */
        private final ImmutableSet<SSTableReader> sstables;

        public Progress(TableMetadata metadata, OperationType operationType, long bytesComplete, long totalBytes, UUID operationId, Collection<? extends SSTableReader> sstables)
        {
            this(metadata, operationType, bytesComplete, totalBytes, Unit.BYTES, operationId, sstables);
        }

        public Progress(TableMetadata metadata, OperationType operationType, long completed, long total, Unit unit, UUID operationId,  Collection<? extends SSTableReader> sstables)
        {
            this.operationType = operationType;
            this.completed = completed;
            this.total = total;
            this.metadata = metadata;
            this.unit = unit;
            this.operationId = operationId;
            this.sstables = ImmutableSet.copyOf(sstables);
        }

        /**
         * @return A copy of this Progress with updated progress.
         */
        public Progress forProgress(long complete, long total)
        {
            return new Progress(metadata, operationType, complete, total, unit, operationId, sstables);
        }

        public static Progress withoutSSTables(TableMetadata metadata, OperationType tasktype, long completed, long total, AbstractTableOperation.Unit unit, UUID compactionId)
        {
            return new Progress(metadata, tasktype, completed, total, unit, compactionId, ImmutableSet.of());
        }

        public Optional<String> getKeyspace()
        {
            return metadata != null ? Optional.of(metadata.keyspace) : Optional.empty();
        }

        public Optional<String> getTable()
        {
            return metadata != null ? Optional.of(metadata.name) : Optional.empty();
        }

        public TableMetadata getTableMetadata()
        {
            return metadata;
        }

        public long getCompleted()
        {
            return completed;
        }

        public long getTotal()
        {
            return total;
        }

        public OperationType getOperationType()
        {
            return operationType;
        }

        public UUID getOperationId()
        {
            return operationId;
        }

        public Unit getUnit()
        {
            return unit;
        }

        public Set<SSTableReader> getSSTables()
        {
            return sstables;
        }

        public String toString()
        {
            StringBuilder buff = new StringBuilder();
            buff.append(getOperationType());
            if (metadata != null)
            {
                buff.append('@').append(metadata.id).append('(');
                buff.append(metadata.keyspace).append(", ").append(metadata.name).append(", ");
            }
            else
            {
                buff.append('(');
            }
            buff.append(getCompleted()).append('/').append(getTotal());
            return buff.append(')').append(unit).toString();
        }

        public Map<String, String> asMap()
        {
            Map<String, String> ret = new HashMap<>(8);
            ret.put(ID, metadata != null ? metadata.id.toString() : "");
            ret.put(KEYSPACE, getKeyspace().orElse(null));
            ret.put(COLUMNFAMILY, getTable().orElse(null));
            ret.put(COMPLETED, Long.toString(completed));
            ret.put(TOTAL, Long.toString(total));
            ret.put(OPERATION_TYPE, operationType.toString());
            ret.put(UNIT, unit.toString());
            ret.put(OPERATION_ID, operationId == null ? "" : operationId.toString());
            return ret;
        }
    }
}