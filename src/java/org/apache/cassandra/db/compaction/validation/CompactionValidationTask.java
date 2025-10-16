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
package org.apache.cassandra.db.compaction.validation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

/// Validates compaction tasks to detect potential data loss during compaction operations caused by skipping some
/// subranges of source sstables, see HCD-130.
/// The validation ensures all boundary keys from input SSTables are either present in output SSTables
/// or properly obsoleted by tombstones.
public class CompactionValidationTask
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionValidationTask.class);

    public enum Mode
    {
        NONE,
        WARN,
        ABORT;

        public boolean shouldValidate()
        {
            return this != NONE;
        }

        public boolean shouldAbortOnDataLoss()
        {
            return this == ABORT;
        }

        public static Mode parseConfig()
        {
            String rawConfig = null;
            try
            {
                rawConfig = CassandraRelevantProperties.COMPACTION_VALIDATION_MODE.getString();
                return Mode.valueOf(rawConfig);
            }
            catch (IllegalArgumentException e)
            {
                logger.error("Unable to pase compaction validation config '{}', fall back to NONE", rawConfig, e);
                return NONE;
            }
        }
    }

    private final TimeUUID id;
    private final Set<SSTableReader> inputSSTables;
    private final Set<SSTableReader> outputSSTables;
    private final CompactionValidationMetrics metrics;

    private final long nowInSec;
    private final Mode mode;

    public CompactionValidationTask(TimeUUID id, Set<SSTableReader> inputSSTables, Set<SSTableReader> outputSSTables, CompactionValidationMetrics metrics)
    {
        this.id = id;
        this.inputSSTables = inputSSTables;
        this.outputSSTables = outputSSTables;
        this.nowInSec = FBUtilities.nowInSeconds();
        this.metrics = metrics;
        this.mode = Mode.parseConfig();
    }

    public void validate()
    {
        if (!mode.shouldValidate())
            return;

        try
        {
            doValidate();
        }
        catch (DataLossException e)
        {
            // abort compaction task
            throw e;
        }
        catch (Throwable t)
        {
            logger.error("Caught unexpected error on validation task for {}: {}", id, t.getMessage(), t);
        }
    }

    private void doValidate()
    {
        logger.info("Starting compaction validation for task {}", id);
        long startedNanos = Clock.Global.nanoTime();
        metrics.incrementValidation();

        Set<DecoratedKey> absentKeys = new HashSet<>();
        for (SSTableReader inputSSTable : inputSSTables)
        {
            DecoratedKey firstKey = inputSSTable.first;
            DecoratedKey lastKey = inputSSTable.last;

            if (isKeyAbsentInOutputSSTables(firstKey))
            {
                if (logger.isTraceEnabled())
                    logger.trace("[Task {}] First key {} from input sstable {} not found in update sstables",
                            id, firstKey, inputSSTable.descriptor);

                absentKeys.add(firstKey);
            }
            
            if (isKeyAbsentInOutputSSTables(lastKey))
            {
                if (logger.isTraceEnabled())
                    logger.trace("[Task {}] Last key {} from input sstable {} not found in update sstables",
                            id, lastKey, inputSSTable.descriptor);

                absentKeys.add(lastKey);
            }
        }

        if (absentKeys.isEmpty())
        {
            metrics.incrementValidationWithoutAbsentKeys();
            logger.info("[Task {}] Compaction validation passed: all first/last keys found in update sstables, took {}ms",
                        id, TimeUnit.NANOSECONDS.toMillis(Clock.Global.nanoTime() - startedNanos));
            return;
        }

        metrics.incrementAbsentKeys(absentKeys.size());
        if (validateAbsentKeysAgainstTombstones(absentKeys))
            logger.info("[Task {}] Compaction validation passed: all absent keys are properly obsoleted due to tombstones, took {} ms",
                        id, TimeUnit.NANOSECONDS.toMillis(Clock.Global.nanoTime() - startedNanos));
    }

    private boolean isKeyAbsentInOutputSSTables(DecoratedKey key)
    {
        for (SSTableReader outputSSTable : outputSSTables)
        {
            if (outputSSTable.first.compareTo(key) <= 0 && outputSSTable.last.compareTo(key) >= 0)
            {
                if (outputSSTable.getPosition(key, SSTableReader.Operator.EQ) >= 0)
                {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean validateAbsentKeysAgainstTombstones(Set<DecoratedKey> absentKeys)
    {
        logger.info("[Task {}] Validating {} absent keys against tombstones from input sstables", id, absentKeys.size());

        for (DecoratedKey absentKey : absentKeys)
        {
            if (!isFullyExpired(absentKey))
            {
                metrics.incrementPotentialDataLosses();
                String errorMsg = String.format(
                    "POTENTIAL DATA LOSS on compaction task %s: Key %s from input sstables not found in update sstables " +
                    "and the partition is not fully expired.", id, absentKey);
                logger.error(errorMsg);
                if (mode.shouldAbortOnDataLoss())
                    throw new DataLossException(errorMsg);

                return false;
            }
        }
        return true;
    }

    private boolean isFullyExpired(DecoratedKey key)
    {
        List<UnfilteredRowIterator> iterators = new ArrayList<>();
        for (SSTableReader sstable : inputSSTables)
        {
            if (sstable.mayContainAssumingKeyIsInRange(key))
                iterators.add(readPartition(key, sstable));
        }

        // merge all input iterators
        try (UnfilteredRowIterator merged = UnfilteredRowIterators.merge(iterators))
        {
            // apply purging function to get rid of all tombstones
            RowIterator purged = UnfilteredRowIterators.filter(merged, nowInSec);
            // if there are non-purgeable content, e.g. live rows or unexpired tombstones, they should appear in output sstables
            if (purged.staticRow() != null && !purged.staticRow().isEmpty())
                return false;
            if (purged.hasNext())
                return false;
        }

        return true;
    }

    private UnfilteredRowIterator readPartition(DecoratedKey partitionKey, SSTableReader sstable)
    {
        return sstable.rowIterator(partitionKey, Slices.ALL, ColumnFilter.all(sstable.metadata()), false, SSTableReadsListener.NOOP_LISTENER);
    }

    public static class DataLossException extends RuntimeException
    {
        public DataLossException(String errorMsg)
        {
            super(errorMsg);
        }
    }
}
