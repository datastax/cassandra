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

package org.apache.cassandra.io.sstable;

import java.util.List;
import java.util.StringJoiner;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.compaction.TableOperation;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Closeable;

public interface IScrubber extends Closeable
{
    List<SSTableReader> scrub();

    void close();

    TableOperation getScrubInfo();

    @VisibleForTesting
    ScrubResult scrubWithResult();

    static Options.Builder options()
    {
        return new Options.Builder();
    }

    /**
     * Controls how the scrubber handles TTL during scrubbing.
     *
     * <ul>
     *   <li>{@link #NONE} — default behaviour; TTL values are left as-is.</li>
     *   <li>{@link #REINSERT_OVERFLOWED_TTL} — fixes cells whose local deletion time overflowed a 32-bit
     *       integer (see CASSANDRA-14092).</li>
     *   <li>{@link #NO_TTL} — strips all TTL information from every cell, useful when TTL data is
     *       corrupt or must be removed entirely.</li>
     * </ul>
     */
    enum OverwriteTTLMode
    {
        // Do nothing
        NONE,
        // Reinserts overflowed TTLs. See CASSANDRA-14092
        REINSERT_OVERFLOWED_TTL,
        // Remove any TTL from data
        NO_TTL
    }

    final class ScrubResult
    {
        public final int goodPartitions;
        public final int badPartitions;
        public final int emptyPartitions;
        public final int noTTLOverwrittenRows;
        public final List<SSTableReader> scrubbed;

        public ScrubResult(int goodPartitions, int badPartitions, int emptyPartitions, List<SSTableReader> scrubbed)
        {
            this(goodPartitions, badPartitions, emptyPartitions, 0, scrubbed);
        }

        public ScrubResult(int goodPartitions, int badPartitions, int emptyPartitions, int noTTLOverwrittenRows, List<SSTableReader> scrubbed)
        {
            this.goodPartitions = goodPartitions;
            this.badPartitions = badPartitions;
            this.emptyPartitions = emptyPartitions;
            this.noTTLOverwrittenRows = noTTLOverwrittenRows;
            this.scrubbed = scrubbed;
        }
    }

    class Options
    {
        public final boolean checkData;
        public final boolean reinsertOverflowedTTLRows;
        public final boolean skipCorrupted;
        public final boolean overrideTxnIsOffline;
        public final OverwriteTTLMode overwriteTTLMode;

        private Options(boolean checkData, boolean reinsertOverflowedTTLRows, boolean skipCorrupted, boolean overrideTxnIsOffline, OverwriteTTLMode overwriteTTLMode)
        {
            this.checkData = checkData;
            this.reinsertOverflowedTTLRows = reinsertOverflowedTTLRows;
            this.skipCorrupted = skipCorrupted;
            this.overrideTxnIsOffline = overrideTxnIsOffline;
            this.overwriteTTLMode = overwriteTTLMode;
        }

        @Override
        public String toString()
        {
            return new StringJoiner(", ", Options.class.getSimpleName() + "[", "]")
                   .add("checkData=" + checkData)
                   .add("reinsertOverflowedTTLRows=" + reinsertOverflowedTTLRows)
                   .add("skipCorrupted=" + skipCorrupted)
                   .add("overwriteTTLMode=" + overwriteTTLMode)
                   .toString();
        }

        public static class Builder
        {
            private boolean checkData = false;
            private boolean reinsertOverflowedTTLRows = false;
            private boolean skipCorrupted = false;
            private boolean overrideTxnIsOffline = false;
            private OverwriteTTLMode overwriteTTLMode = OverwriteTTLMode.NONE;

            public Builder checkData()
            {
                this.checkData = true;
                return this;
            }

            public Builder checkData(boolean checkData)
            {
                this.checkData = checkData;
                return this;
            }

            public Builder reinsertOverflowedTTLRows()
            {
                this.reinsertOverflowedTTLRows = true;
                return this;
            }

            public Builder reinsertOverflowedTTLRows(boolean reinsertOverflowedTTLRows)
            {
                this.reinsertOverflowedTTLRows = reinsertOverflowedTTLRows;
                return this;
            }

            public Builder skipCorrupted()
            {
                this.skipCorrupted = true;
                return this;
            }

            public Builder skipCorrupted(boolean skipCorrupted)
            {
                this.skipCorrupted = skipCorrupted;
                return this;
            }

            public Builder overrideTxnIsOffline()
            {
                this.overrideTxnIsOffline = true;
                return this;
            }

            public Builder overrideTxnIsOffline(boolean overrideTxnIsOffline)
            {
                this.overrideTxnIsOffline = overrideTxnIsOffline;
                return this;
            }

            public Builder overwriteTTLMode(OverwriteTTLMode overwriteTTLMode)
            {
                this.overwriteTTLMode = overwriteTTLMode;
                return this;
            }

            public Options build()
            {
                // reinsertOverflowedTTLRows takes precedence when overwriteTTLMode is NONE (for backward compat)
                OverwriteTTLMode effectiveMode = (overwriteTTLMode == OverwriteTTLMode.NONE && reinsertOverflowedTTLRows)
                                                 ? OverwriteTTLMode.REINSERT_OVERFLOWED_TTL
                                                 : overwriteTTLMode;
                return new Options(checkData, reinsertOverflowedTTLRows, skipCorrupted, overrideTxnIsOffline, effectiveMode);
            }
        }
    }
}