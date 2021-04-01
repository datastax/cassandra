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

/** Implements serializable to allow structured info to be returned via JMX. */
public class CompactionLevelStats implements Serializable
{
    private static final long serialVersionUID = 3695927592357744816L;

    /** The number of sstables that are part of this level */
    private final int numSSTables;

    /** The number of sstables that are currently part of a compaction operation */
    private final int numCompactingSSTables;

    /** The score of a level is the level size in bytes of all its files dived by the ideal
     * level size if applicable, or zero for tiered strategies */
    private final double score;

    /** Total bytes read during compaction between this level and the next. This includes bytes read from this level (N) and from the next level (N+1) */
    private final long totRead;

    /** Bytes read from this level (N) during compaction between levels N and N+1*/
    private final long levelRead;

    /** Total bytes written during compaction between levels N and N+1 */
    private final long totWritten;

    /**
     * Moved(GB): Bytes moved to level N+1 during compaction. In this case there is no IO other than updating the manifest to indicate that a file which used to be in level X is now in level Y
     * Rd(MB/s): The rate at which data is read during compaction between levels N and N+1. This is (Read(GB) * 1024) / duration where duration is the time for which compactions are in progress from level N to N+1.
     * Wr(MB/s): The rate at which data is written during compaction. See Rd(MB/s).
     * Rn(cnt): Total files read from level N during compaction between levels N and N+1
     * Rnp1(cnt): Total files read from level N+1 during compaction between levels N and N+1
     * Wnp1(cnt): Total files written to level N+1 during compaction between levels N and N+1
     * Wnew(cnt): (Wnp1(cnt) - Rnp1(cnt)) -- Increase in file count as result of compaction between levels N and N+1
     * Comp(sec): Total time spent doing compactions between levels N and N+1
     * Comp(cnt): Total number of compactions between levels N and N+1
     * Avg(sec): Average time per compaction between levels N and N+1
     * Stall(sec): Total time writes were stalled because level N+1 was uncompacted (compaction score was high)
     * Stall(cnt): Total number of writes stalled because level N+1 was uncompacted
     * Avg(ms): Average time in milliseconds a write was stalled because level N+1 was uncompacted
     * KeyIn: number of records compared during compaction
     * KeyDrop: number of records dropped (not written out) during compaction
     */

    public CompactionLevelStats(int numSSTables, int numCompactingSSTables, double score, long totRead, long levelRead, long totWritten)
    {
        this.numSSTables = numSSTables;
        this.numCompactingSSTables = numCompactingSSTables;
        this.score = score;
        this.totRead = totRead;
        this.levelRead = levelRead;
        this.totWritten = totWritten;
    }

    /** The number of sstables that are part of this level */
    public int numSSTables()
    {
        return numSSTables;
    }

    /** The number of sstables that are currently part of a compaction operation */
    public int numCompactingSSTables()
    {
        return numCompactingSSTables;
    }

    /** The score of a level is the level size in bytes of all its files dived by the ideal
     * level size if applicable, or zero for tiered strategies */
    public double score()
    {
        return score;
    }

    /** Total bytes read during compaction between this level and the next. This includes bytes read from this level (N) and from the next level (N+1) */
    public long totRead()
    {
        return totRead;
    }

    /** Bytes read from this level (N) during compaction between levels N and N+1*/
    public long read()
    {
        return levelRead;
    }

    /** Bytes read from the next level (N+1) during compaction between levels N and N+1 */
    public long readNext()
    {
        return totRead - levelRead;
    }

    /** Total bytes written during compaction between levels N and N+1 */
    public long written()
    {
        return totWritten;
    }

    /** New bytes written to level N+1, calculated as (total bytes written to N+1) - (bytes read from N+1 during compaction with level N) */
    public long newlyWritten()
    {
        return totWritten - readNext();
    }

    /** W-Amp: (total bytes written to level N+1) / (total bytes read from level N). This is the write amplification from compaction between levels N and N+1 */
    public double writeAmpl()
    {
        return levelRead > 0 ? totWritten / levelRead : Double.NaN;
    }


    @Override
    public String toString()
    {
        StringBuilder ret = new StringBuilder(1024);
        toString(ret);
        return ret.toString();
    }

    void toString(StringBuilder ret)
    {
        ret.append("\tNum sstables: ").append(numSSTables()).append("\n");
        ret.append("\tCompacting: ").append(numCompactingSSTables()).append("\n");
        ret.append("\tScore: ").append(score()).append("\n");
        ret.append("\tRead tot: ").append(totRead()).append("\n");
        ret.append("\tRead this level: ").append(read()).append("\n");
        ret.append("\tRead next level: ").append(readNext()).append("\n");
        ret.append("\tWritten: ").append(written()).append("\n");
        ret.append("\tNewly written: ").append(newlyWritten()).append("\n");
        ret.append("\tWrite amplif.: ").append(writeAmpl()).append("\n");
    }
}
