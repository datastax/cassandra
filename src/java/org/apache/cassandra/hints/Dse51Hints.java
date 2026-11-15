/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.hints;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.net.DseCompatRefusals;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.StorageCompatibilityMode;

/**
 * The hints of {@code storage_compatibility_mode: DSE_5_1}, which writes every hints file at hints version 1, its
 * mutations at messaging version 11 ({@link HintsDescriptor#currentWriteMessagingVersion()}), the files DSE 5.1 reads,
 * so that a node rolled back to DSE 5.1 delivers the hints it stored in the mode. Before a hint is sized and written
 * into the hints buffer, {@link #refuse} checks what the pre-4.0 codec would refuse or the buffer would not take at
 * 11, so that the hint is refused, counted and logged, rather than failing inside the buffer, uncounted:
 * <ul>
 *     <li>a local deletion or expiration time past 2038 in any part of the mutation (a partition deletion, a row's
 *     liveness or deletion, a cell, a complex deletion, a range tombstone marker), which the 32-bit times of the
 *     pre-4.0 layout cannot hold and DSE 5.1 would read wrapped, counted under {@link #REFUSED_HINT};</li>
 *     <li>a hint larger at 11 than the largest the hints buffer takes, counted under {@link #REFUSED_LARGE_HINT}.</li>
 * </ul>
 * Such a hint is not stored for any of its targets, counted by {@link DseCompatRefusals} and logged at WARN at most
 * once a minute for each of the two reasons, with the advice to repair its tables. In every other storage mode nothing
 * is checked.
 */
public final class Dse51Hints
{
    private static final Logger logger = LoggerFactory.getLogger(Dse51Hints.class);

    /** The reason of {@link DseCompatRefusals} a hint with a time past 2038 is counted under. */
    public static final String REFUSED_HINT = "hint DSE 5.1 cannot read";
    /** The reason of {@link DseCompatRefusals} a hint larger than the hints buffer takes is counted under. */
    public static final String REFUSED_LARGE_HINT = "hint larger than the hints buffer takes";

    private static final String WARNING = "DSE 5.1 compatibility mode: a hint for {} of {} was not stored: {}; the " +
                                          "write reaches those nodes only by a repair: repair {} once the cluster " +
                                          "runs one release";

    private Dse51Hints()
    {
    }

    /** Why a hint is refused: the reason it is counted under and the text the WARN gives. */
    static final class Refusal
    {
        final String counted;
        final String reason;

        private Refusal(String counted, String reason)
        {
            this.counted = counted;
            this.reason = reason;
        }
    }

    /** Whether the hints the node writes are those of the DSE 5.1 mode: {@code storage_compatibility_mode: DSE_5_1}. */
    private static boolean isActive()
    {
        return StorageCompatibilityMode.current() == StorageCompatibilityMode.DSE_5_1;
    }

    /**
     * In {@code storage_compatibility_mode: DSE_5_1}, refuses a hint DSE 5.1 could not read, or the hints buffer
     * would not take, at the messaging version the mode writes hints at: counts it and logs it. Checks nothing in the
     * other storage modes.
     *
     * @param hostIds the targets of the hint, named by the log
     * @param hint the hint to check
     * @param maxHintSize the largest hint, serialized, the hints buffer takes
     * @return whether the hint is refused
     */
    static boolean refuse(Collection<UUID> hostIds, Hint hint, int maxHintSize)
    {
        if (!isActive())
            return false;
        Refusal refusal = refusal(hint, maxHintSize);
        if (refusal == null)
            return false;
        DseCompatRefusals.refused(refusal.counted);
        String tables = tables(hint.mutation());
        // one statement per reason, so that one reason does not silence the other
        NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, refusal.counted, 1, TimeUnit.MINUTES, WARNING, hostIds,
                         tables, refusal.reason, tables);
        return true;
    }

    /**
     * Why the pre-4.0 codec or the hints buffer would refuse the hint at messaging version 11, null when neither
     * would: a deletion or expiration time past 2038, or a size above {@code maxHintSize}.
     */
    @Nullable
    static Refusal refusal(Hint hint, int maxHintSize)
    {
        int version = HintsDescriptor.currentWriteMessagingVersion();
        String deletionTime = pre40DeletionTimeRefusal(hint.mutation(), version);
        if (deletionTime != null)
            return new Refusal(REFUSED_HINT, deletionTime);
        long size = Hint.serializer.serializedSize(hint, version);
        if (size > maxHintSize)
            return new Refusal(REFUSED_LARGE_HINT,
                               "its size at messaging version " + version + ", the version of the hints files DSE " +
                               "5.1 reads, " + size + " bytes, is above the largest hint the hints buffer takes, " +
                               maxHintSize + " bytes");
        return null;
    }

    /**
     * The refusal of the pre-4.0 codec at {@code version}, made without writing the mutation: the deletion times of
     * each partition update, its static row, its rows and its range tombstone markers, as
     * {@code UnfilteredRowIteratorSerializer} and {@link UnfilteredSerializer} check them while they write; null when
     * there is none. The cached bytes of the mutation do not serve: only small mutations have them, and the codec
     * checks the times only while it writes.
     */
    @Nullable
    private static String pre40DeletionTimeRefusal(Mutation mutation, int version)
    {
        for (PartitionUpdate update : mutation.getPartitionUpdates())
        {
            try
            {
                DeletionTime partitionDeletion = update.partitionLevelDeletion();
                if (!partitionDeletion.isLive())
                    UnfilteredSerializer.checkPre40DeletionTime(partitionDeletion.localDeletionTime(), version);
                Row staticRow = update.staticRow();
                if (!staticRow.isEmpty())
                    UnfilteredSerializer.checkPre40DeletionTimes(staticRow, version);
                try (UnfilteredRowIterator unfiltereds = update.unfilteredIterator())
                {
                    while (unfiltereds.hasNext())
                        UnfilteredSerializer.checkPre40DeletionTimes(unfiltereds.next(), version);
                }
            }
            catch (UnfilteredSerializer.Pre40DeletionTimeException e)
            {
                return "a local deletion or expiration time, " + e.localDeletionTime + ", is past 2038, and the " +
                       "hints files DSE 5.1 reads hold 32-bit times (messaging version " + version + ')';
            }
        }
        return null;
    }

    private static String tables(Mutation mutation)
    {
        return mutation.getPartitionUpdates()
                       .stream()
                       .map(update -> update.metadata().toString())
                       .distinct()
                       .sorted()
                       .collect(Collectors.joining(", "));
    }
}
