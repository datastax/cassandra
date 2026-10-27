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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.compaction.CompactionRealm;
import org.apache.cassandra.db.compaction.writers.SSTableDataSink;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Transactional;

/**
 * Wraps one or more writers as output for rewriting one or more readers: every sstable_preemptive_open_interval
 * we look in the summary we're collecting for the latest writer for the penultimate key that we know to have been fully
 * flushed to the index file, and then double check that the key is fully present in the flushed data file.
 * Then we move the starts of each reader forwards to that point, replace them in the Tracker, and attach a runnable
 * for on-close (i.e. when all references expire) that drops the page cache prior to that key position
 *
 * hard-links are created for each partially written sstable so that readers opened against them continue to work past
 * renaming of the temporary file, which is deleted once all readers against the hard-link have been closed.
 * If for any reason the writer is rolled over, we immediately rename and fully expose the completed file in the Tracker.
 *
 * On abort, we restore the original lower bounds to the existing readers and delete any temporary files we had in progress,
 * but leave any hard-links in place for the readers we opened, and clean-up when the readers finish as we would do
 * if we had finished successfully.
 */
public class SSTableRewriter extends Transactional.AbstractTransactional implements Transactional, SSTableDataSink
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableRewriter.class);

    @VisibleForTesting
    public static boolean disableEarlyOpeningForTests = false;

    private final long preemptiveOpenInterval;
    private final long maxAge;
    private long repairedAt = -1;
    // the set of final readers we will expose on commit
    private final ILifecycleTransaction transaction; // the readers we are rewriting (updated as they are replaced)
    private final List<SSTableReader> preparedForCommit = new ArrayList<>();

    private long currentlyOpenedEarlyAt; // the position (in MiB) in the target file we last (re)opened at
    private long bytesWritten; // the bytes written by previous writers, or zero if the current writer is the first writer

    private final List<SSTableWriter> writers = new ArrayList<>();
    private final boolean keepOriginals; // true if we do not want to obsolete the originals
    private final boolean eagerWriterMetaRelease; // true if the writer metadata should be released when switch is called

    private SSTableWriter writer;

    // for testing (TODO: remove when have byteman setup)
    private boolean throwEarly, throwLate;

    /** @deprecated See CASSANDRA-11148 */
    @Deprecated(since = "3.4")
    public SSTableRewriter(ILifecycleTransaction transaction, long maxAge, long preemptiveOpenInterval, boolean keepOriginals)
    {
        this(transaction, maxAge, preemptiveOpenInterval, keepOriginals, false);
    }

    SSTableRewriter(ILifecycleTransaction transaction, long maxAge, long preemptiveOpenInterval, boolean keepOriginals, boolean eagerWriterMetaRelease)
    {
        this.transaction = transaction;
        this.maxAge = maxAge;
        this.preemptiveOpenInterval = preemptiveOpenInterval;
        this.keepOriginals = keepOriginals;
        this.eagerWriterMetaRelease = eagerWriterMetaRelease;
    }

    public static SSTableRewriter constructKeepingOriginals(ILifecycleTransaction transaction, boolean keepOriginals, long maxAge)
    {
        return new SSTableRewriter(transaction, maxAge, calculateOpenInterval(true), keepOriginals, true);
    }

    public static SSTableRewriter constructWithoutEarlyOpening(ILifecycleTransaction transaction, boolean keepOriginals, long maxAge)
    {
        return new SSTableRewriter(transaction, maxAge, calculateOpenInterval(false), keepOriginals, true);
    }

    public static SSTableRewriter construct(CompactionRealm realm, ILifecycleTransaction transaction, boolean keepOriginals, long maxAge)
    {
        return new SSTableRewriter(transaction, maxAge, calculateOpenInterval(realm.supportsEarlyOpen()), keepOriginals, true);
    }

    public static SSTableRewriter construct(CompactionRealm realm, ILifecycleTransaction transaction, boolean keepOriginals, long maxAge, boolean earlyOpenAllowed)
    {
        return new SSTableRewriter(transaction, maxAge, calculateOpenInterval(earlyOpenAllowed && realm.supportsEarlyOpen()), keepOriginals, true);
    }

    private static long calculateOpenInterval(boolean shouldOpenEarly)
    {
        long interval = DatabaseDescriptor.getSSTablePreemptiveOpenIntervalInMiB() * (1L << 20);
        if (disableEarlyOpeningForTests || !shouldOpenEarly || interval < 0)
            interval = Long.MAX_VALUE;
        return interval;
    }

    public SSTableWriter currentWriter()
    {
        return writer;
    }

    public long bytesWritten()
    {
        return bytesWritten + (writer == null ? 0 : writer.getFilePointer());
    }

    public void forEachWriter(Consumer<SSTableWriter> op)
    {
        for (SSTableWriter writer : writers)
            op.accept(writer);
        if (writer != null)
            op.accept(writer);
    }

    @Override
    public AbstractRowIndexEntry append(UnfilteredRowIterator partition)
    {
        // we do this before appending to ensure we can resetAndTruncate() safely if appending fails
        DecoratedKey key = partition.partitionKey();
        maybeReopenEarly(key);
        return writer.append(partition);
    }

    // attempts to append the row, if fails resets the writer position
    public AbstractRowIndexEntry tryAppend(UnfilteredRowIterator partition)
    {
        writer.mark();
        try
        {
            return append(partition);
        }
        catch (Throwable t)
        {
            writer.resetAndTruncate();
            throw t;
        }
    }

    @Override
    public boolean startPartition(DecoratedKey key, DeletionTime deletionTime) throws IOException
    {
        maybeReopenEarly(key);
        return writer.startPartition(key, deletionTime);
    }

    @Override
    public void addUnfiltered(Unfiltered unfiltered) throws IOException
    {
        writer.addUnfiltered(unfiltered);
    }

    @Override
    public AbstractRowIndexEntry endPartition() throws IOException
    {
        return writer.endPartition();
    }

    private void maybeReopenEarly(DecoratedKey key)
    {
        if (writer.getFilePointer() - currentlyOpenedEarlyAt > preemptiveOpenInterval)
        {
            if (transaction.isOffline())
            {
                for (SSTableReader reader : transaction.originals())
                {
                    reader.trySkipFileCacheBefore(key);
                }
            }
            else
            {
                writer.setMaxDataAge(maxAge);
                writer.openEarly(reader -> {
                    // Advance the boundary before anything that can fail, so that a failed early open is retried only
                    // after another interval's worth of data and not at every following partition.
                    currentlyOpenedEarlyAt = writer.getFilePointer();
                    boolean staged = false;
                    try
                    {
                        transaction.update(reader, false);
                        staged = true;
                        moveStarts(reader.getLast());
                    }
                    catch (Throwable ex)
                    {
                        abortEarlyOpen(reader, staged, ex);
                        return;
                    }
                    transaction.checkpoint();
                });
            }
        }
    }

    /**
     * A failed periodic early open is not fatal (unless {@link JVMStabilityInspector} rethrows the error): the readers
     * staged since the last checkpoint ({@code reader}, and the clones with moved starts) are released and the tracker
     * keeps the previous view, which is complete. The final early open in switchWriter is different, see there.
     */
    private void abortEarlyOpen(SSTableReader reader, boolean staged, Throwable ex)
    {
        ex = transaction.abortCheckpoint(ex);
        // update() stages nothing when it throws; the reader is then still ours to release
        if (!staged)
            ex = reader.selfRef().ensureReleased(ex);
        logger.debug("Aborted early opening attempt of {} due to error", reader.descriptor, ex);
        NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.MINUTES,
                         "Aborted early opening attempt of {} due to error", reader.descriptor, ex);
        // Last, as it rethrows some errors (e.g. OutOfMemoryError, or an interruption found in the
        // cause chain); it also applies the disk failure policy to FSError and CorruptSSTableException.
        JVMStabilityInspector.inspectThrowable(ex);
    }

    protected Throwable doAbort(Throwable accumulate)
    {
        // abort the writers
        for (SSTableWriter writer : writers)
            accumulate = writer.abort(accumulate);
        // abort the lifecycle transaction
        accumulate = transaction.abort(accumulate);
        return accumulate;
    }

    protected Throwable doCommit(Throwable accumulate)
    {
        for (SSTableWriter writer : writers)
            accumulate = writer.commit(accumulate);

        accumulate = transaction.commit(accumulate);
        return accumulate;
    }

    /**
     * Replace the readers we are rewriting with cloneWithNewStart, reclaiming any page cache that is no longer
     * needed, and transferring any key cache entries over to the new reader, expiring them from the old. if reset
     * is true, we are instead restoring the starts of the readers from before the rewriting began
     *
     * note that we replace an existing sstable with a new *instance* of the same sstable, the replacement
     * sstable .equals() the old one, BUT, it is a new instance, so, for example, since we releaseReference() on the old
     * one, the old *instance* will have reference count == 0 and if we were to start a new compaction with that old
     * instance, we would get exceptions.
     *
     * @param lowerbound if !reset, must be non-null, and marks the exclusive lowerbound of the start for each sstable
     */
    private void moveStarts(DecoratedKey lowerbound)
    {
        if (transaction.isOffline() || preemptiveOpenInterval == Long.MAX_VALUE)
            return;

        for (SSTableReader sstable : transaction.originals())
        {
            // we call getCurrentReplacement() to support multiple rewriters operating over the same source readers at once.
            // note: only one such writer should be written to at any moment
            final SSTableReader latest = transaction.current(sstable);

            // skip any sstables that we know to already be shadowed
            if (latest.getFirst().compareTo(lowerbound) > 0)
                continue;

            if (lowerbound.compareTo(latest.getLast()) >= 0)
            {
                if (!transaction.isObsolete(latest))
                    transaction.obsolete(latest);
                continue;
            }

            if (!transaction.isObsolete(latest))
            {
                DecoratedKey newStart = latest.firstKeyBeyond(lowerbound);
                assert newStart != null;
                SSTableReader replacement = latest.cloneWithNewStart(newStart);
                try
                {
                    transaction.update(replacement, true);
                }
                catch (Throwable t)
                {
                    // update() stages nothing when it throws: nobody else will release the clone
                    throw Throwables.throwAsUncheckedException(replacement.selfRef().ensureReleased(t));
                }
            }
        }
    }

    public void switchWriter(SSTableWriter newWriter)
    {
        if (newWriter != null)
        {
            newWriter.setMaxDataAge(maxAge);
            writers.add(newWriter);
        }

        if (eagerWriterMetaRelease && writer != null)
            writer.releaseMetadataOverhead();

        if (writer == null || writer.getFilePointer() == 0)
        {
            if (writer != null)
            {
                writer.abort();

                transaction.untrackNew(writer);
                writers.remove(writer);
            }
            writer = newWriter;

            return;
        }

        // Open fully completed sstables early. This is also required for the final sstable in a set (where newWriter
        // is null) to permit the compilation of a canonical set of sstables (see View.select).
        if (preemptiveOpenInterval != Long.MAX_VALUE)
        {
            // Unlike the periodic early open in maybeReopenEarly, a failure here is fatal for the operation: the
            // next writer's early opens will move the starts of the originals past the end of this sstable, so its
            // data from the last periodic boundary on would not be visible in any live reader until the operation
            // completes. The exception aborts the rewriter, whose transaction abort releases whatever was staged.

            // we leave it as a tmp file, but we open it and add it to the Tracker
            writer.setMaxDataAge(maxAge);
            SSTableReader reader = writer.openFinalEarly();
            try
            {
                transaction.update(reader, false);
            }
            catch (Throwable t)
            {
                // update() stages nothing when it throws, so the transaction abort would not release the reader
                throw Throwables.throwAsUncheckedException(reader.selfRef().ensureReleased(t));
            }
            moveStarts(reader.getLast());
            transaction.checkpoint();
        }

        currentlyOpenedEarlyAt = 0;
        bytesWritten += writer.getFilePointer();
        writer.onSSTableWriterSwitched();
        writer = newWriter;
    }

    /**
     * @param repairedAt the repair time, -1 if we should use the time we supplied when we created
     *                   the SSTableWriter (and called rewriter.switchWriter(..)), actual time if we want to override the
     *                   repair time.
     */
    public SSTableRewriter setRepairedAt(long repairedAt)
    {
        this.repairedAt = repairedAt;
        return this;
    }

    /**
     * Finishes the new file(s)
     *
     * Creates final files, adds the new files to the Tracker (via replaceReader).
     *
     * We add them to the tracker to be able to get rid of the tmpfiles
     *
     * It is up to the caller to do the compacted sstables replacement
     * gymnastics (ie, call Tracker#markCompactedSSTablesReplaced(..))
     *
     *
     */
    public List<SSTableReader> finish()
    {
        super.finish();
        return finished();
    }

    // returns, in list form, the
    public List<SSTableReader> finished()
    {
        assert state() == State.COMMITTED || state() == State.READY_TO_COMMIT;
        return preparedForCommit;
    }

    protected void doPrepare()
    {
        switchWriter(null);

        if (throwEarly)
            throw new RuntimeException("exception thrown early in finish, for testing");

        // No early open to finalize and replace
        for (SSTableWriter writer : writers)
        {
            assert writer.getFilePointer() > 0;
            writer.setRepairedAt(repairedAt);
            writer.prepareToCommit();
            writer.openResult(null);
            SSTableReader reader = writer.finished();
            transaction.update(reader, false);
            preparedForCommit.add(reader);
        }
        // staged sstables will be made visible in Tracker
        transaction.checkpoint();

        if (throwLate)
            throw new RuntimeException("exception thrown after all sstables finished, for testing");

        if (!keepOriginals)
            transaction.obsoleteOriginals();

        transaction.prepareToCommit();
    }

    public void throwDuringPrepare(boolean earlyException)
    {
        if (earlyException)
            throwEarly = true;
        else
            throwLate = true;
    }
}
