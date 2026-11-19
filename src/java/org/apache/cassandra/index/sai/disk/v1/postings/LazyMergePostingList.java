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

package org.apache.cassandra.index.sai.disk.v1.postings;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;

import org.agrona.collections.LongHashSet;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.TopKSelector;

import static com.google.common.base.Preconditions.checkState;

/**
 * A {@link PostingList} that lazily merges many sorted posting lists into one sorted posting list. The merge is a
 * union: it returns each posting once. The class keeps only a bounded number of the posting lists open at any time.
 * <p>
 * <b>Why use it.</b> Some queries need the union of a very large number of posting lists. For example, a range
 * query over a literal index has one posting list per term in the range. The range can have millions of terms.
 * {@link MergePostingList} merges eagerly. It opens every list at the start, so memory grows with the number of
 * lists. This is also true when the consumer needs only the first few postings, for example because of a small
 * {@code LIMIT}. This class makes memory proportional to a configured capacity. It opens more lists only if the
 * consumer reads far enough to need them. A query that stops early pays only for the lists that it touched.
 * <p>
 * <b>How it works.</b> The class walks the posting lists of the {@link Source} once. It keeps open only the
 * {@code capacity} lists whose first postings are the smallest, in a {@link CandidateQueue}. The smallest first
 * posting among the lists that the class left out is the {@code bound}. All postings that are less than the
 * {@code bound} are in the kept lists. Thus, the merge of the kept lists is exact up to the {@code bound} (exclusive).
 * The class serves reads from the kept lists, one posting at a time. When the next posting is at or above the
 * {@code bound}, the class walks the source again from the {@code bound}. The new walk has a doubled capacity. The
 * lists that are open stay open as they are. The class does not read them again from their beginning.
 * <p>
 * <b>Tuning.</b> The capacity starts at {@code initCapacity}. It never exceeds {@code maxCapacity}, which is the
 * largest number of posting lists that are open at any time. These two values control the tradeoff between memory and
 * speed. A bigger capacity keeps more lists open and needs fewer walks of the source. A good {@code initCapacity} is
 * large enough for the consumer to finish in a single walk. For example, derive it from the query limit. Each walk
 * takes time proportional to the number of posting lists in the source. Doubling the capacity keeps the number of
 * walks logarithmic.
 */
@NotThreadSafe
public final class LazyMergePostingList implements PostingList
{
    /** The source that supplies the posting list IDs and opens the individual posting lists. */
    private final Source source;
    /** The maximum number of posting lists that are open at any time. */
    private final int maxCapacity;
    /** The total number of postings in all matching posting lists, including duplicate postings. */
    private int size;
    /** The maximum number of posting lists that the current queue keeps open. */
    private int capacity;
    /** The priority queue that manages the open posting lists. It is never null. */
    private CandidateQueue queue;
    /**
     * The ids of the posting lists that the queue holds open. {@link #offer} adds the id of every posting list that
     * it puts in the queue. It removes the id of every posting list that it closes. Thus, the set is always the same
     * as the set of the posting lists in the queue, and its size is not more than the capacity. {@link #grow()} does
     * not change it, because it keeps the posting lists.
     */
    private final LongHashSet openPostingListIds = new LongHashSet();
    /**
     * The smallest posting that the queue does not know about (exclusive upper bound). It is 0 before the initial
     * scan, and {@link Long#MAX_VALUE} if the queue left out no posting list.
     */
    private long bound;

    /**
     * Creates a posting list that lazily merges the posting lists of the source. It takes ownership of the source.
     * {@link #close()} closes the source.
     *
     * @param initCapacity the maximum number of posting lists that the first walk keeps open. It is a lower bound for
     *                     the size of the queue. The constructor clamps it to between 1 and {@code maxCapacity}.
     * @param maxCapacity  the largest number of open posting lists. It is an upper bound for the size of the queue.
     *                     A value less than 1 counts as 1.
     */
    public LazyMergePostingList(Source source,
                                int initCapacity,
                                int maxCapacity) throws IOException
    {
        this.source = source;
        this.maxCapacity = Math.max(1, maxCapacity);
        this.capacity = Math.max(1, Math.min(initCapacity, maxCapacity));
        this.queue = new CandidateQueue(capacity);
        this.bound = 0;
        refill(0);
    }

    @Override
    public int nextPosting() throws IOException
    {
        if (queue.head() >= bound)
        {
            grow();
            refill((int) bound);
        }
        return queue.nextPosting();
    }

    @Override
    public int advance(int targetPosting) throws IOException
    {
        queue.skipTo(targetPosting);
        if (targetPosting >= bound)
        {
            grow();
            refill(targetPosting);
        }
        return nextPosting();
    }

    /**
     * @return the total number of postings in the lists of the range. A posting that is in several lists counts
     * several times, as in {@link MergePostingList}.
     */
    @Override
    public int size()
    {
        return size;
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(queue.candidates());
        FileUtils.closeQuietly(source);
    }

    /**
     * Prepares the queue for {@link #refill(int)}. It doubles the capacity of the queue, up to {@code maxCapacity}.
     * If the capacity is already {@code maxCapacity}, the method keeps the queue and only turns it back to
     * selection mode.
     */
    private void grow() throws IOException
    {
        int newCapacity = (int) Math.min(2L * capacity, maxCapacity);
        if (newCapacity == capacity)
            queue.startSelecting();
        else
            resize(newCapacity);
    }

    /**
     * Replaces the queue with a new queue of the given capacity. The new queue is in selection mode. It takes over
     * the open posting lists of the old queue as they are. The method does not open them again. The new capacity
     * must not be less than the old capacity, so that the new queue does not displace any posting list.
     * If the new capacity is not lower than the old capacity, the set of open posting list ids stays the same.
     */
    private void resize(int newCapacity) throws IOException
    {
        // A view over the heap array of the old queue. The old queue is dropped below, so nothing modifies the array.
        List<Candidate> retained = queue.candidates();
        queue = new CandidateQueue(newCapacity);
        capacity = newCapacity;

        int i = 0;
        try
        {
            for (; i < retained.size(); i++)
                offer(retained.get(i));
        }
        catch (Exception e)
        {
            FileUtils.closeQuietly(queue.candidates());
            // The candidate at index i was already handed to offer; the ones after it were not.
            FileUtils.closeQuietly(retained.subList(i, retained.size()));
            FileUtils.closeQuietly(source);
            throw e;
        }
    }

    /**
     * Refills the queue, which must be in selection mode (see {@link #grow()}). The queue then holds the
     * posting lists of the range that have the smallest next postings at or after {@code from}. The posting lists
     * that are already in the queue take part in the selection as they are. The method does not open them again.
     * The method switches the queue to serving mode.
     */
    private void refill(int from) throws IOException
    {
        boolean initialScan = bound == 0;
        bound = Long.MAX_VALUE;

        try (CloseableIterator<Long> ids = source.postingListIds())
        {
            while (ids.hasNext())
            {
                long id = ids.next();
                if (openPostingListIds.contains(id))
                    continue;

                Candidate candidate = open(id);
                candidate.skipTo(from);
                offer(candidate);
                if (initialScan)
                    size += candidate.size();
            }
        }
        catch (Exception e)
        {
            FileUtils.closeQuietly(queue.candidates());
            FileUtils.closeQuietly(source);
            throw e;
        }

        queue.startServing();

        // bound is the smallest head of the displaced lists, or Long.MAX_VALUE if none was displaced.
        // If more than capacity lists with the same head were displaced, bound == head, and every refill would
        // start from the same posting. The +1 guarantees progress: the displaced lists only have a duplicate
        // of the head posting, so it can be served now, and the next refill reopens them past it.
        bound = Math.max(bound, (long) queue.head() + 1);
    }

    private Candidate open(long id) throws IOException
    {
        openPostingListIds.add(id);
        return new Candidate(source.open(id), id);
    }

    /**
     * Offers an open candidate to the queue. If the queue is full, the method closes an exhausted candidate and does
     * not add it to the queue. If the queue has room, it keeps the exhausted candidate, so that {@link #refill(int)}
     * does not open the posting list again. The method also closes the candidate that the queue displaces. For each
     * candidate that it closes, it removes the id from {@link #openPostingListIds}. A displaced candidate that is
     * not exhausted also updates the bound. An exhausted candidate has no postings, so it does not change the bound.
     */
    private void offer(Candidate candidate) throws IOException
    {
        // Optional optimization - if we are at the capacity, adding an exhausted posting list to the queue
        // would fail anyway and such candidate would be immediately displaced; so we just close it here.
        // However, if we have space in the queue, it is beneficial to rememeber it to avoid reopening that posting list
        // again during refill.
        if (candidate.head == END_OF_STREAM && queue.size() == capacity)
        {
            openPostingListIds.remove(candidate.postingListId);
            candidate.close();
            return;
        }

        Candidate displaced = queue.add(candidate);
        if (displaced != null)
        {
            if (displaced.head != END_OF_STREAM)
                bound = Math.min(bound, displaced.head);
            openPostingListIds.remove(displaced.postingListId);
            displaced.close();
        }
    }

    /**
     * An open posting list with its next posting already read. This is the look-ahead that {@link PostingList} does
     * not offer.
     */
    private static final class Candidate implements Closeable
    {
        /** The underlying open posting list. */
        final PostingList postings;
        /** Identifies the posting list across windows. */
        final long postingListId;
        /** The next posting of the list, or {@link PostingList#END_OF_STREAM} if the list has no more postings. */
        int head;

        Candidate(PostingList postings, long postingListId) throws IOException
        {
            this.postings = postings;
            this.postingListId = postingListId;
            try
            {
                this.head = postings.nextPosting();
            }
            catch (Exception e)
            {
                FileUtils.closeQuietly(postings);
                throw e;
            }
        }

        void next() throws IOException
        {
            try
            {
                head = postings.nextPosting();
            }
            catch (Exception e)
            {
                FileUtils.closeQuietly(postings);
                throw e;
            }
        }

        void skipTo(int from) throws IOException
        {
            try
            {
                if (head < from)
                    head = postings.advance(from);
            }
            catch (Exception e)
            {
                FileUtils.closeQuietly(postings);
                throw e;
            }
        }

        int size()
        {
            return postings.size();
        }

        @Override
        public void close() throws IOException
        {
            postings.close();
        }
    }

    /**
     * Selects the {@code capacity} candidates with the smallest heads. Then it serves their postings in order from
     * the same heap array. It owns its candidates.
     * <p>
     * During selection, it is a {@link TopKSelector}. The largest head is on top, and a better candidate evicts it.
     * {@link #startServing()} reverses the order, so that the smallest head is on top. From then on, the queue only
     * serves. The heap hooks never remove a candidate. An exhausted candidate has the head
     * {@link PostingList#END_OF_STREAM} and sinks to the bottom.
     */
    private static final class CandidateQueue extends TopKSelector<Candidate>
    {
        /** Whether the queue is in serving mode (min-heap). The queue is in selection mode (max-heap) before that. */
        private boolean serving;

        CandidateQueue(int capacity)
        {
            super(Comparator.comparingInt((Candidate c) -> c.head), capacity);
        }

        void startServing()
        {
            if (!serving)
            {
                serving = true;
                heapify();
            }
        }

        /** Turns the queue back to selection mode. It keeps the candidates. */
        void startSelecting()
        {
            if (serving)
            {
                serving = false;
                heapify();
            }
        }

        /** @return the smallest head, or {@link PostingList#END_OF_STREAM} if there are no candidates. */
        int head()
        {
            checkState(serving);
            return isEmpty() ? END_OF_STREAM : ((Candidate) top()).head;
        }

        /** @return the smallest head. The method also consumes the equal heads of the other candidates. */
        int nextPosting() throws IOException
        {
            try
            {
                int posting = head();
                if (posting == END_OF_STREAM)
                    return posting;
                do
                    pop();
                while (head() == posting);
                return posting;
            }
            catch (UncheckedIOException e)
            {
                throw e.getCause();
            }
        }

        /** Advances each candidate whose head is less than the target. */
        void skipTo(int target) throws IOException
        {
            checkState(serving);
            try
            {
                if (!isEmpty())
                    advanceTo(target);
            }
            catch (UncheckedIOException e)
            {
                throw e.getCause();
            }
        }

        List<Candidate> candidates()
        {
            return getUnsortedShared();
        }

        @Override
        protected boolean greaterThan(Object a, Object b)
        {
            return serving ? key(a) > key(b) : key(a) < key(b);
        }

        /** Compares a candidate by its head. The target of {@link #skipTo} is the Integer to advance to. */
        private static int key(Object o)
        {
            return o instanceof Candidate ? ((Candidate) o).head : (Integer) o;
        }

        @Override
        protected Object advanceItem(Object item)
        {
            try
            {
                ((Candidate) item).next();
                return item;
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        protected Object advanceItemTo(Object item, Object target)
        {
            try
            {
                ((Candidate) item).skipTo((Integer) target);
                return item;
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }
    }

    /**
     * The posting lists of a range of terms, each identified by an id, as {@link LazyMergePostingList} reads them.
     * <p>
     * The ids are opaque to the reader of the source. The reader only compares them for equality. An id must not be
     * {@link Long#MIN_VALUE}. The posting lists that a source opens can share resources. Close them before you close
     * the source.
     */
    public interface Source extends Closeable
    {
        /**
         * @return a new walk over the ids of all the posting lists of the range. The walk visits each id once. You can
         * call the method again. Every walk starts from the beginning and visits the ids in the same order. A walk can
         * be long. To check for query cancellation or timeout, throw an exception from the methods of the iterator.
         */
        CloseableIterator<Long> postingListIds();

        /**
         * @return a new posting list that is independent of the other posting lists that this source opened. It is
         * positioned before its first posting. The caller closes it.
         */
        PostingList open(long postingListId) throws IOException;

        /**
         * Releases the resources that the posting lists share. The caller calls this method after it closes all the
         * posting lists.
         */
        @Override
        void close() throws IOException;
    }
}
