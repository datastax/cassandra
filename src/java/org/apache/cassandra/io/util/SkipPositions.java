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

package org.apache.cassandra.io.util;

/**
 * Helpers for the skip arithmetic of {@link ReaderFileProxy#positionForSkip}.
 */
final class SkipPositions
{
    private SkipPositions()
    {
    }

    /**
     * Returns the largest number of bytes, at most {@code maxBytes}, that can be skipped from {@code position} without
     * {@link ReaderFileProxy#positionForSkip} going past {@code limit}, i.e. the number of bytes (not counting holes)
     * between {@code position} and {@code limit}, capped to {@code maxBytes}. {@code position} must not be after
     * {@code limit}.
     * <p>
     * This relies on {@code positionForSkip} being strictly increasing in the number of bytes skipped, and costs
     * O(log maxBytes) calls to it. Its callers are {@link RandomAccessReader#skipBytes}, when a skip reaches the end
     * of the data, and {@code TailOverridingRebufferer.positionForSkip}, when a skip crosses the cutoff.
     */
    static int bytesSkippableBefore(ReaderFileProxy proxy, long position, int maxBytes, long limit)
    {
        assert position <= limit : position + " > " + limit;
        int low = 0;
        int high = maxBytes;
        while (low < high)
        {
            int mid = (int) (((long) low + high + 1) >>> 1);
            if (proxy.positionForSkip(position, mid) <= limit)
                low = mid;
            else
                high = mid - 1;
        }
        return low;
    }
}
