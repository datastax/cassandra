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

package org.apache.cassandra.net;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

import com.google.common.annotations.VisibleForTesting;

/**
 * Counts the requests this node refused because of the DSE 6 compatibility mode, by reason. The counters are shown by
 * {@code nodetool compatstatus}, so that an operator sees what the upgrade window is holding back.
 * <p>
 * The reasons are free-form names chosen by the callers; a counter is created on first use.
 */
public final class DseCompatRefusals
{
    private static final ConcurrentMap<String, LongAdder> COUNTERS = new ConcurrentHashMap<>();

    private DseCompatRefusals()
    {
    }

    /** Counts one refused request under the given reason. */
    public static void refused(String reason)
    {
        COUNTERS.computeIfAbsent(reason, r -> new LongAdder()).increment();
    }

    /** The number of requests refused so far under the given reason, zero if it never happened. */
    public static long count(String reason)
    {
        LongAdder adder = COUNTERS.get(reason);
        return adder == null ? 0 : adder.sum();
    }

    /** The total of all refused requests. */
    public static long total()
    {
        long total = 0;
        for (LongAdder adder : COUNTERS.values())
            total += adder.sum();
        return total;
    }

    /** A sorted, detached copy of the counters: reason to count. */
    public static Map<String, Long> snapshot()
    {
        Map<String, Long> result = new TreeMap<>();
        for (Map.Entry<String, LongAdder> e : COUNTERS.entrySet())
            result.put(e.getKey(), e.getValue().sum());
        return result;
    }

    @VisibleForTesting
    public static void reset()
    {
        COUNTERS.clear();
    }
}
