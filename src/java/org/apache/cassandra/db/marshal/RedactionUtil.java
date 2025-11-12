/*
 * Copyright DataStax, Inc.
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

package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import javax.annotation.Nullable;

/**
 * Utility class for redacting sensitive data values while preserving some orientative size information.
 * <p>
 * This class provides methods to replace actual data values with a redacted placeholder ("?"), occasionally including
 * size hints to help with debugging and troubleshooting without exposing the actual data content. No size hints will be
 * included for values smaller than 100 bytes, or for values of fixed-size data types (e.g., int, UUID, timestamp).
 * <p>
 * Size hints are provided in logarithmic buckets (e.g., ">100B", ">1KiB", ">10KiB", ">100KiB") to give a rough
 * indication of data size while maintaining privacy:
 * <ul>
 *   <li>Up to 100B: no size hint (just "?")</li>
 *   <li>(100 B, 1 KiB]: "?[>100B]"</li>
 *   <li>(1 KiB, 10 KiB]: "?[>1KiB]"</li>
 *   <li>(10 KiB, 100 KiB]: "?[>10KiB]"</li>
 *   <li>(100 KiB, 1 MiB]: "?[>100KiB]"</li>
 *   <li>(1 MiB, 10 MiB]: "?[>1MiB]"</li>
 *   <li>(10 MiB, 100 MiB]: "?[>10MiB]"</li>
 *   <li>(100 MiB, 1 GiB]: "?[>100MiB]"</li>
 *   <li>Over 1 GiB: "?[>1GiB]"</li>
 * </ul>
 */
public final class RedactionUtil
{
    // Pre-computed redacted values for each size bucket
    private static final String REDACTED = "?";
    private static final String REDACTED_100B = "?[>100B]";
    private static final String REDACTED_1KIB = "?[>1KiB]";
    private static final String REDACTED_10KIB = "?[>10KiB]";
    private static final String REDACTED_100KIB = "?[>100KiB]";
    private static final String REDACTED_1MIB = "?[>1MiB]";
    private static final String REDACTED_10MIB = "?[>10MiB]";
    private static final String REDACTED_100MIB = "?[>100MiB]";
    private static final String REDACTED_1GIB = "?[>1GiB]";

    // Pre-computed size thresholds for each size bucket
    private static final int B_100 = 100;
    private static final int KIB = 1024;
    private static final int KIB_10 = 10 * KIB;
    private static final int KIB_100 = 100 * KIB;
    private static final int MIB = 1024 * KIB;
    private static final int MIB_10 = 10 * MIB;
    private static final int MIB_100 = 100 * MIB;
    private static final int GIB = 1024 * MIB;

    private RedactionUtil()
    {
    }

    /**
     * Redacts a byte buffer value, optionally including size information.
     * <p>
     * If the value is null, it's not greater than 100B, or has a fixed length (where size information would not be
     * useful), returns a simple "?" placeholder. Otherwise, returns a placeholder with a size hint indicating the
     * approximate size of the data, according to {@link #redact(int)}.
     *
     * @param bytes the value to redact
     * @param isValueLengthFixed whether the value has a fixed length (e.g., int, UUID, timestamp)
     * @return a redacted string representation, either "?" or "?[size_hint]"
     */
    public static String redact(@Nullable ByteBuffer bytes, boolean isValueLengthFixed)
    {
        if (bytes == null || isValueLengthFixed)
            return REDACTED;

        int remaining = bytes.remaining();
        // Early return for small values to avoid method call overhead
        if (remaining <= B_100)
            return REDACTED;

        return redact(remaining);
    }

    /**
     * Generates a redacted string with a size hint based on the provided size.
     * <p>
     * The size hint uses logarithmic buckets to provide a rough indication of size:
     * <ul>
     *   <li>Up to 100B: no size hint (just "?")</li>
     *   <li>(100 B, 1 KiB]: "?[>100B]"</li>
     *   <li>(1 KiB, 10 KiB]: "?[>1KiB]"</li>
     *   <li>(10 KiB, 100 KiB]: "?[>10KiB]"</li>
     *   <li>(100 KiB, 1 MiB]: "?[>100KiB]"</li>
     *   <li>And so on, up to "?[>1GiB]" for very large values</li>
     * </ul>
     *
     * @param size the size in bytes
     * @return a redacted string with an appropriate size hint
     */
    public static String redact(int size)
    {
        assert size >= 0 : "Size must be non-negative";

        // Byte range, don't include size information for the values in the smallest bucket
        if (size <= B_100)
            return REDACTED;
        if (size <= KIB)
            return REDACTED_100B;

        // KiB range
        if (size <= KIB_10)
            return REDACTED_1KIB;
        if (size <= KIB_100)
            return REDACTED_10KIB;
        if (size <= MIB)
            return REDACTED_100KIB;

        // MiB range
        if (size <= MIB_10)
            return REDACTED_1MIB;
        if (size <= MIB_100)
            return REDACTED_10MIB;
        if (size <= GIB)
            return REDACTED_100MIB;

        // above 1 GiB
        return REDACTED_1GIB;
    }
}
