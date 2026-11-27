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

package org.apache.cassandra.index.sai.utils;

import java.util.Arrays;

/**
 * The range of the terms that start with a prefix, {@code [prefix, nextOf(prefix))}.
 * <p>
 * The terms matching a CQL {@code LIKE} pattern all start with the literal bytes at the start of the pattern (see
 * {@link org.apache.cassandra.db.tries.TriePattern#literalPrefix()}), so their range serves the estimates of the
 * pattern expressions and the selection of the indexes to search. Non-composite literal SAI terms are stored in the
 * trie dictionaries as their raw, unterminated UTF-8 bytes at every index version (see
 * {@code OnDiskFormat#encodeForTrie}), so the bytes of the range are those of the terms, in the order of the
 * dictionaries.
 */
public final class PrefixRange
{
    private PrefixRange()
    {
    }

    /**
     * Returns the smallest byte sequence that sorts after every byte sequence starting with {@code prefix}, i.e.
     * the exclusive upper bound of the range {@code [prefix, nextOf(prefix))}, or {@code null} if no such bound
     * exists (the prefix is empty or consists solely of {@code 0xFF} bytes). It is only meaningful for tries storing
     * raw unterminated term bytes.
     */
    public static byte[] upperBound(byte[] prefix)
    {
        int last = prefix.length - 1;
        while (last >= 0 && prefix[last] == (byte) 0xFF)
            last--;
        if (last < 0)
            return null;
        byte[] upperBytes = Arrays.copyOf(prefix, last + 1);
        upperBytes[last]++;
        return upperBytes;
    }
}
