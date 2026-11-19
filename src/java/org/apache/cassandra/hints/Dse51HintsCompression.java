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

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nullable;

/**
 * The compressions of a hints file DSE 5.1.51 can build. DSE 5.1.51 builds the compressor of a hints file from the
 * {@code compression} parameter of its descriptor as it builds the compressor of a table: a class name without a dot
 * gets the prefix {@code org.apache.cassandra.io.compress.}, and an option the class does not support, or a value
 * outside its range, fails the file's first dispatch after each start, which leaves the file on disk, out of the
 * dispatch queue until the next start: never delivered. Of the compressors of this software it reads
 * {@code LZ4Compressor}, with {@code lz4_compressor_type} {@code fast} or {@code high} and
 * {@code lz4_high_compressor_level} from 1 to 17, {@code SnappyCompressor} and {@code DeflateCompressor}, with no
 * option. {@code crc_check_chance}, which DSE 5.1.51 removes with a warning, is an option no class supports for this
 * rule: its callers remove it first.
 * <p>
 * The rule refuses, in {@code internode_compatibility_mode: DSE_5_1}, a {@code hints_compression} the node would
 * write such files with ({@code DatabaseDescriptor}, which refuses {@code crc_check_chance} with a text of its own: it
 * has no use in a hints file). The rollback check of {@code dsepreflight} reads a hints file of version 1 as DSE
 * 5.1.51 does: it removes {@code crc_check_chance}, then refuses with this rule.
 */
public final class Dse51HintsCompression
{
    /** The package DSE 5.1.51 resolves a class name without a dot in. */
    private static final String PACKAGE_NAME = "org.apache.cassandra.io.compress";
    private static final String COMPRESSOR_PACKAGE = PACKAGE_NAME + '.';

    private static final String LZ4 = "LZ4Compressor";
    private static final String SNAPPY = "SnappyCompressor";
    private static final String DEFLATE = "DeflateCompressor";
    private static final String LZ4_COMPRESSOR_TYPE = "lz4_compressor_type";
    private static final String LZ4_HIGH_COMPRESSOR_LEVEL = "lz4_high_compressor_level";
    /** The option DSE 5.1.51 removes, with a warning, before it builds a compressor. */
    public static final String CRC_CHECK_CHANCE = "crc_check_chance";
    private static final Set<String> LZ4_TYPES = Set.of("fast", "high");
    private static final int LZ4_MIN_LEVEL = 1;
    private static final int LZ4_MAX_LEVEL = 17;

    /** What DSE 5.1.51 reads, for the texts of the refusals. */
    public static final String ACCEPTED = LZ4 + " (with no option but " + LZ4_COMPRESSOR_TYPE + " fast or high and " +
                                          LZ4_HIGH_COMPRESSOR_LEVEL + " from " + LZ4_MIN_LEVEL + " to " +
                                          LZ4_MAX_LEVEL + "), " + SNAPPY + " or " + DEFLATE + " (with no option), " +
                                          "by the short name or under " + PACKAGE_NAME;

    private Dse51HintsCompression()
    {
    }

    /**
     * Why DSE 5.1.51 cannot build the compressor of a hints file with the given class and options, null when it can;
     * the callers remove {@code crc_check_chance} first (see the class javadoc). Each value is read as text, a null
     * value as no value (the default of the option); when several options are refused, the first in their order by
     * name is named.
     *
     * @param className the class name of the compression, as written in the descriptor
     * @param options its options, null for none
     */
    @Nullable
    public static String refusal(@Nullable String className, @Nullable Map<String, ?> options)
    {
        if (className == null || className.isEmpty())
            return "it names no class";
        String simpleName = simpleName(className);
        if (simpleName == null || !(LZ4.equals(simpleName) || SNAPPY.equals(simpleName) || DEFLATE.equals(simpleName)))
            return "the class " + className + " is not one DSE 5.1 builds";
        if (options == null)
            return null;
        for (String name : options.keySet())
            if (name == null)
                return "an option has no name";
        for (Map.Entry<String, ?> option : new TreeMap<String, Object>(options).entrySet())
        {
            String name = option.getKey();
            String value = option.getValue() == null ? null : String.valueOf(option.getValue());
            boolean lz4Option = LZ4_COMPRESSOR_TYPE.equals(name) || LZ4_HIGH_COMPRESSOR_LEVEL.equals(name);
            if (!LZ4.equals(simpleName) || !lz4Option)
                return "the option " + name + " is not one " + simpleName + " supports in DSE 5.1";
            if (value == null)
                continue;
            if (LZ4_COMPRESSOR_TYPE.equals(name) && !LZ4_TYPES.contains(value))
                return "the value " + value + " of " + name + " is not fast or high";
            if (LZ4_HIGH_COMPRESSOR_LEVEL.equals(name) && !isLevel(value))
                return "the value " + value + " of " + name + " is not a number from " + LZ4_MIN_LEVEL + " to " +
                       LZ4_MAX_LEVEL;
        }
        return null;
    }

    /**
     * The name of a compressor class as DSE 5.1.51 resolves it in {@code org.apache.cassandra.io.compress}: the name
     * itself without a dot, the name after that package with it, null for a class of another package.
     */
    @Nullable
    public static String simpleName(String className)
    {
        if (className.indexOf('.') < 0)
            return className;
        return className.startsWith(COMPRESSOR_PACKAGE) ? className.substring(COMPRESSOR_PACKAGE.length()) : null;
    }

    private static boolean isLevel(String value)
    {
        try
        {
            int level = Integer.parseInt(value);
            return level >= LZ4_MIN_LEVEL && level <= LZ4_MAX_LEVEL;
        }
        catch (NumberFormatException e)
        {
            return false;
        }
    }
}
