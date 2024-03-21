/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.utils;

import net.nicoulaj.compilecommand.annotations.Inline;

public interface Flags
{
    @Inline
    static boolean isEmpty(int flags)
    {
        return flags == 0;
    }

    @Inline
    static boolean containsAll(int flags, int testFlags)
    {
        return (flags & testFlags) == testFlags;
    }

    @Inline
    static boolean contains(int flags, int testFlags)
    {
        return (flags & testFlags) != 0;
    }

    @Inline
    static int add(int flags, int toAdd)
    {
        return flags | toAdd;
    }

    @Inline
    static int remove(int flags, int toRemove)
    {
        return flags & ~toRemove;
    }
}
