/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
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
