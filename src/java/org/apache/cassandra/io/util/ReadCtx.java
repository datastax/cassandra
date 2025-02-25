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

package org.apache.cassandra.io.util;

public interface ReadCtx extends AutoCloseable
{
    ReadCtx FOR_TEST = new Default(Kind.TEST);

    enum Kind
    {
        USER_REQUEST,

        SSTABLE_OPEN,
        SSTABLE_MOVED_START,
        SSTABLE_METADATA_CHANGE,

        /** Used for some reads thare are done to decide what compactions to do, but are not part of a compaction per-se. */
        COMPACTION_PREPARATION,
        COMPACTION,

        INDEX_SEARCHER_CREATION,
        INDEX_LOAD,
        INDEX_BUILD,
        VECTOR_INDEX_FLUSH,

        STEAMING,

        MATERIALIZED_VIEW_BUILD,

        REPAIR_VALIDATION,

        SCRUB,
        SSTABLE_EXPORT, SSTABLE_IMPORT,
        SSTABLE_VERIFIER,
        SSTABLE_METADATA_VIEWER,
        SSTABLE_UPGRADER,
        SIZE_ESTIMATE_RECORDER,
        DESCRIBE_OWNERSHIP,
        NODEPROBE,

        TEST,
    }

    /** Overrides to indicate that we this should not throw "checked" exceptions (both because in most places where
     * this is used, there is no real good way to deal with an exception and having to deal with "unchecked" exceptions
     * would be annoying, but also because it is strongly advised for implementations to not have this method throw;
     * it's only to release potential resources kept for the duration of the read, and any issue released them can
     * be logged but shouldn't otherwise fail the read). */
    @Override
    void close();

    class Default implements ReadCtx
    {
        private final Kind kind;

        public Default(Kind kind)
        {
            this.kind = kind;
        }

        @Override
        public void close()
        {
        }

        @Override
        public String toString()
        {
            return kind + "@" + System.identityHashCode(this);
        }
    }
}
