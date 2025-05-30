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

import java.util.Collection;

import javax.annotation.Nullable;

import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Transactional;

public interface SSTableMultiWriter extends Transactional
{

    /**
     * Writes a partition in an implementation specific way
     * @param partition the partition to append
     * @return true if the partition was written, false otherwise
     */
    void append(UnfilteredRowIterator partition);

    Collection<SSTableReader> finish(boolean openResult, @Nullable StorageHandler handler);
    Collection<SSTableReader> finished();

    /**
     * Opens the resulting sstables after writing has finished. If those readers need to be accessed, then this must
     * be called after `prepareToCommit` (so the writing is complete) but before `commit` (because committing closes
     * some of the resources used to create the underlying readers). When used, the readers can then be accessed by
     * calling `finished()`.
     *
     * @param storageHandler the underlying storage handler. This is used in case of a failure opening the
     *                       SSTableReader(s) to call the `StorageHandler#onOpeningWrittenSSTableFailure` callback, which
     *                       in some implementations may attempt to recover from the error. If `null`, the said callback
     *                       will not be called on failure (and the exception will propagate).
     */
    void openResult(@Nullable StorageHandler storageHandler);

    String getFilename();
    long getBytesWritten();
    long getOnDiskBytesWritten();
    int getSegmentCount();
    TableId getTableId();

    static void abortOrDie(SSTableMultiWriter writer)
    {
        Throwables.maybeFail(writer.abort(null));
    }
}
