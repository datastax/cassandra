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

package org.apache.cassandra.index;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.UncheckedInternalRequestExecutionException;
import org.apache.cassandra.service.StorageServiceMBean;

import java.util.Set;

/**
 * Thrown if a secondary index or any of its components is still in an old version that doesn't support the requested
 * feature.
 * </p>
 * Users hitting this exception should probably set the right index version and upgrade their sstables. That way, the
 * new replacement sstables will use that new version.
 * </p>
 * Please note that rebuilding the index with {@code nodetool rebuild_index},
 * {@link StorageServiceMBean#rebuildSecondaryIndex(String, String, String...)},
 * {@link ColumnFamilyStore#rebuildSecondaryIndex(String, String, String...)},
 * {@link SecondaryIndexManager#rebuildIndexesBlocking(Set)}, etc. will not be enough to upgrade the index version,
 * because sstables are fixed to the index version active when they were first indexed. See CNDB-8756 for details.
 * </p>
 * If this error is hit during an index build, when the index has been recorded in the schema, and before it's marked
 * as queryable, it means that the index also needs to be rebuilt after upgrading the sstables, so it gets marked as
 * queryable. CNDB-16824 is meant to simplify that.
 */
public final class FeatureNeedsIndexRebuildException extends UncheckedInternalRequestExecutionException
{
    /**
     * Creates a new {@link FeatureNeedsIndexRebuildException} for the specified message.
     *
     * @param message the explanation of the error
     */
    public FeatureNeedsIndexRebuildException(String message)
    {
        super(RequestFailureReason.FEATURE_NEEDS_INDEX_REBUILD, message);
    }
}
