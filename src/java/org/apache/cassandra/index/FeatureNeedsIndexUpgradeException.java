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

import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.UncheckedInternalRequestExecutionException;

/**
 * Thrown if a secondary index is still in an old version that doesn't support the requested feature.
 * </p>
 * Users hitting this exception should probably upgrade their sstables so their indexes are upgraded to a newer version.
 * </p>
 * If this error is hit by during an index build, when the index has been recorded in the schema, and before it's marked
 * as queryable, it means that the index also needs to be rebuilt after upgrading the sstables, so it gets marked as
 * queryable.
 */
public final class FeatureNeedsIndexUpgradeException extends UncheckedInternalRequestExecutionException
{
    /**
     * Creates a new {@link FeatureNeedsIndexUpgradeException} for the specified message.
     *
     * @param message the explanation of the error
     */
    public FeatureNeedsIndexUpgradeException(String message)
    {
        super(RequestFailureReason.FEATURE_NEEDS_INDEX_REBUILD, message);
    }
}
