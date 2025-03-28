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
 * Thrown if a secondary index is not currently available because it is building.
 */
public final class IndexBuildInProgressException extends UncheckedInternalRequestExecutionException
{
    /**
     * Creates a new <code>IndexIsBuildingException</code> for the specified index.
     * @param index the index
     */
    public IndexBuildInProgressException(Index index)
    {
        super(RequestFailureReason.INDEX_BUILD_IN_PROGRESS,
                String.format("The secondary index '%s' is not yet available as it is building", index.getIndexMetadata().name));
    }
}
