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

import org.apache.cassandra.exceptions.InternalRequestExecutionException;
import org.apache.cassandra.exceptions.RequestFailureReason;

/**
 * Thrown if a secondary index is still in an old version that doesn't support the requested feature.
 * </p>
 * Users hitting this exception should probably rebuild their index to a newer version.
 */
public final class FeatureNeedsIndexRebuildException extends RuntimeException implements InternalRequestExecutionException
{
    /**
     * Creates a new {@link FeatureNeedsIndexRebuildException} for the specified message.
     *
     * @param message the explanation of the error
     */
    public FeatureNeedsIndexRebuildException(String message)
    {
        super(message);
    }

    @Override
    public RequestFailureReason getReason()
    {
        return RequestFailureReason.FEATURE_NEEDS_INDEX_REBUILD;
    }
}
