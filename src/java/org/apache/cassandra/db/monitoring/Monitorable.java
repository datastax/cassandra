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

package org.apache.cassandra.db.monitoring;

import java.util.function.Supplier;

public interface Monitorable
{
    String name();
    long creationTimeNanos();
    long timeoutNanos();
    long slowTimeoutNanos();

    boolean isInProgress();
    boolean isAborted();
    boolean isCompleted();
    boolean isSlow();
    boolean isCrossNode();

    boolean abort();
    boolean complete();

    /**
     * Returns the specific {@link ExecutionInfo} for this monitorable operation.
     *
     * @return the execution info for this operation
     */
    default ExecutionInfo executionInfo()
    {
        return ExecutionInfo.EMPTY;
    }

    /**
     * Specific execution details for a monitorable operation.
     * </p>
     * {@link Monitorable} implementations should use this interface to hold and provide additional information about
     * the execution of the operation. This information will be logged when the operation is reported as slow.
     */
    interface ExecutionInfo
    {
        /**
         * An empty no-op implementation.
         */
        ExecutionInfo EMPTY = unique -> "";

        /**
         * A supplier for the empty implementation.
         */
        Supplier<ExecutionInfo> EMPTY_SUPPLIER = () -> EMPTY;

        /**
         * Returns a string representation of this execution info, suitable for logging.
         *
         * @param unique whether the execution info is for a single operation or an aggregation of operations
         * @return a log-suitable string representation of this execution info
         */
        String toLogString(boolean unique);
    }
}
