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

package org.apache.cassandra.service.context;

import java.util.function.Supplier;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadCommand;

/**
 * Default implementation of {@link OperationContext}.
 * <p>
 * This default implementation is mostly only useful for debugging as the only concrete method is provices is a
 * {@link #toString()} method giving details on the operation the context corresponds to (though the context object
 * also identify the operation, so it could also theoretically be used from 2 separate place in the code to decide
 * if they execute as part of the same operation).
 */
public class DefaultOperationContext implements OperationContext
{
    private final Supplier<String> toDebugString;

    private DefaultOperationContext(Supplier<String> toDebugString)
    {
        this.toDebugString = toDebugString;
    }

    @Override
    public void close()
    {
    }

    @Override
    public String toString()
    {
        return String.format("[%d] %s", System.identityHashCode(this), toDebugString.get());
    }

    /**
     * Simple default implementation of {@link OperationContext.Factory} that creates {@link DefaultOperationContext}.
     */
    static class Factory implements OperationContext.Factory
    {
        @Override
        public OperationContext forRead(ReadCommand command, ColumnFamilyStore cfs)
        {
            return new DefaultOperationContext(command::toUnredactedCQLString);
        }
    }
}
