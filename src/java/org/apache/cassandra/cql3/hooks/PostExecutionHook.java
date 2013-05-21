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
package org.apache.cassandra.cql3.hooks;

import org.apache.cassandra.cql3.CQLExecutionContext;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.exceptions.RequestExecutionException;

/**
 * Run after the CQL Statement is executed in
 * {@link org.apache.cassandra.cql3.QueryProcessor}.
 */
public interface PostExecutionHook
{
    /**
     * Perform post-processing on a CQL statement directly after
     * it being executed by the QueryProcessor.
     *
     * @param statement the statement to perform post-processing on
     * @param context execution context containing additional info
     *                about the operation and statement
     * @throws RequestExecutionException
     */
    public void execute(CQLStatement statement, CQLExecutionContext context) throws RequestExecutionException;

    public static final PostExecutionHook NO_OP = new PostExecutionHook()
    {
        @Override
        public void execute(CQLStatement statement, CQLExecutionContext context) {}
    };
}
