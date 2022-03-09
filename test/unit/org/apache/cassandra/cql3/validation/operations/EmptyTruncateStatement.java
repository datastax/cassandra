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

package org.apache.cassandra.cql3.validation.operations;

import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.TruncateStatement;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.TruncateException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

/**
 * A class extending TruncateStatement to test calling a remote implementation provided through
 * configuration properties. Used in TruncateTest.
 */
public class EmptyTruncateStatement extends TruncateStatement
{

    public static class TestTruncateStatementDriver implements TruncateStatement.TruncateStatementDriver
    {
        @Override
        public TruncateStatement createTruncateStatement(String queryString, QualifiedName name)
        {
            if (TruncateTest.emptyTruncate)
                return new EmptyTruncateStatement(queryString, name);
            else
                return new TruncateStatement(queryString, name);
        }
    }

    public EmptyTruncateStatement(String queryString, QualifiedName name)
    {
        super(queryString, name);
    }

    @Override
    public ResultMessage execute(QueryState state, QueryOptions options, long queryStartNanoTime) throws InvalidRequestException, TruncateException
    {
        // Do nothing to differentiate from original TruncateStatement
        return null;
    }

    @Override
    public ResultMessage executeLocally(QueryState state, QueryOptions options)
    {
        // Do nothing to differentiate from original TruncateStatement
        return null;
    }
}
