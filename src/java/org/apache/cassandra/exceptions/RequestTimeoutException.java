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
package org.apache.cassandra.exceptions;

import org.apache.cassandra.db.ConsistencyLevel;

public class RequestTimeoutException extends RequestExecutionException
{
    public final ConsistencyLevel consistency;
    public final int received;
    public final int blockFor;

    protected RequestTimeoutException(ExceptionCode code, ConsistencyLevel consistency, int received, int blockFor)
    {
        super(code, String.format("Operation timed out - received only %d responses.", received));
        this.consistency = consistency;
        this.received = received;
        this.blockFor = blockFor;
    }

    protected RequestTimeoutException(ExceptionCode code, ConsistencyLevel consistency, int received, int blockFor, String msg)
    {
        super(code, msg);
        this.consistency = consistency;
        this.received = received;
        this.blockFor = blockFor;
    }

    public RequestTimeoutException(ExceptionCode exceptionCode, ConsistencyLevel consistency)
    {
        super(exceptionCode, "Operation timeout out");
        this.consistency = consistency;
        this.received = 0;
        this.blockFor = 0;
    }
}
