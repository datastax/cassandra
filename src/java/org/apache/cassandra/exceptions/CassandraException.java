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

import javax.annotation.Nullable;

import org.apache.cassandra.db.marshal.RedactionUtil;
import org.apache.cassandra.utils.Shared;

import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

@Shared(scope = SIMULATION)
public abstract class CassandraException extends RuntimeException implements TransportException, RedactionUtil.WithRedactedMessage
{
    private final ExceptionCode code;

    /**
     * A version of the message without sensitive data, such as user-provided values, so it can be written to logs.
     * It is {@code null} when the exception doesn't provide any, which is the case when the message doesn't contain
     * sensitive data or when it has not been analyzed. The regular message is always the one sent to the client.
     */
    @Nullable
    private final String redactedMessage;

    protected CassandraException(ExceptionCode code, String msg)
    {
        this(code, msg, (String) null);
    }

    protected CassandraException(ExceptionCode code, String msg, @Nullable String redactedMessage)
    {
        super(msg);
        this.code = code;
        this.redactedMessage = redactedMessage;
    }

    protected CassandraException(ExceptionCode code, String msg, Throwable cause)
    {
        super(msg, cause);
        this.code = code;
        this.redactedMessage = null;
    }

    public ExceptionCode code()
    {
        return code;
    }

    /**
     * @return the message without sensitive data, safe to be written to logs, or {@code null} if there isn't any.
     * @see org.apache.cassandra.db.marshal.RedactionUtil#redact(Throwable)
     */
    @Override
    @Nullable
    public String getRedactedMessage()
    {
        return redactedMessage;
    }
}
