/*
 * Copyright IBM Corp.
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

package org.apache.cassandra.net;

/**
 * A message of a DSE peer that this software refuses on purpose while reading it, in a DSE compatibility mode: the
 * reads of a Thrift client that a DSE 5.1 coordinator forwards. The inbound handler answers it with a failure response,
 * as any message it cannot read, when the sender asked for one, and logs it at WARN at most once a minute rather than
 * with a stack trace at ERROR each time: such reads are expected for as long as a Thrift client runs, and each is
 * counted in {@code nodetool compatstatus}.
 */
public final class DseRefusedMessageException extends IllegalStateException
{
    public DseRefusedMessageException(String message)
    {
        super(message);
    }
}
