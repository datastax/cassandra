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

package org.apache.cassandra.db.memtable;

import org.apache.cassandra.exceptions.InternalRequestExecutionException;
import org.apache.cassandra.exceptions.RequestFailureReason;

/// Thrown by a memtable that cannot take an update holding data of a column newer than the metadata the memtable was
/// created with, before the memtable has taken any of the update. Such an update is made between a schema change
/// publishing the column and switching the memtable, and the memtable asks to be switched, and waits for the switch,
/// before it throws this if the table's metadata is newer than its own ([#switched]), so
/// [org.apache.cassandra.db.Keyspace] applies the mutation once more, in a write group started after the switch. It
/// does not otherwise: a write made with metadata newer than the table's, for instance, which no memtable of the table
/// can take, fails with this at once. A write that fails again fails with this as well, as one that reaches a next
/// memtable created with the old metadata by a concurrent switch does.
public class ColumnNewerThanMemtableException extends RuntimeException implements InternalRequestExecutionException
{
    /// Whether the memtable asked to be switched for the schema change and waited for the switch before throwing this.
    public final boolean switched;

    public ColumnNewerThanMemtableException(String message, boolean switched)
    {
        super(message);
        this.switched = switched;
    }

    @Override
    public RequestFailureReason getReason()
    {
        return RequestFailureReason.INCOMPATIBLE_SCHEMA;
    }
}
