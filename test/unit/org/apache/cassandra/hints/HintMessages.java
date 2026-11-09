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

package org.apache.cassandra.hints;

import java.util.UUID;

import org.apache.cassandra.db.Mutation;

/** Test access to the package-private {@link HintMessage} constructor and hint, for the serialization audits. */
public final class HintMessages
{
    private HintMessages()
    {
    }

    public static HintMessage create(UUID hostId, Hint hint)
    {
        return new HintMessage(hostId, hint);
    }

    /** The mutation of the hint {@code message} carries. */
    public static Mutation mutationOf(HintMessage message)
    {
        return message.hint.mutation;
    }
}
