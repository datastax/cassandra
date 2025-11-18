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
package org.apache.cassandra.db;

import java.util.regex.Pattern;
import javax.annotation.Nullable;

import org.apache.cassandra.cql3.CQLTester;
import org.assertj.core.api.Assertions;

public abstract class ReadCommandCQLTester<T extends ReadCommand> extends CQLTester
{
    private static final Pattern PATTERN = Pattern.compile("%");

    protected abstract T parseCommand(String query);

    protected void assertToCQLString(String query, String expectedUnredactedCQL, String expectedRedactedCQL)
    {
        assertToCQLString(query, expectedUnredactedCQL, expectedRedactedCQL, null);
    }

    protected void assertToCQLString(String query,
                                     String expectedUnredactedCQL,
                                     String expectedRedactedCQL,
                                     @Nullable String expectedErrorMessage)
    {
        T command = parseCommand(query);

        String actualUnredactedCQL = command.toUnredactedCQLString();
        Assertions.assertThat(actualUnredactedCQL)
                  .isEqualTo(formatQuery(expectedUnredactedCQL));

        String actualRedactedCQL = command.toRedactedCQLString();
        Assertions.assertThat(actualRedactedCQL)
                  .isEqualTo(formatQuery(expectedRedactedCQL));

        if (expectedErrorMessage == null)
            execute(PATTERN.matcher(actualUnredactedCQL).replaceAll("%%"));
        else
            Assertions.assertThatThrownBy(() -> execute(actualUnredactedCQL)).hasMessageContaining(expectedErrorMessage);
    }
}
