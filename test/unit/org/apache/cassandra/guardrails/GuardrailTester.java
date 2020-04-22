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

package org.apache.cassandra.guardrails;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.annotation.Nullable;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class GuardrailTester extends CQLTester
{
    static final String USERNAME = "guardrail_user";
    static final String PASSWORD = "guardrail_password";

    private static boolean guardRailsEnabled;
    private static Set<String> tablePropertiesDisallowed;

    protected TestListener listener;

    @BeforeClass
    public static void setupGuardrailTester()
    {
        guardRailsEnabled = DatabaseDescriptor.getGuardrailsConfig().enabled;
        DatabaseDescriptor.getGuardrailsConfig().enabled = true;

        tablePropertiesDisallowed = DatabaseDescriptor.getGuardrailsConfig().table_properties_disallowed;
        DatabaseDescriptor.getGuardrailsConfig().table_properties_disallowed = Collections.emptySet();

        requireAuthentication();
        requireNetwork();
    }

    @AfterClass
    public static void tearDownGuardrailTester()
    {
        DatabaseDescriptor.getGuardrailsConfig().enabled = guardRailsEnabled;
        DatabaseDescriptor.getGuardrailsConfig().table_properties_disallowed = tablePropertiesDisallowed;
    }

    /**
     * Creates an ordinary user that is not excluded from guardrails, that is, a user that is not super not internal.
     */
    @Before
    public void beforeGuardrailTest() throws Throwable
    {
        useSuperUser();
        executeNet(format("CREATE USER IF NOT EXISTS %s WITH PASSWORD '%s'", USERNAME, PASSWORD));
        executeNet(format("GRANT ALL ON KEYSPACE %s TO %s", KEYSPACE, USERNAME));
        useUser(USERNAME, PASSWORD);

        listener = new TestListener(null);
        Guardrails.register(listener);

        execute("USE " + keyspace());
        executeNet("USE " + keyspace());
    }

    @After
    public void afterGuardrailTest() throws Throwable
    {
        Guardrails.unregister(listener);

        useSuperUser();
        executeNet("DROP USER " + USERNAME);
    }

    QueryState userQueryState()
    {
        return queryState(new AuthenticatedUser(USERNAME));
    }

    QueryState superQueryState()
    {
        return queryState(new AuthenticatedUser("cassandra"));
    }

    QueryState internalQueryState()
    {
        return QueryState.forInternalCalls();
    }

    private QueryState queryState(AuthenticatedUser user)
    {
        ClientState clientState = ClientState.forExternalCalls(user);
        return new QueryState(clientState);
    }

    static GuardrailsConfig config()
    {
        return DatabaseDescriptor.getGuardrailsConfig();
    }

    static class TestListener implements Guardrails.Listener
    {
        @Nullable
        private final Guardrail guardrail;
        private List<String> failures = new CopyOnWriteArrayList<>();
        private List<String> warnings = new CopyOnWriteArrayList<>();

        private TestListener(@Nullable Guardrail guardrail)
        {
            this.guardrail = guardrail;
        }

        synchronized void assertFailed(String... expectedMessages)
        {
            assertThat(failures).isNotEmpty();
            assertThat(failures.size()).isEqualTo(expectedMessages.length);

            for (int i = 0; i < failures.size(); i++)
            {
                String actual = failures.get(i);
                String expected = expectedMessages[i];
                assertThat(actual).contains(expected);
            }
        }

        synchronized void assertNotFailed()
        {
            assertThat(failures).isEmpty();
        }

        synchronized void assertWarned(String... expectedMessages)
        {
            assertThat(warnings).isNotEmpty();
            assertThat(warnings.size()).isEqualTo(expectedMessages.length);

            for (int i = 0; i < warnings.size(); i++)
            {
                String actual = warnings.get(i);
                String expected = expectedMessages[i];
                assertThat(actual).contains(expected);
            }
        }

        synchronized void assertContainsWarns(String... expectedMessages)
        {
            assertThat(warnings).isNotEmpty();
            for (String msg : expectedMessages)
            {
                assertThat(warnings).containsAnyOf(msg);
            }
        }

        synchronized void assertNotWarned()
        {
            assertThat(warnings).isEmpty();
        }

        synchronized void clear()
        {
            failures.clear();
            warnings.clear();
        }

        @Override
        public synchronized void onWarningTriggered(String guardrailName, String message)
        {
            if (guardrail == null || guardrailName.equals(guardrail.name))
            {
                warnings.add(message);
            }
        }

        @Override
        public void onFailureTriggered(String guardrailName, String message)
        {
            if (guardrail == null || guardrailName.equals(guardrail.name))
            {
                failures.add(message);
            }
        }
    }
}
