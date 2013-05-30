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
package org.apache.cassandra.transport;

import com.google.common.collect.Maps;
import com.sun.security.auth.module.Krb5LoginModule;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

public interface SaslAuthenticator
{
    public byte[] initialResponse();
    public byte[] evaluateChallenge(byte[] challenge);
    public boolean isComplete();

    static class PlainTextAuthenticator implements SaslAuthenticator
    {
        private byte[] username;
        private byte[] password;

        PlainTextAuthenticator(String username, String password)
        {
            this.username = username.getBytes(Charset.forName("UTF-8"));
            this.password = password.getBytes(Charset.forName("UTF-8"));
        }

        public byte[] initialResponse()
        {
            byte[] initialResponse = new byte[username.length + password.length + 2];
            initialResponse[0] = 0;
            System.arraycopy(username, 0, initialResponse, 1, username.length);
            initialResponse[username.length + 1] = 0;
            System.arraycopy(password, 0, initialResponse, username.length + 2, password.length);
            return initialResponse;
        }

        @Override
        public byte[] evaluateChallenge(byte[] challenge)
        {
            return null;
        }

        @Override
        public boolean isComplete()
        {
            return true;
        }
    }

    static class KerberosAuthenticator implements SaslAuthenticator
    {
        private static final Map<String, String> DEFAULT_PROPERTIES = new HashMap<String, String>()
        {{
                put(Sasl.SERVER_AUTH, "true");
                put(Sasl.QOP, "auth");
        }};

        private static final CallbackHandler NULL_CBH = new CallbackHandler()
        {
            @Override
            public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException
            {
            }
        };

        private SaslClient saslClient;
        private Subject subject;

        KerberosAuthenticator(final String hostname)
        {
            subject = new Subject();
            try
            {
                LoginContext login = new LoginContext("Client", subject, null, new KerberosUserConfiguration());
                login.login();
            } catch (LoginException e)
            {
                throw new RuntimeException(e);
            }
            saslClient = Subject.doAs(subject, new PrivilegedAction<SaslClient>()
            {
                @Override
                public javax.security.sasl.SaslClient run()
                {
                    try
                    {
                        return Sasl.createSaslClient(
                                new String[]{"GSSAPI"},
                                null,
                                "dse",
                                hostname,
                                DEFAULT_PROPERTIES,
                                NULL_CBH);
                    } catch (Exception e)
                    {
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        @Override
        public byte[] initialResponse()
        {
            return Subject.doAs(subject, new PrivilegedAction<byte[]>()
            {
                @Override
                public byte[] run()
                {
                    try
                    {
                        return saslClient.evaluateChallenge(new byte[0]);
                    } catch (SaslException e)
                    {
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        @Override
        public byte[] evaluateChallenge(final byte[] challenge)
        {
            if (saslClient.isComplete())
            {
                return null;
            }
            byte[] retval = Subject.doAs(subject, new PrivilegedAction<byte[]>()
            {
                @Override
                public byte[] run()
                {
                    try
                    {
                        return saslClient.evaluateChallenge(challenge);
                    }
                    catch (SaslException e)
                    {
                        throw new RuntimeException(e);
                    }
                }
            });
            return retval;
        }

        @Override
        public boolean isComplete()
        {
            return saslClient.isComplete();
        }

        private static class KerberosUserConfiguration extends javax.security.auth.login.Configuration
        {
            private final Map<String, String> kerberosOptions = Maps.newHashMap();

            {
                kerberosOptions.put("doNotPrompt", "true");
                kerberosOptions.put("renewTGT", "true");
                kerberosOptions.put("storeKey", "false");
                kerberosOptions.put("useKeyTab", "false");
                kerberosOptions.put("useTicketCache", "true");
                kerberosOptions.put("ticketCache", System.getenv("KRB5CCNAME"));

            }

            private final AppConfigurationEntry kerberosLogin =
                    new AppConfigurationEntry(Krb5LoginModule.class.getName(),
                            AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL,
                            kerberosOptions);

            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String arg0)
            {
                return new AppConfigurationEntry[]{kerberosLogin};
            }
        }

    }

}
