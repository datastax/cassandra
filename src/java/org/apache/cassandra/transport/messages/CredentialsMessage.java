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
package org.apache.cassandra.transport.messages;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.transport.ProtocolException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;

/**
 * Message to indicate that the server is ready to receive requests.
 */
public class CredentialsMessage extends Message.Request
{
    public static final Message.Codec<CredentialsMessage> codec = new Message.Codec<CredentialsMessage>()
    {
        public CredentialsMessage decode(ChannelBuffer body)
        {
            CredentialsMessage msg = new CredentialsMessage();
            int count = body.readUnsignedShort();
            for (int i = 0; i < count; i++)
            {
                String key = CBUtil.readString(body);
                String value = CBUtil.readString(body);
                msg.credentials.put(key, value);
            }
            return msg;
        }

        public ChannelBuffer encode(CredentialsMessage msg)
        {
            ChannelBuffer cb = ChannelBuffers.dynamicBuffer();

            cb.writeShort(msg.credentials.size());
            for (Map.Entry<String, String> entry : msg.credentials.entrySet())
            {
                cb.writeBytes(CBUtil.stringToCB(entry.getKey()));
                cb.writeBytes(CBUtil.stringToCB(entry.getValue()));
            }
            return cb;
        }
    };

    public final Map<String, String> credentials = new HashMap<String, String>();

    public CredentialsMessage()
    {
        super(Message.Type.CREDENTIALS);
    }

    public ChannelBuffer encode()
    {
        return codec.encode(this);
    }

    public Message.Response execute(QueryState state)
    {
        // C* 1.2 for DSE 3.1 includes a backport of CASSANDRA-5545
        // to allow us to plugin in Kerberos authentication. However,
        // because we want to stay as compatible as possible with
        // Apache C* and the community tools/drivers etc, we don't
        // bump the protocol version, which means that a DSE server
        // can be configured with KerberosAuthenticator, but clients
        // may still send v1 CREDENTIALS messages. In such a case, the
        // Kerberos authentication would be bypassed & users could log
        // in with any credentials.
        // To prevent that, here we verify the IAuthenticator class &
        // reject the request if it is anything other than one of the 2
        // PasswordAuthenticator implementations.

        // An alternative would be to bump the protocol version to 2,
        // but
        // a) version 2 isn't complete yet - the SASL part should be
        // b) it would mean pulling in more feature that we don't want to
        // support yet & may cause issues for clients who don't need
        // kerberos and want to use the open source drivers.

        // This will cease to be an issue when we eventually catch up
        // to the protocol in C* - i.e. when DSE moves to C* 2.0

        // On top of that, this horrible hack to accomodate the internal
        // version of PasswordAuthenticator made me throw in my mouth
        // a little. DSP-1936 would remove the need for it.

        Class c = DatabaseDescriptor.getAuthenticator().getClass();
        if (! org.apache.cassandra.auth.PasswordAuthenticator.class.isAssignableFrom(c)
             || ! ( c.getName().equals("com.datastax.bdp.cassandra.auth.PasswordAuthenticator")
                    || c.equals(PasswordAuthenticator.class)))
        {
            return ErrorMessage.fromException(new AuthenticationException("Unsupported Authenticator " + c.getName()));
        }

        try
        {
            AuthenticatedUser user = DatabaseDescriptor.getAuthenticator().authenticate(credentials);
            state.getClientState().login(user);
            return new ReadyMessage();
        }
        catch (AuthenticationException e)
        {
            return ErrorMessage.fromException(e);
        }
    }

    @Override
    public String toString()
    {
        return "CREDENTIALS " + credentials;
    }
}
