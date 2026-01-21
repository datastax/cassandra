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
package org.apache.cassandra.repair.messages;

import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.RepairException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.nodes.NodeInfo;
import org.apache.cassandra.nodes.Nodes;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.utils.CassandraVersion;

import static org.apache.cassandra.net.MessageFlag.CALL_BACK_ON_FAILURE;

/**
 * Base class of all repair related request/response messages.
 *
 * @since 2.0
 */
public abstract class RepairMessage
{
    /**
     * If true, we will always consider remote nodes to support repair message timeouts,
     * and fail the repair if a response is not received on time.
     * Default: false, to preserve backward compatibility.
     */
    @VisibleForTesting
    protected static final String ALWAYS_CONSIDER_TIMEOUTS_SUPPORTED_PROPERTY = "cassandra.repair.always_consider_timeouts_supported";
    private static final boolean ALWAYS_CONSIDER_TIMEOUTS_SUPPORTED_DEFAULT = false;

    /**
     * The first C* version to support repair message timeouts.
     */
    @VisibleForTesting
    protected static final CassandraVersion SUPPORTS_TIMEOUTS = new CassandraVersion("4.0.7-SNAPSHOT");
    private static final Logger logger = LoggerFactory.getLogger(RepairMessage.class);
    public final RepairJobDesc desc;

    protected RepairMessage(RepairJobDesc desc)
    {
        this.desc = desc;
    }

    public interface RepairFailureCallback
    {
        void onFailure(Exception e);
    }

    public static void sendMessageWithFailureCB(RepairMessage request, Verb verb, InetAddressAndPort endpoint, RepairFailureCallback failureCallback)
    {
        RequestCallback<?> callback = new RequestCallback<Object>()
        {
            @Override
            public void onResponse(Message<Object> msg)
            {
                logger.info("[#{}] {} received by {}", request.desc.parentSessionId, verb, endpoint);
                // todo: at some point we should make repair messages follow the normal path, actually using this
            }

            @Override
            public boolean invokeOnFailure()
            {
                return true;
            }

            public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
            {
                logger.error("[#{}] {} failed on {}: {}", request.desc.parentSessionId, verb, from, failureReason);

                if (supportsTimeouts(from, request.desc.parentSessionId))
                    failureCallback.onFailure(new RepairException(request.desc, String.format("Got %s failure from %s: %s", verb, from, failureReason)));
            }
        };

        MessagingService.instance().sendWithCallback(Message.outWithFlag(verb, request, CALL_BACK_ON_FAILURE),
                                                     endpoint,
                                                     callback);
    }

    @VisibleForTesting
    protected static boolean supportsTimeouts(InetAddressAndPort from, UUID parentSessionId)
    {
        /*
         * In CNDB, repair services won't be added to the Nodes.peers() map, so there's no clear way
         * to check the version of the remove peer. This is the reason why a system property is introduced
         * to skip the version check, in case it's known that the deployed C* version supports repair message
         * timeouts.
         */
        if (areTimeoutsAlwaysSupported())
            return true;
        CassandraVersion remoteVersion = Nodes.peers().map(from, NodeInfo::getReleaseVersion, () -> null);
        if (remoteVersion != null && remoteVersion.compareTo(SUPPORTS_TIMEOUTS, true) >= 0)
            return true;
        logger.warn("[#{}] Not failing repair due to remote host {} not supporting repair message timeouts (version = {})", parentSessionId, from, remoteVersion);
        return false;
    }

    private static boolean areTimeoutsAlwaysSupported()
    {
        return Boolean.parseBoolean(System.getProperty(ALWAYS_CONSIDER_TIMEOUTS_SUPPORTED_PROPERTY, Boolean.toString(ALWAYS_CONSIDER_TIMEOUTS_SUPPORTED_DEFAULT)));
    }
}
