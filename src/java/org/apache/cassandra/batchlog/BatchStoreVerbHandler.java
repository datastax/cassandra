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
package org.apache.cassandra.batchlog;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.WriteResponse;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.UUIDGen;

public final class BatchStoreVerbHandler implements IVerbHandler<Batch>
{
    private static final Logger logger = LoggerFactory.getLogger(BatchStoreVerbHandler.class);

    public void doVerb(MessageIn<Batch> message, int id)
    {
        if (Math.abs(System.currentTimeMillis() - UUIDGen.getAdjustedTimestamp(message.payload.id)) > 2000L)
            logger.warn("Received batchlog store with timestamp {} from {}, which is at least two seconds off our system clock {}",
                        new Date(UUIDGen.getAdjustedTimestamp(message.payload.id)),
                        message.from,
                        new Date(System.currentTimeMillis()));
        BatchlogManager.store(message.payload);
        MessagingService.instance().sendReply(WriteResponse.createMessage(), id, message.from);
    }
}
