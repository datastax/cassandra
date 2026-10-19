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

package org.apache.cassandra.repair;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.MerkleTrees;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ValidationTaskTest
{
    @BeforeClass
    public static void initDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private ListAppender<ILoggingEvent> appender;

    @Before
    public void attachAppender()
    {
        appender = new ListAppender<>();
        appender.start();
        Logger logger = (Logger) LoggerFactory.getLogger(ValidationTask.class);
        logger.addAppender(appender);
        logger.setLevel(Level.ALL);
    }

    @After
    public void detachAppender()
    {
        Logger logger = (Logger) LoggerFactory.getLogger(ValidationTask.class);
        logger.detachAppender(appender);
        appender.stop();
    }

    private List<String> infoMessages()
    {
        return appender.list.stream()
                            .filter(e -> e.getLevel() == Level.INFO)
                            .map(ILoggingEvent::getFormattedMessage)
                            .collect(Collectors.toList());
    }

    private List<String> warnMessages()
    {
        return appender.list.stream()
                            .filter(e -> e.getLevel() == Level.WARN)
                            .map(ILoggingEvent::getFormattedMessage)
                            .collect(Collectors.toList());
    }

    @Test
    public void shouldDeactivateOnFailure() throws UnknownHostException
    {
        ValidationTask task = createTask();
        assertTrue(task.isActive());
        task.treesReceived(null);
        assertFalse(task.isActive());
    }

    @Test
    public void shouldIgnoreTreesWhenDeactivated() throws Exception
    {
        ValidationTask task = createTask();
        assertTrue(task.isActive());
        task.abort(new RuntimeException());
        assertFalse(task.isActive());
        task.treesReceived(new MerkleTrees(null));
        // REVIEW: setting null would cause NPEs in sync task, so it was never correct to set null
        assertTrue(task.isDone());
        assertFalse(task.isSuccess());
    }

    @Test
    public void shouldReleaseTreesOnAbort() throws Exception
    {
        ValidationTask task = createTask();
        assertTrue(task.isActive());

        IPartitioner partitioner = Murmur3Partitioner.instance;
        MerkleTrees trees = new MerkleTrees(partitioner);
        trees.addMerkleTree(128, new Range<>(partitioner.getMinimumToken(), partitioner.getMaximumToken()));
        task.treesReceived(trees);
        assertEquals(1, trees.size());
        
        // This relies on the fact that MerkleTrees clears its range -> tree map on release.
        task.abort(new RuntimeException());
        assertEquals(0, trees.size());
    }
    

    /**
     * run() must emit INFO containing parentSession and the target endpoint.
     * The actual network send is intercepted by the outbound sink so the test
     * stays self-contained without a running cluster.
     */
    @Test
    public void run_logsInfoWithParentSessionAndEndpoint() throws Exception
    {
        // Swallow the outbound VALIDATION_REQ so run() doesn't throw
        org.apache.cassandra.net.MessagingService.instance().outboundSink.add((msg, to) -> true);
        try
        {
            InetAddressAndPort endpoint = InetAddressAndPort.getByName("10.0.5.1");
            RepairJobDesc desc = makeDesc(endpoint);
            ValidationTask task = new ValidationTask(SharedContext.Global.instance, desc, endpoint, 0, PreviewKind.NONE);

            task.run();

            String msg = infoMessages().stream()
                                       .filter(m -> m.contains("Sending validation request"))
                                       .findFirst()
                                       .orElse("");

            assertFalse("run() must emit 'Sending validation request' INFO", msg.isEmpty());
            assertTrue("log must contain parentSessionId", msg.contains(desc.parentSessionId.toString()));
            assertTrue("log must contain target endpoint",  msg.contains(endpoint.toString()));
        }
        finally
        {
            org.apache.cassandra.net.MessagingService.instance().outboundSink.clear();
        }
    }

    /**
     * The INFO log from run() must not appear at WARN level.
     */
    @Test
    public void run_sendLogNotAtWarn() throws Exception
    {
        org.apache.cassandra.net.MessagingService.instance().outboundSink.add((msg, to) -> true);
        try
        {
            InetAddressAndPort endpoint = InetAddressAndPort.getByName("10.0.5.2");
            ValidationTask task = new ValidationTask(SharedContext.Global.instance,
                                                     makeDesc(endpoint), endpoint, 0, PreviewKind.NONE);
            task.run();

            assertTrue("run() send log must not appear at WARN",
                       warnMessages().stream().noneMatch(m -> m.contains("Sending validation request")));
        }
        finally
        {
            org.apache.cassandra.net.MessagingService.instance().outboundSink.clear();
        }
    }

    /**
     * treesReceived(null) must emit WARN containing parentSession and the endpoint.
     */
    @Test
    public void treesReceived_null_logsWarnWithParentSessionAndEndpoint() throws Exception
    {
        InetAddressAndPort endpoint = InetAddressAndPort.getByName("10.0.5.3");
        RepairJobDesc desc = makeDesc(endpoint);
        ValidationTask task = new ValidationTask(SharedContext.Global.instance, desc, endpoint, 0, PreviewKind.NONE);

        try { task.treesReceived(null); } catch (Exception ignored) { /* RepairException expected */ }

        String msg = warnMessages().stream()
                                   .filter(m -> m.contains("Validation failed on"))
                                   .findFirst()
                                   .orElse("");

        assertFalse("treesReceived(null) must emit 'Validation failed on' WARN", msg.isEmpty());
        assertTrue("log must contain parentSessionId", msg.contains(desc.parentSessionId.toString()));
        assertTrue("log must contain endpoint",        msg.contains(endpoint.toString()));
    }

    /**
     * treesReceived(null) WARN must not fire when trees are non-null (success path).
     */
    @Test
    public void treesReceived_success_noWarn() throws Exception
    {
        InetAddressAndPort endpoint = InetAddressAndPort.getByName("10.0.5.4");
        ValidationTask task = new ValidationTask(SharedContext.Global.instance,
                                                 makeDesc(endpoint), endpoint, 0, PreviewKind.NONE);

        task.treesReceived(new MerkleTrees(Murmur3Partitioner.instance));

        assertTrue("success path must not produce a WARN",
                   warnMessages().stream().noneMatch(m -> m.contains("Validation failed")));
    }

    private static RepairJobDesc makeDesc(InetAddressAndPort endpoint)
    {
        IPartitioner p = Murmur3Partitioner.instance;
        Range<Token> range = new Range<>(p.getMinimumToken(), p.getMaximumToken());
        return new RepairJobDesc(nextTimeUUID(), nextTimeUUID(),
                                 "ks_vt", endpoint.toString(),
                                 Collections.singletonList(range));
    }

    private ValidationTask createTask() throws UnknownHostException
    {
        InetAddressAndPort addressAndPort = InetAddressAndPort.getByName("127.0.0.1");
        RepairJobDesc desc = new RepairJobDesc(nextTimeUUID(), nextTimeUUID(), UUID.randomUUID().toString(), UUID.randomUUID().toString(), null);
        return new ValidationTask(SharedContext.Global.instance, desc, addressAndPort, 0, PreviewKind.NONE);
    }
}
