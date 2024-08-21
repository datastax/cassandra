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

package org.apache.cassandra.db.commitlog;

import java.util.concurrent.ExecutionException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.utils.concurrent.Future;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(BMUnitRunner.class)
public class CommitLogReplayerTest
{
    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }
    @Test
    @BMRules(rules = { @BMRule(name = "Fail applying mutation",
            targetClass = "org.apache.cassandra.concurrent.Stage",
            targetMethod = "submit",
            action = "return CompletableFuture.failedFuture(new RuntimeException(\"mutation failed\"));") } )
    public void testTrackingSegmentsWhenMutationFails()
    {
        CommitLogReplayer.MutationInitiator mutationInitiator = new CommitLogReplayer.MutationInitiator();
        CommitLogReplayer replayer = new CommitLogReplayer(CommitLog.instance, CommitLogPosition.NONE, null, CommitLogReplayer.ReplayFilter.create());
        CommitLogDescriptor descriptor = mock(CommitLogDescriptor.class);
        String failedSegment = "failedSegment";
        when(descriptor.fileName()).thenReturn(failedSegment);
        Future<Integer> mutationFuture = mutationInitiator.initiateMutation(mock(Mutation.class), descriptor, 0, 0, replayer);
        Assert.assertThrows(ExecutionException.class, () -> mutationFuture.get());
        Assert.assertTrue(!replayer.getSegmentWithInvalidOrFailedMutations().isEmpty());
        Assert.assertTrue(replayer.getSegmentWithInvalidOrFailedMutations().contains(failedSegment));
    }
}
