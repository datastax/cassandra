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

package org.apache.cassandra.service;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_MUTATOR_CLASS;

/**
 * Provides an instance of {@link Mutator} that facilitates mutation writes for standard mutations, unlogged batches,
 * counters and paxos commits (LWT)s.
 * <br/>
 * An implementation may choose to fallback to the default implementation ({@link StorageProxy.DefaultMutator})
 * obtained via {@link #getDefaultMutator()}.
 */
public abstract class MutatorProvider
{
    private static final Logger logger = LoggerFactory.getLogger(MutatorProvider.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    // public so that the paxos engines (org.apache.cassandra.service.paxos) can reach the
    // installed singleton for Mutator.onCasCommit notifications without re-constructing it
    public static final Mutator instance = getCustomOrDefault();

    /**
     * Notifies the installed Mutator of a commit dispatch (see {@link Mutator#onCasCommit}),
     * containing any exception a misbehaving implementation throws: a notification failure must
     * never abort the paxos operation, serial read or repair that is dispatching the commit.
     */
    public static void notifyCasCommit(Commit committed, ConsistencyLevel consistencyLevel, Mutator.CasCommitOrigin origin)
    {
        try
        {
            instance.onCasCommit(committed, consistencyLevel, origin);
        }
        catch (Throwable t)
        {
            // Let fatal errors (OOM etc.) reach the JVM failure policy before we swallow.
            JVMStabilityInspector.inspectThrowable(t);
            noSpamLogger.warn("Custom mutator onCasCommit({}) failed; ignoring", origin, t);
        }
    }

    /**
     * Notifies the installed Mutator of the terminal outcome of a previously-announced commit (see
     * {@link Mutator#onCasCommitCompleted}), containing any exception a misbehaving implementation
     * throws: a notification failure must never abort the paxos operation, serial read or repair.
     */
    public static void notifyCasCommitCompleted(Commit committed, ConsistencyLevel consistencyLevel,
                                                Mutator.CasCommitOrigin origin, Mutator.CasCommitOutcome outcome)
    {
        try
        {
            instance.onCasCommitCompleted(committed, consistencyLevel, origin, outcome);
        }
        catch (Throwable t)
        {
            // Let fatal errors (OOM etc.) reach the JVM failure policy before we swallow.
            JVMStabilityInspector.inspectThrowable(t);
            noSpamLogger.warn("Custom mutator onCasCommitCompleted({}, {}) failed; ignoring", origin, outcome, t);
        }
    }

    public static Mutator getCustomOrDefault()
    {
        if (CUSTOM_MUTATOR_CLASS.isPresent())
        {
            return FBUtilities.construct(CUSTOM_MUTATOR_CLASS.getString(),
                                         "custom mutator class (set with " + CUSTOM_MUTATOR_CLASS.getKey() + ")");
        }
        else
        {
            return getDefaultMutator();
        }
    }

    public static Mutator getDefaultMutator()
    {
        return new StorageProxy.DefaultMutator();
    }
}
