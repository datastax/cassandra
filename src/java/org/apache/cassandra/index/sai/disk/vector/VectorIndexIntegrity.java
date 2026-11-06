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

package org.apache.cassandra.index.sai.disk.vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * Integrity violations in the vector graph merge path are fatal to the daemon, by policy.
 *
 * <p>An empty vector index on an sstable that has rows is not a recoverable condition here. Left
 * alone it is silent: the sstable carries a completion marker and no graph, queries simply do not
 * see its vectors, and the only later symptom is every compaction that inherits it failing the
 * ordinal-identity join ("no merge source segments") one round later — which was observed on
 * 2026-08-24 as a 46% merge-failure rate whose cause had happened hours earlier and logged
 * nothing. A failed compaction is retried and the node carries on, so that signal is easy to miss
 * and the damage keeps compounding.
 *
 * <p>So the two places that can observe the condition — a merge about to <em>produce</em> such a
 * segment, and a merge about to <em>consume</em> one as input — stop the process instead. The
 * operator finds a dead node and an ERROR with this class's prefix at the top of the log, which is
 * the point: an unmissable signal at the moment the cause is still in view.
 *
 * <p>Known cost of the policy: an sstable whose vector column is null on every row also carries
 * an empty index, legitimately, and is indistinguishable on disk. On such a table this abort
 * fires for a non-bug. The workloads this fork targets index every row; if that changes, the
 * input-side check needs a row-level fact it does not have today.
 *
 * <p>Note {@code systemd}'s {@code Restart=on-failure} turns a persistent bad input into a crash
 * loop (exit 100 is a failure). That is loud, which is the intent, but consider
 * {@code RestartPreventExitStatus=100} on the unit so the node stays down for inspection.
 */
public final class VectorIndexIntegrity
{
    private static final Logger logger = LoggerFactory.getLogger(VectorIndexIntegrity.class);

    public static final String PREFIX = "VECTOR INDEX INTEGRITY ABORT";

    private VectorIndexIntegrity()
    {
    }

    /**
     * Logs {@code message} at ERROR under {@link #PREFIX}, asks the JVM to exit (via
     * {@link JVMStabilityInspector#killCurrentJVM}, so tests can substitute a killer), and returns
     * an exception for the caller to throw. Callers write {@code throw VectorIndexIntegrity.abort(...)}
     * so control flow is explicit even when a test killer declines to exit.
     */
    public static IllegalStateException abort(String message)
    {
        IllegalStateException cause = new IllegalStateException(PREFIX + ": " + message);
        logger.error("{}: {} — stopping the daemon so this is not missed. See VectorIndexIntegrity for the policy.",
                     PREFIX, message);
        JVMStabilityInspector.killCurrentJVM(cause, false);
        return cause;
    }
}
