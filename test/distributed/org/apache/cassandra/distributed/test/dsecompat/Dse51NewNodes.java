/*
 * Copyright IBM Corp.
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

package org.apache.cassandra.distributed.test.dsecompat;

import java.util.Map;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;

import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.service.DseUpgradeGuards;

import static net.bytebuddy.matcher.ElementMatchers.named;

/**
 * Nodes of this software that stand for upgraded DSE 5.1 nodes in the in-JVM tests, which cannot hold a DSE 5.1 node:
 * they run in {@code internode_compatibility_mode: DSE_5_1} with the storage settings of the mode from their first
 * start on, so that every sstable they write is a big {@code me} one, as on a node that ran DSE 5.1 before (a first
 * start without the mode would leave big {@code nb} sstables, which a start in the mode refuses). A node in the mode
 * refuses to bootstrap; an upgraded DSE 5.1 node has bootstrapped already, on DSE 5.1, so the refusal is lifted for
 * these nodes by {@link #install}, at every start (a later start has completed its bootstrap and is not refused
 * anyway), their only change to the code under test. The schema of the tests is created with {@code allow_ddl}, as the
 * schema is frozen in the mode.
 */
public final class Dse51NewNodes
{
    private Dse51NewNodes()
    {
    }

    /** The settings of the mode: the internode and storage modes, the big format and numeric generations. */
    public static void configure(IInstanceConfig config)
    {
        config.set("internode_compatibility_mode", "DSE_5_1")
              .set("storage_compatibility_mode", "DSE_5_1")
              .set("uuid_sstable_identifiers_enabled", false)
              .set("sstable", Map.of("selected_format", "big"));
    }

    /** The instance initializer that lifts the refusal of a start in the mode that has not bootstrapped yet. */
    public static void install(ClassLoader classLoader, Integer node)
    {
        new ByteBuddy().rebase(DseUpgradeGuards.class)
                       .method(named("refuseAtStartupWhileUpgrading"))
                       .intercept(MethodDelegation.to(Dse51NewNodes.class))
                       .make()
                       .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
    }

    /** In place of {@code DseUpgradeGuards.refuseAtStartupWhileUpgrading}: nothing is refused. */
    @SuppressWarnings("unused")
    public static void refuseAtStartupWhileUpgrading(String operation)
    {
    }
}
