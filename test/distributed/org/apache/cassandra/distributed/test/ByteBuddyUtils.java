/*
 * Copyright DataStax, Inc.
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

package org.apache.cassandra.distributed.test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.net.MessagingService;

import java.util.concurrent.Future;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

import static net.bytebuddy.matcher.ElementMatchers.named;
public class ByteBuddyUtils
{
    /**
     * Injection to set the current version of a node to DS 10.
     */
    public static class BB
    {
        public static void install(ClassLoader classLoader, int node)
        {
            // inject randomly first or second node to make sure it works if the node is a coordinator or replica
            if (node == ThreadLocalRandom.current().nextInt(1, 3))
            {
                new ByteBuddy().rebase(MessagingService.class)
                               .method(named("currentVersion"))
                               .intercept(MethodDelegation.to(BB.class))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static int currentVersion()
        {
            return MessagingService.VERSION_DS_10;
        }
    }

    /**
     * Injection to block SAI index building for 30 seconds.
     */
    public static class IndexBuildBlocker
    {
        public static void blockIndexBuilding(ClassLoader cl, int nodeNumber)
        {
            new ByteBuddy().rebase(StorageAttachedIndex.class)
                           .method(named("startInitialBuild"))
                           .intercept(MethodDelegation.to(IndexBuildInterceptor.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }
    }

    public static class IndexBuildInterceptor
    {
        @RuntimeType
        public static Future<?> intercept(@SuperCall Callable<Future<?>> zuper) throws Exception
        {
            Thread.sleep(30000); // 30 seconds delay
            return zuper.call();
        }
    }
}