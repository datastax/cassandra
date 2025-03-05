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

package org.apache.cassandra.distributed.test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.net.MessagingService;

import static net.bytebuddy.matcher.ElementMatchers.named;

public class ByteBuddyUtils
{
    /**
     * Utility class for modifying the messaging protocol version during tests.
     * Uses ByteBuddy to intercept and override the messaging version specifically for node 1.
     */
    public static class MessagingVersionSetter
    {
        /**
         * Forces node 1 to use Messaging Service version DS_10 by intercepting the currentVersion() method.
         * This is useful for testing backward compatibility and version-specific behavior.
         *
         * @param classLoader The classloader to use for loading the modified class
         * @param node The node number to apply this modification to (only applies to node 1)
         */
        public static void setDS10OnNode1(ClassLoader classLoader, int node)
        {
            if (node == 1)
            {
                new ByteBuddy().rebase(MessagingService.class)
                               .method(named("currentVersion"))
                               .intercept(MethodDelegation.to(MessagingVersionSetter.class))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        /**
         * Replacement implementation for MessagingService.currentVersion()
         * Always returns VERSION_DS_10 when called.
         *
         * @return MessagingService.VERSION_DS_10
         */
        @SuppressWarnings("unused")
        public static int currentVersion()
        {
            return MessagingService.VERSION_DS_10;
        }
    }
}