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
    public static class MessagingVersionSetter
    {
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

        @SuppressWarnings("unused")
        public static int currentVersion()
        {
            return MessagingService.VERSION_DS_10;
        }
    }
}