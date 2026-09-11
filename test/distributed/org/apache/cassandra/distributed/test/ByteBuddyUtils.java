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
import net.bytebuddy.implementation.bind.annotation.*;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.index.sai.StorageAttachedIndex;

import java.util.concurrent.Future;
import java.util.concurrent.Callable;

import static net.bytebuddy.matcher.ElementMatchers.named;
public class ByteBuddyUtils
{
    public static class IndexBuildBlocker
    {
        public static void blockIndexBuildingOnNode3(ClassLoader cl, int nodeNumber)
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
        public static Future<?> intercept(@Argument(0) ColumnFamilyStore cfs,
                                          @Argument(1) boolean isInitialBuild,
                                          @Argument(2) boolean isRebuilding,
                                          @SuperCall Callable<Future<?>> zuper) throws Exception
        {
            Thread.sleep(30000); // 30 seconds delay
            return zuper.call();
        }
    }

}
