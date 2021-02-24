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

package org.apache.cassandra.index.sai.utils;

import java.io.Closeable;

import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.SharedCloseable;

public class SharedTidy implements RefCounted.Tidy
{
    private Ref<?>[] references;
    private SharedCloseable[] sharedCloseables;
    private Closeable[] closeables;

    public SharedTidy add(Ref<?>...references)
    {
        this.references = references;
        return this;
    }

    public SharedTidy add(SharedCloseable...sharedCloseables)
    {
        this.sharedCloseables = sharedCloseables;
        return this;
    }

    public SharedTidy add(Closeable...closeables)
    {
        this.closeables = closeables;
        return this;
    }

    @Override
    public void tidy() throws Exception
    {
        Throwable t = null;
        if (references != null)
            for (int index = 0; index < references.length; index++)
                t = references[index].ensureReleased(t);
        if (sharedCloseables != null)
            for (int index = 0; index < sharedCloseables.length; index++)
                t = Throwables.close(t, sharedCloseables[index]);
        if (closeables != null)
            for (int index = 0; index < closeables.length; index++)
                t = Throwables.close(t, closeables[index]);

        Throwables.maybeFail(t);
    }

    @Override
    public String name()
    {
        return null;
    }
}
