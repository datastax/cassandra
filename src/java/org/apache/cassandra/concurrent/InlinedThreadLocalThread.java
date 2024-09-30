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
package org.apache.cassandra.concurrent;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.io.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.FastThreadLocalThread;
import net.nicoulaj.compilecommand.annotations.Inline;
import org.jctools.util.UnsafeAccess;
import org.jctools.util.UnsafeRefArrayAccess;

/**
 * An intermediate class for forcing the position and ordering of reference fields which we intend to use as the backing
 * storage for {@link InlinedThreadLocal} instances.
 */
@SuppressWarnings("unused")
abstract class InlinedThreadLocalThreadLocalRefs extends FastThreadLocalThread
{
    // DO NOT ADD FURTHER FIELDS TO THIS CLASS OTHER THAN REFS FOR TL STORAGE
    // Static fields are fine ;-), See note below after ref fields.
    static final int MAX_LOCALS = 70;
    static final long REF_BASE_OFFSET;
    private static final Logger LOGGER = LoggerFactory.getLogger(InlinedThreadLocalThread.class);
    private static final long REF_SHIFT = UnsafeRefArrayAccess.REF_ELEMENT_SHIFT;
    private static final AtomicInteger FIRST_UNUSED_INDEX = new AtomicInteger();

    public static int maxLocalsOverflow;

    static
    {
        long minOffset = Long.MAX_VALUE;
        // fields order is not guaranteed
        for (int i = 0; i < MAX_LOCALS; i++)
        {
            String fieldName = "a" + i;
            long fieldOffset = UnsafeAccess.fieldOffset(InlinedThreadLocalThreadLocalRefs.class, fieldName);
            minOffset = Math.min(fieldOffset, minOffset);
        }
        REF_BASE_OFFSET = minOffset;
    }

    // Add further refs as needed and adjust the MAX_LOCALS constant accordingly
    private Object a0, a1, a2, a3, a4, a5, a6, a7, a8, a9;
    private Object a10, a11, a12, a13, a14, a15, a16, a17, a18, a19;
    private Object a20, a21, a22, a23, a24, a25, a26, a27, a28, a29;
    private Object a30, a31, a32, a33, a34, a35, a36, a37, a38, a39;
    private Object a40, a41, a42, a43, a44, a45, a46, a47, a48, a49;
    private Object a50, a51, a52, a53, a54, a55, a56, a57, a58, a59;
    private Object a60, a61, a62, a63, a64, a65, a66, a67, a68, a69;
    // DO NOT ADD FURTHER FIELDS TO THIS CLASS OTHER THAN REFS FOR TL REF STORAGE
    // Adding further fields may cause these fields to mingle with the above allocate fields in term of their relative
    // position. The get/set methods we use below rely on the reference fields above being laid out as a contiguous
    // space.

    InlinedThreadLocalThreadLocalRefs()
    {
        super();
    }

    InlinedThreadLocalThreadLocalRefs(Runnable target)
    {
        super(target);
    }

    InlinedThreadLocalThreadLocalRefs(ThreadGroup group, Runnable target)
    {
        super(group, target);
    }

    InlinedThreadLocalThreadLocalRefs(String name)
    {
        super(name);
    }

    InlinedThreadLocalThreadLocalRefs(ThreadGroup group, String name)
    {
        super(group, name);
    }

    InlinedThreadLocalThreadLocalRefs(Runnable target, String name)
    {
        super(target, name);
    }

    InlinedThreadLocalThreadLocalRefs(ThreadGroup group, Runnable target, String name)
    {
        super(group, target, name);
    }

    public InlinedThreadLocalThreadLocalRefs(ThreadGroup group, Runnable target, String name, long stackSize)
    {
        super(group, target, name, stackSize);
    }

    static long getRefOffset(long index)
    {
        if (index > MAX_LOCALS)
        {
            throw new IllegalStateException("FIRST_UNUSED_INDEX:" + index + " exceeds max: " + MAX_LOCALS);
        }
        return REF_BASE_OFFSET + (index << REF_SHIFT);
    }

    static long nextRefIndex()
    {
        int currIndex;
        do
        {
            currIndex = FIRST_UNUSED_INDEX.get();
            if (currIndex == MAX_LOCALS)
            {
                maxLocalsOverflow++;
                return -1;
            }
        }
        while (!FIRST_UNUSED_INDEX.compareAndSet(currIndex, currIndex + 1));
        return currIndex;
    }

    @VisibleForTesting
    static void resetIndexForTestingOnly()
    {
        // ONLY FOR TESTING!!!!
        FIRST_UNUSED_INDEX.set(0);
    }

    @Inline
    Object getThreadLocalRef(long offset)
    {
        return UnsafeAccess.UNSAFE.getObject(this, offset);
    }

    @Inline
    void setThreadLocalRef(long offset, Object v)
    {
        UnsafeAccess.UNSAFE.putObject(this, offset, v);
    }

    void cleanupOnExit()
    {
        for (int i = 0; i < MAX_LOCALS; i++)
        {
            long offset = getRefOffset(i);
            Object threadLocal = getThreadLocalRef(offset);
            if (threadLocal == null)
            {
                continue;
            }
            // Maybe a pointless gesture, but might prevent GC nepotism (where dead objects in old gen keep young
            // objects they refer to alive until an old gen GC removes them). In all likelihood the TL object have
            // similar lifecycle to the thread and are therefore in the same generation.
            setThreadLocalRef(offset, null);

            // Rather than wait for the GC to pick these up, clean them now (particularly important for offheap buffers).
            if (threadLocal instanceof ByteBuffer)
            {
                cleanupByteBuffer((ByteBuffer) threadLocal);
            }
            else if (threadLocal instanceof Closeable)
            {
                cleanupCloseable((Closeable) threadLocal);
            }
        }
    }

    private void cleanupCloseable(Closeable threadLocal)
    {
        try
        {
            threadLocal.close();
        }
        catch (IOException e)
        {
            LOGGER.error("Failed to close thread local resource on thread exit", e);
        }
    }

    private void cleanupByteBuffer(ByteBuffer threadLocal)
    {
        try
        {
            FileUtils.clean(threadLocal);
        }
        catch (Throwable e)
        {
            LOGGER.error("Failed to clean DirectBuffer thread local resource on thread exit", e);
        }
    }

}

public class InlinedThreadLocalThread extends InlinedThreadLocalThreadLocalRefs
{

    public InlinedThreadLocalThread()
    {
        super();
    }

    public InlinedThreadLocalThread(Runnable target)
    {
        super(target);
    }

    public InlinedThreadLocalThread(ThreadGroup group, Runnable target)
    {
        super(group, target);
    }

    public InlinedThreadLocalThread(String name)
    {
        super(name);
    }

    public InlinedThreadLocalThread(ThreadGroup group, String name)
    {
        super(group, name);
    }

    public InlinedThreadLocalThread(Runnable target, String name)
    {
        super(target, name);
    }

    public InlinedThreadLocalThread(ThreadGroup group, Runnable target, String name)
    {
        super(group, target, name);
    }

    public InlinedThreadLocalThread(ThreadGroup group, Runnable target, String name, long stackSize)
    {
        super(group, target, name, stackSize);
    }

    @Override
    public void run()
    {
        try
        {
            super.run();
        }
        finally
        {
            cleanupOnExit();
        }
    }
}
