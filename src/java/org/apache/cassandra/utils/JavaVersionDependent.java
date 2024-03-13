/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.utils;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

import sun.nio.ch.DirectBuffer;

import org.apache.commons.lang3.SystemUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JavaVersionDependent
{

    private static final long maxDirectMemory;

    static
    {
        try
        {
            Class<?> cVM = Class.forName("sun.misc.VM");
            maxDirectMemory = (Long) cVM.getDeclaredMethod("maxDirectMemory").invoke(null);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        try
        {
            Class<?> clsDirectBuffer = Class.forName("sun.nio.ch.DirectBuffer");
            Method mDirectBufferCleaner = clsDirectBuffer.getMethod("cleaner");
            MethodHandle mhDirectBufferCleaner = MethodHandles.lookup().unreflect(mDirectBufferCleaner);
            Method mCleanerClean = mDirectBufferCleaner.getReturnType().getMethod("clean");
            MethodHandle mhCleanerClean = MethodHandles.lookup().unreflect(mCleanerClean);

            if (mhDirectBufferCleaner == null || mhCleanerClean == null)
                throw new RuntimeException("Cleaner not available");

            ByteBuffer buf = ByteBuffer.allocateDirect(1);
            invokeCleaner(buf);
        }
        catch (Throwable t)
        {
            final Logger logger = LoggerFactory.getLogger(JavaVersionDependent.class);
            logger.error("FATAL: Cannot initialize optimized memory deallocator. Some data, both in-memory and on-disk, may live longer due to garbage collection.", t);
            JVMStabilityInspector.inspectThrowable(t);
            throw new RuntimeException(t);
        }
    }

    public static boolean isJava11()
    {
        return false;
    }

    public static void doCheck()
    {
        if (!System.getProperty("java.version").startsWith("1.8.0"))
            throw new RuntimeException("Java 8 required, but running " + System.getProperty("java.version"));
    }

    public static void onSpinWait(Supplier<Object> dummy)
    {
        dummy.get();
    }

    /**
     * Hack to prevent the ugly "illegal access" warnings in Java 9+ like the following.
     */
    public static void preventIllegalAccessWarnings()
    {
        // no-op for Java 8
    }


    public static long getProcessID(Process process)
    {
        if (!SystemUtils.IS_OS_UNIX)
        {
            throw new UnsupportedOperationException("This method can be run only on Unix-like OS");
        }
        try
        {
            Class cls = Class.forName("java.lang.UNIXProcess");
            Field field = FieldUtils.getDeclaredField(cls, "pid", true);
            return field.getInt(process);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to get PID of a process " + process, e);
        }
    }

    /**
     * Invokes the cleaner on a direct byte buffer. Prefer calling {@link org.apache.cassandra.io.util.FileUtils#clean(ByteBuffer)}
     * instead this method. This method is not capable of cleaning the parents of sliced byte buffers, for example the
     * parents of buffers that were page aligned will not be cleaned causing a memory leak.
     *
     * @param directByteBuffer - the buffer to clean
     * @return true if it was possible to clear the buffer memory, false otherwise
     */
    public static boolean invokeCleaner(ByteBuffer directByteBuffer)
    {
        if (directByteBuffer == null || !directByteBuffer.isDirect())
            return false;

        DirectBuffer db = (DirectBuffer) directByteBuffer;
        if (db.cleaner() != null)
        {
            db.cleaner().clean();
            return true;
        }

        return false;
    }

    public static long maxDirectMemory()
    {
        return maxDirectMemory;
    }
}
