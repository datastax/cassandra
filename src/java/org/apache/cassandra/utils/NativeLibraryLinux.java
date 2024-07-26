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

package org.apache.cassandra.utils;

import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jna.Native;
import com.sun.jna.Pointer;

/**
 * A {@code NativeLibraryWrapper} implementation for Linux.
 * <p>
 * When JNA is initialized, all methods that have the 'native' keyword
 * will be attmpted to be linked against. As Java doesn't have the equivalent
 * of a #ifdef, this means if a native method like posix_fadvise is defined in the
 * class but not available on the target operating system (e.g.
 * posix_fadvise is not availble on Darwin/Mac) this will cause the entire
 * initial linking and initialization of JNA to fail. This means other
 * native calls that are supported on that target operating system will be
 * unavailable simply because of one native defined method not supported
 * on the runtime operating system.
 * @see org.apache.cassandra.utils.NativeLibraryWrapper
 * @see INativeLibrary
 */
@Shared
public class NativeLibraryLinux implements NativeLibraryWrapper
{
    private static boolean available;

    private static final Logger logger = LoggerFactory.getLogger(NativeLibraryLinux.class);

    static
    {
        try
        {
            Native.register(com.sun.jna.NativeLibrary.getInstance("c", Collections.emptyMap()));
            available = true;
        }
        catch (NoClassDefFoundError e)
        {
            logger.warn("JNA not found. Native methods will be disabled.");
        }
        catch (UnsatisfiedLinkError e)
        {
            logger.error("Failed to link the C library against JNA. Native methods will be unavailable.", e);
        }
        catch (NoSuchMethodError e)
        {
            logger.warn("Obsolete version of JNA present; unable to register C library. Upgrade to JNA 3.2.7 or later");
        }
    }

    private static native int mlockall(int flags);
    private static native int munlockall();
    private static native int fcntl(int fd, int command, long flags);
    private static native int posix_fadvise(int fd, long offset, int len, int flag);
    private static native int posix_madvise(Pointer addr, long length, int advice);
    private static native int open(String path, int flags);
    private static native int fsync(int fd);
    private static native int close(int fd);
    private static native Pointer strerror(int errnum);
    private static native long getpid();

    private void throwNativeError() throws NativeError
    {
        var errno = Native.getLastError();
        throw new NativeError(strerror(errno).getString(0), errno);
    }

    @Override
    public void callMlockall(int flags) throws NativeError
    {
        int r = mlockall(flags);
        if (r != 0)
            throwNativeError();
    }

    @Override
    public void callMunlockall() throws NativeError
    {
        int r = munlockall();
        if (r != 0)
            throwNativeError();
    }

    @Override
    public int callFcntl(int fd, int command, long flags) throws NativeError
    {
        int r = fcntl(fd, command, flags);
        if (r < 0)
            throwNativeError();
        return r;
    }

    @Override
    public void callPosixFadvise(int fd, long offset, int len, int flag) throws NativeError
    {
        int r = posix_fadvise(fd, offset, len, flag);
        if (r != 0)
            throwNativeError();
    }

    @Override
    public void callPosixMadvise(Pointer addr, long length, int advice) throws NativeError
    {
        int r = posix_madvise(addr, length, advice);
        if (r != 0)
            throwNativeError();
    }

    @Override
    public int callOpen(String path, int flags) throws NativeError
    {
        int r = open(path, flags);
        if (r < 0)
            throwNativeError();
        return r;
    }

    @Override
    public void callFsync(int fd) throws NativeError
    {
        int r = fsync(fd);
        if (r != 0)
            throwNativeError();
    }

    @Override
    public void callClose(int fd) throws NativeError
    {
        int r = close(fd);
        if (r != 0)
            throwNativeError();
    }

    @Override
    public long callGetpid() throws NativeError
    {
        long r = getpid();
        if (r < 0)
            throwNativeError();
        return r;
    }

    @Override
    public boolean isAvailable()
    {
        return available;
    }
}
