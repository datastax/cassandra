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

import java.io.FileDescriptor;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.MappedByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jna.Native;
import com.sun.jna.Pointer;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.utils.NativeLibraryWrapper.NativeError;

import static org.apache.cassandra.config.CassandraRelevantProperties.OS_ARCH;
import static org.apache.cassandra.config.CassandraRelevantProperties.OS_NAME;
import static org.apache.cassandra.utils.INativeLibrary.OSType.AIX;
import static org.apache.cassandra.utils.INativeLibrary.OSType.LINUX;
import static org.apache.cassandra.utils.INativeLibrary.OSType.MAC;
import static org.apache.cassandra.utils.INativeLibrary.OSType.WINDOWS;

public class NativeLibrary implements INativeLibrary
{
    private static final Logger logger = LoggerFactory.getLogger(NativeLibrary.class);

    public static final OSType osType;

    private static final int MCL_CURRENT;
    private static final int MCL_FUTURE;

    private static final int ENOMEM = 12;

    private static final int F_GETFL   = 3;  /* get file status flags */
    private static final int F_SETFL   = 4;  /* set file status flags */
    private static final int F_NOCACHE = 48; /* Mac OS X specific flag, turns cache on/off */
    private static final int O_DIRECT  = 040000; /* fcntl.h */
    private static final int O_RDONLY  = 00000000; /* fcntl.h */

    private static final int POSIX_FADV_NORMAL     = 0; /* fadvise.h */
    private static final int POSIX_FADV_RANDOM     = 1; /* fadvise.h */
    private static final int POSIX_FADV_SEQUENTIAL = 2; /* fadvise.h */
    private static final int POSIX_FADV_WILLNEED   = 3; /* fadvise.h */
    private static final int POSIX_FADV_DONTNEED   = 4; /* fadvise.h */
    private static final int POSIX_FADV_NOREUSE    = 5; /* fadvise.h */

    private static final int MADV_NORMAL     = 0; /* mman.h */
    private static final int MADV_RANDOM     = 1; /* mman.h */
    private static final int MADV_SEQUENTIAL = 2; /* mman.h */
    private static final int MADV_WILLNEED   = 3; /* mman.h */
    private static final int MADV_DONTNEED   = 4; /* mman.h */

    private static final NativeLibraryWrapper wrappedLibrary;
    private static boolean jnaLockable = false;

    private static final Field FILE_DESCRIPTOR_FD_FIELD;
    private static final Field FILE_CHANNEL_FD_FIELD;
    private static final Field FILE_ASYNC_CHANNEL_FD_FIELD;

    static
    {
        FILE_DESCRIPTOR_FD_FIELD = FBUtilities.getProtectedField(FileDescriptor.class, "fd");
        try
        {
            FILE_CHANNEL_FD_FIELD = FBUtilities.getProtectedField(Class.forName("sun.nio.ch.FileChannelImpl"), "fd");
            FILE_ASYNC_CHANNEL_FD_FIELD = FBUtilities.getProtectedField(Class.forName("sun.nio.ch.AsynchronousFileChannelImpl"), "fdObj");
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException(e);
        }

        // detect the OS type the JVM is running on and then set the CLibraryWrapper
        // instance to a compatable implementation of CLibraryWrapper for that OS type
        osType = getOsType();
        switch (osType)
        {
            case MAC: wrappedLibrary = new NativeLibraryDarwin(); break;
            case WINDOWS: wrappedLibrary = new NativeLibraryWindows(); break;
            case LINUX:
            case AIX:
            case OTHER:
            default: wrappedLibrary = new NativeLibraryLinux();
        }

        if (OS_ARCH.getString().toLowerCase().contains("ppc"))
        {
            if (osType == LINUX)
            {
               MCL_CURRENT = 0x2000;
               MCL_FUTURE = 0x4000;
            }
            else if (osType == AIX)
            {
                MCL_CURRENT = 0x100;
                MCL_FUTURE = 0x200;
            }
            else
            {
                MCL_CURRENT = 1;
                MCL_FUTURE = 2;
            }
        }
        else
        {
            MCL_CURRENT = 1;
            MCL_FUTURE = 2;
        }
    }

    NativeLibrary() {}

    /**
     * @return the detected OSType of the Operating System running the JVM using crude string matching
     */
    private static OSType getOsType()
    {
        String osName = OS_NAME.getString().toLowerCase();
        if  (osName.contains("linux"))
            return LINUX;
        else if (osName.contains("mac"))
            return MAC;
        else if (osName.contains("windows"))
            return WINDOWS;

        logger.warn("the current operating system, {}, is unsupported by cassandra", osName);
        if (osName.contains("aix"))
            return AIX;
        else
            // fall back to the Linux impl for all unknown OS types until otherwise implicitly supported as needed
            return LINUX;
    }

    @Override
    public boolean isOS(INativeLibrary.OSType type)
    {
        return osType == type;
    }

    @Override
    public boolean isAvailable()
    {
        return wrappedLibrary.isAvailable();
    }

    @Override
    public boolean jnaMemoryLockable()
    {
        return jnaLockable;
    }

    @Override
    public void tryMlockall()
    {
        try
        {
            wrappedLibrary.callMlockall(MCL_CURRENT);
            jnaLockable = true;
            logger.info("JNA mlockall successful");
        }
        catch (NativeError e)
        {
            if (e.getErrno() == ENOMEM && osType == LINUX)
            {
                logger.warn("Unable to lock JVM memory (ENOMEM)."
                        + " This can result in part of the JVM being swapped out, especially with mmapped I/O enabled."
                        + " Increase RLIMIT_MEMLOCK.");
            }
            else if (osType != MAC)
            {
                // OS X allows mlockall to be called, but always returns an error
                logger.error("Unknown mlockall error", e);
            }
        }
    }

    @Override
    public void trySkipCache(File f, long offset, long len)
    {
        if (!f.exists())
            return;

        try (FileInputStreamPlus fis = new FileInputStreamPlus(f))
        {
            trySkipCache(getfd(fis.getChannel()), offset, len, f.path());
        }
        catch (IOException e)
        {
            logger.error("Could not open file to skip cache", e);
        }
    }

    @Override
    public void trySkipCache(int fd, long offset, long len, String fileName)
    {
        if (len == 0)
            trySkipCache(fd, 0, 0, fileName);

        while (len > 0)
        {
            int sublen = (int) Math.min(Integer.MAX_VALUE, len);
            trySkipCache(fd, offset, sublen, fileName);
            len -= sublen;
            offset -= sublen;
        }
    }

    @Override
    public void trySkipCache(int fd, long offset, int len, String fileName)
    {
        if (fd < 0)
            return;

        try
        {
            wrappedLibrary.callPosixFadvise(fd, offset, len, POSIX_FADV_DONTNEED);
        }
        catch (NativeError e)
        {
            NoSpamLogger.log(logger,
                             NoSpamLogger.Level.ERROR,
                             10,
                             TimeUnit.MINUTES,
                             "Failed trySkipCache on file: {} Error: " + e.getMessage(),
                             fileName);
        }
    }

    /**
     * @param buffer
     * @param length
     * @param filename -- source file backing buffer; logged on error
     * <p>
     * adviseRandom works even on buffers that are not aligned to page boundaries (which is the
     * common case for how MmappedRegions is used).
     */
    public void adviseRandom(MappedByteBuffer buffer, long length, String filename) {
        assert buffer != null;

        var rawAddress = Native.getDirectBufferPointer(buffer);
        // align to the nearest lower page boundary
        var alignedAddress = new Pointer(Pointer.nativeValue(rawAddress) & -PageAware.PAGE_SIZE);
        // we want to advise the whole buffer, so if the aligned address is lower than the raw one,
        // we need to pad the length accordingly.  (we do not need to align `length`, Linux
        // takes care of rounding it up for us.)
        length += Pointer.nativeValue(rawAddress) - Pointer.nativeValue(alignedAddress);

        try
        {
            wrappedLibrary.callPosixMadvise(alignedAddress, length, MADV_RANDOM);
        }
        catch (NativeError e)
        {
            NoSpamLogger.log(logger,
                             NoSpamLogger.Level.ERROR,
                             10,
                             TimeUnit.MINUTES,
                             "Failed madvise on file: {}. Error: " + e.getMessage(),
                             filename);
        }
    }

    @Override
    public int tryFcntl(int fd, int command, int flags)
    {
        try
        {
            return wrappedLibrary.callFcntl(fd, command, flags);
        }
        catch (UnsatisfiedLinkError e)
        {
            // Unsupported on this platform
        }
        catch (NativeError e)
        {
            logger.error("fcntl({}, {}, {}) failed, error {}", fd, command, flags, e.getMessage());
        }
        return -1;
    }

    @Override
    public int tryOpenDirectory(File file)
    {
        return tryOpenDirectory(file.path());
    }

    @Override
    public int tryOpenDirectory(String path)
    {
        try
        {
            return wrappedLibrary.callOpen(path, O_RDONLY);
        }
        catch (UnsatisfiedLinkError e)
        {
            // Unsupported on this platform
        }
        catch (NativeError e)
        {
            logger.error("open({}, O_RDONLY) failed, error {}", path, e.getMessage());
        }
        return -1;
    }

    @Override
    public void trySync(int fd)
    {
        if (fd == -1)
            return;

        try
        {
            wrappedLibrary.callFsync(fd);
        }
        catch (NativeError e)
        {
            String errMsg = String.format("fsync(%s) failed, error %s", fd, e.getMessage());
            throw new FSWriteError(e, errMsg);
        }
    }

    @Override
    public void tryCloseFD(int fd)
    {
        if (fd == -1)
            return;

        try
        {
            wrappedLibrary.callClose(fd);
        }
        catch (NativeError e)
        {
            String errMsg = String.format("close(%d) failed, error %s", fd, e.getMessage());
            throw new FSWriteError(e, errMsg);
        }
    }

    @Override
    public int getfd(AsynchronousFileChannel channel)
    {
        try
        {
            return getfd((FileDescriptor) FILE_ASYNC_CHANNEL_FD_FIELD.get(channel));
        }
        catch (IllegalArgumentException|IllegalAccessException e)
        {
            logger.error("Unable to read fd field from FileChannel");
        }
        return -1;
    }

    @Override
    public FileDescriptor getFileDescriptor(AsynchronousFileChannel channel)
    {
        try
        {
            return (FileDescriptor) FILE_ASYNC_CHANNEL_FD_FIELD.get(channel);
        }
        catch (IllegalArgumentException | IllegalAccessException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getfd(FileChannel channel)
    {
        try
        {
            return getfd((FileDescriptor)FILE_CHANNEL_FD_FIELD.get(channel));
        }
        catch (IllegalArgumentException|IllegalAccessException e)
        {
            logger.error("Unable to read fd field from FileChannel");
        }
        return -1;
    }

    /**
     * Get system file descriptor from FileDescriptor object.
     * @param descriptor - FileDescriptor objec to get fd from
     * @return file descriptor, -1 or error
     */
    @Override
    public int getfd(FileDescriptor descriptor)
    {
        try
        {
            return FILE_DESCRIPTOR_FD_FIELD.getInt(descriptor);
        }
        catch (Exception e)
        {
            JVMStabilityInspector.inspectThrowable(e);
            logger.error("Unable to read fd field from FileDescriptor");
        }

        return -1;
    }

    /**
     * @return the PID of the JVM or -1 if we failed to get the PID
     */
    @Override
    public long getProcessID()
    {
        try
        {
            return wrappedLibrary.callGetpid();
        }
        catch (NativeError e)
        {
            logger.error("Failed to get PID from JNA", e);
        }

        return -1;
    }

    @Override
    public FileDescriptor getFileDescriptor(FileChannel channel)
    {
        try
        {
            return (FileDescriptor)FILE_CHANNEL_FD_FIELD.get(channel);
        }
        catch (IllegalArgumentException | IllegalAccessException e)
        {
            throw new RuntimeException(e);
        }
    }
}
