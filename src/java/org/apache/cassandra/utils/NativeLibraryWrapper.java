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

import com.sun.jna.Pointer;

/**
 * An interface to implement for using OS specific native methods.
 * @see INativeLibrary
 */
@Shared
// Implementors are advised to NOT use JNA's convenient LastErrorException because it relies
// on checking errno(), which is not reliable.  Linux man page explains,
//       The value in errno is significant only when the return value of
//       the call indicated an error (i.e., -1 from most system calls; -1
//       or NULL from most library functions); ***a function that succeeds is
//       allowed to change errno.***
public interface NativeLibraryWrapper
{
    /**
     * Checks if the library has been successfully linked.
     * @return {@code true} if the library has been successfully linked, {@code false} otherwise.
     */
    boolean isAvailable();

    void callMlockall(int flags) throws NativeError;
    void callMunlockall() throws NativeError;
    int callFcntl(int fd, int command, long flags) throws NativeError;
    void callPosixFadvise(int fd, long offset, int len, int flag) throws NativeError;
    void callPosixMadvise(Pointer addr, long length, int advice) throws NativeError;
    int callOpen(String path, int flags) throws NativeError;
    void callFsync(int fd) throws NativeError;
    void callClose(int fd) throws NativeError;
    long callGetpid() throws NativeError;

    /**
     * This is a checked exception because the correct handling of the error is almost
     * always to log it and move on, not to propagate it up the stack.
     */
    class NativeError extends Exception
    {
        private final int errno;

        public NativeError(String nativeMessage, int errno)
        {
            super(nativeMessage);
            this.errno = errno;
        }

        public int getErrno()
        {
            return errno;
        }
    }
}
