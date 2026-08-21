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

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;
import java.util.zip.CRC32;
import java.util.zip.Adler32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.FastThreadLocal;
import software.amazon.awssdk.crt.checksums.CRC64NVME;

public enum ChecksumType
{
    ADLER32
    {

        @Override
        public Checksum newInstance()
        {
            return new Adler32();
        }

        @Override
        public void update(Checksum checksum, ByteBuffer buf)
        {
            ((Adler32)checksum).update(buf);
        }

    },
    CRC32
    {

        @Override
        public Checksum newInstance()
        {
            return new CRC32();
        }

        @Override
        public void update(Checksum checksum, ByteBuffer buf)
        {
            ((CRC32)checksum).update(buf);
        }

    },
    CRC32C
    {

        @Override
        public Checksum newInstance()
        {
            return new CRC32C();
        }

        @Override
        public void update(Checksum checksum, ByteBuffer buf)
        {
            ((CRC32C)checksum).update(buf);
        }

    },
    CRC64NVME
    {

        @Override
        public Checksum newInstance()
        {
            if (HAS_AWS_CRT_CRC64NVME)
                return new CRC64NVME();
            return new PureJavaCRC64NVME();
        }

        @Override
        public void update(Checksum checksum, ByteBuffer buf)
        {
            ((CRC32C)checksum).update(buf);
        }

    };

    private static final Logger logger = LoggerFactory.getLogger(ChecksumType.class);
    private static final boolean HAS_AWS_CRT_CRC64NVME;

    static {
        boolean available = false;
        try {
            Class.forName("software.amazon.awssdk.crt.checksums.CRC64NVME");
            available = true;
        } catch (ClassNotFoundException e) {
            logger.debug("software.amazon.awssdk.crt.checksums.CRC64NVME not found, " +
                         "falling back to PureJavaCRC64NVME for CRC64NVME checksum");
        }
        HAS_AWS_CRT_CRC64NVME = available;
    }

    public abstract Checksum newInstance();
    public abstract void update(Checksum checksum, ByteBuffer buf);

    private FastThreadLocal<Checksum> instances = new FastThreadLocal<Checksum>()
    {
        protected Checksum initialValue() throws Exception
        {
            return newInstance();
        }
    };

    public long of(ByteBuffer buf)
    {
        Checksum checksum = instances.get();
        checksum.reset();
        update(checksum, buf);
        return checksum.getValue();
    }

    public long of(byte[] data, int off, int len)
    {
        Checksum checksum = instances.get();
        checksum.reset();
        checksum.update(data, off, len);
        return checksum.getValue();
    }
}
