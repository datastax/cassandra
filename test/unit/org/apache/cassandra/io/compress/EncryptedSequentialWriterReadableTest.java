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
package org.apache.cassandra.io.compress;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.CompressionParams;

import static org.junit.Assert.*;

public class EncryptedSequentialWriterReadableTest
{
    static final ICompressor AESEncryptor;
    static final CompressionParams AESEncryptorParams;
    
    static
    {
        DatabaseDescriptor.daemonInitialization();
        
        Map<String, String> opts = new HashMap<>();
        opts.put(CompressionParams.CLASS, Encryptor.class.getName());
        opts.put(EncryptionConfig.CIPHER_ALGORITHM, "AES/CBC/PKCS5Padding");
        opts.put(EncryptionConfig.SECRET_KEY_STRENGTH, Integer.toString(128));
        opts.put(EncryptionConfig.KEY_PROVIDER, EncryptorTest.KeyProviderFactoryStub.class.getName());
        AESEncryptor = Encryptor.create(opts);
        AESEncryptorParams = CompressionParams.fromMap(opts);
    }
    
    @BeforeClass
    public static void setupDD()
    {
        // Already initialized in static block
    }

    @Test
    public void testEncryptedWriterWithReadableChannel() throws IOException
    {
        File tempFile = FileUtils.createTempFile("encrypted", "test");
        tempFile.tryDelete();
        
        byte[] testData = new byte[8192];
        ThreadLocalRandom.current().nextBytes(testData);
        
        ICompressor encryptor = AESEncryptor;
        SequentialWriterOption option = SequentialWriterOption.newBuilder()
                                                              .bufferSize(1024)
                                                              .build();
        
        try (EncryptedSequentialWriter writer = new EncryptedSequentialWriter(tempFile, option, encryptor))
        {
            writer.write(testData);
            writer.finish();
            
            assertTrue(tempFile.exists());
            assertTrue(tempFile.length() > 0);
            
            try (FileChannel channel = FileChannel.open(tempFile.toPath(), StandardOpenOption.READ))
            {
                ByteBuffer headerBuffer = ByteBuffer.allocate(128);
                int bytesRead = channel.read(headerBuffer);
                assertTrue(bytesRead > 0);
            }
        }
        finally
        {
            tempFile.tryDelete();
        }
    }

    @Test
    public void testEncryptedWriterConstructorPassesReadableTrue() throws IOException
    {
        File tempFile = FileUtils.createTempFile("encrypted2", "test");
        tempFile.tryDelete();
        
        ICompressor encryptor = AESEncryptor;
        SequentialWriterOption option = SequentialWriterOption.newBuilder()
                                                              .bufferSize(1024)
                                                              .finishOnClose(true)
                                                              .build();
        
        byte[] testData = "test data for encryption".getBytes();
        
        try (EncryptedSequentialWriter writer = new EncryptedSequentialWriter(tempFile, option, encryptor))
        {
            writer.write(testData);
        }
        
        assertTrue(tempFile.exists());
        assertTrue(tempFile.length() > testData.length);
        
        tempFile.tryDelete();
    }

}