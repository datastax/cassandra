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
package org.apache.cassandra.io.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.BufferType;

import static org.junit.Assert.*;

public class SequentialWriterReadableTest
{
    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testWriteOnlyChannel() throws IOException
    {
        File tempFile = FileUtils.createTempFile("writeonly", "test");
        tempFile.tryDelete();
        
        byte[] testData = new byte[1024];
        ThreadLocalRandom.current().nextBytes(testData);
        
        SequentialWriterOption option = SequentialWriterOption.newBuilder()
                                                              .bufferSize(256)
                                                              .build();
        
        try (SequentialWriter writer = new SequentialWriter(tempFile, false, option, true))
        {
            writer.write(testData);
            writer.finish();
            
            try (FileChannel channel = FileChannel.open(tempFile.toPath(), StandardOpenOption.READ))
            {
                ByteBuffer readBuffer = ByteBuffer.allocate(testData.length);
                int bytesRead = channel.read(readBuffer);
                assertEquals(testData.length, bytesRead);
                
                readBuffer.flip();
                byte[] readData = new byte[testData.length];
                readBuffer.get(readData);
                assertArrayEquals(testData, readData);
            }
        }
        finally
        {
            tempFile.tryDelete();
        }
    }

    @Test
    public void testReadableChannel() throws IOException
    {
        File tempFile = FileUtils.createTempFile("readable", "test");
        tempFile.tryDelete();
        
        byte[] testData = new byte[1024];
        ThreadLocalRandom.current().nextBytes(testData);
        
        SequentialWriterOption option = SequentialWriterOption.newBuilder()
                                                              .bufferSize(256)
                                                              .build();
        
        try (SequentialWriter writer = new SequentialWriter(tempFile, true, option, true))
        {
            writer.write(testData);
            writer.finish();
            
            assertTrue(tempFile.exists());
            assertEquals(testData.length, tempFile.length());
        }
        finally
        {
            tempFile.tryDelete();
        }
    }

    @Test
    public void testUpdateFileHandle() throws IOException
    {
        File tempFile = FileUtils.createTempFile("filehandle", "test");
        tempFile.tryDelete();
        
        byte[] testData = new byte[2048];
        ThreadLocalRandom.current().nextBytes(testData);
        
        SequentialWriterOption option = SequentialWriterOption.newBuilder()
                                                              .bufferSize(512)
                                                              .build();
        
        try (SequentialWriter writer = new SequentialWriter(tempFile, option))
        {
            writer.write(testData, 0, 1024);
            writer.sync();
            
            FileHandle.Builder fhBuilder = new FileHandle.Builder(tempFile);
            
            writer.updateFileHandle(fhBuilder);
            FileHandle fileHandle1 = fhBuilder.complete();
            assertEquals(1024, fileHandle1.onDiskLength);
            fileHandle1.close();
            
            writer.write(testData, 1024, 1024);
            writer.sync();
            
            fhBuilder = new FileHandle.Builder(tempFile);
            writer.updateFileHandle(fhBuilder, 2048);
            FileHandle fileHandle2 = fhBuilder.complete();
            assertEquals(2048, fileHandle2.onDiskLength);
            fileHandle2.close();
            
            writer.finish();
        }
        finally
        {
            tempFile.tryDelete();
        }
    }

    @Test
    public void testUpdateFileHandleWithoutDataLength() throws IOException
    {
        File tempFile = FileUtils.createTempFile("filehandle2", "test");
        tempFile.tryDelete();
        
        byte[] testData = new byte[1536];
        ThreadLocalRandom.current().nextBytes(testData);
        
        SequentialWriterOption option = SequentialWriterOption.newBuilder()
                                                              .bufferSize(512)
                                                              .build();
        
        try (SequentialWriter writer = new SequentialWriter(tempFile, option))
        {
            writer.write(testData);
            writer.sync();
            
            FileHandle.Builder fhBuilder = new FileHandle.Builder(tempFile);
            writer.updateFileHandle(fhBuilder);
            
            FileHandle fileHandle = fhBuilder.complete();
            assertEquals(testData.length, fileHandle.onDiskLength);
            fileHandle.close();
            
            writer.finish();
        }
        finally
        {
            tempFile.tryDelete();
        }
    }

    @Test
    public void testEstablishEndAddressablePosition() throws IOException
    {
        File tempFile = FileUtils.createTempFile("endaddressable", "test");
        tempFile.tryDelete();
        
        byte[] testData = new byte[1024];
        ThreadLocalRandom.current().nextBytes(testData);
        
        SequentialWriterOption option = SequentialWriterOption.newBuilder()
                                                              .bufferSize(256)
                                                              .build();
        
        try (SequentialWriter writer = new SequentialWriter(tempFile, option))
        {
            writer.write(testData);
            
            writer.establishEndAddressablePosition(128);
            
            writer.finish();
            
            assertTrue(tempFile.exists());
            assertEquals(testData.length, tempFile.length());
        }
        finally
        {
            tempFile.tryDelete();
        }
    }

    @Test
    public void testExistingFileWithReadableTrue() throws IOException
    {
        File tempFile = FileUtils.createTempFile("existing", "test");
        Files.write(tempFile.toPath(), "existing content".getBytes());
        
        byte[] newData = "new content that is longer than the existing content".getBytes();
        
        SequentialWriterOption option = SequentialWriterOption.newBuilder()
                                                              .bufferSize(256)
                                                              .build();
        
        try (SequentialWriter writer = new SequentialWriter(tempFile, true, option, true))
        {
            writer.truncate(0);
            writer.write(newData);
            writer.finish();
            
            assertEquals(newData.length, tempFile.length());
            assertArrayEquals(newData, Files.readAllBytes(tempFile.toPath()));
        }
        finally
        {
            tempFile.tryDelete();
        }
    }

    @Test
    public void testExistingFileWithReadableFalse() throws IOException
    {
        File tempFile = FileUtils.createTempFile("existing2", "test");
        Files.write(tempFile.toPath(), "existing content".getBytes());
        
        byte[] newData = "new content that is longer than the existing content".getBytes();
        
        SequentialWriterOption option = SequentialWriterOption.newBuilder()
                                                              .bufferSize(256)
                                                              .build();
        
        try (SequentialWriter writer = new SequentialWriter(tempFile, false, option, true))
        {
            writer.truncate(0);
            writer.write(newData);
            writer.finish();
            
            assertEquals(newData.length, tempFile.length());
            assertArrayEquals(newData, Files.readAllBytes(tempFile.toPath()));
        }
        finally
        {
            tempFile.tryDelete();
        }
    }
}