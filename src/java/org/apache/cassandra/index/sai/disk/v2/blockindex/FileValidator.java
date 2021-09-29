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

package org.apache.cassandra.index.sai.disk.v2.blockindex;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.zip.CRC32;

import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.v2.V2PerIndexFiles;
import org.apache.lucene.store.IndexInput;

public class FileValidator
{
    public static class FileInfo implements Serializable
    {
        public final long headerCRC;
        public final long middleCRC;
        public final long footerCRC;

        public FileInfo(long headerCRC, long middleCRC, long footerCRC)
        {
            this.headerCRC = headerCRC;
            this.middleCRC = middleCRC;
            this.footerCRC = footerCRC;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FileInfo fileInfo = (FileInfo) o;
            return headerCRC == fileInfo.headerCRC && middleCRC == fileInfo.middleCRC && footerCRC == fileInfo.footerCRC;
        }

        @Override
        public String toString()
        {
            return "FileInfo{" +
                   "headerCRC=" + headerCRC +
                   ", middleCRC=" + middleCRC +
                   ", footerCRC=" + footerCRC +
                   '}';
        }
    }

    public static long generateCRC(final IndexInput input,
                                   final byte[] buffer) throws IOException
    {
        input.readBytes(buffer, 0, buffer.length);
        CRC32 crc = new CRC32();
        crc.update(buffer);
        return crc.getValue();
    }

    public static HashMap<IndexComponent, FileInfo> generate(Collection<IndexComponent> comps,
                                                             V2PerIndexFiles perIndexFiles) throws IOException
    {
        final HashMap<IndexComponent, FileInfo> map = new HashMap<>();
        for (IndexComponent comp : comps)
        {
            FileInfo fileInfo = generate(comp, perIndexFiles);
            map.put(comp, fileInfo);
        }
        return map;
    }

    public static FileInfo generate(IndexComponent indexComponent,
                                    V2PerIndexFiles perIndexFiles) throws IOException
    {
        final IndexInput input = perIndexFiles.openInput(indexComponent);
        final long fileLength = input.length();

        int bufferLen = fileLength < 1024 ? (int)fileLength / 3 : 1024;
        byte[] buffer = new byte[bufferLen];

        final long headerCRC = generateCRC(input, buffer);

        input.seek(fileLength / 2);
        final long middleCRC = generateCRC(input, buffer);

        input.seek(fileLength - bufferLen);
        final long footerCRC = generateCRC(input, buffer);
        return new FileInfo(headerCRC, middleCRC, footerCRC);
    }
}
