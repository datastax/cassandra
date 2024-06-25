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
package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.cassandra.index.sai.disk.io.IndexInput;
import org.apache.cassandra.index.sai.disk.oldlucene.ByteArrayIndexInput;

@NotThreadSafe
public class MetadataSource
{
    private final Version version;
    private final Map<String, Supplier<ByteArrayIndexInput>> components;

    private MetadataSource(Version version, Map<String, Supplier<ByteArrayIndexInput>> components)
    {
        this.version = version;
        this.components = components;
    }

    public static MetadataSource loadMetadata(IndexComponents.ForRead components) throws IOException
    {
        IndexComponent.ForRead metadataComponent = components.get(components.metadataComponent());
        try (var input = metadataComponent.openCheckSummedInput())
        {
            return MetadataSource.load(input, components.version(), metadataComponent.byteOrder());
        }
    }

    private static MetadataSource load(ChecksumIndexInput input, Version expectedVersion, ByteOrder order) throws IOException
    {
        Map<String, Supplier<ByteArrayIndexInput>> components = new HashMap<>();
        Version version = SAICodecUtils.checkHeader(input);
        if (version != expectedVersion)
            throw new IllegalStateException("Unexpected version " + version + " in " + input + ", expected " + expectedVersion);

        final int num = input.readInt();

        for (int x = 0; x < num; x++)
        {
            if (input.length() == input.getFilePointer())
            {
                // we should never get here, because we always add footer to the file
                throw new IllegalStateException("Unexpected EOF in " + input);
            }

            final String name = input.readString();
            final int length = input.readInt();
            final byte[] bytes = new byte[length];
            input.readBytes(bytes, 0, length);

            components.put(name, () -> new ByteArrayIndexInput(name, bytes, order));
        }

        SAICodecUtils.checkFooter(input);

        return new MetadataSource(version, components);
    }

    public IndexInput get(IndexComponent component)
    {
        return get(component.fileNamePart());
    }

    public IndexInput get(String name)
    {
        var supplier = components.get(name);

        if (supplier == null)
        {
            throw new IllegalArgumentException(String.format("Could not find component '%s'. Available properties are %s.",
                                                             name, components.keySet()));
        }

        return supplier.get();
    }

    public Version getVersion()
    {
        return version;
    }
}
