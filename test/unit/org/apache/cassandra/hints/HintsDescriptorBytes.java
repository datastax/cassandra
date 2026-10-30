/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.hints;

import java.io.IOException;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.io.util.DataOutputBuffer;

/** The header of a hints file as {@link HintsDescriptor} writes it, for the tests of readers outside this package. */
public final class HintsDescriptorBytes
{
    private HintsDescriptorBytes()
    {
    }

    /** The serialized descriptor of a hints file of {@code version} compressed with {@code compressorClass}. */
    public static byte[] header(UUID hostId, int version, long timestamp, String compressorClass) throws IOException
    {
        ImmutableMap<String, Object> compression = ImmutableMap.of(ParameterizedClass.CLASS_NAME, compressorClass,
                                                                   ParameterizedClass.PARAMETERS, ImmutableMap.of());
        HintsDescriptor descriptor = new HintsDescriptor(hostId, version, timestamp, ImmutableMap.of(HintsDescriptor.COMPRESSION, compression));
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            descriptor.serialize(out);
            return out.toByteArray();
        }
    }
}
