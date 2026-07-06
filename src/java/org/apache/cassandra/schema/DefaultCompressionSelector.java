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
package org.apache.cassandra.schema;

import java.util.Optional;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.ICompressor;

public class DefaultCompressionSelector implements CompressionParams.Selector
{
    @Override
    public CompressionParams newTableCompression(String keyspace)
    {
        return DatabaseDescriptor.shouldUseAdaptiveCompressionByDefault() ? CompressionParams.ADAPTIVE : CompressionParams.FAST;
    }

    @Override
    public CompressionParams flushCompression(String keyspace, CompressionParams tableParams)
    {
        final ICompressor compressor = tableParams.getSstableCompressor();
        if (compressor == null)
            return tableParams;

        switch (DatabaseDescriptor.getFlushCompression())
        {
            case none:
                return CompressionParams.NOOP;
            case fast:
                if (!compressor.recommendedUses().contains(ICompressor.Uses.FAST_COMPRESSION))
                    return CompressionParams.FAST;
                // else fall through
            case adaptive:
                if (!compressor.recommendedUses().contains(ICompressor.Uses.FAST_COMPRESSION))
                    return CompressionParams.FAST_ADAPTIVE;
                // else fall through
            case table:
            default:
                return Optional.ofNullable(tableParams.forUse(ICompressor.Uses.FAST_COMPRESSION))
                               .orElse(tableParams);
        }
    }

    @Override
    public CompressionParams compactionCompression(String keyspace, CompressionParams tableParams)
    {
        return tableParams;
    }
}
