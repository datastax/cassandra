/*
 * Copyright DataStax, Inc.
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
package org.apache.cassandra.crypto;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Maps;

import org.apache.cassandra.config.OptionMap;
import org.apache.cassandra.io.util.File;


public class LocalFileSystemKeyProviderFactory implements IKeyProviderFactory
{
    // Allows keyProvider reusing and guarantees at most one key provider per file:
    private static final ConcurrentMap<Path, LocalFileSystemKeyProvider> keyProviders = Maps.newConcurrentMap();

    public static final String SECRET_KEY_FILE = "secret_key_file";
    private static final String DEFAULT_SECRET_KEY_FILE = "/etc/cassandra/conf/data_encryption_keys";

    @Override
    public IKeyProvider getKeyProvider(Map<String, String> options) throws IOException
    {
        OptionMap optionMap = new OptionMap(options);
        Path secretKeyPath = new File(optionMap.get(SECRET_KEY_FILE, DEFAULT_SECRET_KEY_FILE)).toPath();

        LocalFileSystemKeyProvider kp = keyProviders.get(secretKeyPath);
        if (kp == null)
        {
            kp = new LocalFileSystemKeyProvider(secretKeyPath);
            LocalFileSystemKeyProvider previous = keyProviders.putIfAbsent(secretKeyPath, kp);
            if (previous != null)
                kp = previous;
        }
        return kp;
    }

    @Override
    public Set<String> supportedOptions()
    {
        return Collections.singleton(SECRET_KEY_FILE);
    }
}
