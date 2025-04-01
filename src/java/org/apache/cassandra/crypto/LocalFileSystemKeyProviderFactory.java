/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.crypto;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Maps;

import org.apache.cassandra.io.compress.OptionMap;


public class LocalFileSystemKeyProviderFactory implements IKeyProviderFactory
{
    // Allows keyProvider reusing and guarantees at most one key provider per file:
    private static final ConcurrentMap<Path, LocalFileSystemKeyProvider> keyProviders = Maps.newConcurrentMap();

    public static final String SECRET_KEY_FILE = "secret_key_file";
    private static final String DEFAULT_SECRET_KEY_FILE = "/etc/dse/conf/data_encryption_keys";

    @Override
    public IKeyProvider getKeyProvider(Map<String, String> options) throws IOException
    {
        OptionMap optionMap = new OptionMap(options);
        Path secretKeyPath = Paths.get(optionMap.get(SECRET_KEY_FILE, DEFAULT_SECRET_KEY_FILE));

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
