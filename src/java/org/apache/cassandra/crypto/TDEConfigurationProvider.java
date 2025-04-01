/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.crypto;

import com.google.common.annotations.VisibleForTesting;

public class TDEConfigurationProvider
{
    private static String systemKeyDirectoryProperty = System.getProperty("dse.system_key_directory", "/etc/dse/conf");

    public static TDEConfiguration getConfiguration()
    {
        //TODO TDE porting: for now, just return a default configuration
        return new TDEConfiguration(systemKeyDirectoryProperty);
    }

    @VisibleForTesting
    public static void setSystemKeyDirectoryProperty(String value)
    {
        systemKeyDirectoryProperty = value;
    }
}
