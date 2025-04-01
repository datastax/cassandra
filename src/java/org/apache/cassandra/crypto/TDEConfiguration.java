/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.crypto;

public class TDEConfiguration
{
    public final String systemKeyDirectory;

    public TDEConfiguration(String systemKeyDirectory)
    {
        this.systemKeyDirectory = systemKeyDirectory;
    }
}
