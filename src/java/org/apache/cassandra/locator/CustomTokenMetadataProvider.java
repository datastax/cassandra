/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.locator;

public class CustomTokenMetadataProvider
{
    public static TokenMetadataProvider make(String customImpl)
    {
        try
        {
            return (TokenMetadataProvider) Class.forName(customImpl).newInstance();
        }
        catch (Throwable ex)
        {
            throw new IllegalStateException("Unknown token metadata provider: " + customImpl);
        }
    }
}
