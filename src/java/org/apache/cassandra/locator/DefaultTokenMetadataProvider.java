/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.locator;

public class DefaultTokenMetadataProvider implements TokenMetadataProvider
{
    private volatile TokenMetadata tokenMetadata;

    public DefaultTokenMetadataProvider()
    {
        this.tokenMetadata = new TokenMetadata();
    }

    @Override
    public TokenMetadata getTokenMetadata()
    {
        return tokenMetadata;
    }

    @Override
    public void replaceTokenMetadata(TokenMetadata newTokenMetadata)
    {
        this.tokenMetadata = newTokenMetadata;
    }
}
