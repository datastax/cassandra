package org.apache.cassandra.locator;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_TMD_PROVIDER_PROPERTY;

/**
 * Provides access to the {@link TokenMetadata} instance used by this node.
 */
public interface TokenMetadataProvider
{
    TokenMetadataProvider instance = CUSTOM_TMD_PROVIDER_PROPERTY.getString() == null ?
                                     new DefaultTokenMetadataProvider() :
                                     CustomTokenMetadataProvider.make(CUSTOM_TMD_PROVIDER_PROPERTY.getString());

    TokenMetadata getTokenMetadata();

    void replaceTokenMetadata(TokenMetadata newTokenMetadata);
}
