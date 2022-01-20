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
    public TokenMetadata getTokenMetadataForKeyspace(String keyspace)
    {
        return tokenMetadata;
    }

    /** @deprecated See STAR-1032 */
    @Deprecated(forRemoval = true, since = "CC 4.0") // since we can select TMDP implementation via config, this method is no longer needed
    public void replaceTokenMetadata(TokenMetadata newTokenMetadata)
    {
        this.tokenMetadata = newTokenMetadata;
    }
}
