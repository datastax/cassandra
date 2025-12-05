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
package org.apache.cassandra.index.sai.disk.v2;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

public class TokenOnlyPrimaryKey implements PrimaryKey
{
    protected final Token token;

    public TokenOnlyPrimaryKey(Token token)
    {
        this.token = token;
    }

    @Override
    public boolean isTokenOnly()
    {
        return true;
    }

    @Override
    public Token token()
    {
        return token;
    }

    @Override
    public DecoratedKey partitionKey()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Clustering<?> clustering()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteSource asComparableBytes(Version version)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compareTo(PrimaryKey o)
    {
        return token().compareTo(o.token());
    }

    @Override
    public long ramBytesUsed()
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'ramBytesUsed'");
    }

    @Override
    public PrimaryKey forStaticRow()
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'forStaticRow'");
    }

    @Override
    public PrimaryKey loadDeferred()
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'loadDeferred'");
    }

    @Override
    public ByteSource asComparableBytesMinPrefix(Version version)
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'asComparableBytesMinPrefix'");
    }

    @Override
    public ByteSource asComparableBytesMaxPrefix(Version version)
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'asComparableBytesMaxPrefix'");
    }

    // @Override
    // public int hashCode()
    // {
    //     return Objects.hash(token(), clusteringComparator);
    // }

    @Override
    public boolean equals(Object o)
    {
        if (o instanceof PrimaryKey)
            return compareTo((PrimaryKey) o) == 0;
        return false;
    }

    // @Override
    // public boolean equals(Object o, boolean strict)
    // {
    //     if (o == null)
    //         return false;
    //     if (o instanceof PrimaryKey)
    //         return compareTo((PrimaryKey) o, strict) == 0;
    //     return false;
    // }

    @Override
    public String toString()
    {
        return String.format("PrimaryKey: { token: %s }", token());
    }
}
