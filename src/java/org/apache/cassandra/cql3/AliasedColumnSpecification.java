package org.apache.cassandra.cql3;

import org.apache.cassandra.db.marshal.AbstractType;

public class AliasedColumnSpecification extends ColumnSpecification
{
    public final ColumnIdentifier aliasOf;

    public AliasedColumnSpecification(String ksName,
                                      String cfName,
                                      ColumnIdentifier name,
                                      ColumnIdentifier aliasOf,
                                      AbstractType<?> type)
    {
        super(ksName, cfName, name, type);
        this.aliasOf = aliasOf;
    }

    // Doesn't implement equals or hashcode because wherever used, we never
    // need to consider the aliased identifier, so the implementations in
    // super class are fine and this makes testing easier.
}
