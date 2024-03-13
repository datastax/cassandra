/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.db.utils.leaks.detection;

import java.util.Objects;

import com.google.common.base.Preconditions;

/**
 * The identifier for a type of resource that can be tracked for leaks by {@link LeaksDetector}.
 * <p/>
 * A resource is normally identified by its class and by a name, used to distinguish resources for the same class,
 * e.g. different buffer pools.
 */
class LeaksResource
{
    /**
     * The maximum length of a resource name. This limitation is for the nodetool formatting: we want to align
     * the names but we also cannot truncate them. Increase this value if required.
     */
    final static int MAX_NAME_LENGTH = 32;

    final String name;
    final Class cls;
    final LeakLevel level;

    private LeaksResource(String name, Class cls, LeakLevel level)
    {
        Preconditions.checkArgument(name.length() <= MAX_NAME_LENGTH, "Name is too long, max characters is %s: %s", MAX_NAME_LENGTH, name);
        this.name = name;
        this.cls = cls;
        this.level = level;
    }

    public static LeaksResource create(String name, Class cls, LeakLevel level)
    {
        return new LeaksResource(name, cls, level);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, cls);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;

        if (!(obj instanceof LeaksResource))
            return false;

        LeaksResource other = (LeaksResource) obj;
        return Objects.equals(name, other.name) && Objects.equals(cls, other.cls);
    }

    @Override
    public String toString()
    {
        return String.format("%s/%s", name, cls.getSimpleName());
    }
}
