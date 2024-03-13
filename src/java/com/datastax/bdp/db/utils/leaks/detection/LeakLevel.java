/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.db.utils.leaks.detection;

/** Leak levels define the seriousness of a leak.  */
public enum LeakLevel
{
    /** First level leaks are for resources that need some manual cleanup, for example by calling a release or close method.
     * When these resources are leaked, and if there is no cleaner for automated cleaning, then a leak is bad news
     * and will have negative consequences. */
    FIRST_LEVEL,

    /**
     * Second level leaks are for resources that own a resource that needs manual cleanup. These resources are
     * normally tracked only to help with debugging and if they are leaked there is no negative consequence.
     * For example, the chunks in the cache live for a very long time, normally the creation of the chunk is
     * not related to a leak. So reporting leaks of resources that temporarily owned a chunk by taking it from
     * the cache, helps with debugging and ensures that leaks are reported even if the chunk is never evicted
     * from the cache.*/
    SECOND_LEVEL
}
