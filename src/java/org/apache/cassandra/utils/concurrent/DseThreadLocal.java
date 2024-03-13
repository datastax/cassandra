/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.utils.concurrent;

/**
 * Common interface for {@link ThreadLocal} alternatives. Note that in this interface `null` is considered a special
 * value used for removal so initial value suppliers which return null are incompatible.
 */
public interface DseThreadLocal<T>
{
    /**
     * @see ThreadLocal#get()
     * @return current value, or if current value is null initialize (assuming an initial supplier is present)
     */
    T get();


    /**
     * @param value to set the thread local value. Setting to `null` may trigger re-initialization if a supplier
     *              is set up.
     */
    void set(T value);

    /**
     * This method is only usable for TLs with no initial value supplier.
     *
     * @return true if set, false otherwise.
     */
    boolean isSet();

    /**
     * Same as set(null). A default implementation is not provided as the method is present in some implementors already.
     */
    void remove();
}
