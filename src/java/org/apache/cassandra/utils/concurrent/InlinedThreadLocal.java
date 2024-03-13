/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.utils.concurrent;

import java.util.Objects;
import java.util.function.Supplier;

import net.nicoulaj.compilecommand.annotations.DontInline;
import net.nicoulaj.compilecommand.annotations.Inline;

/**
 * An implementation of {@link DseThreadLocal} which specializes for {@link InlinedThreadLocalThread} to treat instances
 * as fields of the current thread instance.
 */
class InlinedThreadLocal<T> extends ThreadLocal<T> implements DseThreadLocal<T>
{
    static final Supplier EMPTY_SUPPLIER = () -> null;
    private final long offset;
    private final Supplier<T> initialValueSupplier;

    InlinedThreadLocal(long index, Supplier<T> initialValueSupplier)
    {
        Objects.requireNonNull(initialValueSupplier);
        this.offset = InlinedThreadLocalThread.getRefOffset(index);
        this.initialValueSupplier = initialValueSupplier;
    }

    @Override
    @Inline
    public T get()
    {
        Thread thread = Thread.currentThread();
        if (thread instanceof InlinedThreadLocalThread)
        {
            InlinedThreadLocalThread inlinedThreadLocalThread = (InlinedThreadLocalThread) thread;
            Object o = inlinedThreadLocalThread.getThreadLocalRef(offset);
            if (o != null)
                return (T) o;

            Supplier<T> initialValueSupplier = this.initialValueSupplier;
            if (initialValueSupplier == EMPTY_SUPPLIER)
                return null;

            return allocate(inlinedThreadLocalThread, initialValueSupplier);
        }
        else
        {
            return getSlow();
        }
    }

    @DontInline
    private T getSlow()
    {
        return super.get();
    }

    @DontInline
    private T allocate(
            InlinedThreadLocalThread inlinedThreadLocalThread,
            Supplier<T> initialValueSupplier)
    {
        T o = initialValueSupplier.get();
        // Supplier returning null is a degenerate case, please fix
        assert  (o != null);

        inlinedThreadLocalThread.setThreadLocalRef(offset, o);
        return o;
    }

    @Override
    @Inline
    public void set(T v)
    {
        Thread thread = Thread.currentThread();
        if (thread instanceof InlinedThreadLocalThread)
        {
            InlinedThreadLocalThread inlinedThreadLocalThread = (InlinedThreadLocalThread) thread;
            inlinedThreadLocalThread.setThreadLocalRef(offset, v);
        }
        else
        {
            setSlow(v);
        }
    }

    @DontInline
    private void setSlow(T v)
    {
        if (v != null)
        {
            super.set(v);
        }
        else
        {
            // this forces consistent use of `null` as special value
            super.remove();
        }
    }

    @Override
    public boolean isSet()
    {
        assert initialValueSupplier == EMPTY_SUPPLIER;
        Thread thread = Thread.currentThread();
        if (thread instanceof InlinedThreadLocalThread)
        {
            InlinedThreadLocalThread inlinedThreadLocalThread = (InlinedThreadLocalThread) thread;
            Object o = inlinedThreadLocalThread.getThreadLocalRef(offset);
            return o != null;
        }
        else
        {
            // note the inconsistency here where isSet can trigger initialization of the thread local.
            return super.get() != null;
        }
    }

    @Override
    public void remove()
    {
        set(null);
    }

    @Override
    protected T initialValue()
    {
        return initialValueSupplier.get();
    }
}
