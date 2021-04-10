package com.github.fbaro.tcache;

import java.util.Iterator;

public class CountingIterator<T> implements Iterator<T> {

    private final Iterator<T> delegate;
    private int count;

    private CountingIterator(Iterator<T> delegate) {
        this.delegate = delegate;
        this.count = 0;
    }

    private CountingIterator(Iterator<T> delegate, int initialCount) {
        this.delegate = delegate;
        this.count = initialCount;
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public T next() {
        T ret = delegate.next();
        count++;
        return ret;
    }

    @Override
    public void remove() {
        delegate.remove();
    }

    public int getCount() {
        return count;
    }

    public static <T> CountingIterator<T> create(Iterator<T> delegate) {
        return new CountingIterator<>(delegate);
    }

    public static <T> CountingIterator<T> create(Iterator<T> delegate, int initialCount) {
        return new CountingIterator<>(delegate, initialCount);
    }
}
