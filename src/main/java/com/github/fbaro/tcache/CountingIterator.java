package com.github.fbaro.tcache;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.util.Iterator;

public class CountingIterator<T> implements PeekingIterator<T> {

    private final PeekingIterator<T> delegate;
    private int count;

    private CountingIterator(PeekingIterator<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public T peek() {
        return delegate.peek();
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
        return new CountingIterator<>(Iterators.peekingIterator(delegate));
    }
}
