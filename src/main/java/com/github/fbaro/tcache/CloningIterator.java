package com.github.fbaro.tcache;

import java.util.Collections;
import java.util.Iterator;
import java.util.function.Function;

final class CloningIterator<E> implements Iterator<E> {

    private final Iterator<E> delegate;
    private final Function<? super E, ? extends E> cloner;

    private CloningIterator(Iterator<E> delegate, Function<? super E, ? extends E> cloner) {
        this.delegate = delegate;
        this.cloner = cloner;
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public E next() {
        return cloner.apply(delegate.next());
    }

    static <E> Iterator<E> from(Iterator<E> delegate, Function<? super E, ? extends E> cloner) {
        if (!delegate.hasNext()) {
            return Collections.emptyIterator();
        }
        return new CloningIterator<>(delegate, cloner);
    }
}
