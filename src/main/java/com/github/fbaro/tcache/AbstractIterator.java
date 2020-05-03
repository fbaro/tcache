package com.github.fbaro.tcache;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class AbstractIterator<E> implements Iterator<E> {

    private enum Status {
        Unknown, HasNext, DoesNotHaveNext
    }

    private E next;
    private Status status = Status.Unknown;

    @Override
    public boolean hasNext() {
        advanceToKnown();
        switch (status) {
            case HasNext:
                return true;
            case DoesNotHaveNext:
                return false;
            default:
                throw new IllegalStateException();
        }
    }

    @Override
    public E next() {
        advanceToKnown();
        switch (status) {
            case HasNext:
                status = Status.Unknown;
                E ret = next;
                next = null;
                return ret;
            case DoesNotHaveNext:
                throw new NoSuchElementException();
            default:
                throw new IllegalStateException();
        }
    }

    private void advanceToKnown() {
        if (status == Status.Unknown) {
            next = computeNext();
            if (status != Status.DoesNotHaveNext) {
                status = Status.HasNext;
            }
        }
    }

    protected abstract E computeNext();

    protected final E endOfData() {
        status = Status.DoesNotHaveNext;
        return null;
    }
}
