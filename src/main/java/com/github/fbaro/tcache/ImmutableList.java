package com.github.fbaro.tcache;

import javax.annotation.Nonnull;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

abstract class ImmutableList<E> extends AbstractList<E> {
    private ImmutableList() {
    }

    public abstract Iterator<E> reverseIterator();

    public abstract ImmutableList<E> invert();

    @Override
    @Nonnull
    public abstract ImmutableList<E> subList(int fromIndex, int toIndex);

    public static <E> ImmutableList<E> of() {
        //noinspection unchecked
        return (ImmutableList<E>) EMPTY;
    }

    public static <E> ImmutableList<E> of(E value) {
        return new Singleton<>(value);
    }

    public static <E> ImmutableList<E> copyOf(Collection<? extends E> data) {
        if (data.isEmpty()) {
            return of();
        } else if (data.size() == 1) {
            return of(data.iterator().next());
        } else {
            return new Sublist<>(new ArrayList<>(data), 0, data.size());
        }
    }

    public static <E> ImmutableList<E> copyOf(Iterator<? extends E> data) {
        if (!data.hasNext()) {
            return of();
        }
        E first = data.next();
        if (!data.hasNext()) {
            return of(first);
        }
        ArrayList<E> copiedData = new ArrayList<>();
        copiedData.add(first);
        data.forEachRemaining(copiedData::add);
        copiedData.trimToSize();
        return new Sublist<>(copiedData, 0, copiedData.size());
    }

    private static final ImmutableList<Object> EMPTY = new ImmutableList<Object>() {
        @Override
        public Object get(int index) {
            throw new IndexOutOfBoundsException();
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public Iterator<Object> iterator() {
            return Collections.emptyIterator();
        }

        @Override
        public Iterator<Object> reverseIterator() {
            return Collections.emptyIterator();
        }

        @Override
        public ImmutableList<Object> invert() {
            return this;
        }

        @Nonnull
        @Override
        public ImmutableList<Object> subList(int from, int to) {
            if (from != 0 || to != 0) {
                throw new IndexOutOfBoundsException();
            }
            return this;
        }
    };

    private static final class Singleton<E> extends ImmutableList<E> {

        private final List<E> value;

        public Singleton(E value) {
            this.value = Collections.singletonList(value);
        }

        @Override
        public E get(int index) {
            return value.get(index);
        }

        @Override
        public int size() {
            return 1;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public Iterator<E> iterator() {
            return value.iterator();
        }

        @Override
        public Iterator<E> reverseIterator() {
            return value.iterator();
        }

        @Override
        public ImmutableList<E> invert() {
            return this;
        }

        @Nonnull
        @Override
        public ImmutableList<E> subList(int from, int to) {
            if (from < 0 || to > 1 || from > to) {
                throw new IndexOutOfBoundsException();
            }
            return from == to ? ImmutableList.of() : this;
        }
    }

    private static final class Sublist<E> extends ImmutableList<E> {
        private final List<E> data;
        private final int from;
        private final int to;

        public Sublist(List<E> data, int from, int to) {
            this.data = data;
            this.from = from;
            this.to = to;
        }

        @Override
        public E get(int index) {
            return data.get(from + index);
        }

        @Override
        public int size() {
            return to - from;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public Iterator<E> iterator() {
            return new Iterator<E>() {
                private int pos = from;

                @Override
                public boolean hasNext() {
                    return pos < to;
                }

                @Override
                public E next() {
                    return data.get(pos++);
                }
            };
        }

        @Override
        public Iterator<E> reverseIterator() {
            return new Iterator<E>() {
                private int pos = to;

                @Override
                public boolean hasNext() {
                    return pos > from;
                }

                @Override
                public E next() {
                    return data.get(--pos);
                }
            };
        }

        @Override
        public ImmutableList<E> invert() {
            return new InvertedSublist<>(this);
        }

        @Nonnull
        @Override
        public ImmutableList<E> subList(int from, int to) {
            if (from < 0 || to > (this.to - this.from) || from > to) {
                throw new IndexOutOfBoundsException();
            }
            if (from == to) {
                return ImmutableList.of();
            } else if (from == to - 1) {
                return ImmutableList.of(get(from));
            } else {
                return new Sublist<>(data, this.from + from, this.from + to);
            }
        }
    }

    private static final class InvertedSublist<E> extends ImmutableList<E> {
        private final Sublist<E> data;

        public InvertedSublist(Sublist<E> data) {
            this.data = data;
        }

        @Override
        public E get(int index) {
            return data.get(data.size() - index);
        }

        @Override
        public int size() {
            return data.size();
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public Iterator<E> iterator() {
            return data.reverseIterator();
        }

        @Override
        public Iterator<E> reverseIterator() {
            return data.iterator();
        }

        @Override
        public ImmutableList<E> invert() {
            return data;
        }

        @Nonnull
        @Override
        public ImmutableList<E> subList(int from, int to) {
            return data.subList(data.size() - to, data.size() - from).invert();
        }
    }
}
