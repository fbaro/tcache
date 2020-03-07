package it.fb.tcache;

import java.util.Collection;

@FunctionalInterface
public interface Loader<K, V, P> {

    Result<V> loadForward(K key, long lowestIncluded, long highestExcluded, int offset, int limit, P param);

    default Result<V> loadBackwards(K key, long lowestIncluded, long highestExcluded, int offset, int limit, P param) {
        throw new UnsupportedOperationException();
    }

    interface Result<V> {
        Collection<V> getData();

        /**
         * This method tells whether there is no more data past the returned data (in the direction
         * of the calling method).
         * @return {@code true} if there is no further data past this
         */
        boolean isEndOfData();

        default int size() {
            return getData().size();
        }
    }

    class StdResult<V> implements Result<V> {
        protected final Collection<V> data;
        protected final boolean isEndOfData;

        public StdResult(Collection<V> data, boolean isEndOfData) {
            this.data = data;
            this.isEndOfData = isEndOfData;
        }

        @Override
        public Collection<V> getData() {
            return data;
        }

        @Override
        public boolean isEndOfData() {
            return isEndOfData;
        }
    }
}
