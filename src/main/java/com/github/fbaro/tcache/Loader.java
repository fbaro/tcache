package com.github.fbaro.tcache;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.Objects;

/**
 * Interface to provide data to a cache.
 *
 * @param <K> Type of the keys in the cache. Should implement {@code hashCode} and {@code equals} as per the typical
 *            {@code Map} contract
 * @param <V> Type of the cache values
 * @param <P> Type of a custom parameter which will be passed as-is from the cache user to the loading function
 */
@FunctionalInterface
public interface Loader<K, V, P> {

    /**
     * Retrieves data in ascending order.
     *
     * @param key             The key to retrieve data for
     * @param lowestIncluded  The lowest (starting) timestamp to load, extreme included
     * @param highestExcluded The highest (ending) timestamp to load, extreme excluded
     * @param offset          The number of items to be skipped from the beginning of the results
     * @param limit           The maximum amount of results to be returned
     * @param param           A parameter received by the cache caller
     * @return Data fulfilling the requirements
     */
    @Nonnull
    Result<V> loadForward(K key, long lowestIncluded, long highestExcluded, int offset, int limit, P param);

    /**
     * Retrieves data in descending order.
     *
     * @param key             The key to retrieve data for
     * @param lowestIncluded  The lowest (ending) timestamp to load, extreme included
     * @param highestExcluded The highest (starting) timestamp to load, extreme excluded
     * @param offset          The number of items to be skipped from the beginning of the results
     * @param limit           The maximum amount of results to be returned
     * @param param           A parameter received by the cache caller
     * @return Data fulfilling the requirements
     */
    @Nonnull
    default Result<V> loadBackwards(K key, long lowestIncluded, long highestExcluded, int offset, int limit, P param) {
        throw new UnsupportedOperationException();
    }

    /**
     * An interface representing the results of a load operation
     *
     * @param <V> Type of the cache values
     */
    interface Result<V> {
        /**
         * The data resulting from the load. The collection should be sorted accordingly to the called method.
         *
         * @return The data
         */
        @Nonnull
        Collection<V> getData();

        /**
         * This method tells whether there is no more data past the returned data (in the direction
         * of the calling method).
         *
         * @return {@code true} if there is no further data past this
         */
        boolean isEndOfData();
    }

    /**
     * A simple implementation of the {@code Result} interface
     *
     * @param <V> Type of the cache values
     */
    class StdResult<V> implements Result<V> {
        protected final Collection<V> data;
        protected final boolean isEndOfData;

        public StdResult(Collection<V> data, boolean isEndOfData) {
            Objects.requireNonNull(data);
            this.data = data;
            this.isEndOfData = isEndOfData;
        }

        @Nonnull
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
