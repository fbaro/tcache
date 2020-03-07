package it.fb.tcache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class TimestampedCache<K, V, P> {

    private final int chunkSize;
    private final long[] slices;
    private final Cache<Key<K>, Chunk<V>> cache = Caffeine.newBuilder().build();
    private final Loader<? super K, ? extends V, ? super P> loader;
    private final Timestamper<? super V> timestamper;

    public TimestampedCache(int chunkSize, long[] slices, Timestamper<? super V> timestamper, Loader<? super K, ? extends V, ? super P> loader) {
        this.chunkSize = chunkSize;
        this.slices = slices;
        this.timestamper = timestamper;
        this.loader = loader;
    }

    /**
     * Retrieves data from the cache belonging to the specified key, starting
     * from {@code startTimestamp}.
     *
     * @param key            The key
     * @param startTimestamp The beginning timestamp (included)
     * @param endTimestamp   The ending timestamp (excluded)
     * @param param          The parameter to pass to the cache loader, if necessary
     * @return An iterator on the results
     */
    public Iterator<V> getForward(K key, long startTimestamp, long endTimestamp, P param) {
        return new AbstractIterator<V>() {

            private long nextTimestamp = (startTimestamp / slices[0]) * slices[0];
            private int nextChunkSeq = 0;
            private Chunk<V> curChunk = Chunk.empty(0, false, false);
            private PeekingIterator<V> curChunkIterator = curChunk.iterator();

            @Override
            protected V computeNext() {
                if (curChunkIterator.hasNext()) {
                    V ret = curChunkIterator.next();
                    if (timestamper.getTs(ret) >= endTimestamp) {
                        return endOfData();
                    }
                    return ret;
                }
                while (!curChunk.endOfDataForward && nextTimestamp < endTimestamp) { // TODO: Posso anche terminare a meta' di un chunk
                    curChunk = getFwd(key, nextTimestamp, nextChunkSeq, param);
                    if (curChunk.hasNextChunk()) {
                        nextChunkSeq++;
                    } else {
                        nextTimestamp = curChunk.getEndTimestamp(nextTimestamp, slices);
                        nextChunkSeq = 0;
                    }
                    curChunkIterator = curChunk.iterator();

                    while (curChunkIterator.hasNext() && timestamper.getTs(curChunkIterator.peek()) < startTimestamp) {
                        curChunkIterator.next();
                    }
                    if (curChunkIterator.hasNext() && timestamper.getTs(curChunkIterator.peek()) < endTimestamp) {
                        return curChunkIterator.next();
                    }
                }
                return endOfData();
            }
        };
    }

    private Chunk<V> getFwd(K key, long timestamp, int chunkSeq, P param) {
        Key<K> k = new Key<>(key, timestamp, chunkSeq);
        @Nullable Chunk<V> chunk = cache.getIfPresent(k);
        if (chunk != null && chunk.complete) {
            return chunk;
        }
        if (chunk != null || chunkSeq > 0) {
            // In sostanza rimetto in una lista tutti i dati dei chunk con stesso timestamp,
            // aggiungo i dati caricati di recente, e rifaccio l'arrange - quindi la rifaccio
            // anche per i chunk che ci sono gia'. Nulla di tragico, ma migliorabile.
            // Attenzione ai dati con stesso timestamp se lo miglioro
            List<V> lResult = new ArrayList<>();
            for (int c = 0; c < chunkSeq; c++) {
                lResult.addAll(getFwd(key, timestamp, c, param).data);
            }
            if (chunk != null) {
                lResult.addAll(chunk.data);
            }
            long resultEndTs = appendForward(key, param, lResult);
            arrangeFwd(key, timestamp, -1, resultEndTs, ImmutableList.copyOf(lResult));
            return cache.getIfPresent(k); // TODO: Problematico in concorrenza
        }
        Loader.Result<? extends V> result = loader.loadForward(key, timestamp, timestamp + slices[0], 0, chunkSize + 1, param);
        List<V> lResult = ImmutableList.copyOf(result.getData());
        long resultEndTs = result.getData().size() == chunkSize + 1 ? timestamper.getTs(lResult.get(lResult.size() - 1)) : timestamp + slices[0];
        return arrangeFwd(key, timestamp, -1, resultEndTs, lResult);
    }

    private long appendForward(K key, P param, List<V> lResult) {
        if (lResult.isEmpty()) {
            throw new IllegalStateException();
        }
        long lastTs = timestamper.getTs(lResult.get(lResult.size() - 1));
        int lastIdx = binarySearch(lResult, timestamper, lastTs, true);
        long highestExcluded = lastTs + slices[0];
        Loader.Result<? extends V> result = loader.loadForward(key, lastTs, highestExcluded, lResult.size() - lastIdx, chunkSize + 1, param);
        lResult.addAll(result.getData());
        return result.getData().size() == chunkSize + 1 ? timestamper.getTs(lResult.get(lResult.size() - 1)) : highestExcluded;
    }

    /**
     * Questa funzione riempie la cache con i dati ricevuti in input.
     * @param key La chiave dei dati
     * @param timestamp Il timestamp da cui i dati iniziano
     * @param chunkSeq La chunk sequence da cui i dati iniziano. Se non si ha la certezza di dover aggiungere dati
     *                 ad un chunk di slicing massimo, mettere -1.
     * @param resultEndTimestamp Indica che tra il timestamp dell'ultimo dato e questo timestamp (escluso) non ci sono altri dati.
     *                           Puo' essere minore o uguale al timestamp dell'ultimo dato, e in questo caso non indica nulla.
     * @param result I dati da inserire in cache
     * @return Il chunk associato a (timestamp, chunkSeq)
     */
    private Chunk<V> arrangeFwd(K key, long timestamp, int chunkSeq, long resultEndTimestamp, List<V> result) {
        Chunk<V> ret = null;
        int l = slices.length;

        if (result.isEmpty()) {
            if (chunkSeq > 0) {
                throw new IllegalStateException("Did not expect this");
            }
            int s = minSliceLevel(timestamp);
            Chunk<V> chunk = new Chunk<>(ImmutableList.of(), s, true, false, false, false);
            cache.put(new Key<>(key, timestamp, 0), chunk);
            return chunk;
        }

        outer: while (!result.isEmpty()) {
            if (chunkSeq >= 0) {
                // So gia' che devo andare allo slicing massimo
                long endTimestamp = timestamp + slices[l - 1];
                int position = binarySearch(result, timestamper, endTimestamp, true);
                position = (position >= 0 ? position : -position - 1);
                int chunkEnd = Math.min(chunkSize, position);

                boolean complete = chunkEnd < result.size() || endTimestamp <= resultEndTimestamp;
                Chunk<V> chunk = new Chunk<>(result.subList(0, chunkEnd), l - 1, complete, position > chunkSize, false, false);
                cache.put(new Key<>(key, timestamp, chunkSeq), chunk);
                result = result.subList(chunkEnd, result.size());
                ret = (ret == null ? chunk : ret);
                chunkSeq = (chunk.hasNextChunk ? chunkSeq + 1 : -1);
                timestamp = (chunk.hasNextChunk ? timestamp : endTimestamp);
            } else {
                int s = minSliceLevel(timestamp);
                for (; s < l; s++) {
                    long endTimestamp = timestamp + slices[s];
                    int chunkEnd = binarySearch(result, timestamper, endTimestamp, true);
                    chunkEnd = (chunkEnd >= 0 ? chunkEnd : -chunkEnd - 1);
                    if (chunkEnd <= chunkSize) {
                        // Ho trovato un livello di slicing che produce uno slice non oltre la massima dimensione
                        // Non ho bisogno di chunking

                        // Il chunk e' completo se ho altri risultati oltre il suo termine,
                        // o se so che non ci sono altri dati oltre endTimestamp
                        boolean complete = chunkEnd < result.size() || endTimestamp <= resultEndTimestamp;
                        Chunk<V> chunk = new Chunk<>(result.subList(0, chunkEnd), s, complete, false, false, false);
                        cache.put(new Key<>(key, timestamp, 0), chunk);
                        result = result.subList(chunkEnd, result.size());
                        ret = (ret == null ? chunk : ret);
                        chunkSeq = -1;
                        timestamp = endTimestamp;
                        continue outer;
                    }
                }
                chunkSeq = 0;
            }
        }

        return Preconditions.checkNotNull(ret);
    }

    static <V> int binarySearch(List<? extends V> elements, Timestamper<? super V> timestamper, long ts, boolean firstIfTied) {
        int last = elements.size() - 1;
        int low = 0;
        int high = last;
        while (low <= high) {
            int mid = (low + high) / 2;
            long midTs = timestamper.getTs(elements.get(mid));
            if (midTs == ts) {
                if (firstIfTied) {
                    while (mid > 0 && timestamper.getTs(elements.get(mid - 1)) == ts) {
                        --mid;
                    }
                } else {
                    while (mid < last && timestamper.getTs(elements.get(mid + 1)) == ts) {
                        ++mid;
                    }
                }
                return mid;
            } else if (ts < midTs) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return -low - 1;
    }

    private int minSliceLevel(long timestamp) {
        for (int i = 0, l = slices.length; i < l; i++) {
            if (timestamp % slices[i] == 0) {
                return i;
            }
        }
        throw new IllegalArgumentException("Timestamp is not a multiple of any slice");
    }

    public Iterator<V> getBackwards(K key, long startTimestamp, P param) {
        throw new UnsupportedOperationException("TODO");
    }

    /**
     * Holds a chunk of data.
     * complete = true: data contains the full data for this timestamp and slice level
     * complete = false && data.size < chunkSize: this chunk was loaded only partially
     * complete = false && data.size == chunkSize: this chunk is complete, but there is
     * more data in the next chunkSeq.
     *
     * @param <V> The data type
     */
    private static final class Chunk<V> {
        private final List<V> data;
        private final int sliceLevel;
        private final boolean complete;
        private final boolean hasNextChunk;
        private final boolean endOfDataForward;
        private final boolean endOfDataBackwards;

        Chunk(List<V> data, int sliceLevel, boolean complete, boolean hasNextChunk, boolean endOfDataForward, boolean endOfDataBackwards) {
            this.data = data;
            this.sliceLevel = sliceLevel;
            this.complete = complete;
            this.hasNextChunk = hasNextChunk;
            this.endOfDataForward = endOfDataForward;
            this.endOfDataBackwards = endOfDataBackwards;
            if (data.isEmpty() && !complete) {
                throw new IllegalStateException("Empty chunks must be complete");
            }
            if (data.isEmpty() && hasNextChunk) {
                throw new IllegalStateException("Empty chunks cannot have a next chunk");
            }
            if (!complete && hasNextChunk) {
                throw new IllegalStateException("An incomplete chunk cannot know it has a next chunk");
            }
        }

        boolean hasNextChunk() {
            if (!complete) {
                throw new IllegalStateException("Chunk is not complete, hasNextChunk is not meaningful");
            }
            return hasNextChunk;
        }

        public long getEndTimestamp(long startTimestamp, long[] slices) {
            return startTimestamp + slices[sliceLevel];
        }

        static <V> Chunk<V> empty(int sliceLevel, boolean endOfDataForward, boolean endOfDataBackwards) {
            // TODO: Cache
            return new Chunk<>(ImmutableList.of(), sliceLevel, true, false, endOfDataForward, endOfDataBackwards);
        }

        public PeekingIterator<V> iterator() {
            return Iterators.peekingIterator(data.iterator());
        }
    }

    private static final class Key<K> {
        private final K key;
        private final long ts;
        private final int chunkSeq;

        Key(K key, long ts, int chunkSeq) {
            this.key = key;
            this.ts = ts;
            this.chunkSeq = chunkSeq;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key<?> key1 = (Key<?>) o;
            return ts == key1.ts &&
                    chunkSeq == key1.chunkSeq &&
                    Objects.equals(key, key1.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, ts, chunkSeq);
        }

        @Override
        public String toString() {
            return "Key{" +
                    "key=" + key +
                    ", ts=" + ts +
                    ", chunkSeq=" + chunkSeq +
                    '}';
        }
    }
}
