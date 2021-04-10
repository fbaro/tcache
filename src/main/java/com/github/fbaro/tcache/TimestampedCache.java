package com.github.fbaro.tcache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.ToLongFunction;

/**
 * A cache for time series, or otherwise linearly organized data.
 *
 * @param <K> Type of the keys in the cache. Should implement {@code hashCode} and {@code equals} as per the typical
 *            {@code Map} contract
 * @param <V> Type of the values in the key
 * @param <P> Type of a custom parameter which will be passed as-is to the loading function
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class TimestampedCache<K, V, P> {

    private final int chunkSize;
    private final long[] slices;
    /**
     * Contiene i dati standard ascendenti, necessari per l'algoritmo di slicing/chunking, ed i dati temporanei
     * discendenti.
     * I chunk discendenti hanno in chiave il timestamp di fine, ed i dati ordinati per timestamp decrescente.
     * Per i dati a slicing massimo, i sequenziali dei chunk sono tutti negativi, con -1 ad indicare
     * il chunk piu' vicino al timestamp di fine dello slice.
     * TODO: Integrare (o piu' probabilmente svuotare) i dati discendenti quando si fanno ricerche all'avanti
     */
    private final Cache<Key<K>, Chunk<V>> cache;
    private final Loader<? super K, V, ? super P> loader;
    private final Function<? super V, ? extends V> cloner;
    private final ToLongFunction<? super V> timestamper;

    /**
     * Constructs a new, empty TimestampedCache instance.
     *
     * @param chunkSize       The maximum size of each chunk of data stored in the cache.
     * @param slices          The size of the timestamp slices. The array must be sorted in descending order. Each slice
     *                        size should be evenly divided by its next slice size.
     * @param timestamper     A function to assign timestamps to the data
     * @param loader          The cache loader, to retrieve data when it is not found in the cache
     * @param cloner          A function to clone V instances. This is used to ensure that the items in the cache and the
     *                        items returned by the iterators are not the same items, and therefore to ensure that
     *                        the cache contents cannot be unwittingly changed. The cloner will also be used with for
     *                        the same reason on items returned by the Loader. It can be set as the identity function
     *                        if these burden is taken care of by the caller.
     * @param caffeineBuilder An Caffeine cache builder, appropriately configured to build the underlying Caffeine cache
     */
    public TimestampedCache(
            int chunkSize,
            @Nonnull long[] slices,
            @Nonnull ToLongFunction<? super V> timestamper,
            @Nonnull Loader<? super K, V, ? super P> loader,
            @Nonnull Function<? super V, ? extends V> cloner,
            @Nonnull Caffeine<? super Key<K>, ? super Chunk<V>> caffeineBuilder) {
        this.chunkSize = chunkSize;
        this.slices = Arrays.copyOf(slices, slices.length);
        this.timestamper = timestamper;
        this.loader = loader;
        this.cloner = cloner;
        this.cache = caffeineBuilder.build();
        for (int i = 1; i < slices.length; i++) {
            if (slices[i - 1] <= slices[i] || slices[i - 1] % slices[i] != 0) {
                throw new IllegalArgumentException("The slices should be sorted in descending order, and smaller slices should evenly divide greater slices");
            }
        }
    }

    /**
     * Constructs a new, empty TimestampedCache instance, with the default Caffeine configuration, and the identity
     * function as cloner.
     *
     * @param chunkSize   The maximum size of each chunk of data stored in the cache.
     * @param slices      The size of the timestamp slices. The array must be sorted in descending order. Each slice
     *                    size should be evenly divided by its next slice size.
     * @param timestamper A function to assign timestamps to the data
     * @param loader      The cache loader, to retrieve data when it is not found in the cache
     */
    public TimestampedCache(int chunkSize, @Nonnull long[] slices, @Nonnull ToLongFunction<? super V> timestamper, @Nonnull Loader<? super K, V, ? super P> loader) {
        this(chunkSize, slices, timestamper, loader, x -> x, Caffeine.newBuilder());
    }

    /**
     * Retrieves the chunk size this cache was constructed with
     *
     * @return The chunk size
     */
    public int getChunkSize() {
        return chunkSize;
    }

    /**
     * Retrieves a copy of the slice sizes size this cache was constructed with. Modifying the returned array will have
     * no impact on the cache.
     *
     * @return The slice sizes
     */
    @Nonnull
    public long[] getSlices() {
        return Arrays.copyOf(slices, slices.length);
    }

    /**
     * Retrieves data from the cache belonging to the specified key,
     * in ascending timestamp order, starting from {@code lowestTimestamp}.
     *
     * @param key              The key
     * @param lowestTimestamp  The beginning timestamp (included)
     * @param highestTimestamp The ending timestamp (excluded)
     * @param param            The parameter to pass to the cache loader, if necessary. <b>Important: the passed
     *                         value must be valid at least as long as the returned iterator is in use</b>
     * @return An iterator on the results, lazily loaded in case of cache misses
     */
    @Nonnull
    public Iterator<V> getAscending(K key, long lowestTimestamp, long highestTimestamp, P param) {
        return new AbstractIterator<V>() {

            private long curTimestamp;
            private int curChunkSeq = 0;
            private Chunk<V> curChunk = null;
            private CountingIterator<V> curChunkIterator = null;

            private void init() {
                // Nella ricerca del primo non devo riempire linearmente la cache,
                // ma posso "saltare" pezzi per arrivare in fretta al punto richiesto dall'utente
                for (long slice : slices) {
                    curTimestamp = roundDown(lowestTimestamp, slice);
                    curChunk = getChunkAsc(key, curTimestamp, 0, false, param);
                    if (curChunk.getEndTimestamp(curTimestamp, slices) >= lowestTimestamp) {
                        if (curChunk.complete || curChunk.getMaxTimestamp(timestamper) >= lowestTimestamp) {
                            curChunkIterator = curChunk.iterator();
                            return;
                        } else {
                            curChunk = getChunkAsc(key, curTimestamp, 0, true, param);
                            if (curChunk.getEndTimestamp(curTimestamp, slices) >= lowestTimestamp) {
                                curChunkIterator = curChunk.iterator();
                                return;
                            }
                        }
                    }
                }
                throw new IllegalStateException("Should not be reachable");
            }

            @Override
            protected V computeNext() {
                if (curChunk == null) {
                    init();
                }
                for (; curChunkIterator.hasNext() || !curChunk.complete || (!curChunk.endOfDataAscending && getNextTimestamp() < highestTimestamp); moveToNextChunk()) {
                    for (; curChunkIterator.hasNext() || !curChunk.complete; completeChunk()) {
                        while (curChunkIterator.hasNext()) {
                            V ret = curChunkIterator.next();
                            long retTs = timestamper.applyAsLong(ret);
                            if (retTs >= highestTimestamp) {
                                return endOfData();
                            } else if (retTs >= lowestTimestamp) {
                                return cloner.apply(ret);
                            }
                        }
                    }
                }
                return endOfData();
            }

            private void completeChunk() {
                if (!curChunk.complete) {
                    int toSkip = curChunkIterator.getCount();
                    curChunk = getChunkAsc(key, curTimestamp, curChunkSeq, true, param);
                    curChunkIterator = curChunk.iterator();
                    while (toSkip > 0) {
                        if (!curChunkIterator.hasNext()) {
                            // Questo puo' succedere se il chunk incompleto era ad un livello di slicing,
                            // ma quando ho cercato di completarlo lo slicing e' diventato piu' fine
                            moveToNextChunk();
                        } else {
                            curChunkIterator.next();
                            toSkip--;
                        }
                    }
                }
            }

            private void moveToNextChunk() {
                if (curChunk.hasNextChunk()) {
                    curChunkSeq++;
                } else {
                    curTimestamp = curChunk.getEndTimestamp(curTimestamp, slices);
                    curChunkSeq = 0;
                }
                curChunk = getChunkAsc(key, curTimestamp, curChunkSeq, false, param);
                curChunkIterator = curChunk.iterator();
            }

            private long getNextTimestamp() {
                return !curChunk.complete || curChunk.hasNextChunk() ? curTimestamp : curChunk.getEndTimestamp(curTimestamp, slices);
            }
        };
    }

    /**
     * Retrieves data from the cache belonging to the specified key, in descending timestamp order
     * from {@code highestTimestamp} and to {@code lowestTimestamp}.
     *
     * @param key              The key
     * @param lowestTimestamp  The beginning timestamp (excluded)
     * @param highestTimestamp The ending timestamp (included)
     * @param param            The parameter to pass to the cache loader, if necessary. <b>Important: the passed
     *                         value must be valid at least as long as the returned iterator is in use</b>
     * @return An iterator on the results, lazily loaded in case of cache misses
     */
    @Nonnull
    public Iterator<V> getDescending(K key, long lowestTimestamp, long highestTimestamp, P param) {
        return new AbstractIterator<V>() {

            private long curEndTimestamp;
            private DescendingResult<V> curDR = null;
            private CountingIterator<V> curChunkIterator;

            private void init() {
                for (long slice : slices) {
                    curEndTimestamp = slice + roundDown(highestTimestamp, slice);
                    curDR = getChunkDesc(key, curEndTimestamp, -1, false, param);
                    if (curEndTimestamp - slices[curDR.chunk.sliceLevel] <= highestTimestamp) {
                        initChunk();
                        return;
                    }
                }
                throw new IllegalStateException("Should not be reachable");
            }

            private void initChunk() {
                curChunkIterator = curDR.backIterator();
                for (int i = 0; i < curDR.toSkip; i++) {
                    curChunkIterator.next();
                }
            }

            @Override
            protected V computeNext() {
                if (curDR == null) {
                    init();
                }
                for (; curChunkIterator.hasNext() || !curDR.chunk.complete || (!curDR.chunk.endOfDataDescending && getPrevEndTimestamp() > lowestTimestamp); moveToPrevChunk()) {
                    for (; curChunkIterator.hasNext() || !curDR.chunk.complete; completeChunk()) {
                        while (curChunkIterator.hasNext()) {
                            V ret = curChunkIterator.next();
                            long retTs = timestamper.applyAsLong(ret);
                            if (retTs <= lowestTimestamp) {
                                return endOfData();
                            } else if (retTs <= highestTimestamp) {
                                return cloner.apply(ret);
                            }
                        }
                    }
                }
                return endOfData();
            }

            private void completeChunk() {
                if (!curDR.chunk.complete) {
                    int toSkip = curChunkIterator.getCount();
                    curDR = getChunkDesc(key, curEndTimestamp, curDR.chunkSeq, true, param);
                    initChunk();
                    while (toSkip > 0) {
                        if (!curChunkIterator.hasNext()) {
                            // Questo puo' succedere se il chunk incompleto era ad un livello di slicing,
                            // ma quando ho cercato di completarlo lo slicing e' diventato piu' fine
                            moveToPrevChunk();
                        } else {
                            curChunkIterator.next();
                            toSkip--;
                        }
                    }
                }
            }

            private void moveToPrevChunk() {
                if (curDR.hasPrevChunk()) {
                    curDR = getChunkDesc(key, curEndTimestamp, curDR.chunkSeq - 1, false, param);
                } else {
                    curEndTimestamp -= slices[curDR.chunk.sliceLevel];
                    curDR = getChunkDesc(key, curEndTimestamp, -1, false, param);
                }
                initChunk();
            }

            private long getPrevEndTimestamp() {
                return !curDR.chunk.complete || curDR.hasPrevChunk() ? curEndTimestamp : curEndTimestamp - slices[curDR.chunk.sliceLevel];
            }
        };
    }

    /**
     * Removes specific data from the cache. Due to the slicing algorithm, more than the requested data might actually
     * be removed from the cache. As the cache has no notion of its own boundaries, using this method with a very high
     * range will make it quite slow.
     *
     * @param key              The key
     * @param lowestTimestamp  The beginning timestamp (included)
     * @param highestTimestamp The ending timestamp (excluded)
     */
    public void invalidate(K key, long lowestTimestamp, long highestTimestamp) {
        long ts = roundDown(lowestTimestamp, slices[0]);
        int seqNo = 0;
        while (ts < highestTimestamp) {
            Key<K> k = Key.asc(key, ts, 0);
            Chunk<V> chunk = cache.getIfPresent(k);
            while (chunk != null) {
                cache.invalidate(k);
                if (!chunk.complete || !chunk.hasNextChunk()) {
                    break;
                }
                k = Key.asc(key, ts, ++seqNo);
                chunk = cache.getIfPresent(k);
            }

            ts += slices[chunk == null ? slices.length - 1 : chunk.sliceLevel];
        }
    }

    /**
     * Completely empties the cache.
     */
    public void invalidateAll() {
        cache.invalidateAll();
    }

    private Chunk<V> getChunkAsc(K key, long timestamp, int chunkSeq, boolean mustBeComplete, P param) {
        Key<K> k = Key.asc(key, timestamp, chunkSeq);
        @Nullable Chunk<V> chunk = cache.getIfPresent(k);
        if (chunk != null && (chunk.complete || !mustBeComplete)) {
            return chunk;
        }
        if (chunk != null || chunkSeq > 0) {
            // In sostanza rimetto in una lista tutti i dati dei chunk con stesso timestamp,
            // aggiungo i dati caricati di recente, e rifaccio l'arrange - quindi la rifaccio
            // anche per i chunk che ci sono gia'. Nulla di tragico, ma migliorabile.
            // Attenzione ai dati con stesso timestamp se lo miglioro
            List<V> lResult = new ArrayList<>();
            for (int c = 0; c < chunkSeq; c++) {
                lResult.addAll(getChunkAsc(key, timestamp, c, true, param).data);
            }
            if (chunk != null) {
                lResult.addAll(chunk.data);
            }
            long lastTs = timestamper.applyAsLong(lResult.get(lResult.size() - 1));
            int lastIdx = binarySearch(lResult, timestamper, lastTs);
            long highestExcluded = lastTs + slices[0];
            Loader.Result<V> result = loader.loadAscending(key, lastTs, highestExcluded, lResult.size() - lastIdx, chunkSize + 1, param);
            int preSize = lResult.size();
            CloningIterator.from(result.getData(), cloner).forEachRemaining(lResult::add);
            int resultSize = lResult.size() - preSize;
            long resultEndTs = resultSize == chunkSize + 1 ? timestamper.applyAsLong(lResult.get(lResult.size() - 1)) : highestExcluded;
            return arrangeAsc(key, timestamp, chunkSeq, resultEndTs, ImmutableList.copyOf(lResult), result.isEndOfData());
        }
        Loader.Result<V> result = loader.loadAscending(key, timestamp, timestamp + slices[0], 0, chunkSize + 1, param);
        ImmutableList<V> lResult = ImmutableList.copyOf(CloningIterator.from(result.getData(), cloner));
        long resultEndTs = lResult.size() == chunkSize + 1 ? timestamper.applyAsLong(lResult.get(lResult.size() - 1)) : timestamp + slices[0];
        return arrangeAsc(key, timestamp, 0, resultEndTs, lResult, result.isEndOfData());
    }

    @Nonnull
    private DescendingResult<V> getChunkDesc(K key, long endTs, int chunkSeq, boolean mustBeComplete, P param) {
        // Da fuori ho gia' iniziato a scorrere i chunk in avanti: continuo sulla cache standard
        if (chunkSeq >= 0) {
            long startTs = endTs - slices[slices.length - 1];
            return new DescendingResult<>(getChunkAsc(key, startTs, chunkSeq, true, param), chunkSeq, 0);
        }

        // Non ho info specifiche: provo a cercare nella cache in avanti
        for (int s = slices.length - 1; s >= minSliceLevel(endTs); s--) {
            long startTs = endTs - slices[s];
            int nc = 0; // Number of loaded chunks
            @Nullable Chunk<V> chunk;
            do {
                Key<K> k = Key.asc(key, startTs, nc);
                chunk = cache.getIfPresent(k);
                nc++;
            } while (chunk != null && chunk.complete && chunk.hasNextChunk());
            if (chunk != null && chunk.complete) {
                if (chunk.sliceLevel != s) {
                    // Ho trovato uno slicing piu' grossolano di quello che mi aspettavo
                    // Significa che lo slice che contiene i miei dati sarebbe a slicing piu' fine,
                    // e lo ho gia' cercato senza trovarlo
                    break;
                }
                // Ho trovato lo slice che cercavo, completo di tutti i chunk, nella cache in avanti
                // Localizzo il chunk in avanti che mi serve, la posizione al suo interno, e lo restituisco
                if (chunkSeq == -1) {
                    return new DescendingResult<>(chunk, nc - 1, 0);
                } else {
                    return new DescendingResult<>(
                            getChunkAsc(key, startTs, nc + chunkSeq, true, param),
                            nc + chunkSeq,
                            chunkSize - chunk.data.size()); // Aggiusto lo skip se i bordi dei chunk non sono allineati
                }
            }
        }

        // A questo punto i dati non sono nella cache standard
        // Cerco nella cache dei parziali all'indietro
        Key<K> pKey = Key.desc(key, endTs, chunkSeq);
        Chunk<V> pChunk = cache.getIfPresent(pKey);
        if (pChunk != null && (!mustBeComplete || pChunk.complete)) {
            return new DescendingResult<>(pChunk, chunkSeq, 0);
        }

        // Non sono neanche li'!
        // Ricostruisco quel che so dello slice, aggiungo un po' di dati e rifaccio una arrange
        List<V> chunkUnion = new ArrayList<>();
        for (int i = -1; i > chunkSeq; i--) {
            DescendingResult<V> chunkDesc = getChunkDesc(key, endTs, i, true, param);
            chunkUnion.addAll(chunkDesc.chunk.data.subList(chunkDesc.toSkip, chunkDesc.chunk.data.size()));
        }
        if (pChunk != null) {
            chunkUnion.addAll(pChunk.data);
        }

        if (chunkUnion.isEmpty()) {
            checkState(chunkSeq == -1);
            // I dati mancano completamente
            Loader.Result<V> result = loader.loadDescending(key, endTs - slices[0], endTs, 0, chunkSize + 1, param);
            ImmutableList<V> lResult = ImmutableList.copyOf(CloningIterator.from(result.getData(), cloner));
            long resultStartTs = lResult.size() == chunkSize + 1 ? timestamper.applyAsLong(lResult.get(lResult.size() - 1)) : endTs - slices[0];
            return arrangeDesc(key, endTs, -1, resultStartTs, lResult, result.isEndOfData());
        } else {
            // Completo i dati parziali facendo un'altra load all'indietro
            long lastTs = timestamper.applyAsLong(chunkUnion.get(chunkUnion.size() - 1));
            int lastTsFirstIdx = binarySearchDesc(chunkUnion, timestamper, lastTs);
            long loadEndTs = (lastTsFirstIdx == 0 ? endTs : timestamper.applyAsLong(chunkUnion.get(lastTsFirstIdx - 1)));
            Loader.Result<V> result = loader.loadDescending(key, loadEndTs - slices[0], loadEndTs, chunkUnion.size() - lastTsFirstIdx, chunkSize + 1, param);
            int preSize = chunkUnion.size();
            CloningIterator.from(result.getData(), cloner).forEachRemaining(chunkUnion::add);
            int resultSize = chunkUnion.size() - preSize;
            long resultStartTs = resultSize == chunkSize + 1 ? timestamper.applyAsLong(chunkUnion.get(chunkUnion.size() - 1)) : endTs - slices[0];
            return arrangeDesc(key, endTs, chunkSeq, resultStartTs, ImmutableList.copyOf(chunkUnion), result.isEndOfData());
        }
    }

    /**
     * Questa funzione riempie la cache con i dati ricevuti in input.
     *
     * @param key                La chiave dei dati
     * @param timestamp          Il timestamp da cui i dati iniziano
     * @param retChunkSeq        Il numero di chunk che si vuole venga restituito
     * @param resultEndTimestamp Indica che tra il timestamp dell'ultimo dato e questo timestamp (escluso) non ci sono altri dati.
     *                           Puo' essere minore o uguale al timestamp dell'ultimo dato, e in questo caso non indica nulla.
     * @param result             I dati da inserire in cache
     * @param endOfData          Indica se abbiamo raggiunto il flag di endOfData leggendo dal Loader
     * @return Il chunk associato a (timestamp, chunkSeq)
     */
    private Chunk<V> arrangeAsc(K key, long timestamp, int retChunkSeq, long resultEndTimestamp, ImmutableList<V> result, boolean endOfData) {
        if (result.isEmpty()) {
            checkState(retChunkSeq == 0);
            int s = minSliceLevel(timestamp);
            Chunk<V> chunk = Chunk.empty(s, endOfData, false);
            cache.put(Key.asc(key, timestamp, 0), chunk);
            return chunk;
        }

        int chunkSeq = -1;
        int l = slices.length;
        Chunk<V> ret = null;
        outer:
        while (!result.isEmpty()) {
            if (chunkSeq >= 0) {
                // So gia' che devo andare allo slicing massimo
                long endTimestamp = timestamp + slices[l - 1];
                int position = binarySearch(result, timestamper, endTimestamp);
                position = (position >= 0 ? position : -position - 1);
                int chunkEnd = Math.min(chunkSize, position);

                boolean complete = chunkEnd < result.size() || endTimestamp <= resultEndTimestamp;
                Chunk<V> chunk = Chunk.create(result.subList(0, chunkEnd), l - 1, complete, position > chunkSize, endOfData && chunkEnd == result.size(), false);
                cache.put(Key.asc(key, timestamp, chunkSeq), chunk);
                result = result.subList(chunkEnd, result.size());
                ret = (ret == null && chunkSeq == retChunkSeq ? chunk : ret);
                chunkSeq = (chunk.hasNextChunk ? chunkSeq + 1 : -1);
                timestamp = (chunk.hasNextChunk ? timestamp : endTimestamp);
            } else {
                int s = minSliceLevel(timestamp);
                for (; s < l; s++) {
                    long endTimestamp = timestamp + slices[s];
                    int chunkEnd = binarySearch(result, timestamper, endTimestamp);
                    chunkEnd = (chunkEnd >= 0 ? chunkEnd : -chunkEnd - 1);
                    if (chunkEnd <= chunkSize) {
                        // Ho trovato un livello di slicing che produce uno slice non oltre la massima dimensione
                        // Non ho bisogno di chunking

                        // Il chunk e' completo se ho altri risultati oltre il suo termine,
                        // o se so che non ci sono altri dati oltre endTimestamp
                        boolean complete = chunkEnd < result.size() || endTimestamp <= resultEndTimestamp;
                        Chunk<V> chunk = Chunk.create(result.subList(0, chunkEnd), s, complete, false, endOfData && chunkEnd == result.size(), false);
                        cache.put(Key.asc(key, timestamp, 0), chunk);
                        result = result.subList(chunkEnd, result.size());
                        ret = (ret == null && 0 == retChunkSeq ? chunk : ret);
                        chunkSeq = -1;
                        timestamp = endTimestamp;
                        continue outer;
                    }
                }
                // Cycle back and start chunking
                chunkSeq = 0;
            }
        }

        return ret != null ? ret : Chunk.empty(minSliceLevel(timestamp), false, false);
    }

    /**
     * Riempie la cache con i dati ordinati al contrario che terminano in endTimestamp
     *
     * @param key                  La chiave dei dati
     * @param endTs                Il timestamp in cui i dati terminano, estremo escluso
     * @param retChunkSeq          Il numero di chunk che si vuole venga restituito (dato che si va all'indietro, -1 e' il primo chunk)
     * @param resultStartTimestamp Indica che tra il timestamp del primo dato e questo timestamp (incluso) non ci sono altri dati.
     *                             Puo' essere maggiore o uguale al timestamp dell'ultimo dato, e in questo caso non indica nulla.
     * @param result               I dati da inserire in cache, ordinati per timestamp decrescente
     * @param endOfData            Indica se abbiamo raggiunto il flag di endOfData leggendo dal Loader
     * @return Il chunk che termina in endTs e contiene i primi dati presenti in result
     */
    private DescendingResult<V> arrangeDesc(K key, long endTs, int retChunkSeq, long resultStartTimestamp, ImmutableList<V> result, boolean endOfData) {
        if (result.isEmpty()) {
            checkState(retChunkSeq == -1);
            int s = minSliceLevel(endTs);
            Chunk<V> chunk = Chunk.empty(s, false, endOfData);
            cache.put(Key.asc(key, endTs - slices[s], 0), chunk);
            return new DescendingResult<>(chunk, 0, 0);
        }

        DescendingResult<V> ret = null;
        int l = slices.length;
        outer:
        while (!result.isEmpty()) {
            int s = minSliceLevel(endTs);
            for (; s < l; s++) {
                long startTs = endTs - slices[s];
                int sliceEnd = binarySearchDesc(result, timestamper, startTs);
                sliceEnd = (sliceEnd >= 0 ? sliceEnd : -sliceEnd - 2) + 1; // L'estremo inferiore, se trovato, va incluso
                if (sliceEnd <= chunkSize) {
                    // Ho trovato un livello di slicing che produce uno slice non oltre la massima dimensione
                    // Non ho bisogno di chunking

                    // Il chunk e' completo se ho altri risultati oltre il suo inizio,
                    // o se so che non ci sono altri dati oltre startTs
                    boolean complete = sliceEnd < result.size() || startTs >= resultStartTimestamp;
                    if (complete) {
                        Chunk<V> chunk = Chunk.create(result.subList(0, sliceEnd).invert(), s, true, false, false, endOfData && sliceEnd == result.size());
                        // TODO: Svuotare la pCache? Come?
                        cache.put(Key.asc(key, startTs, 0), chunk);
                        ret = (ret == null ? new DescendingResult<>(chunk, 0, 0) : ret);
                    } else {
                        Chunk<V> chunk = Chunk.create(result.subList(0, sliceEnd), s, false, false, false, endOfData && sliceEnd == result.size());
                        cache.put(Key.desc(key, endTs, -1), chunk);
                        ret = (ret == null ? new DescendingResult<>(chunk, -1, 0) : ret);
                    }
                    result = result.subList(sliceEnd, result.size());
                    endTs = startTs;
                    continue outer;
                }
                if (s == l - 1) {
                    // Sono arrivato allo slicing massimo
                    boolean sliceComplete = sliceEnd < result.size() || startTs >= resultStartTimestamp;
                    int numChunks = sliceEnd / chunkSize + (sliceEnd % chunkSize == 0 ? 0 : 1);
                    if (sliceComplete) {
                        // Lo slice e' completo: metto tutti i chunk nella cache standard
                        ImmutableList<V> sliceData = result.subList(0, sliceEnd).invert();
                        int remainder = sliceEnd % chunkSize;
                        for (int i = numChunks - 1; i >= 0; i--) {
                            int startIdx = i * chunkSize;
                            int endIdx = Math.min((i + 1) * chunkSize, sliceEnd);
                            Chunk<V> chunk = Chunk.create(sliceData.subList(startIdx, endIdx), s, true, i < numChunks - 1, false, endOfData && sliceEnd == result.size() && i == 0);
                            cache.put(Key.asc(key, startTs, i), chunk);
                            // Sto raddrizzando i dati, quindi devo restituire un chunk parecchio diverso da quello richiesto
                            // e se il raddrizzamento ha cambiato i "bordi" dei chunk devo riprendere da un certo indice
                            ret = (ret == null && i == numChunks + retChunkSeq ? new DescendingResult<>(chunk, i, remainder == 0 || retChunkSeq == -1 ? 0 : chunkSize - remainder) : ret);
                        }
                        // TODO: Svuotare la pCache? Come?
                    } else {
                        // Lo slice e' incompleto: metto tutti i chunk nella cache prev, non raddrizzati e con i sequenziali negativi
                        for (int i = 0; i < numChunks; i++) {
                            int startIdx = i * chunkSize;
                            int endIdx = Math.min((i + 1) * chunkSize, sliceEnd);
                            Chunk<V> chunk = Chunk.create(result.subList(startIdx, endIdx), s, i < numChunks - 1, i < numChunks - 1, false, endOfData && sliceEnd == result.size() && i == numChunks - 1);
                            cache.put(Key.desc(key, endTs, -i - 1), chunk);
                            ret = (ret == null && retChunkSeq == -i - 1 ? new DescendingResult<>(chunk, -i - 1, 0) : ret);
                        }
                    }
                    result = result.subList(sliceEnd, result.size());
                    endTs = startTs;
                    continue outer;
                }
            }
        }

        return ret != null ? ret : new DescendingResult<>(Chunk.empty(minSliceLevel(endTs), false, false), retChunkSeq, 0);
    }

    private int minSliceLevel(long timestamp) {
        for (int i = 0, l = slices.length; i < l; i++) {
            if (timestamp % slices[i] == 0) {
                return i;
            }
        }
        throw new IllegalArgumentException("Timestamp is not a multiple of any slice");
    }

    static long roundDown(long value, long rounding) {
        long remainder = value % rounding;
        if (remainder < 0) {
            return value - remainder - rounding;
        } else {
            return value - remainder;
        }
    }

    static <V> int binarySearch(List<? extends V> elements, ToLongFunction<? super V> timestamper, long ts) {
        return binarySearch(elements, timestamper, ts, false);
    }

    static <V> int binarySearchDesc(List<? extends V> elements, ToLongFunction<? super V> timestamper, long ts) {
        return binarySearch(elements, timestamper, ts, true);
    }

    private static <V> int binarySearch(List<? extends V> elements, ToLongFunction<? super V> timestamper, long ts, boolean descending) {
        int last = elements.size() - 1;
        int low = 0;
        int high = last;
        while (low <= high) {
            int mid = (low + high) / 2;
            long midTs = timestamper.applyAsLong(elements.get(mid));
            if (midTs == ts) {
                while (mid > 0 && timestamper.applyAsLong(elements.get(mid - 1)) == ts) {
                    --mid;
                }
                return mid;
            } else if (descending == ts > midTs) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return -low - 1;
    }

    private static void checkState(boolean expression) {
        if (!expression) {
            throw new IllegalStateException();
        }
    }

    /**
     * Holds a chunk of data.
     * complete = true: data contains the full data for this timestamp and slice level
     * complete = false &amp;&amp; data.size &lt; chunkSize: this chunk was loaded only partially
     * complete = false &amp;&amp; data.size == chunkSize: this chunk is complete, but there is
     * more data in the next chunkSeq.
     *
     * @param <V> The data type
     */
    public static final class Chunk<V> {
        private final ImmutableList<V> data;
        private final int sliceLevel;
        private final boolean complete;
        private final boolean hasNextChunk;
        private final boolean endOfDataAscending;
        private final boolean endOfDataDescending;

        private Chunk(ImmutableList<V> data, int sliceLevel, boolean complete, boolean hasNextChunk, boolean endOfDataAscending, boolean endOfDataDescending) {
            this.data = data;
            this.sliceLevel = sliceLevel;
            this.complete = complete;
            this.hasNextChunk = hasNextChunk;
            this.endOfDataAscending = endOfDataAscending;
            this.endOfDataDescending = endOfDataDescending;
            if (data.isEmpty() && !complete) {
                throw new IllegalStateException("Empty chunks must be complete; incomplete chunks must have some data");
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

        long getEndTimestamp(long startTimestamp, long[] slices) {
            return startTimestamp + slices[sliceLevel];
        }

        long getMaxTimestamp(ToLongFunction<? super V> timestamper) {
            return timestamper.applyAsLong(data.get(data.size() - 1));
        }

        CountingIterator<V> iterator() {
            return CountingIterator.create(data.iterator());
        }

        CountingIterator<V> reverseIterator() {
            return CountingIterator.create(data.reverseIterator());
        }

        /**
         * Retrieves the data contained by this Chunk. The returned List is immutable. It is bad practice to
         * change the content of the list elements.
         *
         * @return The data contained in this cache Chunk
         */
        @Nonnull
        public List<V> getData() {
            return data;
        }

        static <V> Chunk<V> create(ImmutableList<V> data, int sliceLevel, boolean complete, boolean hasNextChunk, boolean endOfDataAscending, boolean endOfDataDescending) {
            if (data.isEmpty()) {
                if (!complete || hasNextChunk) {
                    throw new IllegalArgumentException("Cannot create empty+incomplete or empty+hasNext chunk");
                }
                return empty(sliceLevel, endOfDataAscending, endOfDataDescending);
            }
            return new Chunk<>(data, sliceLevel, complete, hasNextChunk, endOfDataAscending, endOfDataDescending);
        }

        static <V> Chunk<V> empty(int sliceLevel, boolean endOfDataAscending, boolean endOfDataDescending) {
            if (sliceLevel < EMPTY_CACHE_SIZE) {
                //noinspection unchecked
                return (Chunk<V>) EMPTY_CACHE.get(sliceLevel + 2 * EMPTY_CACHE_SIZE * (endOfDataAscending ? 1 : 0) + EMPTY_CACHE_SIZE * (endOfDataDescending ? 1 : 0));
            }
            return new Chunk<>(ImmutableList.of(), sliceLevel, true, false, endOfDataAscending, endOfDataDescending);
        }

        private static final int EMPTY_CACHE_SIZE = 32;
        private static final List<Chunk<Object>> EMPTY_CACHE = new ArrayList<>(2 * 2 * 2 * EMPTY_CACHE_SIZE);

        static {
            for (boolean endOfDataAscending : new boolean[]{false, true}) {
                for (boolean endOfDataDescending : new boolean[]{false, true}) {
                    for (int s = 0; s < EMPTY_CACHE_SIZE; s++) {
                        EMPTY_CACHE.add(new Chunk<>(ImmutableList.of(), s, true, false, endOfDataAscending, endOfDataDescending));
                    }
                }
            }
        }
    }

    private static final class DescendingResult<V> {
        public final Chunk<V> chunk;
        public final int chunkSeq;
        public final int toSkip;

        public DescendingResult(Chunk<V> chunk, int chunkSeq, int toSkip) {
            this.chunk = chunk;
            this.chunkSeq = chunkSeq;
            this.toSkip = toSkip;
        }

        boolean hasPrevChunk() {
            if (!chunk.complete) {
                throw new IllegalStateException("Chunk is not complete, hasPrevChunk is not meaningful");
            }
            return (chunkSeq < 0 && chunk.hasNextChunk) || chunkSeq > 0;
        }

        CountingIterator<V> backIterator() {
            if (chunkSeq < 0) {
                return chunk.iterator();
            } else {
                return chunk.reverseIterator();
            }
        }
    }

    /**
     * This data structure wraps one "external" key, associating internal data to it.
     *
     * @param <K> The TimestampedCache key type
     */
    public static final class Key<K> {
        private final boolean ascending;
        private final K key;
        private final long ts;
        private final int chunkSeq;

        private Key(boolean ascending, K key, long ts, int chunkSeq) {
            this.ascending = ascending;
            this.key = key;
            this.ts = ts;
            this.chunkSeq = chunkSeq;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key<?> key1 = (Key<?>) o;
            return ascending == key1.ascending &&
                    ts == key1.ts &&
                    chunkSeq == key1.chunkSeq &&
                    Objects.equals(key, key1.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(ascending, key, ts, chunkSeq);
        }

        @Override
        public String toString() {
            return "Key{" +
                    "ascending=" + ascending +
                    ", key=" + key +
                    ", ts=" + ts +
                    ", chunkSeq=" + chunkSeq +
                    '}';
        }

        /**
         * Retrieves the underlying key value
         *
         * @return The key value
         */
        public K getKey() {
            return key;
        }

        /**
         * Retrieves the timestamp this key represents. It is the minimum timestamp of the chunk if the key is
         * ascending, or the maximum if the key is descending
         *
         * @return The key timestamp
         */
        public long getTimestamp() {
            return ts;
        }

        /**
         * Determines the direction of the data associated with this Key
         *
         * @return {@code true} if the data is ascending, {@code false} if it is descending
         */
        public boolean isAscending() {
            return ascending;
        }

        static <K> Key<K> asc(K key, long ts, int chunkSeq) {
            return new Key<>(true, key, ts, chunkSeq);
        }

        static <K> Key<K> desc(K key, long ts, int chunkSeq) {
            return new Key<>(false, key, ts, chunkSeq);
        }
    }
}
