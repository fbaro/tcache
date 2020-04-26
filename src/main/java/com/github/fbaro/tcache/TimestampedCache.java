package com.github.fbaro.tcache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
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

    public static final int LAST = Integer.MAX_VALUE;

    private final int chunkSize;
    private final long[] slices;
    /**
     * Contiene i dati standard ascendenti, necessari per l'algoritmo di slicing/chunking, ed i dati temporanei
     * discendenti,
     * I chunk discendenti hanno in chiave il timestamp di fine, ed i dati ordinati per timestamp decrescente.
     * Per i dati a slicing massimo, i sequenziali dei chunk sono tutti negativi, con -1 ad indicare
     * il chunk piu' vicino al timestamp di fine dello slice.
     * TODO: Integrare (o piu' probabilmente svuotare) i dati discendenti quando si fanno ricerche all'avanti
     */
    private final Cache<Key<K>, Chunk<V>> cache;
    private final Loader<? super K, ? extends V, ? super P> loader;
    private final ToLongFunction<? super V> timestamper;

    /**
     * Constructs a new, empty TimestampedCache instance.
     *
     * @param chunkSize       The maximum size of each chunk of data stored in the cache.
     * @param slices
     * @param timestamper
     * @param loader
     * @param caffeineBuilder
     */
    public TimestampedCache(
            int chunkSize,
            long[] slices,
            ToLongFunction<? super V> timestamper,
            Loader<? super K, ? extends V, ? super P> loader,
            Caffeine<Object, Object> caffeineBuilder) {
        this.chunkSize = chunkSize;
        this.slices = Arrays.copyOf(slices, slices.length);
        this.timestamper = timestamper;
        this.loader = loader;
        this.cache = caffeineBuilder.build();
        for (int i = 1; i < slices.length; i++) {
            if (slices[i - 1] <= slices[i] || slices[i - 1] % slices[i] != 0) {
                throw new IllegalArgumentException("The slices should be sorted in descending order, and smaller slices should evenly divide greater slices");
            }
        }
    }

    public TimestampedCache(int chunkSize, long[] slices, ToLongFunction<? super V> timestamper, Loader<? super K, ? extends V, ? super P> loader) {
        this(chunkSize, slices, timestamper, loader, Caffeine.newBuilder());
    }

    public int getChunkSize() {
        return chunkSize;
    }

    /**
     * Retrieves data from the cache belonging to the specified key, starting
     * from {@code lowestTimestamp}.
     *
     * @param key              The key
     * @param lowestTimestamp  The beginning timestamp (included)
     * @param highestTimestamp The ending timestamp (excluded)
     * @param param            The parameter to pass to the cache loader, if necessary. <b>Important: the passed
     *                         value must be valid at least as long as the returned iterator is in use</b>
     * @return An iterator on the results, lazily loaded in case of cache misses
     */
    public Iterator<V> getForward(K key, long lowestTimestamp, long highestTimestamp, P param) {
        return new AbstractIterator<V>() {

            private long nextTimestamp;
            private int nextChunkSeq = 0;
            private Chunk<V> curChunk = null;
            private PeekingIterator<V> curChunkIterator = null;

            private void init() {
                // Nella ricerca del primo non devo riempire linearmente la cache,
                // ma posso "saltare" pezzi per arrivare in fretta al punto richiesto dall'utente
                for (long slice : slices) {
                    nextTimestamp = roundDown(lowestTimestamp, slice);
                    curChunk = getChunkFwd(key, nextTimestamp, 0, param);
                    if (nextTimestamp + slices[curChunk.sliceLevel] >= lowestTimestamp) {
                        if (curChunk.hasNextChunk()) {
                            nextChunkSeq = 1;
                        } else {
                            nextTimestamp += slices[curChunk.sliceLevel];
                        }
                        curChunkIterator = curChunk.iterator();
                        return;
                    }
                }
                throw new IllegalStateException("Should not be reachable");
            }

            @Override
            protected V computeNext() {
                if (curChunk == null) {
                    init();
                }
                while (curChunkIterator.hasNext()) {
                    V ret = curChunkIterator.next();
                    long retTs = timestamper.applyAsLong(ret);
                    if (retTs >= highestTimestamp) {
                        return endOfData();
                    } else if (retTs >= lowestTimestamp) {
                        return ret;
                    }
                }
                while (!curChunk.endOfDataForward && nextTimestamp < highestTimestamp) { // TODO: Posso anche terminare a meta' di un chunk
                    // TODO: Non sarebbe male gestire qui anche i chunk parziali
                    // TODO: in modo da sfruttare piu' possibile cio' che ho gia' caricato,
                    // TODO: andando a richiedere dati al loader solo quando assolutamente necessario
                    curChunk = getChunkFwd(key, nextTimestamp, nextChunkSeq, param);
                    if (curChunk.hasNextChunk()) {
                        nextChunkSeq++;
                    } else {
                        nextTimestamp = curChunk.getEndTimestamp(nextTimestamp, slices);
                        nextChunkSeq = 0;
                    }
                    curChunkIterator = curChunk.iterator();

                    while (curChunkIterator.hasNext() && timestamper.applyAsLong(curChunkIterator.peek()) < lowestTimestamp) {
                        curChunkIterator.next();
                    }
                    if (curChunkIterator.hasNext() && timestamper.applyAsLong(curChunkIterator.peek()) < highestTimestamp) {
                        return curChunkIterator.next();
                    }
                }
                return endOfData();
            }
        };
    }

    /**
     * Retrieves data backwards from the cache belonging to the specified key, starting
     * from {@code highestTimestamp} and going back to {@code lowestTimestamp}.
     *
     * @param key              The key
     * @param lowestTimestamp  The beginning timestamp (excluded)
     * @param highestTimestamp The ending timestamp (included)
     * @param param            The parameter to pass to the cache loader, if necessary. <b>Important: the passed
     *                         value must be valid at least as long as the returned iterator is in use</b>
     * @return An iterator on the results, lazily loaded in case of cache misses
     */
    public Iterator<V> getBackwards(K key, long lowestTimestamp, long highestTimestamp, P param) {
        return new AbstractIterator<V>() {

            private long nextEndTimestamp;
            private int nextChunkSeq;
            private GetBackResult<V> curChunk = null;
            private PeekingIterator<V> curChunkIterator;

            private void init() {
                for (long slice : slices) {
                    nextEndTimestamp = slice + roundDown(highestTimestamp, slice);
                    curChunk = getChunkBack(key, nextEndTimestamp, LAST, param);
                    if (nextEndTimestamp - slices[curChunk.chunk.sliceLevel] <= highestTimestamp) {
                        initChunk();
                        return;
                    }
                }
                throw new IllegalStateException("Should not be reachable");
            }

            private void initChunk() {
                curChunkIterator = curChunk.chunk.backIterator();
                if (curChunk.hasPrevChunk()) {
                    nextChunkSeq = curChunk.chunkSeq - 1;
                } else {
                    nextEndTimestamp -= slices[curChunk.chunk.sliceLevel];
                    nextChunkSeq = LAST;
                }
                for (int i = 0; i < curChunk.toSkip; i++) {
                    curChunkIterator.next();
                }
            }

            @Override
            protected V computeNext() {
                if (curChunk == null) {
                    init();
                }
                while (true) {
                    while (curChunkIterator.hasNext()) {
                        V ret = curChunkIterator.next();
                        long retTs = timestamper.applyAsLong(ret);
                        if (retTs <= lowestTimestamp) {
                            return endOfData();
                        } else if (retTs <= highestTimestamp) {
                            return ret;
                        }
                    }

                    if (nextEndTimestamp <= lowestTimestamp) {
                        return endOfData();
                    }

                    curChunk = getChunkBack(key, nextEndTimestamp, nextChunkSeq, param);
                    initChunk();
                }
            }
        };
    }

    /**
     * Removes specific data from the cache.
     *
     * @param key              The key
     * @param lowestTimestamp  The beginning timestamp (included)
     * @param highestTimestamp The ending timestamp (excluded)
     */
    public void invalidate(K key, long lowestTimestamp, long highestTimestamp) {
        throw new UnsupportedOperationException("TODO");
    }

    /**
     * Completely empties the cache.
     */
    public void invalidateAll() {
        cache.invalidateAll();
    }

    private Chunk<V> getChunkFwd(K key, long timestamp, int chunkSeq, P param) {
        Key<K> k = Key.asc(key, timestamp, chunkSeq);
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
                lResult.addAll(getChunkFwd(key, timestamp, c, param).data);
            }
            if (chunk != null) {
                lResult.addAll(chunk.data);
            }
            long resultEndTs = appendForward(key, param, lResult);
            return arrangeFwd(key, timestamp, chunkSeq, resultEndTs, ImmutableList.copyOf(lResult));
        }
        Loader.Result<? extends V> result = loader.loadForward(key, timestamp, timestamp + slices[0], 0, chunkSize + 1, param);
        List<V> lResult = ImmutableList.copyOf(result.getData());
        long resultEndTs = result.getData().size() == chunkSize + 1 ? timestamper.applyAsLong(lResult.get(lResult.size() - 1)) : timestamp + slices[0];
        return arrangeFwd(key, timestamp, 0, resultEndTs, lResult);
    }

    /**
     * Cerca il chunk che termina con il timestamp indicato, se e' gia' in cache.
     * Se non c'e', ritorna {@code null}.
     *
     * @param endTs Il timestamp di fine del chunk da cercare
     * @return Un chunk, o {@code null}
     */
    private GetBackResult<V> getChunkBack(K key, long endTs, int chunkSeq, P param) {
        // Da fuori ho gia' iniziato a scorrere i chunk in avanti: continuo sulla cache standard
        if (chunkSeq != LAST && chunkSeq >= 0) {
            long startTs = endTs - slices[slices.length - 1];
            return new GetBackResult<>(getChunkFwd(key, startTs, chunkSeq, param), chunkSeq, 0);
        }

        if (chunkSeq == LAST) {
            // Non ho info specifiche: provo a cercare nella cache in avanti
            int l = slices.length;
            for (int s = l - 1; s >= minSliceLevel(endTs); s--) {
                long startTs = endTs - slices[s];
                int cs = 0;
                @Nullable Chunk<V> chunk;
                do {
                    Key<K> k = Key.asc(key, startTs, cs);
                    chunk = cache.getIfPresent(k);
                    cs++;
                } while (chunk != null && chunk.complete && chunk.hasNextChunk());
                if (chunk != null && chunk.complete) {
                    if (chunk.sliceLevel == s) {
                        return new GetBackResult<>(chunk, cs - 1, 0);
                    }
                    // Ho trovato uno slicing piu' basso di quello che mi aspettavo
                    // Significa che lo slice che contiene i miei dati sarebbe a slicing piu' alto,
                    // e lo ho gia' cercato senza trovarlo
                    break;
                }
            }
            chunkSeq = -1;
        }

        // A questo punto i dati non sono nella cache standard
        // Cerco nella cache dei parziali all'indietro
        Key<K> pKey = Key.desc(key, endTs, chunkSeq);
        Chunk<V> pChunk = cache.getIfPresent(pKey);
        if (pChunk != null && pChunk.complete) {
            return new GetBackResult<>(pChunk, chunkSeq, 0);
        }

        // Non sono neanche li'!
        // Ricostruisco quel che so dello slice, aggiungo un po' di dati e rifaccio una arrange
        List<V> chunkUnion = new ArrayList<>();
        for (int i = -1; i > chunkSeq; i--) {
            GetBackResult<V> chunkBack = getChunkBack(key, endTs, i, param);
            if (chunkBack.chunkSeq != i) {
                throw new IllegalStateException("Unexpected"); // TODO: Problema di concorrenza?
            }
            chunkUnion.addAll(chunkBack.chunk.data);
        }
        if (pChunk != null) {
            chunkUnion.addAll(pChunk.data);
        }

        if (chunkUnion.isEmpty()) {
            Preconditions.checkArgument(chunkSeq == -1);
            // I dati mancano completamente
            Loader.Result<? extends V> result = loader.loadBackwards(key, endTs - slices[0], endTs, 0, chunkSize + 1, param);
            List<V> lResult = ImmutableList.copyOf(result.getData());
            long resultStartTs = result.getData().size() == chunkSize + 1 ? timestamper.applyAsLong(lResult.get(lResult.size() - 1)) : endTs - slices[0];
            return arrangeBack(key, endTs, resultStartTs, lResult);
        } else {
            // Completo i dati parziali facendo un'altra load all'indietro
            long lastTs = timestamper.applyAsLong(chunkUnion.get(chunkUnion.size() - 1));
            int lastTsFirstIdx = binarySearchBack(chunkUnion, timestamper, lastTs, true);
            long loadEndTs = (lastTsFirstIdx == 0 ? endTs : timestamper.applyAsLong(chunkUnion.get(lastTsFirstIdx - 1)));
            Loader.Result<? extends V> result = loader.loadBackwards(key, loadEndTs - slices[0], loadEndTs, chunkUnion.size() - lastTsFirstIdx, chunkSize + 1, param);
            chunkUnion.addAll(result.getData());
            long resultStartTs = result.getData().size() == chunkSize + 1 ? timestamper.applyAsLong(chunkUnion.get(chunkUnion.size() - 1)) : endTs - slices[0];
            GetBackResult<V> arranged = arrangeBack(key, endTs, resultStartTs, ImmutableList.copyOf(chunkUnion));
            if (arranged.chunk.inverted) {
                return new GetBackResult<>(cache.getIfPresent(pKey), chunkSeq, 0); // TODO: Problemi di concorrenza sulla getIfPresent?
            } else {
                // Ho raddrizzato i dati
                // Quindi in pCache non trovo quello che volevo, devo andare sulla cache normale
                // Devo restituire quindi un chunk parecchio diverso da quello richiesto
                // e probabilmente devo riprendere da un certo indice dentro quel chunk
                return new GetBackResult<>(
                        getChunkFwd(key, endTs - slices[slices.length - 1], arranged.chunkSeq + chunkSeq + 1, param),
                        arranged.chunkSeq + chunkSeq + 1,
                        chunkSize - arranged.chunk.data.size());
            }
        }
    }

    private long appendForward(K key, P param, List<V> lResult) {
        if (lResult.isEmpty()) {
            throw new IllegalStateException();
        }
        long lastTs = timestamper.applyAsLong(lResult.get(lResult.size() - 1));
        int lastIdx = binarySearch(lResult, timestamper, lastTs, true);
        long highestExcluded = lastTs + slices[0];
        Loader.Result<? extends V> result = loader.loadForward(key, lastTs, highestExcluded, lResult.size() - lastIdx, chunkSize + 1, param);
        lResult.addAll(result.getData());
        return result.getData().size() == chunkSize + 1 ? timestamper.applyAsLong(lResult.get(lResult.size() - 1)) : highestExcluded;
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
     * @return Il chunk associato a (timestamp, chunkSeq)
     */
    private Chunk<V> arrangeFwd(K key, long timestamp, int retChunkSeq, long resultEndTimestamp, List<V> result) {
        if (result.isEmpty()) {
            Preconditions.checkState(retChunkSeq == 0);
            int s = minSliceLevel(timestamp);
            Chunk<V> chunk = Chunk.empty(s, false, false);
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
                int position = binarySearch(result, timestamper, endTimestamp, true);
                position = (position >= 0 ? position : -position - 1);
                int chunkEnd = Math.min(chunkSize, position);

                boolean complete = chunkEnd < result.size() || endTimestamp <= resultEndTimestamp;
                Chunk<V> chunk = new Chunk<>(result.subList(0, chunkEnd), l - 1, complete, position > chunkSize, false, false, false);
                cache.put(Key.asc(key, timestamp, chunkSeq), chunk);
                result = result.subList(chunkEnd, result.size());
                ret = (ret == null && chunkSeq == retChunkSeq ? chunk : ret);
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
                        Chunk<V> chunk = new Chunk<>(result.subList(0, chunkEnd), s, complete, false, false, false, false);
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

        return ret != null ?  ret : Chunk.empty(minSliceLevel(timestamp), false, false);
    }

    /**
     * Riempie la cache con i dati ordinati al contrario che terminano in endTimestamp
     *
     * @param key                  La chiave dei dati
     * @param endTs                Il timestamp in cui i dati terminano, estremo escluso
     * @param resultStartTimestamp Indica che tra il timestamp del primo dato e questo timestamp (incluso) non ci sono altri dati.
     *                             Puo' essere maggiore o uguale al timestamp dell'ultimo dato, e in questo caso non indica nulla.
     * @param result               I dati da inserire in cache, ordinati per timestamp decrescente
     * @return Il chunk che termina in endTs e contiene i primi dati presenti in result
     */
    private GetBackResult<V> arrangeBack(K key, long endTs, long resultStartTimestamp, List<V> result) {
        GetBackResult<V> ret = null;
        int l = slices.length;

        if (result.isEmpty()) {
            int s = minSliceLevel(endTs);
            Chunk<V> chunk = new Chunk<>(ImmutableList.of(), s, true, false, false, false, false);
            cache.put(Key.asc(key, endTs - slices[s], 0), chunk);
            return new GetBackResult<>(chunk, 0, 0);
        }

        outer:
        while (!result.isEmpty()) {
            int s = minSliceLevel(endTs);
            for (; s < l; s++) {
                long startTs = endTs - slices[s];
                int sliceEnd = binarySearchBack(result, timestamper, startTs, true);
                sliceEnd = (sliceEnd >= 0 ? sliceEnd : -sliceEnd - 2) + 1; // L'estremo inferiore, se trovato, va incluso
                if (sliceEnd <= chunkSize) {
                    // Ho trovato un livello di slicing che produce uno slice non oltre la massima dimensione
                    // Non ho bisogno di chunking

                    // Il chunk e' completo se ho altri risultati oltre il suo inizio,
                    // o se so che non ci sono altri dati oltre startTs
                    boolean complete = sliceEnd < result.size() || startTs >= resultStartTimestamp;
                    if (complete) {
                        Chunk<V> chunk = new Chunk<>(Lists.reverse(result.subList(0, sliceEnd)), s, true, false, false, false, false);
                        // TODO: Svuotare la pCache? Come?
                        cache.put(Key.asc(key, startTs, 0), chunk);
                        ret = (ret == null ? new GetBackResult<>(chunk, 0, 0) : ret);
                    } else {
                        Chunk<V> chunk = new Chunk<>(result.subList(0, sliceEnd), s, false, false, false, false, true);
                        cache.put(Key.desc(key, endTs, 0), chunk);
                        ret = (ret == null ? new GetBackResult<>(chunk, 0, 0) : ret);
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
                        List<V> sliceData = Lists.reverse(result.subList(0, sliceEnd));
                        for (int i = numChunks - 1; i >= 0; i--) {
                            int startIdx = i * chunkSize;
                            int endIdx = Math.min((i + 1) * chunkSize, sliceEnd);
                            // Lo slice e' completo: metto tutti i chunk nella cache standard
                            Chunk<V> chunk = new Chunk<>(sliceData.subList(startIdx, endIdx), s, true, i < numChunks - 1, false, false, false);
                            cache.put(Key.asc(key, startTs, i), chunk);
                            ret = (ret == null ? new GetBackResult<>(chunk, i, 0) : ret);
                        }
                        // TODO: Svuotare la pCache? Come?
                    } else {
                        for (int i = 0; i < numChunks; i++) {
                            int startIdx = i * chunkSize;
                            int endIdx = Math.min((i + 1) * chunkSize, sliceEnd);
                            // Lo slice e' incompleto: metto tutti i chunk nella cache prev, non raddrizzati e con i sequenziali negativi
                            Chunk<V> chunk = new Chunk<>(result.subList(startIdx, endIdx), s, i < numChunks - 1, i < numChunks - 1, false, false, true);
                            cache.put(Key.desc(key, endTs, -i - 1), chunk);
                            ret = (ret == null ? new GetBackResult<>(chunk, -i - 1, 0) : ret);
                        }
                    }
                    result = result.subList(sliceEnd, result.size());
                    endTs = startTs;
                    continue outer;
                }
            }
        }

        return Preconditions.checkNotNull(ret);
    }

    private int minSliceLevel(long timestamp) {
        for (int i = 0, l = slices.length; i < l; i++) {
            if (timestamp % slices[i] == 0) {
                return i;
            }
        }
        throw new IllegalArgumentException("Timestamp is not a multiple of any slice");
    }

    @VisibleForTesting
    static long roundDown(long value, long rounding) {
        long remainder = value % rounding;
        if (remainder < 0) {
            return value - remainder - rounding;
        } else {
            return value - remainder;
        }
    }

    static <V> int binarySearch(List<? extends V> elements, ToLongFunction<? super V> timestamper, long ts, boolean firstIfTied) {
        return binarySearch(elements, timestamper, ts, false, firstIfTied);
    }

    static <V> int binarySearchBack(List<? extends V> elements, ToLongFunction<? super V> timestamper, long ts, boolean firstIfTied) {
        return binarySearch(elements, timestamper, ts, true, firstIfTied);
    }

    private static <V> int binarySearch(List<? extends V> elements, ToLongFunction<? super V> timestamper, long ts, boolean backwards, boolean firstIfTied) {
        int last = elements.size() - 1;
        int low = 0;
        int high = last;
        while (low <= high) {
            int mid = (low + high) / 2;
            long midTs = timestamper.applyAsLong(elements.get(mid));
            if (midTs == ts) {
                if (firstIfTied) {
                    while (mid > 0 && timestamper.applyAsLong(elements.get(mid - 1)) == ts) {
                        --mid;
                    }
                } else {
                    while (mid < last && timestamper.applyAsLong(elements.get(mid + 1)) == ts) {
                        ++mid;
                    }
                }
                return mid;
            } else if (backwards == ts > midTs) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return -low - 1;
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
        private final boolean inverted;

        Chunk(List<V> data, int sliceLevel, boolean complete, boolean hasNextChunk, boolean endOfDataForward, boolean endOfDataBackwards, boolean inverted) {
            this.data = data;
            this.sliceLevel = sliceLevel;
            this.complete = complete;
            this.hasNextChunk = hasNextChunk;
            this.endOfDataForward = endOfDataForward;
            this.endOfDataBackwards = endOfDataBackwards;
            this.inverted = inverted;
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
            return new Chunk<>(ImmutableList.of(), sliceLevel, true, false, endOfDataForward, endOfDataBackwards, false);
        }

        public PeekingIterator<V> iterator() {
            if (inverted) {
                return Iterators.peekingIterator(Lists.reverse(data).iterator());
            } else {
                return Iterators.peekingIterator(data.iterator());
            }
        }

        public PeekingIterator<V> backIterator() {
            if (!inverted) {
                return Iterators.peekingIterator(Lists.reverse(data).iterator());
            } else {
                return Iterators.peekingIterator(data.iterator());
            }
        }
    }

    private static final class GetBackResult<V> {
        public final Chunk<V> chunk;
        public final int chunkSeq;
        public final int toSkip;

        public GetBackResult(Chunk<V> chunk, int chunkSeq, int toSkip) {
            this.chunk = chunk;
            this.chunkSeq = chunkSeq;
            this.toSkip = toSkip;
        }

        boolean hasPrevChunk() {
            if (!chunk.complete) {
                throw new IllegalStateException("Chunk is not complete, hasPrevChunk is not meaningful");
            }
            return (chunk.inverted && chunk.hasNextChunk) || (!chunk.inverted && chunkSeq > 0);
        }
    }

    private static final class Key<K> {
        private final boolean ascending;
        private final K key;
        private final long ts;
        private final int chunkSeq;

        Key(boolean ascending, K key, long ts, int chunkSeq) {
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

        public static <K> Key<K> asc(K key, long ts, int chunkSeq) {
            return new Key<>(true, key, ts, chunkSeq);
        }

        public static <K> Key<K> desc(K key, long ts, int chunkSeq) {
            return new Key<>(false, key, ts, chunkSeq);
        }
    }
}
