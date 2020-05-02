package com.github.fbaro.tcache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class TimestampedCacheTest {

    /**
     * Contiene i 50 numeri [0, 100, 200, ..., 4900]
     */
    private final List<Long> data;
    private final Loader<String, Long, Void> loader;
    private int loadCount = 0;

    public TimestampedCacheTest() {
        data = new ArrayList<>();
        for (long l = 0; l < 50; l++) {
            data.add(l * 100);
        }
        loader = new Loader<String, Long, Void>() {
            @Override
            public Result<Long> loadForward(String key, long lowestIncluded, long highestExcluded, int offset, int limit, Void param) {
                //System.out.println("loadForward key = [" + key + "], lowestIncluded = [" + lowestIncluded + "], highestExcluded = [" + highestExcluded + "], offset = [" + offset + "], limit = [" + limit + "], param = [" + param + "]");
                loadCount++;
                return TimestampedCacheTest.this.loadFwd(lowestIncluded, highestExcluded, offset, limit);
            }

            @Override
            public Result<Long> loadBackwards(String key, long lowestIncluded, long highestExcluded, int offset, int limit, Void param) {
                //System.out.println("loadBackwards key = [" + key + "], lowestIncluded = [" + lowestIncluded + "], highestExcluded = [" + highestExcluded + "], offset = [" + offset + "], limit = [" + limit + "], param = [" + param + "]");
                loadCount++;
                return TimestampedCacheTest.this.loadBkw(lowestIncluded, highestExcluded, offset, limit);
            }
        };
    }

    private Loader.Result<Long> loadFwd(long lowestIncluded, long highestExcluded, int offset, int limit) {
        List<Long> ret = data.stream()
                .filter(l -> l >= lowestIncluded)
                .filter(l -> l < highestExcluded)
                .skip(offset)
                .limit(limit)
                .collect(Collectors.toList());
        return new Loader.StdResult<>(ret, data.isEmpty() || (ret.size() < limit && highestExcluded >= data.get(data.size() - 1)));
    }

    private Loader.Result<Long> loadBkw(long lowestIncluded, long highestExcluded, int offset, int limit) {
        List<Long> ret = Lists.reverse(data).stream()
                .filter(l -> l >= lowestIncluded)
                .filter(l -> l < highestExcluded)
                .skip(offset)
                .limit(limit)
                .collect(Collectors.toList());
        return new Loader.StdResult<>(ret, data.isEmpty() || (ret.size() < limit && lowestIncluded < data.get(data.size() - 1)));
    }

    private List<Long> getBkw(long lowestExcluded, long highestIncluded, int offset, int limit) {
        return Lists.reverse(data).stream()
                .filter(l -> l > lowestExcluded)
                .filter(l -> l <= highestIncluded)
                .skip(offset)
                .limit(limit)
                .collect(Collectors.toList());
    }

    @Test
    public void verifyRoundDown() {
        assertEquals(0L, TimestampedCache.roundDown(0, 200));
        assertEquals(0L, TimestampedCache.roundDown(123, 200));
        assertEquals(200L, TimestampedCache.roundDown(200, 200));
        assertEquals(-200L, TimestampedCache.roundDown(-123, 200));
        assertEquals(-200L, TimestampedCache.roundDown(-200, 200));
    }

    @Test
    public void verifyCustomBinarySearchWorksAsStandard() {
        ImmutableList<Long> data0 = ImmutableList.of(2L, 4L, 6L, 8L, 10L);
        for (long search = -1; search < 12; search++) {
            assertEquals(
                    Collections.binarySearch(data0, search),
                    TimestampedCache.binarySearch(data0, v -> v, search));
        }
    }

    @Test
    public void verifyCustomBinarySearchBackwardsWorks() {
        ImmutableList<Long> data0 = ImmutableList.of(10L, 8L, 6L, 4L, 2L);
        for (int i = 0; i < 5; i++) {
            assertEquals(i, TimestampedCache.binarySearchBack(data0, v -> v, 10L - 2 * i));
        }
        for (int i = -1; i >= -6; i--) {
            assertEquals(i, TimestampedCache.binarySearchBack(data0, v -> v, 13L + 2 * i));
        }
    }

    @Test
    public void verifyWithoutChunking() {
        long[] chunks = {4000, 2000, 1000, 200};
        test(chunks, 4, 800, 1225); // In questo test un chunk incompleto cambia slicing quando si completa
        for (int chunkSize = 2; chunkSize < 55; chunkSize++) {
            for (long start = 0; start < 1000; start += 25) {
                for (long end = start + 100; end < start + 500; end += 25) {
                    test(chunks, chunkSize, start, end);
                    back(chunks, chunkSize, start, end);
                }
            }
        }
    }

    @Test
    public void verifyWithChunking() {
        long[] chunks = {4000, 2000, 1000};
        test(chunks, 4, 0, 425); // If a specific combination fails, test it here
        back(chunks, 6, 0, 1000);

        for (int chunkSize = 4; chunkSize < 55; chunkSize++) {
            for (long start = 0; start < 1500; start += 25) {
                for (long end = start + 100; end < start + 1250; end += 25) {
                    test(chunks, chunkSize, start, end);
                    back(chunks, chunkSize, start, end);
                }
            }
        }
    }

    @Test
    public void verifyWithChunkingAndDataHole1() {
        long[] chunks = {4000, 2000, 1000};
        data.removeIf(l -> l > 1000 && l <= 4000);
        for (int chunkSize = 4; chunkSize < 55; chunkSize++) {
            for (long start = 0; start < 1500; start += 25) {
                for (long end = start + 100; end < start + 1250; end += 25) {
                    test(chunks, chunkSize, start, end);
                    back(chunks, chunkSize, start, end);
                }
            }
        }
    }

    @Test
    public void verifyWithChunkingAndDataHole2() {
        long[] chunks = {4000, 2000, 1000};
        data.removeIf(l -> l >= 1000 && l < 4000);
        for (int chunkSize = 4; chunkSize < 55; chunkSize++) {
            for (long start = 0; start < 1500; start += 25) {
                for (long end = start + 100; end < start + 1250; end += 25) {
                    test(chunks, chunkSize, start, end);
                    back(chunks, chunkSize, start, end);
                }
            }
        }
    }

    @Test
    public void verifyWithConstantData() {
        data.clear();
        for (int i = 0; i < 5; i++) {
            data.add(500L);
        }
        for (int i = 0; i < 5; i++) {
            data.add(1500L);
        }
        long[] chunks = {4000, 2000, 1000};
        for (int chunkSize = 4; chunkSize < 55; chunkSize++) {
            for (long start = 0; start < 1500; start += 25) {
                for (long end = start + 100; end < start + 1250; end += 25) {
                    test(chunks, chunkSize, start, end);
                    back(chunks, chunkSize, start, end);
                }
            }
        }
    }

    @Test
    public void verifyDoesNotLoadFullTopLevelSliceToAnswerRequestForSliceEnd() {
        test(new long[]{5000, 1000, 100}, 4, 4900, 5000);
        assertEquals(3, loadCount);
        test(new long[]{5000, 1000, 100}, 4, 0, 2000);
    }

    @Test
    public void verifyDoesNotLoadTwoFullSlicesToAnswerRequestWithPartiallyLoadedChunk() {
        test(new long[]{5000, 1000}, 4, 0, 400);
        assertEquals(1, loadCount);
    }

    @Test
    public void verifyDoesNotKeepLoadingPastEndOfData() {
        test(new long[]{1000}, 20, 4500, 10000);
        assertEquals(1, loadCount);
        test(new long[]{1000}, 20, 10000, 20000);
        assertEquals(2, loadCount);

        back(new long[]{1000}, 20, -10000, 500);
        assertEquals(3, loadCount);
        back(new long[]{1000}, 20, -20000, -10000);
        assertEquals(4, loadCount);
    }

    @Test
    public void verifyInvalidationWithoutChunks() {
        TimestampedCache<String, Long, Void> cache = new TimestampedCache<>(12, new long[]{5000, 1000}, v -> v, loader);
        test(cache, 1200, 2500);
        int l = loadCount;
        cache.invalidate("", 1200, 2500);
        test(cache, 1200, 2500);
        assertEquals(l * 2, loadCount);
    }

    @Test
    public void verifyInvalidationWithChunks() {
        TimestampedCache<String, Long, Void> cache = new TimestampedCache<>(4, new long[]{5000, 1000}, v -> v, loader);
        test(cache, 1200, 2500);
        int l = loadCount;
        cache.invalidate("", 1200, 2500);
        test(cache, 1200, 2500);
        assertEquals(l * 2, loadCount);
    }

    @Test
    public void verifyDoubleBackwardsLoading() {
        // Tengo un iteratore aperto e parzialmente avanzato
        // Con un altro iteratore carico quello che vorrebbe il primo
        // Voglio che il raddrizzamento non mi causi una doppia load
        TimestampedCache<String, Long, Void> cache = new TimestampedCache<>(4, new long[]{5000, 1000}, v -> v, loader);
        Iterator<Long> backIt = cache.getBackwards("", 1000, 1900, null);
        for (int i = 0; i < 4; i++) {
            assertEquals(1900L - i * 100, backIt.next().longValue());
        }
        back(cache, 1000, 1900);
        int lc = loadCount;
        for (int i = 4; i < 9; i++) {
            assertEquals(1900L - i * 100, backIt.next().longValue());
        }
        assertEquals(lc, loadCount);
        back(cache, 1000, 1900);
        assertEquals(lc, loadCount);
    }

    @Test
    @Category(SlowTests.class)
    public void randomized() { // TODO: Fare un po' di "concorrenza" tenendo gli iteratori aperti e facendo altre query nel frattempo
        List<Long> dataBackup = new ArrayList<>(data);
        for (int d = 0; d < 1000; d += 1) {
            long seed = System.currentTimeMillis();
            System.out.println(seed);
            Random rnd = new Random(seed);
            double addProbability = rnd.nextDouble();
            data.clear();
            for (int i = 0; i < dataBackup.size(); i++) {
                if (rnd.nextDouble() <= addProbability) {
                    data.add(dataBackup.get(i));
                }
            }

            long[] chunks = {5000, 1000, 500};
            for (int chunkSize = 2; chunkSize < 55; chunkSize++) {
                for (int i = 0; i < 100; i++) {
                    TimestampedCache<String, Long, Void> cache = new TimestampedCache<>(chunkSize, chunks, v -> v, loader);
                    for (int j = 0; j < 20; j++) {
                        long start = rnd.nextInt(5500) - 250;
                        long end = rnd.nextInt(2000) + start;
                        switch (rnd.nextInt(4)) {
                            case 0:
                                test(cache, start, end);
                                break;
                            case 1:
                                back(cache, start, end);
                                break;
                            case 2:
                                test(cache, start, end);
                                back(cache, start, end);
                                break;
                            case 3:
                                back(cache, start, end);
                                test(cache, start, end);
                                break;
                        }
                    }
                }
                for (long start = 0; start < 1500; start += 25) {
                    for (long end = start + 100; end < start + 1250; end += 25) {
                        test(chunks, chunkSize, start, end);
                        back(chunks, chunkSize, start, end);
                    }
                }
            }
        }
    }

    private void test(long[] chunks, int chunkSize, long start, long end) {
        TimestampedCache<String, Long, Void> cache = new TimestampedCache<>(chunkSize, chunks, v -> v, loader);
        test(cache, start, end);
    }

    private void test(TimestampedCache<String, Long, Void> cache, long start, long end) {
        //System.out.println("Forward start = [" + start + "], end = [" + end + "]");
        List<Long> result = new ArrayList<>();
        try {
            cache.getForward("", start, end, null)
                    .forEachRemaining(result::add);
        } catch (RuntimeException ex) {
            throw new AssertionError("Error at chunk size " + cache.getChunkSize() + " start = " + start + " end = " + end + " data = " + data, ex);
        }
        assertEquals("Error at chunk size " + cache.getChunkSize() + " start = " + start + " end = " + end + " data = " + data, loadFwd(start, end, 0, 1000).getData(), result);
    }

    private void back(long[] chunks, int chunkSize, long start, long end) {
        TimestampedCache<String, Long, Void> cache = new TimestampedCache<>(chunkSize, chunks, v -> v, loader);
        back(cache, start, end);
    }

    private void back(TimestampedCache<String, Long, Void> cache, long start, long end) {
        //System.out.println("Backwards start = [" + start + "], end = [" + end + "]");
        List<Long> result = new ArrayList<>();
        try {
            cache.getBackwards("", start, end, null)
                    .forEachRemaining(result::add);
        } catch (RuntimeException ex) {
            throw new AssertionError("Error at chunk size " + cache.getChunkSize() + " start = " + start + " end = " + end + " data = " + data, ex);
        }
        assertEquals("Error at chunk size " + cache.getChunkSize() + " start = " + start + " end = " + end + " data = " + data, getBkw(start, end, 0, 1000), result);
    }
}
