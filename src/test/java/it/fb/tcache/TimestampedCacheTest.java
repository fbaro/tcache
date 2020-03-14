package it.fb.tcache;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
        loader = (key, lowestIncluded, highestExcluded, offset, limit, param) -> {
            //System.out.println("key = [" + key + "], lowestIncluded = [" + lowestIncluded + "], highestExcluded = [" + highestExcluded + "], offset = [" + offset + "], limit = [" + limit + "], param = [" + param + "]");
            loadCount++;
            return loadFwd(lowestIncluded, highestExcluded, offset, limit);
        };
    }

    private Loader.Result<Long> loadFwd(long lowestIncluded, long highestExcluded, int offset, int limit) {
        List<Long> ret = data.stream()
                .filter(l -> l >= lowestIncluded)
                .filter(l -> l < highestExcluded)
                .skip(offset)
                .limit(limit)
                .collect(Collectors.toList());
        return new Loader.StdResult<>(ret, ret.size() < limit && highestExcluded > data.get(data.size() - 1));
    }

    @Test
    public void verifyCustomBinarySearchWorksAsStandard() {
        ImmutableList<Long> data0 = ImmutableList.of(2L, 4L, 6L, 8L, 10L);
        for (long search = -1; search < 12; search++) {
            assertEquals(
                    Collections.binarySearch(data0, search),
                    TimestampedCache.binarySearch(data0, v -> v, search, true));
        }
    }

    @Test
    public void verifyCustomBinarySearchBackwardsWorks() {
        ImmutableList<Long> data0 = ImmutableList.of(10L, 8L, 6L, 4L, 2L);
        for (int i = 0; i < 5; i++) {
            assertEquals(i, TimestampedCache.binarySearchBack(data0, v -> v, 10L - 2 * i, true));
        }
        for (int i = -1; i >= -6; i--) {
            assertEquals(i, TimestampedCache.binarySearchBack(data0, v -> v, 13L + 2 * i, true));
        }
    }

    @Test
    public void verifyWithoutChunking() {
        long[] chunks = {4000, 2000, 1000, 200};
        for (int chunkSize = 2; chunkSize < 55; chunkSize++) {
            for (long start = 0; start < 1000; start += 25) {
                for (long end = start + 100; end < start + 500; end += 25) {
                    test(chunks, chunkSize, start, end);
                }
            }
        }
    }

    @Test
    public void verifyWithChunking() {
        long[] chunks = {4000, 2000, 1000};
        test(chunks, 4, 0, 325);
        for (int chunkSize = 4; chunkSize < 55; chunkSize++) {
            for (long start = 0; start < 1500; start += 25) {
                for (long end = start + 100; end < start + 1250; end += 25) {
                    test(chunks, chunkSize, start, end);
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
                }
            }
        }
    }

    private void test(long[] chunks, int chunkSize, long start, long end) {
        //System.out.println("Testing chunkSize = [" + chunkSize + "], start = [" + start + "], end = [" + end + "]");
        TimestampedCache<String, Long, Void> cache = new TimestampedCache<>(chunkSize, chunks, v -> v, loader);
        List<Long> result = new ArrayList<>();
        cache.getForward("", start, end, null)
                .forEachRemaining(result::add);
        assertEquals("Error a chunk size " + chunkSize + " start = " + start + " end = " + end, loadFwd(start, end, 0, 1000).getData(), result);
    }
}
