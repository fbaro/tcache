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
    private final ImmutableList<Long> data;
    private final Loader<String, Long, Void> loader;

    public TimestampedCacheTest() {
        ImmutableList.Builder<Long> builder = new ImmutableList.Builder<>();
        for (long l = 0; l < 50; l++) {
            builder.add(l * 100);
        }
        data = builder.build();
        loader = (key, lowestIncluded, highestExcluded, offset, limit, param) -> {
            //System.out.println("key = [" + key + "], lowestIncluded = [" + lowestIncluded + "], highestExcluded = [" + highestExcluded + "], offset = [" + offset + "], limit = [" + limit + "], param = [" + param + "]");
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
        return new Loader.StdResult<>(ret, ret.get(ret.size() - 1).equals(data.get(data.size() - 1)));
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
    public void verifyWithoutChunking() {
        for (int chunkSize = 2; chunkSize < 100; chunkSize++) {
            for (long start = 0; start < 1000; start += 25) {
                for (long end = start + 100; end < start + 500; end += 25) {
                    TimestampedCache<String, Long, Void> cache = new TimestampedCache<>(chunkSize, new long[]{4000, 2000, 1000, 200}, v -> v, loader);
                    List<Long> result = new ArrayList<>();
                    cache.getForward("", start, end, null)
                            .forEachRemaining(result::add);
                    assertEquals("Error a chunk size " + chunkSize + " start = " + start + " end = " + end, loadFwd(start, end, 0, 1000).getData(), result);
                }
            }
        }
    }


    @Test
    public void verifyWithChunking() {
        for (int chunkSize = 4; chunkSize < 100; chunkSize++) {
            for (long start = 0; start < 1000; start += 25) {
                for (long end = start + 100; end < start + 500; end += 25) {
                    test(chunkSize, start, end);
                }
            }
        }
    }

    private void test(int chunkSize, long start, long end) {
        //System.out.println("chunkSize = [" + chunkSize + "], start = [" + start + "], end = [" + end + "]");
        TimestampedCache<String, Long, Void> cache = new TimestampedCache<>(chunkSize, new long[]{4000, 2000, 1000}, v -> v, loader);
        List<Long> result = new ArrayList<>();
        cache.getForward("", start, end, null)
                .forEachRemaining(result::add);
        assertEquals("Error a chunk size " + chunkSize + " start = " + start + " end = " + end, loadFwd(start, end, 0, 1000).getData(), result);
    }
}
