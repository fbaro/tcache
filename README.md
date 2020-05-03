[![Build Status](https://travis-ci.com/fbaro/tcache.svg?branch=master)](https://travis-ci.com/fbaro/tcache)

# TCache
A cache for time series data

## I'm in a hurry!

1. Decide what the cache key, K, will be (hint: some unique identifier of the whole time series)
2. Decide what the cache value, V, will be (it must contain a representation of the element timestamp)
3. Implement a ToLongFunction from V which will extract a timestamp from V (hint: epoch time should work fine for most situations)
4. Decide the maximum number of V's you want to keep together, called chunk size (hint: this will both be the maximum number of values you will have to provide to the cache in a single batch, and the maximum number of values that will be evicted all together from the cache)
5. Decide the slice sizes (think about the maximum and the minimum time slice you expect to need to reach the chunk size. Put some values in between the two. Convert them to longs. Ensure each value is a divisor of its closes higher value. E.g. 7 days, 1 day, 6 hours, 1 hour, 15 minutes, 5 minutes, 1 minute, 15 seconds, 1 second).
6. Implement the Loader, to provide data to the cache. You can pass a custom P type from the cache to the Loader, if you need it (e.g. a JDBC Connection?). Take extra care with the lifetime of this (see documentation).
7. ???
8. Profit!

### Example
```java
class Data {
    Instant instant;
    // ...
}

TimestampedCache<String, Data, Connection> cache = new TimestampedCache<>(
        512,
        new long[]{
                TimeUnit.DAYS.toMillis(1),
                TimeUnit.HOURS.toMillis(4),
                TimeUnit.MINUTES.toMillis(30),
                TimeUnit.SECONDS.toMillis(30)
        },
        d -> d.instant.toEpochMilli(),
        (key, lowestIncluded, highestExcluded, offset, limit, param) -> {
            Instant from = Instant.ofEpochMilli(lowestIncluded);
            Instant to = Instant.ofEpochMilli(highestExcluded);
            // results = ...
            // isEndOfdata = ...
            return new Loader.StdResult<>(results, isEndOfdata);
        });
```

## Why?

In standard caching, the cache keys are discrete and unrelated values. The cache values are finite, and it makes sense to load them completely in memory. When caching time series data however, things can be more complicated. It probably does not make sense to load a whole time series in memory; the data might be too big, or loading it all when you likely need only a small portion of it can cause more performance problems than it solves. It is also inconvenient to use a different regular cache for each time series. First of all, it makes very difficult to employ proper eviction policies across different time series. Second, with this data it often makes sense to read and cache a contiguous block of values, rather than a single value at a time. So you could cache time slices, but how big should they be? Some kinds of time series are not equally spaced over time, and a fixed slice size of say 5 minutes might make sense during work days, but not over the weekend.

## How?

TCache is based upon a single standard cache data structure (provided by [Caffeine](https://github.com/ben-manes/caffeine)). Each element of the cache is a slice of a time series. The time range of a slice is variable, and is kept as large as possible while keeping the slice content below a threshold number of elements (the chunk size). The slice size is dynamically varying over all the time series span; so the slices will become bigger when fewer data is present, and smaller when more data is present.

With this design, all the cache entries are occupying a comparable amount of memory, and standard eviction policies can be simply applied.

## Requirements

TCache depends on [guava](https://github.com/google/guava) for some collections-related classes, and on [Caffeine](https://github.com/ben-manes/caffeine) as the underlying cache implementation.

The timestamps in TCache are represented as long values. There can be more data points on the same timestamp, but too many of them will make the cache perform worse.

## Usage

TCache has three type variables. K represents the cache key, excluding the timestamp. It could be the time series name, for instance. V is a single time series element type. It must contain a representation of the timestamp. P is a free type which can be passed when querying the cache, and will be forwarded to the function responsible to provide data to the cache, in case the cache needs to fill up.

To create an instance of TCache, you must provide a Loader, a Timestamper, a Caffeine builder, and a some sizing parameters.

The Loader is the interface that TCache will use to retrieve the data. If you plan to query the cache only for ascending data, the Loader needs to implement a single function called loadForward. The function receives the cache key, a timestamp range represented as a two longs, a non-negative offset to request skipping some amount of data from the beginning of the range, a limit to curb the maximum amount of retrieved data, and a free parameter. It must answer with a collection of ordered data, and an additional flag to signal if the end of the time series has been reached.

If the cache will be used also to query for descending data, also the loadBackwards function should be implemented. The parameters are the same; the data however should be returned in descending order. Please note that TCache is biased towards ascending data. If you use it only to query for descending data, it's probably worth to invert the sign of the timestamps, and query only for ascending data.

The Timestamper is actually a ToLongFunction, which must be able to extract a long timestamp from the cache elements. Different cache elements can have the same timestamp; the original order provided by the Loader will be maintained. If too many elements have the same timestamp however, the cache will perform poorly; you should define the timestamp in order to have as few duplicates as possible. 

Finally, two more parameters define the cache behaviour. The first is the chunk size. This is a positive number which limits the amount of data points that will be stored together.

The second is an array of descending longs. Each long represents a duration; the smaller longs must be divisors of the bigger longs.

### How - example

Let's say you used a chunk size of 10, and slice sizes of 1 day, 6 hours, 1 hour.

Now you have an empty TCache, and you call the getForward iterator on key k, from 2020/04/04T10:00 to 2020/04/04T14:00. Since the cache is empty, TCache will call the loader. The first call will try to load a full chunk at maximum slice size: TCache will call loadForward(k, 2020/04/04T00:00, 2020/04/05T00:00, 0, 11). Let's say this call returns 11 values, but none of them are past 10:00. TCache will store them, and call again the loader with a lower slice size, and a lower rounding. For instance, it might call loadForward(k, 2020/04/04T06:00, 2020/04/04T12:00, 0, 11). Again if too many results are returned, the slice size will be lowered again. Let's say however that the last value of the previous call was at 2020/04/04T11:38. TCache will now call loadForward(k, 2020/04/04T11:38, 2020/04/04T12:38, 1, 11). TCache is thus striving to avoid asking you for data you already provided in a previous call.

