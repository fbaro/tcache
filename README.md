[![Build Status](https://travis-ci.com/fbaro/tcache.svg?branch=master)](https://travis-ci.com/fbaro/tcache)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

# TCache
A cache for time series data.

## I'm in a hurry!

1. Decide what the cache key, K, will be (hint: some unique identifier of the whole time series)
2. Decide what the cache value, V, will be (it must contain a representation of the element timestamp)
3. Implement a ToLongFunction from V which will extract a timestamp from V (hint: epoch time should work fine for most situations)
4. Decide the maximum number of V's you want to keep together, called chunk size (hint: this will both be the maximum number of values you will have to provide to the cache in a single batch, and the maximum number of values that will be evicted all together from the cache)
5. Decide the slice sizes (think about the maximum and the minimum time period you expect to need to reach the chunk size. Put some values in between the two. Convert them to longs. Ensure each value is a divisor of its closes higher value. E.g. 7 days, 1 day, 6 hours, 1 hour, 15 minutes, 5 minutes, 1 minute, 15 seconds, 1 second).
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

In standard caching, the cache keys are discrete and unrelated values. Each cache element is finite, and it makes sense to load them completely in memory. When caching time series data however, things can be more complicated. It probably does not make sense to load a whole time series in memory; the data might be too big, or loading it all when you likely need only a small portion of it can cause more performance problems than it solves. It is also inconvenient to use a different regular cache for each time series. First of all, it makes very difficult to employ proper eviction policies across different time series. Second, with this data it often makes sense to read and cache a contiguous block of values, rather than a single value at a time. So you could cache time slices! But how big should they be? Some kinds of time series are not equally spaced over time, and a fixed slice size of say 5 minutes might make sense during work days, but not over the weekend. You need to _dynamically_ adjust the time slice to the time series. This is what TCache does. 

## How?

TCache is based upon a single standard cache data structure (provided by [Caffeine](https://github.com/ben-manes/caffeine)). Each element of the cache is a slice of a time series. The time range of a slice is variable, and is kept as large as possible while keeping the slice content below or equal to threshold number of elements (the chunk size). The slice size is dynamically varying over all the time series span; so the slices will become bigger when fewer data is present, and smaller when more data is present.

With this design, all the cache entries are occupying a comparable amount of memory, and standard eviction policies can be simply applied.

## Requirements

TCache depends on [Caffeine](https://github.com/ben-manes/caffeine) as the underlying cache implementation.

The timestamps in TCache are represented as long values. There can be more data points on the same timestamp, but too many of them will make the cache perform worse. Choose a timestamp representation that you expect not to cause too many collisions.

## Usage

TCache has three type variables. K represents the cache key, excluding the timestamp. It could be the time series name, for instance. V is a single time series element type. It must contain a representation of the timestamp. P is a free type which can be passed when querying the cache, and will be forwarded to the function responsible to provide data to the cache, in case the cache needs to fill up.

To create an instance of TCache, you must provide a Loader, a timestamper, a cloner, a Caffeine builder, and a some sizing parameters.

The Loader is the interface that TCache will use to retrieve the data. If you plan to query the cache only for ascending data, the Loader needs to implement a single function called `loadAscending`. The function receives the cache key, a timestamp range represented as a two longs, a non-negative offset to request skipping some amount of data from the beginning of the range, a limit to curb the maximum amount of retrieved data, and a free parameter. It must answer with a iterator over ordered data, and an additional flag to signal if the end of the time series has been reached.

If the cache will be used also to query for descending data, also the `loadDescending` function should be implemented. The parameters are the same; the data however should be returned in descending order. Please note that TCache is biased towards ascending data. If you use it only to query for descending data, it's probably worth to invert the sign of the timestamps, and query only for ascending data.

The timestamper is actually a ToLongFunction, which must be able to extract a `long` timestamp from the cache elements. Different cache elements can have the same timestamp; the original order provided by the `Loader` will be maintained. If too many elements have the same timestamp however, the cache will perform poorly; you should define the timestamp in order to have as few duplicates as possible. 

The cloner is a function which is used to acquire (from the `Loader`) and return (to the user) copies of the cache elements. It's helpful to avoid accidentally modifying the cache contents. If your data is immutable, or you take extra care to not change the data returned by the cache, you can set it to the identity function.

Finally, two more parameters define the cache behaviour. The first is the chunk size. This is a positive number which limits the amount of data points that will be stored together.

The second is an array of descending longs, which are the slice sizes. Each long represents a duration; the smaller longs must be divisors of the bigger longs.

For example, see the following graph. The straight arrow represents time; the blue dots are events in a time series. Slicing values are 1000, 200, 100. Chunk size is 4.

![Timeline](Diagram.png)

In the slice between timestamp 1000 and 2000, 3 values are present. This is less than the 4 limit, so the maximum slicing is used.

Between timestamp 2000 and 3000 there are more than 4 values. The slicing is then set to the second level, 200. This is sufficient for the slices 1000-1200, 1200-1400 and 1400-1600, where we have less than 4 values each. The slice 1600-1800 however has 5 values, so a further lowering of the slice range is necessary. The 100 slicing is then applied, obtaining the two slices 1600-1700 and 1700-1800.

The slice 2000-3000 is back to the upper slicing level, since it contains less than 4 elements. 

## Thread safety

TCache has been designed with extra care to build upon Caffeine's thread safety, while avoiding to introduce its own locking mechanisms. As thread safety is hard, I hope I did not miss anything!

## License

TCache is distributed under GNU LGPL v3. 

## Disclaimer

THIS SOFTWARE IS PROVIDED "AS IS" AND ANY EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

