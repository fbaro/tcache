package com.github.fbaro.tcache;

@FunctionalInterface
public interface Timestamper<V> {

    long getTs(V value);
}
