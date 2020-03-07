package it.fb.tcache;

@FunctionalInterface
public interface Timestamper<V> {

    long getTs(V value);
}
