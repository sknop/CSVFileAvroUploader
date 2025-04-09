package io.confluent.bootcamp;

import java.util.concurrent.atomic.AtomicInteger;

public class AtomicCounter implements Counter{
    private final AtomicInteger counter = new AtomicInteger(0);

    @Override
    public int increment() {
        return counter.incrementAndGet();
    }

    @Override
    public int decrement() {
        return counter.decrementAndGet();
    }

    @Override
    public int get() {
        return counter.get();
    }
}
