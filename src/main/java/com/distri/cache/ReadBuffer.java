package com.distri.cache;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Consumer;

/**
 * A lossy, striped, multi-producer buffer used to record cache hits without taking the cache's write lock.
 *
 * <p>Any number of threads call {@link #offer}; they never block and never take a lock. Each thread is mapped to
 * one of a few stripes (small ring buffers), so threads rarely contend for the same ring. One thread at a time (the
 * caller of {@link #drain}, which must be serialised by the caller, here by the cache's write lock) empties the
 * rings and hands every recorded element to a consumer.
 *
 * <p>The buffer is deliberately <b>lossy</b>: when a ring is full, or two producers race for the same slot, the
 * element is dropped and {@link #offer} says so. A dropped hit only means that entry's recency is not refreshed.
 * Nothing recorded is ever delivered twice, and elements of one stripe are delivered in the order they were
 * recorded.
 *
 * <p>Each ring is a classic single-consumer queue: producers claim a slot with a compare-and-set on
 * {@code writeCount} and then publish the element with an ordered store; the consumer stops at the first slot that
 * is claimed but not yet published and picks it up next time.
 */
final class ReadBuffer<E> {

    /** Result of {@link #offer}. */
    enum Result {
        /** Recorded, and the buffer is still mostly empty. */
        RECORDED,
        /** Recorded, and the stripe is now at least half full: a drain is advised. */
        RECORDED_DRAIN_ADVISED,
        /** Not recorded (ring full, or a producer race lost). A drain is advised if the ring was full. */
        DROPPED
    }

    private static final class Ring<E> {
        final AtomicReferenceArray<E> slots;
        final int mask;
        final int size;
        final AtomicLong writeCount = new AtomicLong(); // next slot index to claim; only grows
        volatile long readCount;                         // slots below this are consumed; written by the drainer only

        Ring(int size) {
            this.size = size;
            this.mask = size - 1;
            this.slots = new AtomicReferenceArray<>(size);
        }
    }

    private final Ring<E>[] rings;
    private final int stripeMask;

    /** @param stripes number of rings, a power of two; @param ringSize slots per ring, a power of two */
    @SuppressWarnings("unchecked")
    ReadBuffer(int stripes, int ringSize) {
        if (Integer.bitCount(stripes) != 1 || Integer.bitCount(ringSize) != 1 || ringSize < 2) {
            throw new IllegalArgumentException("stripes and ringSize must be powers of two (ringSize >= 2)");
        }
        this.rings = (Ring<E>[]) new Ring<?>[stripes];
        for (int i = 0; i < stripes; i++) {
            rings[i] = new Ring<>(ringSize);
        }
        this.stripeMask = stripes - 1;
    }

    /** A sensible default: one stripe per core (at least 2, at most 16) and 64 slots per stripe. */
    static <E> ReadBuffer<E> withDefaults() {
        int cpus = Runtime.getRuntime().availableProcessors();
        int stripes = Math.min(16, Integer.highestOneBit(Math.max(2, cpus) * 2 - 1));
        return new ReadBuffer<>(stripes, 64);
    }

    int stripes() {
        return rings.length;
    }

    /** Records {@code element} in the calling thread's stripe. Never blocks. */
    Result offer(E element) {
        return offer(element, stripeOfCurrentThread());
    }

    /** Records {@code element} in the given stripe (taken modulo the stripe count). Never blocks. */
    Result offer(E element, int stripe) {
        Ring<E> ring = rings[stripe & stripeMask];
        long w = ring.writeCount.get();
        long used = w - ring.readCount;
        if (used >= ring.size) {
            return Result.DROPPED;
        }
        if (!ring.writeCount.compareAndSet(w, w + 1)) {
            return Result.DROPPED; // another producer claimed this slot first
        }
        ring.slots.lazySet((int) (w & ring.mask), element);
        return used + 1 >= ring.size / 2 ? Result.RECORDED_DRAIN_ADVISED : Result.RECORDED;
    }

    /**
     * Delivers every recorded element to {@code consumer} and returns how many. The caller must make sure at most
     * one thread drains at a time.
     */
    int drain(Consumer<? super E> consumer) {
        int total = 0;
        for (Ring<E> ring : rings) {
            long r = ring.readCount;
            long w = ring.writeCount.get();
            while (r < w) {
                int i = (int) (r & ring.mask);
                E e = ring.slots.get(i);
                if (e == null) {
                    break; // claimed by a producer that has not stored yet: take it next time
                }
                ring.slots.lazySet(i, null);
                r++;
                consumer.accept(e);
                total++;
            }
            ring.readCount = r;
        }
        return total;
    }

    private int stripeOfCurrentThread() {
        int h = System.identityHashCode(Thread.currentThread()) * 0x9E3779B9;
        return (h >>> 16) & stripeMask;
    }
}
