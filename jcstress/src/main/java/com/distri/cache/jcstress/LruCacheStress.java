package com.distri.cache.jcstress;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.FORBIDDEN;

import com.distri.cache.LRUCache;
import com.distri.cache.ShardedLRUCache;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Description;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.II_Result;

/**
 * Concurrency stress tests for {@link LRUCache} and {@link ShardedLRUCache}.
 *
 * <p>Each test is two threads doing a short run of operations each (two actors only, because JCStress skips tests with more actors than CPUs) on a fresh cache, then an arbiter that looks at the
 * final state. Every outcome that can happen when the operations run one after another in SOME order is listed as
 * acceptable; every other outcome is forbidden, as is a failure of {@code verifyInternalConsistency()} (the map and the
 * linked lists disagree), reported as -99 in the second result slot.
 *
 * <p>Background cleanup is switched off (interval 0) so a test creates no threads. Expiry (TTL) is not covered: JCStress
 * cannot control time.
 *
 * <p>The outcome sets were checked by running every sequential order of the actors by hand (see docs/JCSTRESS.md);
 * what JCStress adds is the interleavings that checking cannot produce.
 */
public final class LruCacheStress {

    private LruCacheStress() {}

    private static final int BROKEN = -99;

    private static int consistentOr(java.util.function.IntSupplier value, Runnable check) {
        try {
            check.run();
        } catch (RuntimeException e) {
            return BROKEN;
        }
        return value.getAsInt();
    }

    /** Two writers, same key: the key ends with one of the two values, never lost or mixed, one entry. */
    @JCStressTest
    @Description("two puts of the same key")
    @Outcome(id = {"1, 1", "2, 1"}, expect = ACCEPTABLE, desc = "last writer wins, one entry")
    @Outcome(expect = FORBIDDEN, desc = "lost key, wrong value, wrong size, or broken structure")
    @State
    public static class PutPutSameKey {
        private final LRUCache<String, Integer> cache = new LRUCache<>(2, 0);

        @Actor
        public void put1() {
            cache.put("k", 1);
        }

        @Actor
        public void put2() {
            cache.put("k", 2);
        }

        @Arbiter
        public void arbiter(II_Result r) {
            r.r1 = cache.get("k").orElse(-1);
            r.r2 = consistentOr(cache::size, cache::verifyInternalConsistency);
        }
    }

    /** A put racing a remove of the same key: the key is either present with one entry or absent with none. */
    @JCStressTest
    @Description("put against remove of the same key")
    @Outcome(id = {"1, 1", "-1, 0"}, expect = ACCEPTABLE, desc = "remove then put, or put then remove")
    @Outcome(expect = FORBIDDEN, desc = "get and size disagree, or broken structure")
    @State
    public static class PutRemoveSameKey {
        private final LRUCache<String, Integer> cache = new LRUCache<>(2, 0);

        @Actor
        public void put() {
            cache.put("k", 1);
        }

        @Actor
        public void remove() {
            cache.remove("k");
        }

        @Arbiter
        public void arbiter(II_Result r) {
            r.r1 = cache.get("k").orElse(-1);
            r.r2 = consistentOr(cache::size, cache::verifyInternalConsistency);
        }
    }

    /** Two writers of different keys into a cache that holds two: it must end with exactly two entries. */
    @JCStressTest
    @Description("four puts from two threads into a cache of capacity two")
    @Outcome(id = "2, 0", expect = ACCEPTABLE, desc = "two entries, structure consistent")
    @Outcome(expect = FORBIDDEN, desc = "over capacity, under capacity, or broken structure")
    @State
    public static class EvictionKeepsCapacity {
        private final LRUCache<String, Integer> cache = new LRUCache<>(2, 0);

        @Actor
        public void putAB() {
            cache.put("a", 1);
            cache.put("b", 2);
        }

        @Actor
        public void putCD() {
            cache.put("c", 3);
            cache.put("d", 4);
        }

        @Arbiter
        public void arbiter(II_Result r) {
            r.r1 = cache.size();
            r.r2 = consistentOr(() -> 0, cache::verifyInternalConsistency);
        }
    }

    /**
     * A read that promotes "a" racing a put that must evict. If the get runs first the least recently used entry is
     * "b" and it goes; if the put runs first "a" goes. Second slot: which of a (1), b (2), c (4) are present.
     */
    @JCStressTest
    @Description("get promotes an entry while a put evicts")
    @Outcome(id = "2, 5", expect = ACCEPTABLE, desc = "get first: b evicted, a and c remain")
    @Outcome(id = "2, 6", expect = ACCEPTABLE, desc = "put first: a evicted, b and c remain")
    @Outcome(expect = FORBIDDEN, desc = "wrong entry evicted, wrong size, or broken structure")
    @State
    public static class GetPromotesWhilePutEvicts {
        private final LRUCache<String, Integer> cache = new LRUCache<>(2, 0);

        public GetPromotesWhilePutEvicts() {
            cache.put("a", 1);
            cache.put("b", 2);
        }

        @Actor
        public void get() {
            cache.get("a");
        }

        @Actor
        public void put() {
            cache.put("c", 3);
        }

        @Arbiter
        public void arbiter(II_Result r) {
            r.r1 = cache.size();
            r.r2 = consistentOr(
                    () -> (cache.peek("a").isPresent() ? 1 : 0)
                            + (cache.peek("b").isPresent() ? 2 : 0)
                            + (cache.peek("c").isPresent() ? 4 : 0),
                    cache::verifyInternalConsistency);
        }
    }

    /** Buffered reads (approximate recency): the exact victim may differ, but size and structure must not. */
    @JCStressTest
    @Description("two buffered reads racing a put that evicts")
    @Outcome(id = "2, 0", expect = ACCEPTABLE, desc = "two entries, structure consistent after draining the buffer")
    @Outcome(expect = FORBIDDEN, desc = "wrong size or broken structure")
    @State
    public static class BufferedReadsKeepStructure {
        private final LRUCache<String, Integer> cache = new LRUCache<>(2, 0, true);

        public BufferedReadsKeepStructure() {
            cache.put("a", 1);
            cache.put("b", 2);
        }

        @Actor
        public void readers() {
            cache.get("a");
            cache.get("b");
        }

        @Actor
        public void writer() {
            cache.put("c", 3);
        }

        @Arbiter
        public void arbiter(II_Result r) {
            cache.drainReadBuffer();
            r.r1 = cache.size();
            r.r2 = consistentOr(() -> 0, cache::verifyInternalConsistency);
        }
    }

    /** The sharded cache under writers on different keys: total size never exceeds the capacity. Slot 1: 1 if size <= 4. */
    @JCStressTest
    @Description("two writers on a sharded cache")
    @Outcome(id = "1, 0", expect = ACCEPTABLE, desc = "size within capacity, every shard consistent")
    @Outcome(expect = FORBIDDEN, desc = "over capacity or broken structure")
    @State
    public static class ShardedStaysWithinCapacity {
        private final ShardedLRUCache<String, Integer> cache = new ShardedLRUCache<>(4, 2, 0);

        @Actor
        public void writer1() {
            cache.put("k1", 1);
            cache.put("k2", 2);
            cache.put("k3", 3);
        }

        @Actor
        public void writer2() {
            cache.put("k4", 4);
            cache.put("k5", 5);
            cache.put("k6", 6);
        }

        @Arbiter
        public void arbiter(II_Result r) {
            r.r1 = cache.size() <= cache.capacity() ? 1 : 0;
            r.r2 = consistentOr(() -> 0, cache::verifyInternalConsistency);
        }
    }
}
