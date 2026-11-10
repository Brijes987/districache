package com.distri.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class ReadBufferTest {

    @Test
    void drainDeliversWhatWasRecordedInOrder() {
        ReadBuffer<Integer> buffer = new ReadBuffer<>(2, 8);
        for (int i = 0; i < 5; i++) {
            assertTrue(buffer.offer(i, 0) != ReadBuffer.Result.DROPPED);
        }
        List<Integer> seen = new ArrayList<>();
        assertEquals(5, buffer.drain(seen::add));
        assertEquals(List.of(0, 1, 2, 3, 4), seen);
        assertEquals(0, buffer.drain(seen::add)); // nothing is delivered twice
    }

    @Test
    void aFullRingDropsAndDrainingMakesRoomAgain() {
        ReadBuffer<Integer> buffer = new ReadBuffer<>(1, 4);
        for (int i = 0; i < 4; i++) {
            assertTrue(buffer.offer(i, 0) != ReadBuffer.Result.DROPPED);
        }
        assertEquals(ReadBuffer.Result.DROPPED, buffer.offer(99, 0));
        List<Integer> seen = new ArrayList<>();
        buffer.drain(seen::add);
        assertEquals(List.of(0, 1, 2, 3), seen);
        assertTrue(buffer.offer(5, 0) != ReadBuffer.Result.DROPPED);
        seen.clear();
        buffer.drain(seen::add);
        assertEquals(List.of(5), seen);
    }

    @Test
    void aDrainIsAdvisedOnceARingIsHalfFull() {
        ReadBuffer<Integer> buffer = new ReadBuffer<>(1, 8);
        assertEquals(ReadBuffer.Result.RECORDED, buffer.offer(1, 0));
        assertEquals(ReadBuffer.Result.RECORDED, buffer.offer(2, 0));
        assertEquals(ReadBuffer.Result.RECORDED, buffer.offer(3, 0));
        assertEquals(ReadBuffer.Result.RECORDED_DRAIN_ADVISED, buffer.offer(4, 0)); // 4 of 8
    }

    @Test
    void stripesAreIndependent() {
        ReadBuffer<Integer> buffer = new ReadBuffer<>(2, 2);
        assertTrue(buffer.offer(1, 0) != ReadBuffer.Result.DROPPED);
        assertTrue(buffer.offer(2, 0) != ReadBuffer.Result.DROPPED);
        assertEquals(ReadBuffer.Result.DROPPED, buffer.offer(3, 0));
        assertTrue(buffer.offer(4, 1) != ReadBuffer.Result.DROPPED); // the other stripe is still empty
    }

    @Test
    void badSizesAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> new ReadBuffer<Integer>(3, 8));
        assertThrows(IllegalArgumentException.class, () -> new ReadBuffer<Integer>(2, 6));
        assertThrows(IllegalArgumentException.class, () -> new ReadBuffer<Integer>(2, 1));
    }

    /**
     * Many producers and one draining thread at the same time. Every element a producer was told was recorded must
     * be delivered exactly once, nothing else may be delivered, and each producer's elements arrive in order.
     */
    @Test
    void nothingRecordedIsLostOrDeliveredTwiceUnderConcurrency() throws Exception {
        int producers = 6;
        int perProducer = 200_000;
        ReadBuffer<Long> buffer = new ReadBuffer<>(4, 16); // small rings: plenty of drops and wrap-arounds
        long[] recorded = new long[producers];
        long[] delivered = new long[producers];
        long[] lastSeen = new long[producers];
        java.util.Arrays.fill(lastSeen, -1);
        AtomicBoolean done = new AtomicBoolean();
        String[] problem = new String[1];

        java.util.function.Consumer<Long> consumer = v -> {
            int producer = (int) (v >>> 32);
            long sequence = v & 0xFFFFFFFFL;
            if (sequence <= lastSeen[producer]) {
                problem[0] = "out of order or duplicate: producer " + producer + " sequence " + sequence;
            }
            lastSeen[producer] = sequence;
            delivered[producer]++;
        };

        Thread drainer = new Thread(() -> {
            while (!done.get()) {
                buffer.drain(consumer);
            }
        });
        drainer.start();
        Thread[] threads = new Thread[producers];
        for (int p = 0; p < producers; p++) {
            final int id = p;
            threads[p] = new Thread(() -> {
                for (int i = 0; i < perProducer; i++) {
                    // producers 0..5 use stripes 0..3, so two stripes are shared by two producers
                    if (buffer.offer(((long) id << 32) | i, id) != ReadBuffer.Result.DROPPED) {
                        recorded[id]++;
                    }
                }
            });
            threads[p].start();
        }
        for (Thread t : threads) {
            t.join();
        }
        done.set(true);
        drainer.join();
        buffer.drain(consumer); // the rest, now that nobody else is running

        assertEquals(null, problem[0]);
        long totalRecorded = 0;
        for (int p = 0; p < producers; p++) {
            assertEquals(recorded[p], delivered[p], "producer " + p);
            totalRecorded += recorded[p];
        }
        assertTrue(totalRecorded > 0);
        assertTrue(totalRecorded < (long) producers * perProducer, "with rings this small some offers must have been dropped");
    }
}
