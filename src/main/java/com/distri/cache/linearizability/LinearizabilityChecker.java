package com.distri.cache.linearizability;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Decides whether a recorded history of SET / GET / DEL operations is <em>linearizable</em>: whether the operations can
 * be put in one single order that (a) respects real time (if A's answer arrived before B started, A comes first) and
 * (b) makes every answer correct for a plain key-value map executed one operation at a time.
 *
 * <p>Method: keys are independent in this model, so each key is checked on its own (linearizability is compositional).
 * For one key the checker searches for such an order depth-first, the Wing and Gong search with the memoisation of
 * Lowe: at each step it may linearize any not-yet-ordered operation that no other unordered operation strictly
 * precedes, applies it to the model, and gives up on a branch when an answer does not fit. States already explored
 * (set of ordered operations + model value) are remembered. Worst-case cost is exponential in the number of operations
 * that overlap in time, which is small here (a few clients), so the search is fast; a state budget guards against
 * pathological input and then the answer is {@link Status#GAVE_UP}, never a guess.
 *
 * <p>What a pass means: the history shows no violation, for the operations that were recorded. It says nothing about
 * operations that were not recorded or about runs that were not made.
 */
public final class LinearizabilityChecker {

    public enum Status { LINEARIZABLE, VIOLATION, GAVE_UP }

    /**
     * @param key the key whose history failed (or ran out of budget); null when linearizable
     * @param explanation for a violation, how far the search got and what could not be placed
     * @param statesExplored total search states visited, for diagnostics
     */
    public record Result(Status status, String key, String explanation, long statesExplored) {
        public boolean ok() {
            return status == Status.LINEARIZABLE;
        }
    }

    private static final long DEFAULT_BUDGET = 3_000_000;

    private final long budget;

    public LinearizabilityChecker() {
        this(DEFAULT_BUDGET);
    }

    /** @param stateBudget most search states to visit per key before answering GAVE_UP */
    public LinearizabilityChecker(long stateBudget) {
        if (stateBudget <= 0) {
            throw new IllegalArgumentException("budget must be positive");
        }
        this.budget = stateBudget;
    }

    public Result check(List<Operation> history) {
        Map<String, List<Operation>> byKey = new LinkedHashMap<>();
        for (Operation op : history) {
            if (op.kind() == Operation.Kind.GET && op.isPending()) {
                continue; // a read nobody saw the answer of constrains nothing
            }
            byKey.computeIfAbsent(op.key(), k -> new ArrayList<>()).add(op);
        }
        long total = 0;
        for (Map.Entry<String, List<Operation>> e : byKey.entrySet()) {
            KeySearch search = new KeySearch(e.getValue(), budget);
            Status status = search.run();
            total += search.visited;
            if (status != Status.LINEARIZABLE) {
                return new Result(status, e.getKey(), search.explain(status), total);
            }
        }
        return new Result(Status.LINEARIZABLE, null, "", total);
    }

    /** The search for one key. Runs on a thread with a large stack because the recursion is as deep as the history. */
    private static final class KeySearch {
        private record Memo(BitSet done, String state) {}

        private final Operation[] ops;
        private final long budget;
        private final Set<Memo> failed = new HashSet<>();
        private final BitSet done;
        private final int completedCount;
        long visited;
        private boolean exhausted;
        private int bestDepth = -1;
        private BitSet bestDone = new BitSet();
        private String bestState;

        KeySearch(List<Operation> list, long budget) {
            this.budget = budget;
            List<Operation> sorted = new ArrayList<>(list);
            sorted.sort(Comparator.comparingLong(Operation::invoke));
            this.ops = sorted.toArray(new Operation[0]);
            this.done = new BitSet(ops.length);
            int completed = 0;
            for (Operation op : ops) {
                if (!op.isPending()) {
                    completed++;
                }
            }
            this.completedCount = completed;
        }

        Status run() {
            boolean[] answer = new boolean[1];
            Thread worker = new Thread(null, () -> answer[0] = search(null, 0), "linearizability-check",
                    128L * 1024 * 1024);
            worker.start();
            try {
                worker.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            if (answer[0]) {
                return Status.LINEARIZABLE;
            }
            return exhausted ? Status.GAVE_UP : Status.VIOLATION;
        }

        /** @param state the map value of the key, or null if absent; @param completedDone completed ops ordered so far */
        private boolean search(String state, int completedDone) {
            if (completedDone == completedCount) {
                return true; // pending operations that were never ordered simply did not take effect
            }
            Memo memo = new Memo((BitSet) done.clone(), state);
            if (failed.contains(memo)) {
                return false;
            }
            if (++visited > budget) {
                exhausted = true;
                return false;
            }
            if (done.cardinality() > bestDepth) {
                bestDepth = done.cardinality();
                bestDone = (BitSet) done.clone();
                bestState = state;
            }
            long minReturn = Long.MAX_VALUE;
            for (int i = done.nextClearBit(0); i < ops.length; i = done.nextClearBit(i + 1)) {
                minReturn = Math.min(minReturn, ops[i].returned());
            }
            for (int i = done.nextClearBit(0); i < ops.length; i = done.nextClearBit(i + 1)) {
                Operation op = ops[i];
                if (op.invoke() > minReturn) {
                    break; // some unordered operation finished before this one started: it must come first
                }
                String next = apply(op, state);
                if (next == INVALID) {
                    continue;
                }
                done.set(i);
                boolean ok = search(next == ABSENT ? null : next, completedDone + (op.isPending() ? 0 : 1));
                done.clear(i);
                if (ok) {
                    return true;
                }
                if (exhausted) {
                    return false;
                }
            }
            failed.add(memo);
            return false;
        }

        private static final String INVALID = new String("<invalid>");
        private static final String ABSENT = new String("<absent>");

        /** The model: the new value of the key (ABSENT if none), or INVALID if the recorded answer does not fit. */
        private static String apply(Operation op, String state) {
            switch (op.kind()) {
                case SET:
                    return op.argument();
                case GET:
                    if (op.isPending()) {
                        return state == null ? ABSENT : state;
                    }
                    if (op.found() ? (state != null && state.equals(op.value())) : state == null) {
                        return state == null ? ABSENT : state;
                    }
                    return INVALID;
                default: // DEL
                    if (op.isPending()) {
                        return ABSENT;
                    }
                    return op.found() == (state != null) ? ABSENT : INVALID;
            }
        }

        String explain(Status status) {
            if (status == Status.GAVE_UP) {
                return "the search for key '" + ops[0].key() + "' hit its budget of " + budget
                        + " states without an answer (" + ops.length + " operations on this key)";
            }
            StringBuilder sb = new StringBuilder();
            sb.append("NOT LINEARIZABLE: key '").append(ops[0].key()).append("' (").append(ops.length)
                    .append(" operations). The search placed at most ").append(Math.max(bestDepth, 0))
                    .append(" of them in a valid order; the model value at that point was ")
                    .append(bestState == null ? "(absent)" : "\"" + bestState + "\"")
                    .append(". None of the remaining operations that could legally come next fits its recorded answer:\n");
            long minReturn = Long.MAX_VALUE;
            for (int i = bestDone.nextClearBit(0); i < ops.length; i = bestDone.nextClearBit(i + 1)) {
                minReturn = Math.min(minReturn, ops[i].returned());
            }
            int shown = 0;
            for (int i = bestDone.nextClearBit(0); i < ops.length && shown < 8; i = bestDone.nextClearBit(i + 1)) {
                if (ops[i].invoke() > minReturn) {
                    break;
                }
                sb.append("  ").append(ops[i]).append('\n');
                shown++;
            }
            return sb.toString();
        }
    }
}
