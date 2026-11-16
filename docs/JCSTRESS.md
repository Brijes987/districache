# Concurrency stress tests (JCStress)

`jcstress/` holds [JCStress](https://github.com/openjdk/jcstress) tests for `LRUCache` and `ShardedLRUCache`. They are a
separate build, not part of `mvn test` or CI: JCStress runs for minutes and its results are statistical.

## What is tested
Each test runs two threads, each doing one or a few operations, on a fresh cache, then an arbiter that inspects the final
state and calls `verifyInternalConsistency()` (the hash map and the linked lists agree). Result slot 2 is -99 if that check
fails. Every outcome that some one-after-another order of the operations can produce is listed as acceptable; anything
else is forbidden.

| Test | Operations | Forbidden outcomes include |
|---|---|---|
| `PutPutSameKey` | two puts of one key | a lost key, a value that is neither, size other than 1 |
| `PutRemoveSameKey` | put against remove | `get` and `size` disagreeing |
| `EvictionKeepsCapacity` | four puts from two threads into capacity 2 | size other than 2 |
| `GetPromotesWhilePutEvicts` | a `get` that promotes an entry against a `put` that evicts | evicting the wrong entry |
| `BufferedReadsKeepStructure` | two buffered reads against a put | wrong size or broken structure (exact victim is not checked: recency is approximate in this mode) |
| `ShardedStaysWithinCapacity` | two writers on a sharded cache | size above capacity |

Not covered: expiry (TTL), because JCStress cannot control time; the server's network code; the Raft code (its tests are
the deterministic simulator and the linearizability checker, docs/RAFT.md).

## What has been checked, and what has not
- Checked by me: the tests compile against stub copies of the JCStress annotations, and running each test's operations in
  every sequential order gives only outcomes listed as acceptable (24 runs, 0 rejected), and every acceptable outcome
  appears. That shows the expected outcomes match the cache's behaviour when operations do not overlap.
- **Run by the user (Windows 10 laptop, 2 CPUs, JDK 23, `-m quick`), first version:** 42 jobs planned, 42 passed,
  0 failed, 0 errors. But JCStress printed "3 actors: No scheduling is possible, these tests would not run": it skips a
  test with more actors than CPUs, so only the three 2-actor tests ran. The three 3-actor tests were silently not run.
  They were then rewritten with 2 actors each.
- **Run by the user, second version (same machine, `-m quick`, 1 normal + 1 stress fork per test, 5 iterations of
  200 ms):** all 6 tests matched and ran; 84 jobs planned, 84 passed, 0 failed, 0 soft errors, 0 hard errors; no
  interesting or failed tests. Limits of that run: JCStress could not pin threads to CPUs on Windows ("Fallback
  topology ... NONE affinity mode", `taskset` not available), and `quick` mode is short.
- A pass means no forbidden outcome was seen in that run on that machine, not a proof.

## Run it
    mvn -q install -DskipTests          (in the project folder: installs the cache classes locally)
    cd jcstress
    mvn -q package
    java -jar target/jcstress.jar -m quick

First run on JDK 23 (user's machine): the build worked but the jar failed at start with a `NullPointerException` in
`TestList.getTests`, because JDK 23 no longer runs annotation processors found on the class path by default and JCStress
generates its tests that way. The build file now sets `<proc>full</proc>`. If you still see the exception, run
`mvn -q clean package` (not just `package`) and check that `target/classes/META-INF/TestList` exists.

`-m quick` takes a few minutes; `-m default` is longer. `-t PutPutSameKey` runs one test. The report is in
`results/index.html`. A FORBIDDEN or ERROR line is a finding: send it, do not delete the test.
