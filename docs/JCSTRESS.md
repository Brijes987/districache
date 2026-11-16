# Concurrency stress tests (JCStress)

`jcstress/` holds [JCStress](https://github.com/openjdk/jcstress) tests for `LRUCache` and `ShardedLRUCache`. They are a
separate build, not part of `mvn test` or CI: JCStress runs for minutes and its results are statistical.

## What is tested
Each test runs two or three threads doing one operation each on a fresh cache, then an arbiter that inspects the final
state and calls `verifyInternalConsistency()` (the hash map and the linked lists agree). Result slot 2 is -99 if that check
fails. Every outcome that some one-after-another order of the operations can produce is listed as acceptable; anything
else is forbidden.

| Test | Operations | Forbidden outcomes include |
|---|---|---|
| `PutPutSameKey` | two puts of one key | a lost key, a value that is neither, size other than 1 |
| `PutRemoveSameKey` | put against remove | `get` and `size` disagreeing |
| `EvictionKeepsCapacity` | three puts into capacity 2 | size other than 2 |
| `GetPromotesWhilePutEvicts` | a `get` that promotes an entry against a `put` that evicts | evicting the wrong entry |
| `BufferedReadsKeepStructure` | buffered reads against a put | wrong size or broken structure (exact victim is not checked: recency is approximate in this mode) |
| `ShardedStaysWithinCapacity` | three writers on a sharded cache | size above capacity |

Not covered: expiry (TTL), because JCStress cannot control time; the server's network code; the Raft code (its tests are
the deterministic simulator and the linearizability checker, docs/RAFT.md).

## What has been checked, and what has not
- Checked by me: the tests compile against stub copies of the JCStress annotations, and running each test's operations in
  every sequential order gives only outcomes listed as acceptable (24 runs, 0 rejected), and every acceptable outcome
  appears. That shows the expected outcomes match the cache's behaviour when operations do not overlap.
- **Not run by me:** JCStress itself could not be downloaded where I work, so no concurrent interleaving has been
  tested yet and the Maven build file is untested. Nothing here is a result until you have run it. A real
  JCStress run is what finds a race; passing would mean no forbidden outcome was seen in that run on that machine, not a proof.

## Run it
    mvn -q install -DskipTests          (in the project folder: installs the cache classes locally)
    cd jcstress
    mvn -q package
    java -jar target/jcstress.jar -m quick

`-m quick` takes a few minutes; `-m default` is longer. `-t PutPutSameKey` runs one test. The report is in
`results/index.html`. A FORBIDDEN or ERROR line is a finding: send it, do not delete the test.
