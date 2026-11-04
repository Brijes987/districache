# Design notes

## Structures
| Structure | Purpose | Cost |
|---|---|---|
| `HashMap<K, CacheEntry>` | key lookup | O(1) |
| Doubly-linked list with sentinel head and tail | recency order | O(1) move and remove |
| `TreeSet<CacheEntry>` ordered by (expiry time, sequence), TTL entries only | find the soonest-expiring entries | O(log n) |

## Decisions
1. **A read lock cannot be upgraded to a write lock.** `ReentrantReadWriteLock` deadlocks if a thread that
   holds the read lock asks for the write lock. `get()` releases the read lock, takes the write lock and
   re-checks the entry, since it may have been removed, expired or replaced in between.
2. **Capacity counts valid entries only.** Comparing the raw map size to capacity can evict a valid entry
   while an expired one stays. `put` first removes expired entries, then evicts only if the cache is still full.
3. **Why an expiry index.** An earlier version counted valid entries by scanning the whole map, so `put` at
   capacity, `size()` and cleanup were all O(n). The TreeSet finds expired entries in order, so each removal
   costs O(log n). `size()` counts expired-but-not-yet-removed entries with a `headSet` probe under the read lock.
4. **Overwriting a key** removes it from the index before its expiry time changes, because the TreeSet orders by
   that field, then re-adds it.
5. **One shared cleanup thread.** A scheduler thread per cache leaks threads when many caches are created. One
   shared daemon thread holds weak references to caches, so abandoned caches can be collected. `shutdown()` cancels a
   cache's task.
6. **TTL contract.** `put(key, value, ttl)` requires `ttl > 0`. Expiry time saturates at `Long.MAX_VALUE`
   instead of overflowing.

## Not claimed
Linearizability, behaviour under wall-clock jumps, and any performance figure that was not measured with JMH on
the reader's own machine.
