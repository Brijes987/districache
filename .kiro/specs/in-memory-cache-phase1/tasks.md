# Implementation Plan: In-Memory Cache Phase 1

## Overview

This implementation plan details the step-by-step construction of a thread-safe, single-node in-memory cache system in Java with LRU eviction and TTL expiry support. The plan follows an incremental approach where each task builds on previous work, with property-based tests integrated alongside implementation to catch errors early. The implementation uses hand-rolled data structures (doubly-linked list, explicit locking) to demonstrate deep understanding of JVM concurrency primitives.

## Tasks

- [x] 1. Set up project structure and core interfaces
  - Create Maven/Gradle project with Java 17+
  - Add dependencies: JUnit 5, JUnit-Quickcheck for property-based testing, SLF4J for logging
  - Define `Cache<K, V>` interface with put, get, remove, size, and capacity methods
  - Define `CacheEntry<K, V>` class with key, value, expiryTime, prev, and next fields
  - Set up test directory structure (properties/, unit/, integration/, generators/)
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6_

- [-] 2. Implement basic cache operations without concurrency
  - [x] 2.1 Implement LRUCache constructor with capacity validation
    - Validate capacity > 0, throw IllegalArgumentException otherwise
    - Initialize HashMap for O(1) key lookup
    - Initialize dummy head and tail nodes for doubly-linked list
    - _Requirements: 9.1, 3.5_
  
  - [x]* 2.2 Write property test for constructor validation
    - **Property 14: Zero Capacity Rejected**
    - **Validates: Requirements 9.1**
  
  - [x] 2.3 Implement null argument validation methods
    - Create validateKey(K key) that throws IllegalArgumentException if null
    - Create validateValue(V value) that throws IllegalArgumentException if null
    - _Requirements: 8.1, 8.2, 8.3, 8.4_
  
  - [x]* 2.4 Write property test for null argument rejection
    - **Property 13: Null Arguments Rejected**
    - **Validates: Requirements 8.1, 8.2, 8.3, 8.4**
  
  - [x] 2.5 Implement doubly-linked list helper methods
    - Write addToFront(CacheEntry<K, V> entry) to insert after head
    - Write removeNode(CacheEntry<K, V> entry) to detach node from list
    - Write moveToFront(CacheEntry<K, V> entry) combining remove and addToFront
    - Ensure all operations maintain bidirectional link invariants
    - _Requirements: 3.5_
  
  - [x] 2.6 Implement basic put(K key, V value) without TTL or eviction
    - Validate key and value are not null
    - If key exists, update value and move to front
    - If key does not exist, create new CacheEntry with expiryTime = -1 (no expiration)
    - Add entry to HashMap and linked list front
    - _Requirements: 1.1, 3.3_
  
  - [-] 2.7 Implement get(K key) without expiry checks
    - Validate key is not null
    - Look up entry in HashMap
    - If entry exists, move to front and return Optional.of(value)
    - If entry does not exist, return Optional.empty()
    - _Requirements: 1.2, 1.3, 3.2_
  
  - [-] 2.8 Implement remove(K key)
    - Validate key is not null
    - Look up entry in HashMap
    - If entry exists, remove from HashMap and linked list
    - _Requirements: 1.4_
  
  - [x] 2.9 Implement size() and capacity() methods
    - capacity() returns the configured capacity field
    - size() returns HashMap.size() (will be refined later for expired entries)
    - _Requirements: 1.5, 1.6_
  
  - [x]* 2.10 Write property tests for basic operations
    - **Property 1: Put/Get Round-Trip**
    - **Property 2: Non-Existent Key Returns Empty**
    - **Property 3: Remove Makes Entry Inaccessible**
    - **Property 4: Size Reflects Entry Count** (without TTL)
    - **Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.5**
  
  - [x]* 2.11 Write unit tests for basic operations
    - Test put and get with multiple distinct keys
    - Test get on non-existent key returns empty
    - Test remove makes entry inaccessible
    - Test size updates correctly after put and remove
    - _Requirements: 7.1_

- [~] 3. Checkpoint - Ensure basic operations pass tests
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 4. Implement LRU eviction logic
  - [x] 4.1 Implement logicalSize() helper method
    - Iterate through HashMap entries
    - Count entries where isExpired() returns false
    - Return count of non-expired entries
    - _Requirements: 4.1_
  
  - [ ] 4.2 Implement findEvictionCandidate() helper method
    - Start from tail.prev (least recently used end)
    - Skip expired entries (they don't count toward capacity)
    - Return first non-expired entry as eviction candidate
    - _Requirements: 3.6, 4.2_
  
  - [~] 4.3 Add eviction logic to put(K key, V value)
    - Before adding new entry, check if logicalSize() >= capacity
    - If at capacity, find eviction candidate using findEvictionCandidate()
    - Remove eviction candidate from HashMap and linked list
    - Then add new entry to HashMap and front of linked list
    - _Requirements: 3.1, 3.3_
  
  - [x]* 4.4 Write property tests for LRU eviction
    - **Property 7: LRU Eviction at Capacity**
    - **Property 8: Get Updates LRU Recency**
    - **Property 9: Update Refreshes LRU Position**
    - **Validates: Requirements 3.1, 3.2, 3.4**
  
  - [x]* 4.5 Write unit tests for LRU eviction scenarios
    - Test eviction when cache reaches capacity
    - Test that get() updates recency and prevents eviction
    - Test that put() on existing key updates recency
    - _Requirements: 7.2, 7.3_

- [ ] 5. Implement TTL expiry support
  - [~] 5.1 Implement isExpired() method in CacheEntry
    - Return true if expiryTime > 0 && System.currentTimeMillis() > expiryTime
    - Return false if expiryTime == -1 (no expiration)
    - _Requirements: 2.3_
  
  - [~] 5.2 Implement put(K key, V value, long ttlMillis) with TTL
    - Validate key and value are not null
    - Create CacheEntry with expiryTime = System.currentTimeMillis() + ttlMillis
    - Handle eviction if at capacity (reuse existing eviction logic)
    - Add entry to HashMap and front of linked list
    - _Requirements: 2.1_
  
  - [~] 5.3 Add lazy expiry to get(K key)
    - After HashMap lookup, check if entry.isExpired()
    - If expired, return Optional.empty() (lazy expiry)
    - If not expired, move to front and return Optional.of(value)
    - _Requirements: 2.2, 2.3_
  
  - [~] 5.4 Refine size() to exclude expired entries
    - Iterate through HashMap entries
    - Count only entries where isExpired() returns false
    - Return count of non-expired entries
    - _Requirements: 1.5, 2.5_
  
  - [x]* 5.5 Write property tests for TTL expiry
    - **Property 5: TTL Entry Initially Accessible**
    - **Property 6: Expired Entry Returns Empty**
    - **Property 11: Expired Entries Don't Count Toward Capacity**
    - **Validates: Requirements 2.1, 2.2, 4.1, 4.3**
  
  - [x]* 5.6 Write unit tests for TTL expiry with real time delays
    - Test entry with TTL is accessible immediately after put
    - Test entry with TTL becomes inaccessible after ttlMillis expires (use Thread.sleep with margin)
    - Test expired entries don't count toward capacity
    - _Requirements: 7.4_
  
  - [x]* 5.7 Write property test for TTL and eviction interaction
    - **Property 10: Expired Entries Preferentially Evicted**
    - **Validates: Requirements 3.6, 4.2**
  
  - [x]* 5.8 Write unit tests for TTL and eviction interaction
    - Test that expired entries are preferentially removed over valid entries during eviction
    - _Requirements: 7.5_

- [~] 6. Checkpoint - Ensure TTL and eviction tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 7. Add thread safety with ReentrantReadWriteLock
  - [~] 7.1 Add ReentrantReadWriteLock field to LRUCache
    - Initialize lock in constructor
    - Document locking strategy in class-level comments
    - Explain read-write lock choice (read-heavy workload, no lock upgrade support)
    - _Requirements: 5.2, 5.3_
  
  - [~] 7.2 Wrap get(K key) with double-checked locking pattern
    - Acquire read lock, lookup entry, check expiry
    - Release read lock before acquiring write lock (avoid deadlock)
    - Acquire write lock, re-check entry exists and not expired
    - If valid, move to front and return value
    - Release write lock
    - _Requirements: 5.1, 5.3_
  
  - [~] 7.3 Wrap put(K key, V value) and put(K key, V value, long ttl) with write lock
    - Acquire write lock before any HashMap or linked list access
    - Perform validation, eviction, and insertion
    - Release write lock in finally block
    - _Requirements: 5.1_
  
  - [~] 7.4 Wrap remove(K key) with write lock
    - Acquire write lock before HashMap and linked list access
    - Perform removal
    - Release write lock in finally block
    - _Requirements: 5.1_
  
  - [~] 7.5 Wrap size() with read lock
    - Acquire read lock before iterating through HashMap
    - Count non-expired entries
    - Release read lock in finally block
    - _Requirements: 5.1_
  
  - [x]* 7.6 Write property test for thread safety
    - **Property 12: Thread Safety Under Concurrent Access**
    - **Validates: Requirements 5.1**
  
  - [x]* 7.7 Write concurrency stress test with multiple threads
    - Create ExecutorService with 10 threads
    - Each thread performs 1000 random operations (put, get, remove)
    - Verify no exceptions thrown (ConcurrentModificationException)
    - Verify cache.size() <= cache.capacity() after completion
    - _Requirements: 7.6_

- [ ] 8. Implement background cleanup for expired entries
  - [~] 8.1 Add ScheduledExecutorService field to LRUCache
    - Initialize in constructor with single daemon thread
    - Schedule cleanup task at fixed rate (1 second interval)
    - _Requirements: 2.4_
  
  - [~] 8.2 Implement cleanupExpiredEntries() method
    - Acquire write lock
    - Iterate through HashMap entries using iterator
    - For each expired entry, remove from linked list and HashMap
    - Release write lock in finally block
    - Wrap in try-catch to log errors without terminating cleanup thread
    - _Requirements: 2.4, 2.5_
  
  - [~] 8.3 Implement shutdown() method for graceful cleanup termination
    - Call cleanupScheduler.shutdown()
    - Wait up to 5 seconds for current cleanup to complete
    - Force shutdown with shutdownNow() if not completed
    - Restore interrupt status if InterruptedException occurs
    - _Requirements: 2.4_
  
  - [x]* 8.4 Write integration test for background cleanup
    - Put entries with short TTL (e.g., 100ms)
    - Wait for cleanup interval plus margin (e.g., 1.5 seconds)
    - Verify entries are physically removed from HashMap (not just logically expired)
    - Verify size() reflects removal
    - _Requirements: 7.7_

- [ ] 9. Implement capacity edge cases
  - [x]* 9.1 Write property tests for capacity edge cases
    - **Property 15: Capacity-One LRU Behavior**
    - **Property 16: Capacity-One Update Behavior**
    - **Validates: Requirements 9.3, 9.4**
  
  - [x]* 9.2 Write unit tests for capacity edge cases
    - Test cache with capacity=1 accepts single entry
    - Test cache with capacity=1 evicts existing entry when adding different key
    - Test cache with capacity=1 updates entry when adding same key
    - _Requirements: 9.2, 9.3, 9.4_

- [ ] 10. Implement baseline cache for benchmarking
  - [~] 10.1 Create BaselineCache class implementing Cache interface
    - Use ConcurrentHashMap as internal storage
    - Implement put, get, remove using ConcurrentHashMap methods
    - Implement size() and capacity() (return -1 for capacity, indicating no limit)
    - Do NOT implement eviction or TTL
    - _Requirements: 6.1, 6.2, 6.3, 6.4_
  
  - [x]* 10.2 Write unit tests for baseline cache
    - Test basic put, get, remove operations
    - Verify no capacity limits enforced
    - Verify no TTL expiry behavior
    - _Requirements: 6.1_

- [ ] 10.5. Set up JMH benchmarks for performance comparison
  - [~] 10.5.1 Add JMH dependency to project
    - Add JMH core and annotation processor dependencies to Maven/Gradle
    - Configure JMH benchmark plugin for running benchmarks
  
  - [~] 10.5.2 Create CacheBenchmark class with JMH annotations
    - Set up @State(Scope.Benchmark) for cache instances
    - Initialize LRUCache and BaselineCache in @Setup method
    - Pre-populate caches with test data
  
  - [~] 10.5.3 Implement benchmark methods for get operations
    - Create @Benchmark method for LRUCache.get() with random keys
    - Create @Benchmark method for BaselineCache.get() with same key distribution
    - Use Blackhole to consume results and prevent dead code elimination
  
  - [~] 10.5.4 Implement benchmark methods for put operations
    - Create @Benchmark method for LRUCache.put() with random keys/values
    - Create @Benchmark method for BaselineCache.put() with same distribution
  
  - [~] 10.5.5 Implement benchmark methods for mixed workloads
    - Create @Benchmark method simulating read-heavy workload (80% get, 20% put)
    - Create @Benchmark method simulating write-heavy workload (20% get, 80% put)
  
  - [ ]* 10.5.6 Run benchmarks and document results
    - Execute JMH benchmarks with appropriate warmup and measurement iterations
    - Compare throughput (ops/sec) between LRUCache and BaselineCache
    - Compare latency percentiles (p50, p95, p99) between implementations
    - Document performance characteristics and overhead of LRU vs baseline

- [ ] 11. Final integration and verification
  - [~] 11.1 Run all property-based tests with 100+ trials
    - Verify all 16 properties pass consistently
    - Check for any flaky tests and investigate root causes
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7_
  
  - [~] 11.2 Run all unit tests and integration tests
    - Verify comprehensive coverage of requirements
    - Ensure no test failures
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7_
  
  - [~] 11.3 Verify test coverage metrics
    - Run code coverage tool (JaCoCo)
    - Ensure line coverage > 90% for cache implementation
    - Ensure branch coverage > 85% for conditional logic
    - _Requirements: 7.1_

- [~] 12. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Property-based tests use JUnit-Quickcheck with minimum 100 trials per property
- Time-based TTL tests use Thread.sleep with safety margins to account for timing variability
- Concurrency tests use ExecutorService with multiple threads to stress-test thread safety
- Background cleanup runs as daemon thread to avoid blocking JVM shutdown
- The implementation uses ReentrantReadWriteLock for thread safety (read-heavy optimization)
- Doubly-linked list is hand-rolled (not using LinkedHashMap) to demonstrate data structure understanding
- Checkpoints ensure incremental validation and allow user feedback before proceeding

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1"] },
    { "id": 1, "tasks": ["2.1", "2.3", "2.5"] },
    { "id": 2, "tasks": ["2.2", "2.4", "2.6"] },
    { "id": 3, "tasks": ["2.7", "2.8", "2.9"] },
    { "id": 4, "tasks": ["2.10", "2.11"] },
    { "id": 5, "tasks": ["4.1", "4.2"] },
    { "id": 6, "tasks": ["4.3"] },
    { "id": 7, "tasks": ["4.4", "4.5"] },
    { "id": 8, "tasks": ["5.1", "5.2"] },
    { "id": 9, "tasks": ["5.3", "5.4"] },
    { "id": 10, "tasks": ["5.5", "5.6", "5.7", "5.8"] },
    { "id": 11, "tasks": ["7.1"] },
    { "id": 12, "tasks": ["7.2", "7.3", "7.4", "7.5"] },
    { "id": 13, "tasks": ["7.6", "7.7"] },
    { "id": 14, "tasks": ["8.1"] },
    { "id": 15, "tasks": ["8.2", "8.3"] },
    { "id": 16, "tasks": ["8.4"] },
    { "id": 17, "tasks": ["9.1", "9.2"] },
    { "id": 18, "tasks": ["10.1"] },
    { "id": 19, "tasks": ["10.2"] },
    { "id": 20, "tasks": ["10.5.1"] },
    { "id": 21, "tasks": ["10.5.2"] },
    { "id": 22, "tasks": ["10.5.3", "10.5.4", "10.5.5"] },
    { "id": 23, "tasks": ["10.5.6"] },
    { "id": 24, "tasks": ["11.1", "11.2"] },
    { "id": 25, "tasks": ["11.3"] }
  ]
}
```
