# Requirements Document

## Introduction

This document specifies the requirements for Phase 1 of an in-memory cache system in Java. This phase focuses on building a correct, thread-safe, single-node key-value cache with LRU (Least Recently Used) eviction and TTL (Time To Live) expiry support. The goal is to demonstrate deep understanding of JVM concurrency primitives and data structure implementation by building these mechanisms from scratch rather than using existing libraries.

## Glossary

- **Cache**: The in-memory key-value storage system being built
- **LRU_Eviction_Policy**: The eviction strategy that removes the least recently used entry when the cache reaches capacity
- **TTL**: Time To Live - the duration in milliseconds after which a cache entry expires
- **Lazy_Expiry**: The strategy of checking entry expiration only when the entry is accessed
- **Background_Cleanup**: A periodic process that physically removes expired entries from the cache
- **Entry**: A key-value pair stored in the cache, potentially with an expiration time
- **Recency**: The measure of how recently an entry was accessed, used by the LRU policy
- **Capacity**: The maximum number of entries the cache can hold before eviction occurs
- **Baseline_Cache**: A simple concurrent cache implementation using ConcurrentHashMap without eviction, used for benchmarking comparison

## Requirements

### Requirement 1: Core Cache Operations

**User Story:** As a developer, I want basic cache operations (put, get, remove), so that I can store and retrieve key-value pairs in memory.

#### Acceptance Criteria

1. WHEN a key-value pair is added using put(key, value), THE Cache SHALL store the entry and make it retrievable via get(key)
2. WHEN a key is requested using get(key) and the key exists without expiration, THE Cache SHALL return Optional containing the value
3. WHEN a key is requested using get(key) and the key does not exist, THE Cache SHALL return an empty Optional
4. WHEN a key is removed using remove(key), THE Cache SHALL delete the entry and subsequent get(key) calls SHALL return empty Optional
5. THE Cache SHALL provide size() method that returns the current number of entries
6. THE Cache SHALL provide capacity() method that returns the maximum number of entries

### Requirement 2: TTL Expiry Support

**User Story:** As a developer, I want entries to expire after a specified time, so that stale data is automatically removed from the cache.

#### Acceptance Criteria

1. WHEN an entry is added using put(key, value, ttlMillis), THE Cache SHALL store the entry with an expiration time
2. WHEN an expired entry is accessed via get(key), THE Cache SHALL return empty Optional as if the entry does not exist
3. WHEN an expired entry is accessed, THE Cache SHALL perform lazy expiry by checking expiration time on access
4. THE Background_Cleanup SHALL periodically scan and physically remove expired entries from the cache
5. WHEN the Background_Cleanup removes an expired entry, THE Cache SHALL update its size accordingly

### Requirement 3: LRU Eviction Policy

**User Story:** As a developer, I want automatic eviction of least recently used entries when the cache is full, so that memory usage stays within bounds.

#### Acceptance Criteria

1. WHEN the Cache reaches capacity and a new entry is added via put(), THE LRU_Eviction_Policy SHALL remove the least recently used entry before adding the new entry
2. WHEN an entry is accessed via get(key), THE Cache SHALL update the entry's recency by moving it to the most-recently-used position
3. WHEN an entry is added via put(key, value) for a new key, THE Cache SHALL mark it as the most-recently-used entry
4. WHEN an entry is updated via put(key, value) for an existing key, THE Cache SHALL refresh the entry's recency to the most-recently-used position
5. THE Cache SHALL implement LRU tracking using a HashMap combined with a hand-rolled doubly-linked list, not using LinkedHashMap's built-in LRU mode
6. WHEN choosing an entry to evict, THE LRU_Eviction_Policy SHALL select the entry that was least recently accessed, ignoring already-expired entries

### Requirement 4: TTL and Eviction Interaction

**User Story:** As a developer, I want expired entries to not count against capacity, so that the cache can accept new entries even when expired entries haven't been physically removed yet.

#### Acceptance Criteria

1. WHEN calculating available space for new entries, THE Cache SHALL treat expired entries as if they do not occupy capacity
2. WHEN an expired entry exists and a new entry is added at capacity, THE Cache SHALL preferentially remove the expired entry rather than evicting a valid entry
3. WHEN multiple expired entries exist at eviction time, THE Cache SHALL remove at least one expired entry to make room
4. WHEN no expired entries exist and the Cache is at capacity, THE Cache SHALL apply normal LRU eviction as specified in Requirement 3

### Requirement 5: Thread Safety and Concurrency

**User Story:** As a developer, I want the cache to be safely usable by multiple threads concurrently, so that my application can access the cache from multiple threads without data corruption.

#### Acceptance Criteria

1. WHEN multiple threads concurrently call put(), get(), and remove() operations, THE Cache SHALL ensure all operations are atomic and no data corruption occurs
2. THE Cache SHALL use java.util.concurrent primitives such as ReentrantReadWriteLock or sharded locks for synchronization
3. THE Cache SHALL include code comments explaining the chosen locking strategy and its suitability for different access patterns
4. THE Cache SHALL NOT use a single global synchronized block as the primary concurrency control mechanism
5. WHEN concurrent operations access different keys under a sharded-lock strategy, THE Cache SHALL NOT block those operations on each other, verifiable by a concurrency test that measures simultaneous execution of operations on distinct keys

### Requirement 6: Baseline Cache Implementation

**User Story:** As a developer, I want a simple baseline cache implementation for benchmarking, so that I can compare the performance of the LRU cache against a minimal implementation.

#### Acceptance Criteria

1. THE Baseline_Cache SHALL provide the same interface as the main Cache (put, get, remove, size)
2. THE Baseline_Cache SHALL be implemented using java.util.concurrent.ConcurrentHashMap
3. THE Baseline_Cache SHALL NOT implement eviction or capacity limits
4. THE Baseline_Cache SHALL NOT implement TTL expiry

### Requirement 7: Comprehensive Testing

**User Story:** As a developer, I want comprehensive tests that verify correctness and thread safety, so that I can be confident the cache implementation is correct.

#### Acceptance Criteria

1. THE Cache SHALL have JUnit 5 tests verifying basic put, get, and remove operations produce correct results
2. THE Cache SHALL have tests verifying that when at capacity, the least recently used entry is evicted on the next put operation
3. THE Cache SHALL have tests verifying that get() updates LRU recency, preventing recently accessed entries from being evicted before less recently accessed entries
4. THE Cache SHALL have tests verifying entries with TTL become inaccessible after ttlMillis expire, using real time-based verification
5. THE Cache SHALL have tests verifying the interaction between TTL expiry and LRU eviction
6. THE Cache SHALL have stress tests using multiple threads concurrently executing put, get, and remove operations with assertions on final state consistency
7. THE Cache SHALL have tests verifying the Background_Cleanup thread correctly removes expired entries

### Requirement 8: Null Handling

**User Story:** As a developer, I want clear behavior when null keys or values are provided, so that I can detect programming errors early.

#### Acceptance Criteria

1. WHEN put(null, value) is called, THE Cache SHALL throw IllegalArgumentException with a descriptive message
2. WHEN put(key, null) is called, THE Cache SHALL throw IllegalArgumentException with a descriptive message
3. WHEN remove(null) is called, THE Cache SHALL throw IllegalArgumentException with a descriptive message
4. WHEN get(null) is called, THE Cache SHALL throw IllegalArgumentException with a descriptive message

### Requirement 9: Capacity Edge Cases

**User Story:** As a developer, I want well-defined behavior for extreme capacity values, so that the cache behaves predictably in edge cases.

#### Acceptance Criteria

1. WHEN the Cache is constructed with capacity equal to 0, THE Cache SHALL throw IllegalArgumentException during construction
2. WHEN the Cache is constructed with capacity equal to 1, THE Cache SHALL accept the single entry
3. WHEN the Cache has capacity equal to 1 and contains one entry, and put() is called with a different key, THE Cache SHALL evict the existing entry before adding the new entry
4. WHEN the Cache has capacity equal to 1 and contains one entry, and put() is called with the same key, THE Cache SHALL update the entry without eviction
