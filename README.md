# DistriCache - In-Memory Cache Phase 1

A thread-safe, single-node in-memory cache system in Java with LRU (Least Recently Used) eviction and TTL (Time To Live) expiry support.

## Project Structure

```
DistriCache/
├── pom.xml                          # Maven configuration
├── src/
│   ├── main/
│   │   └── java/
│   │       └── com/distri/cache/
│   │           ├── Cache.java       # Cache interface
│   │           └── CacheEntry.java  # Cache entry class with LRU and TTL support
│   └── test/
│       └── java/
│           └── com/distri/cache/
│               ├── properties/      # Property-based tests
│               ├── unit/            # Example-based unit tests
│               ├── integration/     # Integration tests
│               └── generators/      # Custom generators for PBT
└── README.md
```

## Requirements

- **Java**: 17 or higher
- **Build Tool**: Maven 3.6+

## Dependencies

- **JUnit 5** (5.10.1): Unit testing framework
- **JUnit-Quickcheck** (1.0): Property-based testing
- **SLF4J** (2.0.9): Logging API
- **Logback** (1.4.14): Logging implementation

## Building the Project

```bash
mvn clean compile
```

## Running Tests

```bash
mvn test
```

## Core Features (Phase 1)

### 1. Basic Cache Operations
- `put(key, value)`: Store key-value pairs
- `get(key)`: Retrieve values by key
- `remove(key)`: Remove entries
- `size()`: Get current entry count
- `capacity()`: Get maximum capacity

### 2. TTL Expiry Support
- Store entries with time-to-live expiration
- Lazy expiry on access
- Background cleanup thread for physical removal

### 3. LRU Eviction Policy
- Automatic eviction when cache reaches capacity
- Hand-rolled doubly-linked list for O(1) LRU tracking
- HashMap for O(1) key lookup

### 4. Thread Safety
- ReentrantReadWriteLock for concurrent access
- Multiple readers or single writer pattern
- No data corruption under concurrent load

### 5. Null Handling
- IllegalArgumentException for null keys or values
- Validation at method entry points

## Design Principles

- **Correctness**: Thread-safe operations with no data corruption
- **Performance**: O(1) operations for put, get, and remove
- **Educational**: Hand-rolled implementations to demonstrate JVM concurrency understanding
- **Testability**: Comprehensive property-based and unit testing

## Next Steps

1. Implement LRU cache with locking strategy
2. Add background cleanup for TTL expiry
3. Implement baseline cache for benchmarking
4. Write comprehensive tests (property-based and unit tests)
5. Performance benchmarking

## License

This is an educational project for demonstrating in-memory cache implementation in Java.
