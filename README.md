# Appenda

[![Go Reference](https://pkg.go.dev/badge/github.com/weedbox/appenda.svg)](https://pkg.go.dev/github.com/weedbox/appenda)
[![Go Report Card](https://goreportcard.com/badge/github.com/weedbox/appenda)](https://goreportcard.com/report/github.com/weedbox/appenda)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Appenda is a high-performance, append-only storage library for Go that provides automatic file rotation and efficient message iteration. It's designed for applications that need to store time-series data, logs, or any sequential data with strong consistency guarantees.

## Features

- **Append-Only Storage**: Optimized for write-heavy workloads with sequential data
- **Automatic File Rotation**: Intelligent segmentation based on time and file size limits
- **Multiple Storage Backends**: In-memory and file-based storage implementations
- **Efficient Iteration**: Support for sequential, filtered, and time-range iterations
- **Thread-Safe**: Concurrent read and write operations with proper synchronization
- **Zero External Dependencies**: Pure Go implementation with minimal dependencies
- **Configurable**: Flexible configuration for different use cases

## Installation

```bash
go get github.com/weedbox/appenda
```

## Quick Start

### Basic Usage with In-Memory Backend

```go
package main

import (
    "fmt"
    "log"
    "time"

    "github.com/weedbox/appenda"
)

func main() {
    // Create an in-memory backend
    backend := appenda.NewInMemoryBackend()
    
    // Create appenda instance
    app, err := appenda.NewAppenda(backend)
    if err != nil {
        log.Fatal(err)
    }
    defer app.Close()

    // Append a message
    msg := appenda.Message{
        Timestamp: time.Now(),
        Payload:   []byte("Hello, Appenda!"),
    }
    
    if err := app.Append(msg); err != nil {
        log.Fatal(err)
    }

    // Iterate over messages
    iterator, err := app.Iterate()
    if err != nil {
        log.Fatal(err)
    }
    defer iterator.Close()

    for iterator.Next() {
        msg := iterator.Message()
        fmt.Printf("Message: %s at %s\n", string(msg.Payload), msg.Timestamp.Format(time.RFC3339))
    }
    
    if iterator.Err() != nil {
        log.Fatal(iterator.Err())
    }
}
```

### File-Based Storage

```go
package main

import (
    "log"
    "time"

    "github.com/weedbox/appenda"
)

func main() {
    // Create file backend configuration
    config := appenda.FileBackendConfig{
        Directory:   "./data",
        Prefix:      "myapp",
        Extension:   "log",
        MaxFileSize: 100 * 1024 * 1024, // 100MB
    }
    
    // Create file backend
    backend := appenda.NewFileBackend(config)
    
    // Create appenda instance
    app, err := appenda.NewAppenda(backend)
    if err != nil {
        log.Fatal(err)
    }
    defer app.Close()

    // Append multiple messages
    messages := []appenda.Message{
        {Timestamp: time.Now(), Payload: []byte("Message 1")},
        {Timestamp: time.Now().Add(time.Second), Payload: []byte("Message 2")},
        {Timestamp: time.Now().Add(2*time.Second), Payload: []byte("Message 3")},
    }
    
    if err := app.AppendBatch(messages); err != nil {
        log.Fatal(err)
    }

    // Force sync to disk
    if err := app.Sync(); err != nil {
        log.Fatal(err)
    }
}
```

### Time-Range Iteration

```go
// Iterate from a specific timestamp
startTime := time.Now().Add(-time.Hour)
iterator, err := app.IterateFrom(startTime, 0)
if err != nil {
    log.Fatal(err)
}
defer iterator.Close()

for iterator.Next() {
    msg := iterator.Message()
    // Process message
}
```

## Architecture

### Core Components

#### Appenda Interface
The main interface that provides append-only storage operations:

```go
type Appenda interface {
    Append(msg Message) error
    AppendBatch(msgs []Message) error
    Iterate() (Iterator, error)
    IterateFrom(timestamp time.Time, offset int64) (Iterator, error)
    Sync() error
    Close() error
}
```

#### Storage Backend Interface
Pluggable storage backend that handles the actual data persistence:

```go
type StorageBackend interface {
    Open() error
    Append(msg Message) error
    AppendBatch(msgs []Message) error
    Read(segmentTimestamp time.Time, offset int64) (Iterator, error)
    ListSegments() ([]SegmentInfo, error)
    GetSegmentInfo(timestamp time.Time) (SegmentInfo, error)
    FindSegmentByTime(messageTimestamp time.Time) (SegmentInfo, error)
    CreateNewSegment(timestamp time.Time) error
    Sync() error
    Close() error
}
```

#### Iterator Interface
Provides efficient iteration over stored messages:

```go
type Iterator interface {
    Next() bool
    Message() Message
    Err() error
    Close() error
}
```

### Data Structure

#### Message
The basic unit of data stored in Appenda:

```go
type Message struct {
    Timestamp time.Time
    Payload   []byte
}
```

#### Segment Information
Metadata about storage segments:

```go
type SegmentInfo struct {
    Timestamp    time.Time
    EndTime      time.Time
    MessageCount int64
    Size         int64
}
```

## Storage Backends

### In-Memory Backend

The in-memory backend stores all data in RAM and is perfect for:
- Testing and development
- Temporary data storage
- High-performance scenarios where persistence is not required

```go
backend := appenda.NewInMemoryBackend()
```

### File Backend

The file backend provides persistent storage with automatic file rotation:

```go
config := appenda.FileBackendConfig{
    Directory:   "./data",      // Storage directory
    Prefix:      "myapp",       // File prefix (optional)
    Extension:   "log",         // File extension
    MaxFileSize: 100 * 1024 * 1024, // Maximum file size (100MB)
}
backend := appenda.NewFileBackend(config)
```

#### File Format

Files are named using the format: `[prefix_]<timestamp_hex>_<sequence_hex>.<extension>`

Example: `myapp_5fee6600_00000001.log`

The file format includes:
- 3-byte relative timestamp (seconds from segment start)
- 2-byte payload length
- Variable-length payload
- 4-byte CRC32 checksum

## Iterator Types

### Basic Iterator
Standard sequential iteration over all messages:

```go
iterator, err := app.Iterate()
```

### Offset Iterator
Start iteration from a specific message offset:

```go
iterator, err := app.IterateFrom(timestamp, offset)
```

### Multi-Segment Iterator
Seamlessly iterate across multiple segments:

```go
// Automatically handles segment boundaries
iterator, err := app.Iterate()
```

### Filtered Iterator
Apply custom filters during iteration:

```go
// Note: Filtered iterators are used internally by IterateFrom
// for timestamp-based filtering
```

### Time-Range Iterator
Iterate within a specific time range:

```go
// Note: Time-range iteration is implemented through 
// the IterateFrom method with timestamp filtering
```

## Configuration

### File Backend Configuration

```go
type FileBackendConfig struct {
    Directory   string // Storage directory path
    Prefix      string // Optional file prefix
    Extension   string // File extension (default: "log")
    MaxFileSize int64  // Maximum file size in bytes (default: 100MB)
}
```

### Default Values

- **Default File Extension**: `log`
- **Default Max File Size**: `100MB`
- **Max Segment Duration**: `6 months`

## Error Handling

Appenda defines several specific error types for different scenarios:

```go
var (
    // Storage backend errors
    ErrStorageClosed         = errors.New("storage backend is closed")
    ErrCorruptData          = errors.New("corrupt data detected")
    ErrInvalidTimestamp     = errors.New("invalid timestamp")
    ErrStorageFull          = errors.New("storage capacity reached")
    ErrSegmentNotFound      = errors.New("segment not found")
    ErrSegmentExists        = errors.New("segment with this timestamp already exists")
    ErrInvalidSegmentOrder  = errors.New("invalid segment order, new segment timestamp must be after current segment")
    
    // Appenda errors
    ErrAppendaClosed        = errors.New("appenda is closed")
    ErrBackendNil          = errors.New("storage backend cannot be nil")
    
    // File backend errors
    ErrInvalidFileFormat    = errors.New("invalid file format")
    ErrChecksumMismatch     = errors.New("checksum mismatch")
    ErrRecordTooLarge       = errors.New("record too large for file size limit")
)
```

## Best Practices

### Message Timestamps
- Always use UTC timestamps to avoid timezone issues
- Ensure timestamps are monotonically increasing for optimal performance
- Truncate timestamps to appropriate precision (e.g., milliseconds)

### Batch Operations
- Use `AppendBatch` for better performance when adding multiple messages
- Batch size should be balanced between memory usage and I/O efficiency

### Resource Management
- Always call `Close()` on iterators and appenda instances
- Use `defer` statements to ensure proper cleanup
- Call `Sync()` periodically for file backends to ensure data persistence

### Error Handling
- Check all returned errors, especially from iterator operations
- Use iterator.Err() to check for iteration errors after the loop

### Performance Tips
- For write-heavy workloads, consider increasing `MaxFileSize`
- Use appropriate buffer sizes for batch operations
- Consider using in-memory backend for temporary or cache-like use cases

## Thread Safety

Appenda is designed to be thread-safe:

- Multiple goroutines can safely perform read operations concurrently
- Write operations are synchronized using mutexes
- Each iterator maintains its own state and can be used safely in separate goroutines
- Storage backends implement proper locking mechanisms

## Limitations

### File Backend
- Maximum payload size per message: 65,535 bytes (2^16 - 1)
- Timestamp precision: 1 second (stored as 3-byte relative timestamp)
- Maximum segment duration: 6 months
- Single writer per segment (multiple readers supported)

### General
- Messages must have non-zero timestamps
- Segments must be created in chronological order
- No built-in compression (can be implemented in payload)

## Testing

Run the test suite:

```bash
go test ./...
```

Run tests with coverage:

```bash
go test -cover ./...
```

Run benchmarks:

```bash
go test -bench=. ./...
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines

- Follow Go conventions and best practices
- Write comprehensive tests for new features
- Update documentation for API changes
- Use descriptive commit messages
- Ensure all tests pass before submitting PR

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Support

For questions, issues, or contributions, please:

1. Check existing [GitHub Issues](https://github.com/weedbox/appenda/issues)
2. Create a new issue with detailed information
3. Join discussions in existing issues
