package appenda

import (
	"errors"
	"sync"
	"time"
)

var (
	// ErrAppendaClosed is returned when operations are performed on a closed appenda
	ErrAppendaClosed = errors.New("appenda is closed")

	// ErrBackendNil is returned when attempting to create a new appenda with a nil backend
	ErrBackendNil = errors.New("storage backend cannot be nil")
)

// AppendaImpl implements the Appenda interface using a StorageBackend
type AppendaImpl struct {
	// Lock for concurrent access
	mu sync.RWMutex

	// The underlying storage backend
	backend StorageBackend

	// Flag indicating if the appenda is closed
	closed bool
}

// NewAppenda creates a new instance of AppendaImpl with the given storage backend
func NewAppenda(backend StorageBackend) (Appenda, error) {
	if backend == nil {
		return nil, ErrBackendNil
	}

	// Open the backend
	if err := backend.Open(); err != nil {
		return nil, err
	}

	return &AppendaImpl{
		backend: backend,
		closed:  false,
	}, nil
}

// Append adds a message to storage
func (a *AppendaImpl) Append(msg Message) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return ErrAppendaClosed
	}

	// Validate message
	if msg.Timestamp.IsZero() {
		return ErrInvalidTimestamp
	}

	// Forward to backend
	return a.backend.Append(msg)
}

// AppendBatch adds multiple messages at once
func (a *AppendaImpl) AppendBatch(msgs []Message) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return ErrAppendaClosed
	}

	// Empty batch is a no-op
	if len(msgs) == 0 {
		return nil
	}

	// Validate messages
	for _, msg := range msgs {
		if msg.Timestamp.IsZero() {
			return ErrInvalidTimestamp
		}
	}

	// Forward to backend
	return a.backend.AppendBatch(msgs)
}

// Iterate creates an iterator over stored messages
func (a *AppendaImpl) Iterate() (Iterator, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.closed {
		return nil, ErrAppendaClosed
	}

	// List all segments
	segments, err := a.backend.ListSegments()
	if err != nil {
		return nil, err
	}

	// If no segments, return empty iterator
	if len(segments) == 0 {
		return &emptyIterator{}, nil
	}

	// Create a multi-segment iterator starting from the earliest segment
	return NewMultiSegmentIterator(a.backend, time.Time{}, 0)
}

// IterateFrom creates an iterator starting from messages at or after the specified timestamp
func (a *AppendaImpl) IterateFrom(timestamp time.Time, offset int64) (Iterator, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.closed {
		return nil, ErrAppendaClosed
	}

	// Validate timestamp
	if timestamp.IsZero() {
		return nil, ErrInvalidTimestamp
	}

	// First use MultiSegmentIterator to get all segments from the starting segment
	baseIterator, err := NewMultiSegmentIterator(a.backend, timestamp, offset)
	if err != nil {
		return nil, err
	}

	// Then wrap it with FilteredIterator to filter messages by timestamp
	filteredIterator, err := NewFilteredIterator(baseIterator, func(msg Message) bool {
		// Only include messages with timestamp >= start timestamp
		return !msg.Timestamp.Before(timestamp)
	})
	if err != nil {
		return nil, err
	}

	// If no offset, return the filtered iterator directly
	if offset <= 0 {
		return filteredIterator, nil
	}

	// Handle offset by skipping messages
	// We'll create an offsetIterator that skips the first 'offset' messages
	return &offsetIterator{
		iterator: filteredIterator,
		offset:   offset,
	}, nil
}

// Sync forces a sync to disk
func (a *AppendaImpl) Sync() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return ErrAppendaClosed
	}

	return a.backend.Sync()
}

// Close closes the append-only storage
func (a *AppendaImpl) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return nil // Already closed
	}

	a.closed = true
	return a.backend.Close()
}

// emptyIterator is an implementation of Iterator that contains no messages
type emptyIterator struct{}

func (it *emptyIterator) Next() bool {
	return false
}

func (it *emptyIterator) Message() Message {
	return Message{}
}

func (it *emptyIterator) Err() error {
	return nil
}

func (it *emptyIterator) Close() error {
	return nil
}

// offsetIterator skips the first N messages from the underlying iterator
type offsetIterator struct {
	iterator Iterator
	offset   int64
	skipped  bool
}

func (it *offsetIterator) Next() bool {
	// If we haven't skipped messages yet, do it now
	if !it.skipped {
		// Skip the first 'offset' messages
		for i := int64(0); i < it.offset; i++ {
			if !it.iterator.Next() {
				return false
			}
		}
		it.skipped = true
	}

	// Now return the next message after the offset
	return it.iterator.Next()
}

func (it *offsetIterator) Message() Message {
	return it.iterator.Message()
}

func (it *offsetIterator) Err() error {
	return it.iterator.Err()
}

func (it *offsetIterator) Close() error {
	return it.iterator.Close()
}
