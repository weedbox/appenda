// Package appenda provides an append-only storage mechanism with automatic file rotation.
package appenda

import (
	"time"
)

// Message represents a record to be stored in an append-only fashion.
type Message struct {
	// Timestamp indicates when the message was created
	Timestamp time.Time

	// Payload contains the actual message data
	Payload []byte
}

// Iterator allows iterating through stored messages.
type Iterator interface {
	// Next advances to the next message
	Next() bool

	// Message returns the current message
	Message() Message

	// Err returns any error encountered during iteration
	Err() error

	// Close releases resources used by the iterator
	Close() error
}

// Appenda defines the main interface for append-only storage.
type Appenda interface {
	// Append adds a message to storage
	Append(msg Message) error

	// AppendBatch adds multiple messages at once
	AppendBatch(msgs []Message) error

	// Iterate creates an iterator over stored messages
	Iterate() (Iterator, error)

	// IterateFrom creates an iterator starting from messages at or after the specified timestamp with optional offset
	IterateFrom(timestamp time.Time, offset int64) (Iterator, error)

	// Sync forces a sync to disk
	Sync() error

	// Close closes the append-only storage
	Close() error
}
