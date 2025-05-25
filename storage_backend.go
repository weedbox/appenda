// Package appenda provides an append-only storage mechanism with automatic file rotation.
package appenda

import (
	"errors"
	"time"
)

var (
	// ErrStorageClosed is returned when operations are performed on a closed storage
	ErrStorageClosed = errors.New("storage backend is closed")

	// ErrCorruptData is returned when corrupt or invalid data is detected
	ErrCorruptData = errors.New("corrupt data detected")

	// ErrInvalidTimestamp is returned when an invalid timestamp is provided
	ErrInvalidTimestamp = errors.New("invalid timestamp")

	// ErrStorageFull is returned when storage capacity is reached
	ErrStorageFull = errors.New("storage capacity reached")

	// ErrSegmentNotFound is returned when a segment cannot be found
	ErrSegmentNotFound = errors.New("segment not found")

	// ErrSegmentExists is returned when attempting to create a segment with a timestamp that already exists
	ErrSegmentExists = errors.New("segment with this timestamp already exists")

	// ErrInvalidSegmentOrder is returned when attempting to create segments out of chronological order
	ErrInvalidSegmentOrder = errors.New("invalid segment order, new segment timestamp must be after current segment")
)

// SegmentInfo contains metadata about a storage segment
type SegmentInfo struct {
	// Timestamp uniquely identifies this segment (replaces the ID concept)
	Timestamp time.Time

	// EndTime represents the timestamp of the last message in this segment
	EndTime time.Time

	// MessageCount indicates how many messages are in this segment
	MessageCount int64

	// Size in bytes of the segment
	Size int64
}

// StorageBackend defines the interface for the underlying storage mechanism
// that Appenda implementations will use
type StorageBackend interface {
	// Open initializes the storage backend
	Open() error

	// Append stores a single message
	// The message's timestamp is used to determine segment placement
	Append(msg Message) error

	// AppendBatch stores multiple messages at once
	// The messages' timestamps are used to determine segment placement
	AppendBatch(msgs []Message) error

	// Read returns an iterator for the specified segment starting at the given offset
	Read(segmentTimestamp time.Time, offset int64) (Iterator, error)

	// ListSegments returns information about all available segments
	ListSegments() ([]SegmentInfo, error)

	// GetSegmentInfo returns information about a specific segment
	GetSegmentInfo(timestamp time.Time) (SegmentInfo, error)

	// FindSegmentByTime returns the segment that contains the specified timestamp
	// This finds a segment where the message timestamp falls between segment's Timestamp and EndTime
	FindSegmentByTime(messageTimestamp time.Time) (SegmentInfo, error)

	// CreateNewSegment closes the current segment and creates a new one with the given timestamp
	CreateNewSegment(timestamp time.Time) error

	// Sync forces a sync of all pending writes to the underlying storage
	Sync() error

	// Close closes the storage backend
	Close() error
}
