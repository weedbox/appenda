package appenda

import (
	"sort"
	"sync"
	"time"
)

// segmentData represents the in-memory structure of a segment
type segmentData struct {
	// Metadata about this segment
	info SegmentInfo

	// Actual messages in this segment
	messages []Message
}

// InMemoryBackend implements the StorageBackend interface using in-memory storage
type InMemoryBackend struct {
	// Lock for concurrent access to the backend
	mu sync.RWMutex

	// Flag indicating if the backend is open/closed
	closed bool

	// Segments stored in memory, keyed by segment timestamp
	segments map[time.Time]*segmentData

	// Timestamp of the current active segment
	currentSegment time.Time
}

// NewInMemoryBackend creates a new in-memory backend
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		segments: make(map[time.Time]*segmentData),
		closed:   true,
	}
}

// Open initializes the storage backend
func (b *InMemoryBackend) Open() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.closed {
		return nil // Already open
	}

	b.closed = false
	return nil
}

// checkClosed is a helper to check if the backend is closed
func (b *InMemoryBackend) checkClosed() error {
	if b.closed {
		return ErrStorageClosed
	}
	return nil
}

// ensureActiveSegment ensures there is an active segment to write to
func (b *InMemoryBackend) ensureActiveSegment(timestamp time.Time) error {
	// Note: Caller should already hold the lock

	// If we have no segments, create one with the given timestamp
	if len(b.segments) == 0 {
		return b.CreateNewSegment(timestamp)
	}

	// Check if the current segment exists
	_, exists := b.segments[b.currentSegment]
	if !exists {
		// Current segment was deleted or doesn't exist; create a new one
		return b.CreateNewSegment(timestamp)
	}

	return nil
}

// Append stores a single message
func (b *InMemoryBackend) Append(msg Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.checkClosed(); err != nil {
		return err
	}

	if msg.Timestamp.IsZero() {
		return ErrInvalidTimestamp
	}

	// Ensure we have an active segment
	if err := b.ensureActiveSegment(msg.Timestamp); err != nil {
		return err
	}

	// Try to find appropriate segment for this message
	segmentTimestamp, err := b.findSegmentForTimeInternal(msg.Timestamp)
	if err != nil {
		// If message doesn't fall within any segment's range,
		// we need to find the most appropriate segment to use

		// Get all segment timestamps and sort them
		timestamps := make([]time.Time, 0, len(b.segments))
		for ts := range b.segments {
			timestamps = append(timestamps, ts)
		}

		sort.Slice(timestamps, func(i, j int) bool {
			return timestamps[i].Before(timestamps[j])
		})

		// First check if the message time is after the end of the last segment
		if len(timestamps) > 0 {
			lastSegmentTime := timestamps[len(timestamps)-1]
			lastSegment := b.segments[lastSegmentTime]

			if msg.Timestamp.After(lastSegment.info.EndTime) {
				// Message is newer than any existing segment, use the last segment
				segmentTimestamp = lastSegmentTime
			} else {
				// Find the segment with start time closest to but not after message time
				found := false
				for i := len(timestamps) - 1; i >= 0; i-- {
					if !timestamps[i].After(msg.Timestamp) {
						segmentTimestamp = timestamps[i]
						found = true
						break
					}
				}

				if !found {
					// If all segments start after message time, use the earliest segment
					segmentTimestamp = timestamps[0]
				}
			}
		} else {
			// This should not happen (we just ensured we have at least one segment)
			segmentTimestamp = b.currentSegment
		}
	}

	segment := b.segments[segmentTimestamp]

	// Add the message to the segment
	segment.messages = append(segment.messages, msg)

	// Update segment metadata
	segment.info.MessageCount++
	segment.info.Size += int64(len(msg.Payload))

	// Update EndTime if this message is newer
	if segment.info.EndTime.Before(msg.Timestamp) {
		segment.info.EndTime = msg.Timestamp
	}

	return nil
}

// AppendBatch stores multiple messages at once
func (b *InMemoryBackend) AppendBatch(msgs []Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.checkClosed(); err != nil {
		return err
	}

	if len(msgs) == 0 {
		return nil
	}

	// Check for invalid timestamps
	for _, msg := range msgs {
		if msg.Timestamp.IsZero() {
			return ErrInvalidTimestamp
		}
	}

	// Find earliest timestamp to ensure we have an appropriate segment
	earliestTimestamp := msgs[0].Timestamp
	for _, msg := range msgs[1:] {
		if msg.Timestamp.Before(earliestTimestamp) {
			earliestTimestamp = msg.Timestamp
		}
	}

	if err := b.ensureActiveSegment(earliestTimestamp); err != nil {
		return err
	}

	// Group messages by segment
	messagesBySegment := make(map[time.Time][]Message)

	// Get all segment timestamps and sort them for potential fallback
	timestamps := make([]time.Time, 0, len(b.segments))
	for ts := range b.segments {
		timestamps = append(timestamps, ts)
	}
	sort.Slice(timestamps, func(i, j int) bool {
		return timestamps[i].Before(timestamps[j])
	})

	for _, msg := range msgs {
		// Try to find appropriate segment for this message
		segmentTime, err := b.findSegmentForTimeInternal(msg.Timestamp)
		if err != nil {
			// If message doesn't fall within any segment's range, find the best segment
			if len(timestamps) > 0 {
				lastSegmentTime := timestamps[len(timestamps)-1]
				lastSegment := b.segments[lastSegmentTime]

				if msg.Timestamp.After(lastSegment.info.EndTime) {
					// Message is newer than any existing segment, use the last segment
					segmentTime = lastSegmentTime
				} else {
					// Find the segment with start time closest to but not after message time
					found := false
					for i := len(timestamps) - 1; i >= 0; i-- {
						if !timestamps[i].After(msg.Timestamp) {
							segmentTime = timestamps[i]
							found = true
							break
						}
					}

					if !found {
						// If all segments start after message time, use the earliest segment
						segmentTime = timestamps[0]
					}
				}
			} else {
				// This should not happen (we just ensured we have at least one segment)
				segmentTime = b.currentSegment
			}
		}

		messagesBySegment[segmentTime] = append(messagesBySegment[segmentTime], msg)
	}

	// Append messages to their respective segments
	for segmentTime, segmentMsgs := range messagesBySegment {
		segment, exists := b.segments[segmentTime]
		if !exists {
			// Should not happen due to findSegmentForTime behavior
			continue
		}

		for _, msg := range segmentMsgs {
			segment.messages = append(segment.messages, msg)
			segment.info.MessageCount++
			segment.info.Size += int64(len(msg.Payload))

			// Update EndTime if this message is newer
			if segment.info.EndTime.Before(msg.Timestamp) {
				segment.info.EndTime = msg.Timestamp
			}
		}
	}

	return nil
}

// findSegmentForTime finds the appropriate segment for a given message timestamp
// This is the public version that acquires a read lock
func (b *InMemoryBackend) findSegmentForTime(messageTime time.Time) (time.Time, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if err := b.checkClosed(); err != nil {
		return time.Time{}, err
	}

	if messageTime.IsZero() {
		return time.Time{}, ErrInvalidTimestamp
	}

	return b.findSegmentForTimeInternal(messageTime)
}

// findSegmentForTimeInternal is the internal implementation used by other methods
// that have already acquired a lock
func (b *InMemoryBackend) findSegmentForTimeInternal(messageTime time.Time) (time.Time, error) {
	if len(b.segments) == 0 {
		return time.Time{}, ErrSegmentNotFound
	}

	// Get all segment timestamps and sort them
	timestamps := make([]time.Time, 0, len(b.segments))
	for ts := range b.segments {
		timestamps = append(timestamps, ts)
	}

	sort.Slice(timestamps, func(i, j int) bool {
		return timestamps[i].Before(timestamps[j])
	})

	// Find segment where messageTime is within the segment's time range
	for _, segmentTime := range timestamps {
		segment := b.segments[segmentTime]

		// Check if message time is within this segment's range
		// A message is within range if:
		// 1. It's not before the segment's start time (segmentTime)
		// 2. And it's not after the segment's end time
		if !messageTime.Before(segmentTime) && !messageTime.After(segment.info.EndTime) {
			return segmentTime, nil
		}
	}

	// For Append and AppendBatch operations, we want to find the closest
	// segment with a start time before the message time.
	// For FindSegmentByTime, we want to strictly return ErrSegmentNotFound
	// for timestamps outside all segment ranges.

	// This method should be strict and return an error if the message time
	// does not fall within any segment's range
	return time.Time{}, ErrSegmentNotFound
}

// Read returns an iterator for the specified segment starting at the given offset
func (b *InMemoryBackend) Read(segmentTimestamp time.Time, offset int64) (Iterator, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if err := b.checkClosed(); err != nil {
		return nil, err
	}

	segment, exists := b.segments[segmentTimestamp]
	if !exists {
		return nil, ErrSegmentNotFound
	}

	if offset < 0 || offset >= int64(len(segment.messages)) {
		offset = 0
	}

	return &inMemoryIterator{
		segment:  segment,
		position: int(offset) - 1, // -1 because Next() will be called before first read
		backend:  b,
	}, nil
}

// ListSegments returns information about all available segments
func (b *InMemoryBackend) ListSegments() ([]SegmentInfo, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if err := b.checkClosed(); err != nil {
		return nil, err
	}

	segments := make([]SegmentInfo, 0, len(b.segments))
	for _, segment := range b.segments {
		segments = append(segments, segment.info)
	}

	// Sort segments by timestamp
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].Timestamp.Before(segments[j].Timestamp)
	})

	return segments, nil
}

// GetSegmentInfo returns information about a specific segment
func (b *InMemoryBackend) GetSegmentInfo(timestamp time.Time) (SegmentInfo, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if err := b.checkClosed(); err != nil {
		return SegmentInfo{}, err
	}

	segment, exists := b.segments[timestamp]
	if !exists {
		return SegmentInfo{}, ErrSegmentNotFound
	}

	return segment.info, nil
}

// FindSegmentByTime returns the segment that contains the specified timestamp
func (b *InMemoryBackend) FindSegmentByTime(messageTimestamp time.Time) (SegmentInfo, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if err := b.checkClosed(); err != nil {
		return SegmentInfo{}, err
	}

	if messageTimestamp.IsZero() {
		return SegmentInfo{}, ErrInvalidTimestamp
	}

	// For FindSegmentByTime, we need to be strict: only return a segment if
	// the timestamp actually falls within the segment's time range
	for segmentTime, segment := range b.segments {
		// Check if message timestamp is within this segment's time range
		// A message is within range if:
		// 1. It's not before the segment's start time (segmentTime)
		// 2. And it's not after the segment's end time
		if !messageTimestamp.Before(segmentTime) && !messageTimestamp.After(segment.info.EndTime) {
			return segment.info, nil
		}
	}

	// No segment contains this timestamp
	return SegmentInfo{}, ErrSegmentNotFound
}

// CreateNewSegment closes the current segment and creates a new one
func (b *InMemoryBackend) CreateNewSegment(timestamp time.Time) error {
	// Note: Caller should already hold the lock

	if err := b.checkClosed(); err != nil {
		return err
	}

	if timestamp.IsZero() {
		return ErrInvalidTimestamp
	}

	// Check if a segment with this timestamp already exists
	if _, exists := b.segments[timestamp]; exists {
		return ErrSegmentExists
	}

	// If there are existing segments, ensure the new one is chronologically after the current one
	if len(b.segments) > 0 && timestamp.Before(b.currentSegment) {
		return ErrInvalidSegmentOrder
	}

	// Create the new segment
	b.segments[timestamp] = &segmentData{
		info: SegmentInfo{
			Timestamp:    timestamp,
			EndTime:      timestamp, // Initially, end time is the same as start time
			MessageCount: 0,
			Size:         0,
		},
		messages: make([]Message, 0),
	}

	// Update current segment
	b.currentSegment = timestamp

	return nil
}

// Sync forces a sync of all pending writes to the underlying storage
// For in-memory, this is a no-op
func (b *InMemoryBackend) Sync() error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.checkClosed()
}

// Close closes the storage backend
func (b *InMemoryBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil // Already closed
	}

	b.closed = true
	return nil
}

// inMemoryIterator implements the Iterator interface for in-memory segments
type inMemoryIterator struct {
	segment  *segmentData
	position int
	backend  *InMemoryBackend
	err      error
}

// Next advances to the next message
func (it *inMemoryIterator) Next() bool {
	// Check for errors or if we're at the end
	if it.err != nil || it.position >= len(it.segment.messages)-1 {
		return false
	}

	it.position++
	return true
}

// Message returns the current message
func (it *inMemoryIterator) Message() Message {
	if it.position < 0 || it.position >= len(it.segment.messages) {
		return Message{}
	}
	return it.segment.messages[it.position]
}

// Err returns any error encountered during iteration
func (it *inMemoryIterator) Err() error {
	return it.err
}

// Close releases resources used by the iterator
func (it *inMemoryIterator) Close() error {
	// For in-memory iterator, there's nothing to close
	return nil
}
