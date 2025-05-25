package appenda

import (
	"errors"
	"sync"
	"time"
)

// segmentIterator iterates through messages in a specific segment
type segmentIterator struct {
	// Reference to the storage backend
	backend StorageBackend

	// Current segment timestamp being iterated
	segmentTimestamp time.Time

	// Current iterator for the segment
	iterator Iterator

	// Current message position
	position int64

	// Error encountered during iteration
	err error

	// Current message
	currentMessage Message

	// Flag indicating if the iterator has been closed
	closed bool

	// Mutex to protect concurrent access
	mu sync.RWMutex
}

// NewSegmentIterator creates a new iterator over a specific segment
func NewSegmentIterator(backend StorageBackend, segmentTimestamp time.Time, offset int64) (Iterator, error) {
	if backend == nil {
		return nil, errors.New("backend cannot be nil")
	}

	iterator, err := backend.Read(segmentTimestamp, offset)
	if err != nil {
		return nil, err
	}

	return &segmentIterator{
		backend:          backend,
		segmentTimestamp: segmentTimestamp,
		iterator:         iterator,
		position:         offset,
		closed:           false,
	}, nil
}

// Next advances to the next message
func (it *segmentIterator) Next() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed || it.err != nil {
		return false
	}

	if !it.iterator.Next() {
		it.err = it.iterator.Err()
		return false
	}

	it.currentMessage = it.iterator.Message()
	it.position++
	return true
}

// Message returns the current message
func (it *segmentIterator) Message() Message {
	it.mu.RLock()
	defer it.mu.RUnlock()

	return it.currentMessage
}

// Err returns any error encountered during iteration
func (it *segmentIterator) Err() error {
	it.mu.RLock()
	defer it.mu.RUnlock()

	return it.err
}

// Close releases resources used by the iterator
func (it *segmentIterator) Close() error {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed {
		return nil
	}

	it.closed = true
	if it.iterator != nil {
		return it.iterator.Close()
	}

	return nil
}

// MultiSegmentIterator iterates through messages across multiple segments
type MultiSegmentIterator struct {
	// Reference to the storage backend
	backend StorageBackend

	// Starting timestamp for iteration
	startTimestamp time.Time

	// Current segment being iterated
	currentSegment time.Time

	// List of segments to iterate through
	segments []SegmentInfo

	// Current segment index
	segmentIndex int

	// Current iterator for the segment
	iterator Iterator

	// Current message
	currentMessage Message

	// Error encountered during iteration
	err error

	// Flag indicating if the iterator has been closed
	closed bool

	// Mutex to protect concurrent access
	mu sync.RWMutex
}

// NewMultiSegmentIterator creates a new iterator that spans multiple segments
func NewMultiSegmentIterator(backend StorageBackend, startTime time.Time, offset int64) (Iterator, error) {
	if backend == nil {
		return nil, errors.New("backend cannot be nil")
	}

	// Get list of all segments
	segments, err := backend.ListSegments()
	if err != nil {
		return nil, err
	}

	if len(segments) == 0 {
		return &MultiSegmentIterator{
			backend:        backend,
			startTimestamp: startTime,
			segments:       []SegmentInfo{},
			segmentIndex:   -1,
			closed:         false,
		}, nil
	}

	// If startTime is zero, start from the first segment
	if startTime.IsZero() {
		segmentIndex := 0
		currentSegment := segments[0].Timestamp

		iterator, err := backend.Read(currentSegment, offset)
		if err != nil {
			return nil, err
		}

		return &MultiSegmentIterator{
			backend:        backend,
			startTimestamp: startTime,
			currentSegment: currentSegment,
			segments:       segments,
			segmentIndex:   segmentIndex,
			iterator:       iterator,
			closed:         false,
		}, nil
	}

	// Check for segments that might contain messages at or after startTime

	// 1. Try to find a segment containing the startTime exactly
	segmentIndex := -1
	for i, segment := range segments {
		// Check if startTime is within this segment's range (between start and end time)
		if !startTime.Before(segment.Timestamp) && !startTime.After(segment.EndTime) {
			segmentIndex = i
			break
		}
	}

	// 2. If no segment contains startTime exactly, find the first segment that might contain
	// messages at or after startTime
	if segmentIndex == -1 {
		// Find the first segment where EndTime is >= startTime
		for i, segment := range segments {
			if !segment.EndTime.Before(startTime) {
				segmentIndex = i
				break
			}
		}
	}

	// 3. If still not found, we need to start from the earliest segment
	// This is important because messages might not be strictly ordered by timestamp
	// across segments
	if segmentIndex == -1 {
		segmentIndex = 0
	}

	// Create iterator for the selected segment
	currentSegment := segments[segmentIndex].Timestamp
	iterator, err := backend.Read(currentSegment, offset)
	if err != nil {
		return nil, err
	}

	return &MultiSegmentIterator{
		backend:        backend,
		startTimestamp: startTime,
		currentSegment: currentSegment,
		segments:       segments,
		segmentIndex:   segmentIndex,
		iterator:       iterator,
		closed:         false,
	}, nil
}

// Next for MultiSegmentIterator should also be modified to properly handle the filtering:

// Next advances to the next message
func (it *MultiSegmentIterator) Next() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed || it.err != nil {
		return false
	}

	// If we haven't started iteration or have no segments, return false
	if it.segmentIndex < 0 || it.segmentIndex >= len(it.segments) {
		return false
	}

	// Keep trying until we find a message with timestamp >= startTimestamp
	// or reach the end of all segments
	for {
		// Try to get next message from current iterator
		if it.iterator != nil && it.iterator.Next() {
			msg := it.iterator.Message()

			// Check if message is at or after startTimestamp
			if it.startTimestamp.IsZero() || !msg.Timestamp.Before(it.startTimestamp) {
				it.currentMessage = msg
				return true
			}

			// Message is before startTimestamp, try the next one
			continue
		}

		// Check for errors in current iterator
		if it.iterator != nil && it.iterator.Err() != nil {
			it.err = it.iterator.Err()
			return false
		}

		// Move to next segment
		it.segmentIndex++
		if it.segmentIndex >= len(it.segments) {
			return false
		}

		// Close current iterator
		if it.iterator != nil {
			if err := it.iterator.Close(); err != nil {
				it.err = err
				return false
			}
		}

		// Create iterator for next segment
		nextSegment := it.segments[it.segmentIndex].Timestamp
		iterator, err := it.backend.Read(nextSegment, 0) // Start from beginning of next segment
		if err != nil {
			it.err = err
			return false
		}

		it.iterator = iterator
		it.currentSegment = nextSegment
	}
}

// Message returns the current message
func (it *MultiSegmentIterator) Message() Message {
	it.mu.RLock()
	defer it.mu.RUnlock()

	return it.currentMessage
}

// Err returns any error encountered during iteration
func (it *MultiSegmentIterator) Err() error {
	it.mu.RLock()
	defer it.mu.RUnlock()

	return it.err
}

// Close releases resources used by the iterator
func (it *MultiSegmentIterator) Close() error {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed {
		return nil
	}

	it.closed = true
	if it.iterator != nil {
		return it.iterator.Close()
	}

	return nil
}

// FilteredIterator filters messages during iteration based on a predicate
type FilteredIterator struct {
	// Underlying iterator
	iterator Iterator

	// Filter function that determines if a message should be included
	filter func(Message) bool

	// Current message
	currentMessage Message

	// Error encountered during iteration
	err error

	// Flag indicating if the iterator has been closed
	closed bool

	// Mutex to protect concurrent access
	mu sync.RWMutex
}

// NewFilteredIterator creates a new iterator that filters messages
func NewFilteredIterator(iterator Iterator, filter func(Message) bool) (Iterator, error) {
	if iterator == nil {
		return nil, errors.New("iterator cannot be nil")
	}

	if filter == nil {
		return nil, errors.New("filter function cannot be nil")
	}

	return &FilteredIterator{
		iterator: iterator,
		filter:   filter,
		closed:   false,
	}, nil
}

// Next advances to the next message that passes the filter
func (it *FilteredIterator) Next() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed || it.err != nil {
		return false
	}

	// Keep moving the underlying iterator until we find a message that passes the filter
	// or reach the end
	for it.iterator.Next() {
		msg := it.iterator.Message()
		if it.filter(msg) {
			it.currentMessage = msg
			return true
		}
	}

	// Check for errors in underlying iterator
	if it.iterator.Err() != nil {
		it.err = it.iterator.Err()
	}

	return false
}

// Message returns the current message
func (it *FilteredIterator) Message() Message {
	it.mu.RLock()
	defer it.mu.RUnlock()

	return it.currentMessage
}

// Err returns any error encountered during iteration
func (it *FilteredIterator) Err() error {
	it.mu.RLock()
	defer it.mu.RUnlock()

	if it.err != nil {
		return it.err
	}

	return it.iterator.Err()
}

// Close releases resources used by the iterator
func (it *FilteredIterator) Close() error {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed {
		return nil
	}

	it.closed = true
	return it.iterator.Close()
}

// TimeRangeIterator iterates messages within a specific time range
type TimeRangeIterator struct {
	// Underlying iterator
	iterator Iterator

	// Start time of the range (inclusive)
	startTime time.Time

	// End time of the range (inclusive)
	endTime time.Time

	// Current message
	currentMessage Message

	// Error encountered during iteration
	err error

	// Flag indicating if the iterator has been closed
	closed bool

	// Mutex to protect concurrent access
	mu sync.RWMutex
}

// NewTimeRangeIterator creates a new iterator that only returns messages within a time range
func NewTimeRangeIterator(backend StorageBackend, startTime, endTime time.Time) (Iterator, error) {
	if backend == nil {
		return nil, errors.New("backend cannot be nil")
	}

	if startTime.After(endTime) {
		return nil, errors.New("start time cannot be after end time")
	}

	// Start with a multi-segment iterator from the start time
	baseIterator, err := NewMultiSegmentIterator(backend, startTime, 0)
	if err != nil {
		return nil, err
	}

	return &TimeRangeIterator{
		iterator:  baseIterator,
		startTime: startTime,
		endTime:   endTime,
		closed:    false,
	}, nil
}

// Next advances to the next message within the time range
func (it *TimeRangeIterator) Next() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed || it.err != nil {
		return false
	}

	// Get next message from underlying iterator
	if !it.iterator.Next() {
		if it.iterator.Err() != nil {
			it.err = it.iterator.Err()
		}
		return false
	}

	msg := it.iterator.Message()

	// Check if message is within time range
	if msg.Timestamp.Before(it.startTime) || msg.Timestamp.After(it.endTime) {
		// If message is after end time, we can stop iteration early
		if msg.Timestamp.After(it.endTime) {
			return false
		}

		// Otherwise, try to get next message
		return it.Next()
	}

	it.currentMessage = msg
	return true
}

// Message returns the current message
func (it *TimeRangeIterator) Message() Message {
	it.mu.RLock()
	defer it.mu.RUnlock()

	return it.currentMessage
}

// Err returns any error encountered during iteration
func (it *TimeRangeIterator) Err() error {
	it.mu.RLock()
	defer it.mu.RUnlock()

	if it.err != nil {
		return it.err
	}

	return it.iterator.Err()
}

// Close releases resources used by the iterator
func (it *TimeRangeIterator) Close() error {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed {
		return nil
	}

	it.closed = true
	return it.iterator.Close()
}
