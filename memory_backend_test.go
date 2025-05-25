package appenda

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestInMemoryBackend_New tests the NewInMemoryBackend function
func TestInMemoryBackend_New(t *testing.T) {
	backend := NewInMemoryBackend()
	assert.NotNil(t, backend, "NewInMemoryBackend should return a non-nil backend")
	assert.True(t, backend.closed, "Backend should start in closed state")
	assert.NotNil(t, backend.segments, "Backend should have initialized segments map")
}

// TestInMemoryBackend_EndToEnd performs an end-to-end test of the InMemoryBackend
func TestInMemoryBackend_EndToEnd(t *testing.T) {
	backend := NewInMemoryBackend()
	err := backend.Open()
	assert.NoError(t, err, "Open should not return an error")

	// Test sequence of operations
	now := time.Now().UTC().Truncate(time.Millisecond)

	// 1. Create an initial segment
	err = backend.CreateNewSegment(now)
	assert.NoError(t, err, "CreateNewSegment should not return an error")

	// 2. Append individual messages
	for i := 0; i < 5; i++ {
		msg := Message{
			Timestamp: now.Add(time.Duration(i) * time.Minute),
			Payload:   []byte(string(rune('A' + i))),
		}
		err = backend.Append(msg)
		assert.NoError(t, err, "Append should not return an error")
	}

	// 3. Create another segment
	laterTime := now.Add(time.Hour)
	err = backend.CreateNewSegment(laterTime)
	assert.NoError(t, err, "CreateNewSegment should not return an error")

	// 4. Append batch of messages to the new segment
	laterMsgs := []Message{
		{
			Timestamp: laterTime,
			Payload:   []byte("later 1"),
		},
		{
			Timestamp: laterTime.Add(time.Minute),
			Payload:   []byte("later 2"),
		},
		{
			Timestamp: laterTime.Add(2 * time.Minute),
			Payload:   []byte("later 3"),
		},
	}
	err = backend.AppendBatch(laterMsgs)
	assert.NoError(t, err, "AppendBatch should not return an error")

	// 5. Force sync (no-op for in-memory backend)
	err = backend.Sync()
	assert.NoError(t, err, "Sync should not return an error")

	// 6. Verify segment information
	segments, err := backend.ListSegments()
	assert.NoError(t, err, "ListSegments should not return an error")
	assert.Len(t, segments, 2, "Should have 2 segments")

	assert.Equal(t, now, segments[0].Timestamp, "First segment timestamp should match")
	assert.Equal(t, int64(5), segments[0].MessageCount, "First segment should have 5 messages")

	assert.Equal(t, laterTime, segments[1].Timestamp, "Second segment timestamp should match")
	assert.Equal(t, int64(3), segments[1].MessageCount, "Second segment should have 3 messages")

	// 7. Read from the first segment
	iterator1, err := backend.Read(now, 0)
	assert.NoError(t, err, "Read should not return an error")

	count1 := 0
	for iterator1.Next() {
		count1++
	}
	assert.Equal(t, 5, count1, "First segment should have 5 messages")
	assert.NoError(t, iterator1.Close(), "Close should not return an error")

	// 8. Read from the second segment
	iterator2, err := backend.Read(laterTime, 0)
	assert.NoError(t, err, "Read should not return an error")

	count2 := 0
	for iterator2.Next() {
		count2++
	}
	assert.Equal(t, 3, count2, "Second segment should have 3 messages")
	assert.NoError(t, iterator2.Close(), "Close should not return an error")

	// 9. Test FindSegmentByTime
	segmentInfo, err := backend.FindSegmentByTime(now.Add(2 * time.Minute))
	assert.NoError(t, err, "FindSegmentByTime should not return an error")
	assert.Equal(t, now, segmentInfo.Timestamp, "FindSegmentByTime should return the first segment")

	segmentInfo, err = backend.FindSegmentByTime(laterTime.Add(time.Minute))
	assert.NoError(t, err, "FindSegmentByTime should not return an error")
	assert.Equal(t, laterTime, segmentInfo.Timestamp, "FindSegmentByTime should return the second segment")

	// 10. Close the backend
	err = backend.Close()
	assert.NoError(t, err, "Close should not return an error")

	// 11. Verify operations fail after closing
	_, err = backend.Read(now, 0)
	assert.Equal(t, ErrStorageClosed, err, "Read should return ErrStorageClosed after closing")

	err = backend.Append(Message{Timestamp: now, Payload: []byte("should fail")})
	assert.Equal(t, ErrStorageClosed, err, "Append should return ErrStorageClosed after closing")
}

// TestInMemoryBackend_Open tests the Open method
func TestInMemoryBackend_Open(t *testing.T) {
	backend := NewInMemoryBackend()

	// Test opening the backend
	err := backend.Open()
	assert.NoError(t, err, "Open should not return an error")
	assert.False(t, backend.closed, "Backend should be open after Open() call")

	// Test opening an already open backend
	err = backend.Open()
	assert.NoError(t, err, "Opening an already open backend should not return an error")
}

// TestInMemoryBackend_CheckClosed tests the checkClosed helper method
func TestInMemoryBackend_CheckClosed(t *testing.T) {
	backend := NewInMemoryBackend()

	// Test with closed backend
	err := backend.checkClosed()
	assert.Equal(t, ErrStorageClosed, err, "checkClosed should return ErrStorageClosed when backend is closed")

	// Test with open backend
	backend.Open()
	err = backend.checkClosed()
	assert.NoError(t, err, "checkClosed should not return an error when backend is open")
}

// TestInMemoryBackend_EnsureActiveSegment tests the ensureActiveSegment helper method
func TestInMemoryBackend_EnsureActiveSegment(t *testing.T) {
	backend := NewInMemoryBackend()
	backend.Open()

	now := time.Now().UTC().Truncate(time.Millisecond)

	// Test with no active segments
	err := backend.ensureActiveSegment(now)
	assert.NoError(t, err, "ensureActiveSegment should not return an error")
	assert.Equal(t, now, backend.currentSegment, "currentSegment should be set to provided timestamp")
	assert.Len(t, backend.segments, 1, "A new segment should have been created")

	// Test with existing active segment
	later := now.Add(time.Hour)
	err = backend.ensureActiveSegment(later)
	assert.NoError(t, err, "ensureActiveSegment should not return an error with existing segment")
	assert.Equal(t, now, backend.currentSegment, "currentSegment should not change")
	assert.Len(t, backend.segments, 1, "No new segment should have been created")

	// Test with missing current segment (simulated by removing it)
	delete(backend.segments, backend.currentSegment)
	err = backend.ensureActiveSegment(later)
	assert.NoError(t, err, "ensureActiveSegment should not return an error")
	assert.Equal(t, later, backend.currentSegment, "currentSegment should be updated")
	assert.Len(t, backend.segments, 1, "A new segment should have been created")
}

// TestInMemoryBackend_Append tests the Append method
func TestInMemoryBackend_Append(t *testing.T) {
	backend := NewInMemoryBackend()

	// Test with closed backend
	now := time.Now().UTC().Truncate(time.Millisecond)
	msg := Message{
		Timestamp: now,
		Payload:   []byte("test message"),
	}
	err := backend.Append(msg)
	assert.Equal(t, ErrStorageClosed, err, "Append should return ErrStorageClosed when backend is closed")

	// Open the backend
	backend.Open()

	// Test with invalid (zero) timestamp
	invalidMsg := Message{
		Timestamp: time.Time{},
		Payload:   []byte("invalid message"),
	}
	err = backend.Append(invalidMsg)
	assert.Equal(t, ErrInvalidTimestamp, err, "Append should return ErrInvalidTimestamp for zero timestamp")

	// Test appending to a new segment
	err = backend.Append(msg)
	assert.NoError(t, err, "Append should not return an error")
	assert.Len(t, backend.segments, 1, "A segment should have been created")

	segment := backend.segments[backend.currentSegment]
	assert.Equal(t, int64(1), segment.info.MessageCount, "Segment should have 1 message")
	assert.Equal(t, int64(len(msg.Payload)), segment.info.Size, "Segment size should match payload length")
	assert.Equal(t, now, segment.info.EndTime, "Segment EndTime should match message timestamp")
	assert.Len(t, segment.messages, 1, "Segment should contain 1 message")
	assert.Equal(t, msg, segment.messages[0], "Segment's message should match appended message")

	// Test appending another message with later timestamp
	laterMsg := Message{
		Timestamp: now.Add(time.Minute),
		Payload:   []byte("later message"),
	}
	err = backend.Append(laterMsg)
	assert.NoError(t, err, "Append should not return an error")
	assert.Len(t, backend.segments, 1, "No new segment should have been created")

	segment = backend.segments[backend.currentSegment]
	assert.Equal(t, int64(2), segment.info.MessageCount, "Segment should have 2 messages")
	assert.Equal(t, int64(len(msg.Payload)+len(laterMsg.Payload)), segment.info.Size, "Segment size should match combined payload lengths")
	assert.Equal(t, laterMsg.Timestamp, segment.info.EndTime, "Segment EndTime should be updated to later timestamp")
	assert.Len(t, segment.messages, 2, "Segment should contain 2 messages")
	assert.Equal(t, laterMsg, segment.messages[1], "Segment's second message should match appended message")

	// Test appending a message with earlier timestamp
	earlierMsg := Message{
		Timestamp: now.Add(-time.Minute),
		Payload:   []byte("earlier message"),
	}
	err = backend.Append(earlierMsg)
	assert.NoError(t, err, "Append should not return an error for earlier timestamp")
	assert.Len(t, backend.segments, 1, "No new segment should have been created")

	segment = backend.segments[backend.currentSegment]
	assert.Equal(t, int64(3), segment.info.MessageCount, "Segment should have 3 messages")
	assert.Len(t, segment.messages, 3, "Segment should contain 3 messages")

	// Test with multiple segments (create new segment first)
	newerTime := now.Add(2 * time.Hour)
	err = backend.CreateNewSegment(newerTime)
	assert.NoError(t, err, "CreateNewSegment should not return an error")

	newerMsg := Message{
		Timestamp: newerTime.Add(time.Minute),
		Payload:   []byte("newer message"),
	}
	err = backend.Append(newerMsg)
	assert.NoError(t, err, "Append should not return an error")

	// Message should be in newer segment
	segment = backend.segments[newerTime]
	assert.Equal(t, int64(1), segment.info.MessageCount, "Newer segment should have 1 message")
	assert.Len(t, segment.messages, 1, "Newer segment should contain 1 message")
	assert.Equal(t, newerMsg, segment.messages[0], "Newer segment's message should match appended message")
}

// TestInMemoryBackend_AppendBatch tests the AppendBatch method
func TestInMemoryBackend_AppendBatch(t *testing.T) {
	backend := NewInMemoryBackend()

	// Test with closed backend
	now := time.Now().UTC().Truncate(time.Millisecond)
	msgs := []Message{
		{
			Timestamp: now,
			Payload:   []byte("message 1"),
		},
		{
			Timestamp: now.Add(time.Minute),
			Payload:   []byte("message 2"),
		},
	}
	err := backend.AppendBatch(msgs)
	assert.Equal(t, ErrStorageClosed, err, "AppendBatch should return ErrStorageClosed when backend is closed")

	// Open the backend
	backend.Open()

	// Test with empty batch
	err = backend.AppendBatch([]Message{})
	assert.NoError(t, err, "AppendBatch with empty batch should not return an error")

	// Test with invalid timestamp in batch
	invalidMsgs := []Message{
		{
			Timestamp: now,
			Payload:   []byte("valid message"),
		},
		{
			Timestamp: time.Time{}, // Invalid timestamp
			Payload:   []byte("invalid message"),
		},
	}
	err = backend.AppendBatch(invalidMsgs)
	assert.Equal(t, ErrInvalidTimestamp, err, "AppendBatch should return ErrInvalidTimestamp for batch with invalid timestamp")

	// Test appending a batch to a new segment
	err = backend.AppendBatch(msgs)
	assert.NoError(t, err, "AppendBatch should not return an error")
	assert.Len(t, backend.segments, 1, "A segment should have been created")

	segment := backend.segments[backend.currentSegment]
	assert.Equal(t, int64(2), segment.info.MessageCount, "Segment should have 2 messages")
	assert.Equal(t, int64(len(msgs[0].Payload)+len(msgs[1].Payload)), segment.info.Size, "Segment size should match combined payload lengths")
	assert.Equal(t, msgs[1].Timestamp, segment.info.EndTime, "Segment EndTime should match latest message timestamp")
	assert.Len(t, segment.messages, 2, "Segment should contain 2 messages")

	// Test appending a batch with messages spanning multiple segments
	err = backend.CreateNewSegment(now.Add(2 * time.Hour))
	assert.NoError(t, err, "CreateNewSegment should not return an error")

	mixedMsgs := []Message{
		{
			Timestamp: now.Add(30 * time.Minute), // Should go to first segment
			Payload:   []byte("first segment message"),
		},
		{
			Timestamp: now.Add(2*time.Hour + 30*time.Minute), // Should go to second segment
			Payload:   []byte("second segment message"),
		},
	}
	err = backend.AppendBatch(mixedMsgs)
	assert.NoError(t, err, "AppendBatch should not return an error")

	// Check first segment
	firstSegment := backend.segments[now]
	assert.Equal(t, int64(3), firstSegment.info.MessageCount, "First segment should have 3 messages")
	assert.Len(t, firstSegment.messages, 3, "First segment should contain 3 messages")

	// Check second segment
	secondSegment := backend.segments[now.Add(2*time.Hour)]
	assert.Equal(t, int64(1), secondSegment.info.MessageCount, "Second segment should have 1 message")
	assert.Len(t, secondSegment.messages, 1, "Second segment should contain 1 message")
}

// TestInMemoryBackend_Read tests the Read method
func TestInMemoryBackend_Read(t *testing.T) {
	backend := NewInMemoryBackend()

	// Test with closed backend
	now := time.Now().UTC().Truncate(time.Millisecond)
	iterator, err := backend.Read(now, 0)
	assert.Equal(t, ErrStorageClosed, err, "Read should return ErrStorageClosed when backend is closed")
	assert.Nil(t, iterator, "Iterator should be nil when backend is closed")

	// Open the backend
	backend.Open()

	// Test with non-existent segment
	iterator, err = backend.Read(now, 0)
	assert.Equal(t, ErrSegmentNotFound, err, "Read should return ErrSegmentNotFound for non-existent segment")
	assert.Nil(t, iterator, "Iterator should be nil when segment doesn't exist")

	// Create a segment and add messages
	err = backend.CreateNewSegment(now)
	assert.NoError(t, err, "CreateNewSegment should not return an error")

	msgs := []Message{
		{
			Timestamp: now,
			Payload:   []byte("message 1"),
		},
		{
			Timestamp: now.Add(time.Minute),
			Payload:   []byte("message 2"),
		},
		{
			Timestamp: now.Add(2 * time.Minute),
			Payload:   []byte("message 3"),
		},
	}
	err = backend.AppendBatch(msgs)
	assert.NoError(t, err, "AppendBatch should not return an error")

	// Test reading with valid segment
	iterator, err = backend.Read(now, 0)
	assert.NoError(t, err, "Read should not return an error for valid segment")
	assert.NotNil(t, iterator, "Iterator should not be nil")

	// Test iteration
	count := 0
	for iterator.Next() {
		msg := iterator.Message()
		assert.Equal(t, msgs[count], msg, "Message should match the expected message")
		count++
	}
	assert.Equal(t, len(msgs), count, "Iterator should return all messages")
	assert.NoError(t, iterator.Err(), "Iterator should not have an error")
	assert.NoError(t, iterator.Close(), "Closing iterator should not return an error")

	// Test reading with offset
	iterator, err = backend.Read(now, 1)
	assert.NoError(t, err, "Read with offset should not return an error")
	assert.NotNil(t, iterator, "Iterator with offset should not be nil")

	// Should skip the first message
	assert.True(t, iterator.Next(), "Next should return true")
	assert.Equal(t, msgs[1], iterator.Message(), "Message should match the second message")

	// Test with out-of-bounds offset (should default to 0)
	iterator, err = backend.Read(now, 10)
	assert.NoError(t, err, "Read with out-of-bounds offset should not return an error")
	assert.NotNil(t, iterator, "Iterator with out-of-bounds offset should not be nil")

	assert.True(t, iterator.Next(), "Next should return true")
	assert.Equal(t, msgs[0], iterator.Message(), "Message should match the first message")
}

// TestInMemoryBackend_ListSegments tests the ListSegments method
func TestInMemoryBackend_ListSegments(t *testing.T) {
	backend := NewInMemoryBackend()

	// Test with closed backend
	segments, err := backend.ListSegments()
	assert.Equal(t, ErrStorageClosed, err, "ListSegments should return ErrStorageClosed when backend is closed")
	assert.Nil(t, segments, "Segments should be nil when backend is closed")

	// Open the backend
	backend.Open()

	// Test with no segments
	segments, err = backend.ListSegments()
	assert.NoError(t, err, "ListSegments should not return an error for empty backend")
	assert.Empty(t, segments, "Segments should be empty for new backend")

	// Create multiple segments in chronological order
	// Note: We need to create them in chronological order due to InMemoryBackend's constraint
	// that new segments must be created with timestamps after the current segment
	now := time.Now().UTC().Truncate(time.Millisecond)
	earlierTime := now.Add(-time.Hour)
	laterTime := now.Add(time.Hour)

	// Create the segments in chronological order (earliest first)
	err = backend.CreateNewSegment(earlierTime)
	assert.NoError(t, err, "CreateNewSegment for earliest segment should not return an error")

	err = backend.CreateNewSegment(now)
	assert.NoError(t, err, "CreateNewSegment for middle segment should not return an error")

	err = backend.CreateNewSegment(laterTime)
	assert.NoError(t, err, "CreateNewSegment for latest segment should not return an error")

	// Test listing all segments
	segments, err = backend.ListSegments()
	assert.NoError(t, err, "ListSegments should not return an error")
	assert.Len(t, segments, 3, "Should return 3 segments")

	// Verify segments are sorted by timestamp
	assert.Equal(t, earlierTime, segments[0].Timestamp, "First segment should be the earliest")
	assert.Equal(t, now, segments[1].Timestamp, "Second segment should be the middle one")
	assert.Equal(t, laterTime, segments[2].Timestamp, "Third segment should be the latest")
}

// TestInMemoryBackend_GetSegmentInfo tests the GetSegmentInfo method
func TestInMemoryBackend_GetSegmentInfo(t *testing.T) {
	backend := NewInMemoryBackend()

	// Test with closed backend
	now := time.Now().UTC().Truncate(time.Millisecond)
	info, err := backend.GetSegmentInfo(now)
	assert.Equal(t, ErrStorageClosed, err, "GetSegmentInfo should return ErrStorageClosed when backend is closed")
	assert.Equal(t, SegmentInfo{}, info, "SegmentInfo should be empty when backend is closed")

	// Open the backend
	backend.Open()

	// Test with non-existent segment
	info, err = backend.GetSegmentInfo(now)
	assert.Equal(t, ErrSegmentNotFound, err, "GetSegmentInfo should return ErrSegmentNotFound for non-existent segment")
	assert.Equal(t, SegmentInfo{}, info, "SegmentInfo should be empty when segment doesn't exist")

	// Create a segment and add a message
	err = backend.CreateNewSegment(now)
	assert.NoError(t, err, "CreateNewSegment should not return an error")

	msg := Message{
		Timestamp: now.Add(time.Minute),
		Payload:   []byte("test message"),
	}
	err = backend.Append(msg)
	assert.NoError(t, err, "Append should not return an error")

	// Test getting segment info
	info, err = backend.GetSegmentInfo(now)
	assert.NoError(t, err, "GetSegmentInfo should not return an error for existing segment")
	assert.Equal(t, now, info.Timestamp, "SegmentInfo Timestamp should match segment timestamp")
	assert.Equal(t, msg.Timestamp, info.EndTime, "SegmentInfo EndTime should match message timestamp")
	assert.Equal(t, int64(1), info.MessageCount, "SegmentInfo MessageCount should be 1")
	assert.Equal(t, int64(len(msg.Payload)), info.Size, "SegmentInfo Size should match payload length")
}

// TestInMemoryBackend_FindSegmentByTime tests the FindSegmentByTime method
func TestInMemoryBackend_FindSegmentByTime(t *testing.T) {
	backend := NewInMemoryBackend()

	// Test with closed backend
	now := time.Now().UTC().Truncate(time.Millisecond)
	info, err := backend.FindSegmentByTime(now)
	assert.Equal(t, ErrStorageClosed, err, "FindSegmentByTime should return ErrStorageClosed when backend is closed")
	assert.Equal(t, SegmentInfo{}, info, "SegmentInfo should be empty when backend is closed")

	// Open the backend
	backend.Open()

	// Test with invalid timestamp
	info, err = backend.FindSegmentByTime(time.Time{})
	assert.Equal(t, ErrInvalidTimestamp, err, "FindSegmentByTime should return ErrInvalidTimestamp for zero timestamp")
	assert.Equal(t, SegmentInfo{}, info, "SegmentInfo should be empty for invalid timestamp")

	// Test with no segments
	info, err = backend.FindSegmentByTime(now)
	assert.Equal(t, ErrSegmentNotFound, err, "FindSegmentByTime should return ErrSegmentNotFound when no segments exist")
	assert.Equal(t, SegmentInfo{}, info, "SegmentInfo should be empty when no segments exist")

	// Create segments with different time ranges
	segmentStart1 := now
	err = backend.CreateNewSegment(segmentStart1)
	assert.NoError(t, err, "CreateNewSegment should not return an error")

	// Add messages to extend the segment's time range
	msgs1 := []Message{
		{
			Timestamp: segmentStart1,
			Payload:   []byte("message 1-1"),
		},
		{
			Timestamp: segmentStart1.Add(30 * time.Minute),
			Payload:   []byte("message 1-2"),
		},
	}
	err = backend.AppendBatch(msgs1)
	assert.NoError(t, err, "AppendBatch should not return an error")

	segmentStart2 := now.Add(time.Hour)
	err = backend.CreateNewSegment(segmentStart2)
	assert.NoError(t, err, "CreateNewSegment should not return an error")

	// Add messages to extend the second segment's time range
	msgs2 := []Message{
		{
			Timestamp: segmentStart2,
			Payload:   []byte("message 2-1"),
		},
		{
			Timestamp: segmentStart2.Add(30 * time.Minute),
			Payload:   []byte("message 2-2"),
		},
	}
	err = backend.AppendBatch(msgs2)
	assert.NoError(t, err, "AppendBatch should not return an error")

	// Test finding segment by timestamp within first segment
	timeInFirstSegment := segmentStart1.Add(15 * time.Minute)
	info, err = backend.FindSegmentByTime(timeInFirstSegment)
	assert.NoError(t, err, "FindSegmentByTime should not return an error for time in first segment")
	assert.Equal(t, segmentStart1, info.Timestamp, "Should return info for first segment")

	// Test finding segment by timestamp within second segment
	timeInSecondSegment := segmentStart2.Add(15 * time.Minute)
	info, err = backend.FindSegmentByTime(timeInSecondSegment)
	assert.NoError(t, err, "FindSegmentByTime should not return an error for time in second segment")
	assert.Equal(t, segmentStart2, info.Timestamp, "Should return info for second segment")

	// Test with timestamp exactly at segment start
	info, err = backend.FindSegmentByTime(segmentStart1)
	assert.NoError(t, err, "FindSegmentByTime should not return an error for timestamp at segment start")
	assert.Equal(t, segmentStart1, info.Timestamp, "Should return info for first segment")

	// Test with timestamp exactly at segment end
	info, err = backend.FindSegmentByTime(msgs1[1].Timestamp)
	assert.NoError(t, err, "FindSegmentByTime should not return an error for timestamp at segment end")
	assert.Equal(t, segmentStart1, info.Timestamp, "Should return info for first segment")

	// Test with timestamp before any segment
	beforeAnySegment := segmentStart1.Add(-time.Hour)
	info, err = backend.FindSegmentByTime(beforeAnySegment)
	assert.Equal(t, ErrSegmentNotFound, err, "FindSegmentByTime should return ErrSegmentNotFound for time before any segment")

	// Test with timestamp after all segments
	afterAllSegments := segmentStart2.Add(time.Hour)
	info, err = backend.FindSegmentByTime(afterAllSegments)
	assert.Equal(t, ErrSegmentNotFound, err, "FindSegmentByTime should return ErrSegmentNotFound for time after all segments")

	// Test with timestamp in gap between segments
	inGap := segmentStart1.Add(45 * time.Minute)
	info, err = backend.FindSegmentByTime(inGap)
	assert.Equal(t, ErrSegmentNotFound, err, "FindSegmentByTime should return ErrSegmentNotFound for time in gap between segments")
}

// TestInMemoryBackend_CreateNewSegment tests the CreateNewSegment method
func TestInMemoryBackend_CreateNewSegment(t *testing.T) {
	backend := NewInMemoryBackend()

	// Test with closed backend
	now := time.Now().UTC().Truncate(time.Millisecond)
	err := backend.CreateNewSegment(now)
	assert.Equal(t, ErrStorageClosed, err, "CreateNewSegment should return ErrStorageClosed when backend is closed")

	// Open the backend
	backend.Open()

	// Test with invalid timestamp
	err = backend.CreateNewSegment(time.Time{})
	assert.Equal(t, ErrInvalidTimestamp, err, "CreateNewSegment should return ErrInvalidTimestamp for zero timestamp")

	// Test creating first segment
	err = backend.CreateNewSegment(now)
	assert.NoError(t, err, "CreateNewSegment should not return an error")
	assert.Equal(t, now, backend.currentSegment, "currentSegment should be set to provided timestamp")
	assert.Len(t, backend.segments, 1, "A segment should have been created")

	segment := backend.segments[now]
	assert.Equal(t, now, segment.info.Timestamp, "Segment Timestamp should match provided timestamp")
	assert.Equal(t, now, segment.info.EndTime, "Segment EndTime should initially match Timestamp")
	assert.Equal(t, int64(0), segment.info.MessageCount, "Segment MessageCount should be 0")
	assert.Equal(t, int64(0), segment.info.Size, "Segment Size should be 0")
	assert.Empty(t, segment.messages, "Segment messages should be empty")

	// Test creating segment with existing timestamp
	err = backend.CreateNewSegment(now)
	assert.Equal(t, ErrSegmentExists, err, "CreateNewSegment should return ErrSegmentExists for existing timestamp")

	// Test creating segment with earlier timestamp
	earlierTime := now.Add(-time.Hour)
	err = backend.CreateNewSegment(earlierTime)
	assert.Equal(t, ErrInvalidSegmentOrder, err, "CreateNewSegment should return ErrInvalidSegmentOrder for earlier timestamp")

	// Test creating segment with later timestamp
	laterTime := now.Add(time.Hour)
	err = backend.CreateNewSegment(laterTime)
	assert.NoError(t, err, "CreateNewSegment should not return an error for later timestamp")
	assert.Equal(t, laterTime, backend.currentSegment, "currentSegment should be updated to later timestamp")
	assert.Len(t, backend.segments, 2, "A new segment should have been created")

	laterSegment := backend.segments[laterTime]
	assert.Equal(t, laterTime, laterSegment.info.Timestamp, "New segment Timestamp should match provided timestamp")
	assert.Equal(t, laterTime, laterSegment.info.EndTime, "New segment EndTime should initially match Timestamp")
	assert.Equal(t, int64(0), laterSegment.info.MessageCount, "New segment MessageCount should be 0")
	assert.Equal(t, int64(0), laterSegment.info.Size, "New segment Size should be 0")
	assert.Empty(t, laterSegment.messages, "New segment messages should be empty")
}

// TestInMemoryBackend_Sync tests the Sync method
func TestInMemoryBackend_Sync(t *testing.T) {
	backend := NewInMemoryBackend()

	// Test with closed backend
	err := backend.Sync()
	assert.Equal(t, ErrStorageClosed, err, "Sync should return ErrStorageClosed when backend is closed")

	// Open the backend
	backend.Open()

	// Test sync on open backend (should be a no-op for in-memory backend)
	err = backend.Sync()
	assert.NoError(t, err, "Sync should not return an error")
}

// TestInMemoryBackend_Close tests the Close method
func TestInMemoryBackend_Close(t *testing.T) {
	backend := NewInMemoryBackend()
	backend.Open()

	// Test closing an open backend
	err := backend.Close()
	assert.NoError(t, err, "Close should not return an error")
	assert.True(t, backend.closed, "Backend should be closed after Close() call")

	// Test closing an already closed backend
	err = backend.Close()
	assert.NoError(t, err, "Closing an already closed backend should not return an error")
	assert.True(t, backend.closed, "Backend should remain closed after second Close() call")
}

// TestInMemoryBackend_FindSegmentForTime tests the findSegmentForTime method
func TestInMemoryBackend_FindSegmentForTime(t *testing.T) {
	backend := NewInMemoryBackend()

	// Test with closed backend
	now := time.Now().UTC().Truncate(time.Millisecond)
	segmentTime, err := backend.findSegmentForTime(now)
	assert.Equal(t, ErrStorageClosed, err, "findSegmentForTime should return ErrStorageClosed when backend is closed")
	assert.Equal(t, time.Time{}, segmentTime, "Returned segment time should be zero when backend is closed")

	// Open the backend
	backend.Open()

	// Test with invalid timestamp
	segmentTime, err = backend.findSegmentForTime(time.Time{})
	assert.Equal(t, ErrInvalidTimestamp, err, "findSegmentForTime should return ErrInvalidTimestamp for zero timestamp")
	assert.Equal(t, time.Time{}, segmentTime, "Returned segment time should be zero for invalid timestamp")

	// Test with no segments
	segmentTime, err = backend.findSegmentForTime(now)
	assert.Equal(t, ErrSegmentNotFound, err, "findSegmentForTime should return ErrSegmentNotFound when no segments exist")
	assert.Equal(t, time.Time{}, segmentTime, "Returned segment time should be zero when no segments exist")

	// Create segments with different time ranges
	segmentStart1 := now
	err = backend.CreateNewSegment(segmentStart1)
	assert.NoError(t, err, "CreateNewSegment should not return an error")

	// Add messages to extend the segment's time range
	msgs1 := []Message{
		{
			Timestamp: segmentStart1,
			Payload:   []byte("message 1-1"),
		},
		{
			Timestamp: segmentStart1.Add(30 * time.Minute),
			Payload:   []byte("message 1-2"),
		},
	}
	err = backend.AppendBatch(msgs1)
	assert.NoError(t, err, "AppendBatch should not return an error")

	segmentStart2 := now.Add(time.Hour)
	err = backend.CreateNewSegment(segmentStart2)
	assert.NoError(t, err, "CreateNewSegment should not return an error")

	// Add messages to extend the second segment's time range
	msgs2 := []Message{
		{
			Timestamp: segmentStart2,
			Payload:   []byte("message 2-1"),
		},
		{
			Timestamp: segmentStart2.Add(30 * time.Minute),
			Payload:   []byte("message 2-2"),
		},
	}
	err = backend.AppendBatch(msgs2)
	assert.NoError(t, err, "AppendBatch should not return an error")

	// Test finding segment by timestamp within first segment
	timeInFirstSegment := segmentStart1.Add(15 * time.Minute)
	segmentTime, err = backend.findSegmentForTime(timeInFirstSegment)
	assert.NoError(t, err, "findSegmentForTime should not return an error for time in first segment")
	assert.Equal(t, segmentStart1, segmentTime, "Should return first segment timestamp")

	// Test finding segment by timestamp within second segment
	timeInSecondSegment := segmentStart2.Add(15 * time.Minute)
	segmentTime, err = backend.findSegmentForTime(timeInSecondSegment)
	assert.NoError(t, err, "findSegmentForTime should not return an error for time in second segment")
	assert.Equal(t, segmentStart2, segmentTime, "Should return second segment timestamp")

	// Test with timestamp before any segment
	beforeAnySegment := segmentStart1.Add(-time.Hour)
	segmentTime, err = backend.findSegmentForTime(beforeAnySegment)
	assert.Equal(t, ErrSegmentNotFound, err, "findSegmentForTime should return ErrSegmentNotFound for time before any segment")

	// Test with timestamp after all segments
	afterAllSegments := segmentStart2.Add(time.Hour)
	segmentTime, err = backend.findSegmentForTime(afterAllSegments)
	assert.Equal(t, ErrSegmentNotFound, err, "findSegmentForTime should return ErrSegmentNotFound for time after all segments")

	// Test with timestamp in gap between segments
	inGap := segmentStart1.Add(45 * time.Minute)
	segmentTime, err = backend.findSegmentForTime(inGap)
	assert.Equal(t, ErrSegmentNotFound, err, "findSegmentForTime should return ErrSegmentNotFound for time in gap between segments")
}

// TestInMemoryBackend_InMemoryIterator tests the inMemoryIterator
func TestInMemoryBackend_InMemoryIterator(t *testing.T) {
	backend := NewInMemoryBackend()
	backend.Open()

	now := time.Now().UTC().Truncate(time.Millisecond)
	err := backend.CreateNewSegment(now)
	assert.NoError(t, err, "CreateNewSegment should not return an error")

	// Add messages to the segment
	msgs := []Message{
		{
			Timestamp: now,
			Payload:   []byte("message 1"),
		},
		{
			Timestamp: now.Add(time.Minute),
			Payload:   []byte("message 2"),
		},
		{
			Timestamp: now.Add(2 * time.Minute),
			Payload:   []byte("message 3"),
		},
	}
	err = backend.AppendBatch(msgs)
	assert.NoError(t, err, "AppendBatch should not return an error")

	// Get an iterator
	iterator, err := backend.Read(now, 0)
	assert.NoError(t, err, "Read should not return an error")

	// Test Next and Message methods
	count := 0
	for iterator.Next() {
		msg := iterator.Message()
		assert.Equal(t, msgs[count], msg, "Message should match expected message")
		count++
	}
	assert.Equal(t, len(msgs), count, "Iterator should return all messages")

	// Test that Next returns false after reaching the end
	assert.False(t, iterator.Next(), "Next should return false after reaching the end")

	// Test that Err returns nil for successful iteration
	assert.NoError(t, iterator.Err(), "Err should return nil for successful iteration")

	// Test Message after the end of iteration
	lastMsg := iterator.Message()
	assert.Equal(t, msgs[len(msgs)-1], lastMsg, "Message should return last message after iteration")

	// Test Close
	assert.NoError(t, iterator.Close(), "Close should not return an error")

	// Test iterating with offset
	iterator, err = backend.Read(now, 1)
	assert.NoError(t, err, "Read with offset should not return an error")

	// Should start from second message
	assert.True(t, iterator.Next(), "Next should return true")
	assert.Equal(t, msgs[1], iterator.Message(), "First message with offset should be second message")

	// Test calling Message before first Next
	iterator, err = backend.Read(now, 0)
	assert.NoError(t, err, "Read should not return an error")

	// Message should return empty message before first Next
	emptyMsg := iterator.Message()
	assert.Equal(t, Message{}, emptyMsg, "Message should return empty message before first Next")
}
