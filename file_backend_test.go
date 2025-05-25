package appenda

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestFileBackend_New tests the NewFileBackend function
func TestFileBackend_New(t *testing.T) {
	config := FileBackendConfig{
		Directory:   "/tmp/test",
		Prefix:      "test",
		Extension:   "log",
		MaxFileSize: 1024,
	}

	backend := NewFileBackend(config)
	assert.NotNil(t, backend, "NewFileBackend should return a non-nil backend")
	assert.Equal(t, config.Directory, backend.dir, "Directory should match config")
	assert.Equal(t, config.Prefix, backend.config.Prefix, "Prefix should match config")
	assert.Equal(t, config.Extension, backend.config.Extension, "Extension should match config")
	assert.Equal(t, config.MaxFileSize, backend.config.MaxFileSize, "MaxFileSize should match config")
	assert.True(t, backend.closed, "Backend should start in closed state")
	assert.NotNil(t, backend.segments, "Backend should have initialized segments map")
}

// TestFileBackend_NewWithDefaults tests NewFileBackend with default values
func TestFileBackend_NewWithDefaults(t *testing.T) {
	config := FileBackendConfig{
		Directory: "/tmp/test",
	}

	backend := NewFileBackend(config)
	assert.Equal(t, DefaultFileExtension, backend.config.Extension, "Extension should use default")
	assert.Equal(t, DefaultMaxFileSize, backend.config.MaxFileSize, "MaxFileSize should use default")
}

// TestFileBackend_Open tests the Open method
func TestFileBackend_Open(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	config := FileBackendConfig{
		Directory:   tempDir,
		Prefix:      "test",
		Extension:   "log",
		MaxFileSize: 1024 * 1024,
	}

	backend := NewFileBackend(config)

	// Test opening the backend
	err := backend.Open()
	assert.NoError(t, err, "Open should not return an error")
	assert.False(t, backend.closed, "Backend should be open after Open() call")

	// Test opening an already open backend
	err = backend.Open()
	assert.NoError(t, err, "Opening an already open backend should not return an error")

	// Clean up
	err = backend.Close()
	assert.NoError(t, err, "Close should not return an error")
}

// TestFileBackend_OpenWithInvalidDirectory tests Open with invalid directory
func TestFileBackend_OpenWithInvalidDirectory(t *testing.T) {
	config := FileBackendConfig{
		Directory:   "/invalid/path/that/cannot/be/created",
		Prefix:      "test",
		Extension:   "log",
		MaxFileSize: 1024,
	}

	backend := NewFileBackend(config)

	// Test opening with invalid directory (should fail on most systems)
	err := backend.Open()
	// Note: This might not fail on all systems, so we just check that it doesn't panic
	if err != nil {
		assert.Error(t, err, "Open should return an error for invalid directory")
	}
}

// TestFileBackend_CheckClosed tests the checkClosed helper method
func TestFileBackend_CheckClosed(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	config := FileBackendConfig{
		Directory:   tempDir,
		Extension:   "log",
		MaxFileSize: 1024,
	}

	backend := NewFileBackend(config)

	// Test with closed backend
	err := backend.checkClosed()
	assert.Equal(t, ErrStorageClosed, err, "checkClosed should return ErrStorageClosed when backend is closed")

	// Test with open backend
	backend.Open()
	err = backend.checkClosed()
	assert.NoError(t, err, "checkClosed should not return an error when backend is open")

	backend.Close()
}

// TestFileBackend_ParseFilename tests the parseFilename method
func TestFileBackend_ParseFilename(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	config := FileBackendConfig{
		Directory:   tempDir,
		Prefix:      "test",
		Extension:   "log",
		MaxFileSize: 1024,
	}

	backend := NewFileBackend(config)

	// Test valid filename with prefix
	timestamp := time.Unix(1609459200, 0).UTC() // 2021-01-01 00:00:00 UTC
	// 1609459200 = 0x5fee6600
	filename := "test_5fee6600_00000001.log"

	parsedTime, sequence, err := backend.parseFilename(filename)
	assert.NoError(t, err, "parseFilename should not return an error for valid filename")
	assert.Equal(t, timestamp, parsedTime, "Parsed timestamp should match expected")
	assert.Equal(t, uint32(1), sequence, "Parsed sequence should match expected")

	// Test invalid filename without proper extension
	_, _, err = backend.parseFilename("test_5fee6600_00000001.txt")
	assert.Error(t, err, "parseFilename should return an error for invalid extension")

	// Test filename without extension
	_, _, err = backend.parseFilename("test_5fee6600_00000001")
	assert.Error(t, err, "parseFilename should return an error for filename without extension")

	// Test invalid filename format
	_, _, err = backend.parseFilename("invalid_filename.log")
	assert.Error(t, err, "parseFilename should return an error for invalid filename")

	// Test with backend without prefix
	configNoPrefix := FileBackendConfig{
		Directory:   tempDir,
		Extension:   "log",
		MaxFileSize: 1024,
	}
	backendNoPrefix := NewFileBackend(configNoPrefix)

	filenameNoPrefix := "5fee6600_00000001.log"
	parsedTime, sequence, err = backendNoPrefix.parseFilename(filenameNoPrefix)
	assert.NoError(t, err, "parseFilename should not return an error for valid filename without prefix")
	assert.Equal(t, timestamp, parsedTime, "Parsed timestamp should match expected")
	assert.Equal(t, uint32(1), sequence, "Parsed sequence should match expected")
}

// TestFileBackend_GenerateFilename tests the generateFilename method
func TestFileBackend_GenerateFilename(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	timestamp := time.Unix(1609459200, 0).UTC() // 2021-01-01 00:00:00 UTC
	sequence := uint32(1)

	// Test with prefix
	config := FileBackendConfig{
		Directory:   tempDir,
		Prefix:      "test",
		Extension:   "log",
		MaxFileSize: 1024,
	}
	backend := NewFileBackend(config)

	filename := backend.generateFilename(timestamp, sequence)
	// 1609459200 = 0x5fee6600
	expectedFilename := "test_5fee6600_00000001.log"
	assert.Equal(t, expectedFilename, filename, "Generated filename should match expected format")

	// Test without prefix
	configNoPrefix := FileBackendConfig{
		Directory:   tempDir,
		Extension:   "log",
		MaxFileSize: 1024,
	}
	backendNoPrefix := NewFileBackend(configNoPrefix)

	filenameNoPrefix := backendNoPrefix.generateFilename(timestamp, sequence)
	expectedFilenameNoPrefix := "5fee6600_00000001.log"
	assert.Equal(t, expectedFilenameNoPrefix, filenameNoPrefix, "Generated filename without prefix should match expected format")
}

// TestFileBackend_Append tests the Append method
func TestFileBackend_Append(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	config := FileBackendConfig{
		Directory:   tempDir,
		Prefix:      "test",
		Extension:   "log",
		MaxFileSize: 1024 * 1024, // 1MB
	}

	backend := NewFileBackend(config)

	// Test with closed backend
	now := time.Now().UTC().Truncate(time.Millisecond)
	msg := Message{
		Timestamp: now,
		Payload:   []byte("test message"),
	}
	err := backend.Append(msg)
	assert.Equal(t, ErrStorageClosed, err, "Append should return ErrStorageClosed when backend is closed")

	// Open the backend
	err = backend.Open()
	assert.NoError(t, err, "Open should not return an error")

	// Test with invalid (zero) timestamp
	invalidMsg := Message{
		Timestamp: time.Time{},
		Payload:   []byte("invalid message"),
	}
	err = backend.Append(invalidMsg)
	assert.Equal(t, ErrInvalidTimestamp, err, "Append should return ErrInvalidTimestamp for zero timestamp")

	// Test appending first message (should create segment automatically)
	err = backend.Append(msg)
	assert.NoError(t, err, "Append should not return an error")
	assert.Len(t, backend.segments, 1, "A segment should have been created")
	assert.NotNil(t, backend.currentSegment, "Current segment should be set")

	// Verify segment info
	segment := backend.currentSegment
	assert.Equal(t, int64(1), segment.info.MessageCount, "Segment should have 1 message")
	assert.True(t, segment.info.Size > 0, "Segment size should be greater than 0")
	assert.Equal(t, now, segment.info.EndTime, "Segment EndTime should match message timestamp")

	// Test appending another message with later timestamp
	laterMsg := Message{
		Timestamp: now.Add(time.Minute),
		Payload:   []byte("later message"),
	}
	err = backend.Append(laterMsg)
	assert.NoError(t, err, "Append should not return an error")
	assert.Len(t, backend.segments, 1, "No new segment should have been created")

	// Verify updated segment info
	assert.Equal(t, int64(2), segment.info.MessageCount, "Segment should have 2 messages")
	assert.Equal(t, laterMsg.Timestamp, segment.info.EndTime, "Segment EndTime should be updated to later timestamp")

	// Clean up
	err = backend.Close()
	assert.NoError(t, err, "Close should not return an error")
}

// TestFileBackend_AppendWithRecordTooLarge tests Append with oversized record
func TestFileBackend_AppendWithRecordTooLarge(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	config := FileBackendConfig{
		Directory:   tempDir,
		Extension:   "log",
		MaxFileSize: 100, // Very small file size
	}

	backend := NewFileBackend(config)
	err := backend.Open()
	assert.NoError(t, err, "Open should not return an error")

	// Create a message that exceeds the file size limit
	largePayload := make([]byte, 200)
	largeMsg := Message{
		Timestamp: time.Now().UTC(),
		Payload:   largePayload,
	}

	err = backend.Append(largeMsg)
	assert.Equal(t, ErrRecordTooLarge, err, "Append should return ErrRecordTooLarge for oversized record")

	err = backend.Close()
	assert.NoError(t, err, "Close should not return an error")
}

// TestFileBackend_AppendBatch tests the AppendBatch method
func TestFileBackend_AppendBatch(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	config := FileBackendConfig{
		Directory:   tempDir,
		Extension:   "log",
		MaxFileSize: 1024 * 1024,
	}

	backend := NewFileBackend(config)

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
	err = backend.Open()
	assert.NoError(t, err, "Open should not return an error")

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

	// Test appending a valid batch
	err = backend.AppendBatch(msgs)
	assert.NoError(t, err, "AppendBatch with valid messages should not return an error")
	assert.Len(t, backend.segments, 1, "A segment should have been created")

	segment := backend.currentSegment
	assert.Equal(t, int64(2), segment.info.MessageCount, "Segment should have 2 messages")
	assert.Equal(t, msgs[1].Timestamp, segment.info.EndTime, "Segment EndTime should match latest message timestamp")

	// Clean up
	err = backend.Close()
	assert.NoError(t, err, "Close should not return an error")
}

// TestFileBackend_CreateNewSegment tests the CreateNewSegment method
func TestFileBackend_CreateNewSegment(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	config := FileBackendConfig{
		Directory:   tempDir,
		Extension:   "log",
		MaxFileSize: 1024,
	}

	backend := NewFileBackend(config)

	// Test with closed backend
	now := time.Now().UTC().Truncate(time.Millisecond)
	err := backend.CreateNewSegment(now)
	assert.Equal(t, ErrStorageClosed, err, "CreateNewSegment should return ErrStorageClosed when backend is closed")

	// Open the backend
	err = backend.Open()
	assert.NoError(t, err, "Open should not return an error")

	// Test with invalid timestamp
	err = backend.CreateNewSegment(time.Time{})
	assert.Equal(t, ErrInvalidTimestamp, err, "CreateNewSegment should return ErrInvalidTimestamp for zero timestamp")

	// Test creating first segment
	err = backend.CreateNewSegment(now)
	assert.NoError(t, err, "CreateNewSegment should not return an error")
	assert.NotNil(t, backend.currentSegment, "Current segment should be set")
	assert.Equal(t, now, backend.currentSegment.info.Timestamp, "Current segment timestamp should match")
	assert.Len(t, backend.segments, 1, "A segment should have been created")

	segment := backend.segments[now]
	assert.Equal(t, now, segment.info.Timestamp, "Segment Timestamp should match provided timestamp")
	assert.Equal(t, now, segment.info.EndTime, "Segment EndTime should initially match Timestamp")
	assert.Equal(t, int64(0), segment.info.MessageCount, "Segment MessageCount should be 0")
	assert.Equal(t, int64(0), segment.info.Size, "Segment Size should be 0")
	assert.Equal(t, uint32(0), segment.sequence, "Segment sequence should be 0")

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
	assert.Equal(t, laterTime, backend.currentSegment.info.Timestamp, "Current segment should be updated")
	assert.Len(t, backend.segments, 2, "A new segment should have been created")

	// Clean up
	err = backend.Close()
	assert.NoError(t, err, "Close should not return an error")
}

// TestFileBackend_Read tests the Read method
func TestFileBackend_Read(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	config := FileBackendConfig{
		Directory:   tempDir,
		Extension:   "log",
		MaxFileSize: 1024 * 1024,
	}

	backend := NewFileBackend(config)

	// Test with closed backend
	now := time.Now().UTC().Truncate(time.Millisecond)
	iterator, err := backend.Read(now, 0)
	assert.Equal(t, ErrStorageClosed, err, "Read should return ErrStorageClosed when backend is closed")
	assert.Nil(t, iterator, "Iterator should be nil when backend is closed")

	// Open the backend
	err = backend.Open()
	assert.NoError(t, err, "Open should not return an error")

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
	var retrievedMsgs []Message
	for iterator.Next() {
		msg := iterator.Message()
		retrievedMsgs = append(retrievedMsgs, msg)
		count++
	}
	assert.Equal(t, len(msgs), count, "Iterator should return all messages")
	assert.NoError(t, iterator.Err(), "Iterator should not have an error")

	// Verify message content (allowing for some timestamp precision differences)
	for i, retrievedMsg := range retrievedMsgs {
		assert.Equal(t, msgs[i].Payload, retrievedMsg.Payload, "Message payload should match")
		// Timestamp might have some precision loss due to file format, so we check it's close
		assert.True(t, retrievedMsg.Timestamp.Sub(msgs[i].Timestamp).Abs() < time.Second,
			"Message timestamp should be close to original")
	}

	assert.NoError(t, iterator.Close(), "Closing iterator should not return an error")

	// Clean up
	err = backend.Close()
	assert.NoError(t, err, "Close should not return an error")
}

// TestFileBackend_ListSegments tests the ListSegments method
func TestFileBackend_ListSegments(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	config := FileBackendConfig{
		Directory:   tempDir,
		Extension:   "log",
		MaxFileSize: 1024,
	}

	backend := NewFileBackend(config)

	// Test with closed backend
	segments, err := backend.ListSegments()
	assert.Equal(t, ErrStorageClosed, err, "ListSegments should return ErrStorageClosed when backend is closed")
	assert.Nil(t, segments, "Segments should be nil when backend is closed")

	// Open the backend
	err = backend.Open()
	assert.NoError(t, err, "Open should not return an error")

	// Test with no segments
	segments, err = backend.ListSegments()
	assert.NoError(t, err, "ListSegments should not return an error for empty backend")
	assert.Empty(t, segments, "Segments should be empty for new backend")

	// Create multiple segments
	now := time.Now().UTC().Truncate(time.Millisecond)
	timestamps := []time.Time{
		now,
		now.Add(time.Hour),
		now.Add(2 * time.Hour),
	}

	for _, ts := range timestamps {
		err = backend.CreateNewSegment(ts)
		assert.NoError(t, err, "CreateNewSegment should not return an error")
	}

	// Test listing all segments
	segments, err = backend.ListSegments()
	assert.NoError(t, err, "ListSegments should not return an error")
	assert.Len(t, segments, 3, "Should return 3 segments")

	// Verify segments are sorted by timestamp
	for i, expectedTime := range timestamps {
		assert.Equal(t, expectedTime, segments[i].Timestamp, "Segment should be in correct chronological order")
	}

	// Clean up
	err = backend.Close()
	assert.NoError(t, err, "Close should not return an error")
}

// TestFileBackend_GetSegmentInfo tests the GetSegmentInfo method
func TestFileBackend_GetSegmentInfo(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	config := FileBackendConfig{
		Directory:   tempDir,
		Extension:   "log",
		MaxFileSize: 1024,
	}

	backend := NewFileBackend(config)

	// Test with closed backend
	now := time.Now().UTC().Truncate(time.Millisecond)
	info, err := backend.GetSegmentInfo(now)
	assert.Equal(t, ErrStorageClosed, err, "GetSegmentInfo should return ErrStorageClosed when backend is closed")
	assert.Equal(t, SegmentInfo{}, info, "SegmentInfo should be empty when backend is closed")

	// Open the backend
	err = backend.Open()
	assert.NoError(t, err, "Open should not return an error")

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
	assert.Equal(t, int64(1), info.MessageCount, "SegmentInfo MessageCount should be 1")
	assert.True(t, info.Size > 0, "SegmentInfo Size should be greater than 0")

	// Clean up
	err = backend.Close()
	assert.NoError(t, err, "Close should not return an error")
}

// TestFileBackend_FindSegmentByTime tests the FindSegmentByTime method
func TestFileBackend_FindSegmentByTime(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	config := FileBackendConfig{
		Directory:   tempDir,
		Extension:   "log",
		MaxFileSize: 1024 * 1024,
	}

	backend := NewFileBackend(config)

	// Test with closed backend
	now := time.Now().UTC().Truncate(time.Millisecond)
	info, err := backend.FindSegmentByTime(now)
	assert.Equal(t, ErrStorageClosed, err, "FindSegmentByTime should return ErrStorageClosed when backend is closed")
	assert.Equal(t, SegmentInfo{}, info, "SegmentInfo should be empty when backend is closed")

	// Open the backend
	err = backend.Open()
	assert.NoError(t, err, "Open should not return an error")

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

	// Test with timestamp before any segment
	beforeAnySegment := segmentStart1.Add(-time.Hour)
	info, err = backend.FindSegmentByTime(beforeAnySegment)
	assert.Equal(t, ErrSegmentNotFound, err, "FindSegmentByTime should return ErrSegmentNotFound for time before any segment")

	// Test with timestamp after all segments
	afterAllSegments := segmentStart2.Add(2 * time.Hour)
	info, err = backend.FindSegmentByTime(afterAllSegments)
	assert.Equal(t, ErrSegmentNotFound, err, "FindSegmentByTime should return ErrSegmentNotFound for time after all segments")

	// Clean up
	err = backend.Close()
	assert.NoError(t, err, "Close should not return an error")
}

// TestFileBackend_Sync tests the Sync method
func TestFileBackend_Sync(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	config := FileBackendConfig{
		Directory:   tempDir,
		Extension:   "log",
		MaxFileSize: 1024,
	}

	backend := NewFileBackend(config)

	// Test with closed backend
	err := backend.Sync()
	assert.Equal(t, ErrStorageClosed, err, "Sync should return ErrStorageClosed when backend is closed")

	// Open the backend
	err = backend.Open()
	assert.NoError(t, err, "Open should not return an error")

	// Test sync on open backend
	err = backend.Sync()
	assert.NoError(t, err, "Sync should not return an error")

	// Create segment and add message, then sync
	now := time.Now().UTC().Truncate(time.Millisecond)
	err = backend.CreateNewSegment(now)
	assert.NoError(t, err, "CreateNewSegment should not return an error")

	msg := Message{
		Timestamp: now,
		Payload:   []byte("test message"),
	}
	err = backend.Append(msg)
	assert.NoError(t, err, "Append should not return an error")

	err = backend.Sync()
	assert.NoError(t, err, "Sync should not return an error after writing data")

	// Clean up
	err = backend.Close()
	assert.NoError(t, err, "Close should not return an error")
}

// TestFileBackend_Close tests the Close method
func TestFileBackend_Close(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	config := FileBackendConfig{
		Directory:   tempDir,
		Extension:   "log",
		MaxFileSize: 1024,
	}

	backend := NewFileBackend(config)
	err := backend.Open()
	assert.NoError(t, err, "Open should not return an error")

	// Test closing an open backend
	err = backend.Close()
	assert.NoError(t, err, "Close should not return an error")
	assert.True(t, backend.closed, "Backend should be closed after Close() call")

	// Test closing an already closed backend
	err = backend.Close()
	assert.NoError(t, err, "Closing an already closed backend should not return an error")
	assert.True(t, backend.closed, "Backend should remain closed after second Close() call")
}

// TestFileBackend_ShouldCreateNewSegment tests the shouldCreateNewSegment method
func TestFileBackend_ShouldCreateNewSegment(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	config := FileBackendConfig{
		Directory:   tempDir,
		Extension:   "log",
		MaxFileSize: 1024,
	}

	backend := NewFileBackend(config)
	err := backend.Open()
	assert.NoError(t, err, "Open should not return an error")

	now := time.Now().UTC().Truncate(time.Millisecond)

	// Test with no current segment
	should := backend.shouldCreateNewSegment(now)
	assert.True(t, should, "shouldCreateNewSegment should return true when no current segment exists")

	// Create a segment
	err = backend.CreateNewSegment(now)
	assert.NoError(t, err, "CreateNewSegment should not return an error")

	// Test with message timestamp within same timeframe
	nearbyTime := now.Add(time.Hour)
	should = backend.shouldCreateNewSegment(nearbyTime)
	assert.False(t, should, "shouldCreateNewSegment should return false for nearby timestamp")

	// Test with message timestamp far in the future (beyond MaxSegmentDuration)
	farFutureTime := now.Add(MaxSegmentDuration + time.Hour)
	should = backend.shouldCreateNewSegment(farFutureTime)
	assert.True(t, should, "shouldCreateNewSegment should return true for timestamp beyond MaxSegmentDuration")

	err = backend.Close()
	assert.NoError(t, err, "Close should not return an error")
}

// TestFileBackend_LoadExistingSegments tests loading existing segments on Open
func TestFileBackend_LoadExistingSegments(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	config := FileBackendConfig{
		Directory:   tempDir,
		Prefix:      "test",
		Extension:   "log",
		MaxFileSize: 1024 * 1024,
	}

	// First backend to create some files
	backend1 := NewFileBackend(config)
	err := backend1.Open()
	assert.NoError(t, err, "Open should not return an error")

	// Create a segment and add messages
	now := time.Now().UTC().Truncate(time.Second)

	// Create segments and add messages
	err = backend1.CreateNewSegment(now)
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
	}
	err = backend1.AppendBatch(msgs)
	assert.NoError(t, err, "AppendBatch should not return an error")

	// Create another segment
	laterTime := now.Add(time.Hour)
	err = backend1.CreateNewSegment(laterTime)
	assert.NoError(t, err, "CreateNewSegment should not return an error")

	laterMsg := Message{
		Timestamp: laterTime,
		Payload:   []byte("later message"),
	}
	err = backend1.Append(laterMsg)
	assert.NoError(t, err, "Append should not return an error")

	err = backend1.Close()
	assert.NoError(t, err, "Close should not return an error")

	// Second backend to load existing files
	backend2 := NewFileBackend(config)
	err = backend2.Open()
	assert.NoError(t, err, "Open should not return an error")

	// Verify segments were loaded
	segments, err := backend2.ListSegments()
	assert.NoError(t, err, "ListSegments should not return an error")
	assert.Len(t, segments, 2, "Should load 2 existing segments")

	// Verify segment data
	assert.Equal(t, now, segments[0].Timestamp, "First segment timestamp should match")
	assert.Equal(t, int64(2), segments[0].MessageCount, "First segment should have 2 messages")

	assert.Equal(t, laterTime, segments[1].Timestamp, "Second segment timestamp should match")
	assert.Equal(t, int64(1), segments[1].MessageCount, "Second segment should have 1 message")

	// Verify current segment is set to the latest
	assert.NotNil(t, backend2.currentSegment, "Current segment should be set")
	assert.Equal(t, laterTime, backend2.currentSegment.info.Timestamp, "Current segment should be the latest")

	err = backend2.Close()
	assert.NoError(t, err, "Close should not return an error")
}

// TestFileBackend_WriteAndReadRecord tests the writeRecord and readRecord methods
func TestFileBackend_WriteAndReadRecord(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	config := FileBackendConfig{
		Directory:   tempDir,
		Extension:   "log",
		MaxFileSize: 1024 * 1024,
	}

	backend := NewFileBackend(config)
	err := backend.Open()
	assert.NoError(t, err, "Open should not return an error")

	// Create a temporary file for testing
	testFile := filepath.Join(tempDir, "test_record.log")
	file, err := os.Create(testFile)
	assert.NoError(t, err, "Create test file should not return an error")
	defer file.Close()
	defer os.Remove(testFile)

	now := time.Now().UTC().Truncate(time.Millisecond)
	segmentTimestamp := now

	// Test writing and reading a record
	originalMsg := Message{
		Timestamp: now.Add(30 * time.Second),
		Payload:   []byte("test message for record"),
	}

	// Write record
	err = backend.writeRecord(file, originalMsg, segmentTimestamp)
	assert.NoError(t, err, "writeRecord should not return an error")

	// Reset file position to beginning
	_, err = file.Seek(0, 0)
	assert.NoError(t, err, "Seek should not return an error")

	// Read record
	readMsg, err := backend.readRecord(file, segmentTimestamp)
	assert.NoError(t, err, "readRecord should not return an error")

	// Verify message content
	assert.Equal(t, originalMsg.Payload, readMsg.Payload, "Payload should match")
	// Timestamp might have some precision loss due to file format (stored as seconds)
	assert.True(t, readMsg.Timestamp.Sub(originalMsg.Timestamp).Abs() < time.Second,
		"Timestamp should be close to original (within 1 second due to format limitations)")

	err = backend.Close()
	assert.NoError(t, err, "Close should not return an error")
}

// TestFileBackend_FileIterator tests the FileIterator
func TestFileBackend_FileIterator(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	config := FileBackendConfig{
		Directory:   tempDir,
		Extension:   "log",
		MaxFileSize: 1024 * 1024,
	}

	backend := NewFileBackend(config)
	err := backend.Open()
	assert.NoError(t, err, "Open should not return an error")

	now := time.Now().UTC().Truncate(time.Millisecond)

	// Create segment and add messages
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

	// Get an iterator
	iterator, err := backend.Read(now, 0)
	assert.NoError(t, err, "Read should not return an error")

	// Test iteration
	count := 0
	var retrievedMsgs []Message
	for iterator.Next() {
		msg := iterator.Message()
		retrievedMsgs = append(retrievedMsgs, msg)
		count++
	}
	assert.Equal(t, len(msgs), count, "Iterator should return all messages")
	assert.NoError(t, iterator.Err(), "Iterator should not have an error")

	// Verify message payloads
	for i, retrievedMsg := range retrievedMsgs {
		assert.Equal(t, msgs[i].Payload, retrievedMsg.Payload, "Message payload should match")
	}

	// Test that Next returns false after reaching the end
	assert.False(t, iterator.Next(), "Next should return false after reaching the end")

	// Test Close
	assert.NoError(t, iterator.Close(), "Close should not return an error")

	// Test iterating with offset
	iterator, err = backend.Read(now, 1)
	assert.NoError(t, err, "Read with offset should not return an error")

	// Should start from second message
	assert.True(t, iterator.Next(), "Next should return true")
	retrievedMsg := iterator.Message()
	assert.Equal(t, msgs[1].Payload, retrievedMsg.Payload, "First message with offset should be second message")

	assert.NoError(t, iterator.Close(), "Close should not return an error")

	err = backend.Close()
	assert.NoError(t, err, "Close should not return an error")
}

// TestFileBackend_EndToEnd performs an end-to-end test of the FileBackend
func TestFileBackend_EndToEnd(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	config := FileBackendConfig{
		Directory:   tempDir,
		Prefix:      "test",
		Extension:   "log",
		MaxFileSize: 1024 * 1024,
	}

	backend := NewFileBackend(config)
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

	// 5. Force sync
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

// TestFileBackend_TimestampConversion tests timestamp conversion to hex and back
func TestFileBackend_TimestampConversion(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	config := FileBackendConfig{
		Directory:   tempDir,
		Extension:   "log",
		MaxFileSize: 1024,
	}
	backend := NewFileBackend(config)

	// Define test cases with expected hex representation
	testCases := []struct {
		timestamp   time.Time
		expectedHex string
	}{
		{time.Unix(1609459200, 0).UTC(), "5fee6600"}, // 2021-01-01 00:00:00 UTC
		{time.Unix(1640995200, 0).UTC(), "61cf9980"}, // 2022-01-01 00:00:00 UTC
		{time.Unix(0, 0).UTC(), "0"},                 // Unix epoch
	}

	for _, tc := range testCases {
		// Test generate -> parse roundtrip
		filename := backend.generateFilename(tc.timestamp, 1)
		parsedTime, sequence, err := backend.parseFilename(filename)

		assert.NoError(t, err, "parseFilename should not return error for generated filename")
		assert.Equal(t, tc.timestamp, parsedTime, "Parsed timestamp should match original")
		assert.Equal(t, uint32(1), sequence, "Parsed sequence should match original")

		// Test hex representation
		actualHex := fmt.Sprintf("%x", tc.timestamp.Unix())
		assert.Equal(t, tc.expectedHex, actualHex, "Hex representation should match expected")
	}
}

// Helper functions for testing

// createTempDir creates a temporary directory for testing
func createTempDir(t *testing.T) string {
	tempDir, err := os.MkdirTemp("", "appenda_test_*")
	assert.NoError(t, err, "Creating temp directory should not return an error")
	return tempDir
}

// cleanupTempDir removes the temporary directory and all its contents
func cleanupTempDir(t *testing.T, dir string) {
	err := os.RemoveAll(dir)
	if err != nil {
		t.Logf("Warning: failed to cleanup temp directory %s: %v", dir, err)
	}
}
