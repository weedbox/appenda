package appenda

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// BenchmarkFileBackend_Append benchmarks single message append operations
func BenchmarkFileBackend_Append(b *testing.B) {
	tempDir := createBenchmarkTempDir(b)
	defer cleanupBenchmarkTempDir(b, tempDir)

	config := FileBackendConfig{
		Directory:   tempDir,
		Prefix:      "bench",
		Extension:   "log",
		MaxFileSize: 100 * 1024 * 1024, // 100MB
	}

	backend := NewFileBackend(config)
	err := backend.Open()
	assert.NoError(b, err, "Open should not return an error")
	defer backend.Close()

	// Create initial segment
	now := time.Now().UTC().Truncate(time.Millisecond)
	err = backend.CreateNewSegment(now)
	assert.NoError(b, err, "CreateNewSegment should not return an error")

	// Prepare test message
	payload := make([]byte, 256) // 256 bytes payload
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		msg := Message{
			Timestamp: now.Add(time.Duration(i) * time.Nanosecond),
			Payload:   payload,
		}
		err := backend.Append(msg)
		if err != nil {
			b.Fatalf("Append failed: %v", err)
		}
	}
}

// BenchmarkFileBackend_AppendBatch benchmarks batch append operations
func BenchmarkFileBackend_AppendBatch(b *testing.B) {
	tempDir := createBenchmarkTempDir(b)
	defer cleanupBenchmarkTempDir(b, tempDir)

	config := FileBackendConfig{
		Directory:   tempDir,
		Prefix:      "bench",
		Extension:   "log",
		MaxFileSize: 100 * 1024 * 1024, // 100MB
	}

	backend := NewFileBackend(config)
	err := backend.Open()
	assert.NoError(b, err, "Open should not return an error")
	defer backend.Close()

	// Create initial segment
	now := time.Now().UTC().Truncate(time.Millisecond)
	err = backend.CreateNewSegment(now)
	assert.NoError(b, err, "CreateNewSegment should not return an error")

	// Prepare test payload
	payload := make([]byte, 256) // 256 bytes payload
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	// Test different batch sizes
	batchSizes := []int{10, 100, 1000}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize-%d", batchSize), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Prepare batch
				msgs := make([]Message, batchSize)
				for j := 0; j < batchSize; j++ {
					msgs[j] = Message{
						Timestamp: now.Add(time.Duration(i*batchSize+j) * time.Nanosecond),
						Payload:   payload,
					}
				}

				err := backend.AppendBatch(msgs)
				if err != nil {
					b.Fatalf("AppendBatch failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkFileBackend_Read benchmarks read operations
func BenchmarkFileBackend_Read(b *testing.B) {
	tempDir := createBenchmarkTempDir(b)
	defer cleanupBenchmarkTempDir(b, tempDir)

	config := FileBackendConfig{
		Directory:   tempDir,
		Prefix:      "bench",
		Extension:   "log",
		MaxFileSize: 100 * 1024 * 1024, // 100MB
	}

	backend := NewFileBackend(config)
	err := backend.Open()
	assert.NoError(b, err, "Open should not return an error")
	defer backend.Close()

	// Setup: Create segment and populate with data
	now := time.Now().UTC().Truncate(time.Millisecond)
	err = backend.CreateNewSegment(now)
	assert.NoError(b, err, "CreateNewSegment should not return an error")

	// Prepare test data
	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	numMessages := 10000
	for i := 0; i < numMessages; i++ {
		msg := Message{
			Timestamp: now.Add(time.Duration(i) * time.Nanosecond),
			Payload:   payload,
		}
		err := backend.Append(msg)
		assert.NoError(b, err, "Setup Append should not return an error")
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		iterator, err := backend.Read(now, 0)
		if err != nil {
			b.Fatalf("Read failed: %v", err)
		}

		count := 0
		for iterator.Next() {
			_ = iterator.Message()
			count++
		}

		if iterator.Err() != nil {
			b.Fatalf("Iterator error: %v", iterator.Err())
		}

		iterator.Close()

		if count != numMessages {
			b.Fatalf("Expected %d messages, got %d", numMessages, count)
		}
	}
}

// BenchmarkFileBackend_IterateWithOffset benchmarks read operations with different offsets
func BenchmarkFileBackend_IterateWithOffset(b *testing.B) {
	tempDir := createBenchmarkTempDir(b)
	defer cleanupBenchmarkTempDir(b, tempDir)

	config := FileBackendConfig{
		Directory:   tempDir,
		Prefix:      "bench",
		Extension:   "log",
		MaxFileSize: 100 * 1024 * 1024, // 100MB
	}

	backend := NewFileBackend(config)
	err := backend.Open()
	assert.NoError(b, err, "Open should not return an error")
	defer backend.Close()

	// Setup: Create segment and populate with data
	now := time.Now().UTC().Truncate(time.Millisecond)
	err = backend.CreateNewSegment(now)
	assert.NoError(b, err, "CreateNewSegment should not return an error")

	// Prepare test data
	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	numMessages := 10000
	for i := 0; i < numMessages; i++ {
		msg := Message{
			Timestamp: now.Add(time.Duration(i) * time.Nanosecond),
			Payload:   payload,
		}
		err := backend.Append(msg)
		assert.NoError(b, err, "Setup Append should not return an error")
	}

	// Test different offsets
	offsets := []int64{0, 100, 1000, 5000, 9000}

	for _, offset := range offsets {
		b.Run(fmt.Sprintf("Offset-%d", offset), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				iterator, err := backend.Read(now, offset)
				if err != nil {
					b.Fatalf("Read failed: %v", err)
				}

				count := 0
				for iterator.Next() {
					_ = iterator.Message()
					count++
				}

				if iterator.Err() != nil {
					b.Fatalf("Iterator error: %v", iterator.Err())
				}

				iterator.Close()

				expectedCount := numMessages - int(offset)
				if expectedCount < 0 {
					expectedCount = 0
				}
				if count != expectedCount {
					b.Fatalf("Expected %d messages, got %d", expectedCount, count)
				}
			}
		})
	}
}

// BenchmarkAppenda_Append benchmarks the high-level Appenda interface append operations
func BenchmarkAppenda_Append(b *testing.B) {
	tempDir := createBenchmarkTempDir(b)
	defer cleanupBenchmarkTempDir(b, tempDir)

	config := FileBackendConfig{
		Directory:   tempDir,
		Prefix:      "bench",
		Extension:   "log",
		MaxFileSize: 100 * 1024 * 1024, // 100MB
	}

	backend := NewFileBackend(config)
	appenda, err := NewAppenda(backend)
	assert.NoError(b, err, "NewAppenda should not return an error")
	defer appenda.Close()

	// Prepare test message
	payload := make([]byte, 256) // 256 bytes payload
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	now := time.Now().UTC().Truncate(time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		msg := Message{
			Timestamp: now.Add(time.Duration(i) * time.Nanosecond),
			Payload:   payload,
		}
		err := appenda.Append(msg)
		if err != nil {
			b.Fatalf("Append failed: %v", err)
		}
	}
}

// BenchmarkAppenda_AppendBatch benchmarks the high-level Appenda interface batch operations
func BenchmarkAppenda_AppendBatch(b *testing.B) {
	tempDir := createBenchmarkTempDir(b)
	defer cleanupBenchmarkTempDir(b, tempDir)

	config := FileBackendConfig{
		Directory:   tempDir,
		Prefix:      "bench",
		Extension:   "log",
		MaxFileSize: 100 * 1024 * 1024, // 100MB
	}

	backend := NewFileBackend(config)
	appenda, err := NewAppenda(backend)
	assert.NoError(b, err, "NewAppenda should not return an error")
	defer appenda.Close()

	// Prepare test payload
	payload := make([]byte, 256) // 256 bytes payload
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	now := time.Now().UTC().Truncate(time.Millisecond)

	// Test different batch sizes
	batchSizes := []int{10, 100, 1000}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize-%d", batchSize), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Prepare batch
				msgs := make([]Message, batchSize)
				for j := 0; j < batchSize; j++ {
					msgs[j] = Message{
						Timestamp: now.Add(time.Duration(i*batchSize+j) * time.Nanosecond),
						Payload:   payload,
					}
				}

				err := appenda.AppendBatch(msgs)
				if err != nil {
					b.Fatalf("AppendBatch failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkAppenda_Iterate benchmarks the high-level Appenda interface iteration
func BenchmarkAppenda_Iterate(b *testing.B) {
	tempDir := createBenchmarkTempDir(b)
	defer cleanupBenchmarkTempDir(b, tempDir)

	config := FileBackendConfig{
		Directory:   tempDir,
		Prefix:      "bench",
		Extension:   "log",
		MaxFileSize: 100 * 1024 * 1024, // 100MB
	}

	backend := NewFileBackend(config)
	appenda, err := NewAppenda(backend)
	assert.NoError(b, err, "NewAppenda should not return an error")
	defer appenda.Close()

	// Setup: Populate with data
	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	now := time.Now().UTC().Truncate(time.Millisecond)
	numMessages := 10000

	for i := 0; i < numMessages; i++ {
		msg := Message{
			Timestamp: now.Add(time.Duration(i) * time.Nanosecond),
			Payload:   payload,
		}
		err := appenda.Append(msg)
		assert.NoError(b, err, "Setup Append should not return an error")
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		iterator, err := appenda.Iterate()
		if err != nil {
			b.Fatalf("Iterate failed: %v", err)
		}

		count := 0
		for iterator.Next() {
			_ = iterator.Message()
			count++
		}

		if iterator.Err() != nil {
			b.Fatalf("Iterator error: %v", iterator.Err())
		}

		iterator.Close()

		if count != numMessages {
			b.Fatalf("Expected %d messages, got %d", numMessages, count)
		}
	}
}

// BenchmarkAppenda_IterateFrom benchmarks the high-level Appenda interface iteration from timestamp
func BenchmarkAppenda_IterateFrom(b *testing.B) {
	tempDir := createBenchmarkTempDir(b)
	defer cleanupBenchmarkTempDir(b, tempDir)

	config := FileBackendConfig{
		Directory:   tempDir,
		Prefix:      "bench",
		Extension:   "log",
		MaxFileSize: 100 * 1024 * 1024, // 100MB
	}

	backend := NewFileBackend(config)
	appenda, err := NewAppenda(backend)
	assert.NoError(b, err, "NewAppenda should not return an error")
	defer appenda.Close()

	// Setup: Populate with data
	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	now := time.Now().UTC().Truncate(time.Millisecond)
	numMessages := 10000

	for i := 0; i < numMessages; i++ {
		msg := Message{
			Timestamp: now.Add(time.Duration(i) * time.Minute),
			Payload:   payload,
		}
		err := appenda.Append(msg)
		assert.NoError(b, err, "Setup Append should not return an error")
	}

	// Test iteration from different starting points
	startOffsets := []int{0, 1000, 5000, 9000}

	for _, startOffset := range startOffsets {
		startTime := now.Add(time.Duration(startOffset) * time.Minute)
		b.Run(fmt.Sprintf("StartOffset-%d", startOffset), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				iterator, err := appenda.IterateFrom(startTime, 0)
				if err != nil {
					b.Fatalf("IterateFrom failed: %v", err)
				}

				count := 0
				for iterator.Next() {
					_ = iterator.Message()
					count++
				}

				if iterator.Err() != nil {
					b.Fatalf("Iterator error: %v", iterator.Err())
				}

				iterator.Close()

				expectedCount := numMessages - startOffset
				if expectedCount < 0 {
					expectedCount = 0
				}
				if count != expectedCount {
					b.Fatalf("Expected %d messages, got %d", expectedCount, count)
				}
			}
		})
	}
}

// BenchmarkFileBackend_PayloadSizes benchmarks append operations with different payload sizes
func BenchmarkFileBackend_PayloadSizes(b *testing.B) {
	tempDir := createBenchmarkTempDir(b)
	defer cleanupBenchmarkTempDir(b, tempDir)

	config := FileBackendConfig{
		Directory:   tempDir,
		Prefix:      "bench",
		Extension:   "log",
		MaxFileSize: 100 * 1024 * 1024, // 100MB
	}

	// Test different payload sizes
	payloadSizes := []int{64, 256, 1024, 4096, 16384} // 64B to 16KB

	for _, payloadSize := range payloadSizes {
		b.Run(fmt.Sprintf("PayloadSize-%dB", payloadSize), func(b *testing.B) {
			backend := NewFileBackend(config)
			err := backend.Open()
			assert.NoError(b, err, "Open should not return an error")
			defer backend.Close()

			// Create initial segment
			now := time.Now().UTC().Truncate(time.Millisecond)
			err = backend.CreateNewSegment(now)
			assert.NoError(b, err, "CreateNewSegment should not return an error")

			// Prepare test payload
			payload := make([]byte, payloadSize)
			for i := range payload {
				payload[i] = byte(i % 256)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				msg := Message{
					Timestamp: now.Add(time.Duration(i) * time.Nanosecond),
					Payload:   payload,
				}
				err := backend.Append(msg)
				if err != nil {
					b.Fatalf("Append failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkFileBackend_MultipleSegments benchmarks operations across multiple segments
func BenchmarkFileBackend_MultipleSegments(b *testing.B) {
	tempDir := createBenchmarkTempDir(b)
	defer cleanupBenchmarkTempDir(b, tempDir)

	config := FileBackendConfig{
		Directory:   tempDir,
		Prefix:      "bench",
		Extension:   "log",
		MaxFileSize: 100 * 1024 * 1024, // 100MB per file to avoid automatic rotation
	}

	backend := NewFileBackend(config)
	err := backend.Open()
	assert.NoError(b, err, "Open should not return an error")
	defer backend.Close()

	// Setup: Create multiple segments with data
	now := time.Now().UTC().Truncate(time.Millisecond)
	payload := make([]byte, 256) // 256B payload
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	// Create 5 segments, each with some data
	numSegments := 5
	messagesPerSegment := 500
	segmentTimestamps := make([]time.Time, numSegments)

	for segIdx := 0; segIdx < numSegments; segIdx++ {
		// Use larger time gaps between segments to ensure they don't overlap
		segmentTime := now.Add(time.Duration(segIdx) * 24 * time.Hour) // 24 hours apart
		segmentTimestamps[segIdx] = segmentTime

		err = backend.CreateNewSegment(segmentTime)
		assert.NoError(b, err, "CreateNewSegment should not return an error")

		// Add messages to this segment with timestamps within the segment's expected range
		for msgIdx := 0; msgIdx < messagesPerSegment; msgIdx++ {
			msg := Message{
				Timestamp: segmentTime.Add(time.Duration(msgIdx) * time.Minute),
				Payload:   payload,
			}
			err := backend.Append(msg)
			assert.NoError(b, err, "Setup Append should not return an error")
		}

		// Verify segment was populated correctly
		segmentInfo, err := backend.GetSegmentInfo(segmentTime)
		assert.NoError(b, err, "GetSegmentInfo should not return an error")
		if segmentInfo.MessageCount != int64(messagesPerSegment) {
			b.Fatalf("Segment %d: expected %d messages, got %d", segIdx, messagesPerSegment, segmentInfo.MessageCount)
		}
	}

	b.Run("ReadAllSegments", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			totalCount := 0

			// Read all segments
			for segIdx, segmentTime := range segmentTimestamps {
				iterator, err := backend.Read(segmentTime, 0)
				if err != nil {
					b.Fatalf("Read segment %d failed: %v", segIdx, err)
				}

				count := 0
				for iterator.Next() {
					_ = iterator.Message()
					count++
				}

				if iterator.Err() != nil {
					b.Fatalf("Iterator error for segment %d: %v", segIdx, iterator.Err())
				}

				iterator.Close()

				// Verify each segment has the expected number of messages
				if count != messagesPerSegment {
					b.Fatalf("Segment %d: expected %d messages, got %d", segIdx, messagesPerSegment, count)
				}

				totalCount += count
			}

			expectedTotal := numSegments * messagesPerSegment
			if totalCount != expectedTotal {
				b.Fatalf("Expected %d total messages, got %d", expectedTotal, totalCount)
			}
		}
	})

	b.Run("ListSegments", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			segments, err := backend.ListSegments()
			if err != nil {
				b.Fatalf("ListSegments failed: %v", err)
			}

			if len(segments) != numSegments {
				b.Fatalf("Expected %d segments, got %d", numSegments, len(segments))
			}
		}
	})

	b.Run("FindSegmentByTime", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Find segment for a time in the middle of the third segment
			searchTime := segmentTimestamps[2].Add(15 * time.Minute)
			_, err := backend.FindSegmentByTime(searchTime)
			if err != nil {
				b.Fatalf("FindSegmentByTime failed: %v", err)
			}
		}
	})
}

// BenchmarkFileBackend_Sync benchmarks sync operations
func BenchmarkFileBackend_Sync(b *testing.B) {
	tempDir := createBenchmarkTempDir(b)
	defer cleanupBenchmarkTempDir(b, tempDir)

	config := FileBackendConfig{
		Directory:   tempDir,
		Prefix:      "bench",
		Extension:   "log",
		MaxFileSize: 100 * 1024 * 1024, // 100MB
	}

	backend := NewFileBackend(config)
	err := backend.Open()
	assert.NoError(b, err, "Open should not return an error")
	defer backend.Close()

	// Setup: Create segment and add some data
	now := time.Now().UTC().Truncate(time.Millisecond)
	err = backend.CreateNewSegment(now)
	assert.NoError(b, err, "CreateNewSegment should not return an error")

	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	// Add some messages before benchmarking sync
	for i := 0; i < 1000; i++ {
		msg := Message{
			Timestamp: now.Add(time.Duration(i) * time.Nanosecond),
			Payload:   payload,
		}
		err := backend.Append(msg)
		assert.NoError(b, err, "Setup Append should not return an error")
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := backend.Sync()
		if err != nil {
			b.Fatalf("Sync failed: %v", err)
		}
	}
}

// Helper functions for benchmarking

// createBenchmarkTempDir creates a temporary directory for benchmarking
func createBenchmarkTempDir(b *testing.B) string {
	tempDir, err := os.MkdirTemp("", "appenda_bench_*")
	if err != nil {
		b.Fatalf("Creating temp directory failed: %v", err)
	}
	return tempDir
}

// cleanupBenchmarkTempDir removes the temporary directory and all its contents
func cleanupBenchmarkTempDir(b *testing.B, dir string) {
	err := os.RemoveAll(dir)
	if err != nil {
		b.Logf("Warning: failed to cleanup temp directory %s: %v", dir, err)
	}
}
