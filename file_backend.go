package appenda

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultMaxFileSize is the default maximum file size in bytes (100MB)
	DefaultMaxFileSize int64 = 100 * 1024 * 1024

	// DefaultFileExtension is the default file extension
	DefaultFileExtension = "log"

	// MaxSegmentDuration is the maximum duration for a segment (6 months)
	MaxSegmentDuration = 6 * 30 * 24 * time.Hour

	// RecordHeaderSize is the size of record header (3 bytes timestamp + 2 bytes length)
	RecordHeaderSize = 5

	// CRC32Size is the size of CRC32 checksum
	CRC32Size = 4
)

var (
	// ErrInvalidFileFormat is returned when file format is invalid
	ErrInvalidFileFormat = errors.New("invalid file format")

	// ErrChecksumMismatch is returned when CRC32 checksum doesn't match
	ErrChecksumMismatch = errors.New("checksum mismatch")

	// ErrRecordTooLarge is returned when a single record exceeds file size limit
	ErrRecordTooLarge = errors.New("record too large for file size limit")
)

// FileBackendConfig contains configuration for FileBackend
type FileBackendConfig struct {
	// Directory where files will be stored
	Directory string

	// Prefix for file names (default: empty string)
	Prefix string

	// Extension for files (default: "log")
	Extension string

	// MaxFileSize is the maximum size of a single file in bytes (default: 100MB)
	MaxFileSize int64
}

// fileSegment represents a file-based segment
type fileSegment struct {
	info     SegmentInfo
	filename string
	sequence uint32
	file     *os.File
	size     int64
	mu       sync.RWMutex
}

// FileBackend implements StorageBackend using file system
type FileBackend struct {
	config FileBackendConfig

	// Directory path
	dir string

	// Current active segments indexed by timestamp
	segments map[time.Time]*fileSegment

	// Current active segment
	currentSegment *fileSegment

	// Flag indicating if backend is open
	closed bool

	// Mutex for concurrent access
	mu sync.RWMutex
}

// NewFileBackend creates a new file-based storage backend
func NewFileBackend(config FileBackendConfig) *FileBackend {
	if config.Extension == "" {
		config.Extension = DefaultFileExtension
	}

	if config.MaxFileSize <= 0 {
		config.MaxFileSize = DefaultMaxFileSize
	}

	return &FileBackend{
		config:   config,
		dir:      config.Directory,
		segments: make(map[time.Time]*fileSegment),
		closed:   true,
	}
}

// Open initializes the storage backend
func (b *FileBackend) Open() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.closed {
		return nil // Already open
	}

	// Create directory if it doesn't exist
	if err := os.MkdirAll(b.dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Load existing segments
	if err := b.loadExistingSegments(); err != nil {
		return fmt.Errorf("failed to load existing segments: %w", err)
	}

	b.closed = false
	return nil
}

// loadExistingSegments scans the directory and loads existing segment files
func (b *FileBackend) loadExistingSegments() error {
	files, err := os.ReadDir(b.dir)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		timestamp, sequence, err := b.parseFilename(file.Name())
		if err != nil {
			continue // Skip files that don't match our naming convention
		}

		// Get file info
		fullPath := filepath.Join(b.dir, file.Name())
		fileInfo, err := os.Stat(fullPath)
		if err != nil {
			continue
		}

		// Calculate segment info by reading the file
		segmentInfo, err := b.calculateSegmentInfo(fullPath, timestamp)
		if err != nil {
			continue
		}

		// Create segment entry
		segment := &fileSegment{
			info:     segmentInfo,
			filename: file.Name(),
			sequence: sequence,
			size:     fileInfo.Size(),
		}

		// Store segment
		if existing, exists := b.segments[timestamp]; !exists || sequence > existing.sequence {
			b.segments[timestamp] = segment
		}
	}

	// Find the most recent segment as current
	var latestTime time.Time
	for timestamp := range b.segments {
		if timestamp.After(latestTime) {
			latestTime = timestamp
			b.currentSegment = b.segments[timestamp]
		}
	}

	return nil
}

// parseFilename parses a filename to extract timestamp and sequence
func (b *FileBackend) parseFilename(filename string) (time.Time, uint32, error) {
	// Check if filename has the correct extension
	expectedSuffix := "." + b.config.Extension
	if !strings.HasSuffix(filename, expectedSuffix) {
		return time.Time{}, 0, fmt.Errorf("invalid file extension")
	}

	// Remove extension
	name := strings.TrimSuffix(filename, expectedSuffix)

	// Check prefix
	if b.config.Prefix != "" {
		expectedPrefix := b.config.Prefix + "_"
		if !strings.HasPrefix(name, expectedPrefix) {
			return time.Time{}, 0, fmt.Errorf("invalid prefix")
		}
		name = strings.TrimPrefix(name, expectedPrefix)
	}

	// Split by underscore to get timestamp and sequence
	parts := strings.Split(name, "_")
	if len(parts) != 2 {
		return time.Time{}, 0, fmt.Errorf("invalid filename format")
	}

	// Parse timestamp (hex format)
	timestampHex := parts[0]
	timestampUnix, err := strconv.ParseInt(timestampHex, 16, 64)
	if err != nil {
		return time.Time{}, 0, fmt.Errorf("invalid timestamp: %w", err)
	}
	timestamp := time.Unix(timestampUnix, 0).UTC()

	// Parse sequence (hex format, 4 bytes)
	sequenceHex := parts[1]
	sequenceInt, err := strconv.ParseUint(sequenceHex, 16, 32)
	if err != nil {
		return time.Time{}, 0, fmt.Errorf("invalid sequence: %w", err)
	}

	return timestamp, uint32(sequenceInt), nil
}

// calculateSegmentInfo reads a file and calculates its segment information
func (b *FileBackend) calculateSegmentInfo(filename string, segmentTimestamp time.Time) (SegmentInfo, error) {
	file, err := os.Open(filename)
	if err != nil {
		return SegmentInfo{}, err
	}
	defer file.Close()

	info := SegmentInfo{
		Timestamp:    segmentTimestamp,
		EndTime:      segmentTimestamp,
		MessageCount: 0,
		Size:         0,
	}

	// Read file stat
	stat, err := file.Stat()
	if err != nil {
		return SegmentInfo{}, err
	}
	info.Size = stat.Size()

	// Read records to find message count and end time
	for {
		record, err := b.readRecord(file, segmentTimestamp)
		if err == io.EOF {
			break
		}
		if err != nil {
			return SegmentInfo{}, err
		}

		info.MessageCount++
		if record.Timestamp.After(info.EndTime) {
			info.EndTime = record.Timestamp
		}
	}

	return info, nil
}

// generateFilename generates a filename for a segment
func (b *FileBackend) generateFilename(timestamp time.Time, sequence uint32) string {
	timestampHex := fmt.Sprintf("%x", timestamp.Unix())
	sequenceHex := fmt.Sprintf("%08x", sequence)

	var filename string
	if b.config.Prefix != "" {
		filename = fmt.Sprintf("%s_%s_%s.%s", b.config.Prefix, timestampHex, sequenceHex, b.config.Extension)
	} else {
		filename = fmt.Sprintf("%s_%s.%s", timestampHex, sequenceHex, b.config.Extension)
	}

	return filename
}

// checkClosed returns error if backend is closed
func (b *FileBackend) checkClosed() error {
	if b.closed {
		return ErrStorageClosed
	}
	return nil
}

// shouldCreateNewSegment determines if a new segment should be created
func (b *FileBackend) shouldCreateNewSegment(msgTimestamp time.Time) bool {
	if b.currentSegment == nil {
		return true
	}

	// Check if message timestamp is more than 6 months after segment start
	if msgTimestamp.Sub(b.currentSegment.info.Timestamp) > MaxSegmentDuration {
		return true
	}

	return false
}

// Append stores a single message
func (b *FileBackend) Append(msg Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.checkClosed(); err != nil {
		return err
	}

	if msg.Timestamp.IsZero() {
		return ErrInvalidTimestamp
	}

	// Check if we need to create a new segment
	if b.shouldCreateNewSegment(msg.Timestamp) {
		if err := b.createNewSegmentInternal(msg.Timestamp); err != nil {
			return err
		}
	}

	// Find appropriate segment for this message
	segment, err := b.findSegmentForMessage(msg)
	if err != nil {
		return err
	}

	// Check record size
	recordSize := RecordHeaderSize + len(msg.Payload) + CRC32Size
	if int64(recordSize) > b.config.MaxFileSize {
		return ErrRecordTooLarge
	}

	// Check if current file would exceed size limit
	if segment.size+int64(recordSize) > b.config.MaxFileSize {
		// Create new file with incremented sequence
		if err := b.createNewFileForSegment(segment.info.Timestamp); err != nil {
			return err
		}
		segment = b.segments[segment.info.Timestamp]
	}

	// Open file if not already open
	if segment.file == nil {
		fullPath := filepath.Join(b.dir, segment.filename)
		file, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return fmt.Errorf("failed to open file: %w", err)
		}
		segment.file = file
	}

	// Write record
	if err := b.writeRecord(segment.file, msg, segment.info.Timestamp); err != nil {
		return err
	}

	// Update segment info
	segment.info.MessageCount++
	segment.size += int64(recordSize)
	segment.info.Size += int64(recordSize)
	if msg.Timestamp.After(segment.info.EndTime) {
		segment.info.EndTime = msg.Timestamp
	}

	return nil
}

// findSegmentForMessage finds the appropriate segment for a message
func (b *FileBackend) findSegmentForMessage(msg Message) (*fileSegment, error) {
	// Try to find segment that contains this timestamp
	for timestamp, segment := range b.segments {
		if !msg.Timestamp.Before(timestamp) &&
			!msg.Timestamp.After(segment.info.EndTime) {
			return segment, nil
		}
	}

	// If no segment contains this message, use current segment
	if b.currentSegment != nil {
		return b.currentSegment, nil
	}

	return nil, ErrSegmentNotFound
}

// createNewFileForSegment creates a new file for an existing segment
func (b *FileBackend) createNewFileForSegment(timestamp time.Time) error {
	segment := b.segments[timestamp]

	// Close current file
	if segment.file != nil {
		segment.file.Close()
		segment.file = nil
	}

	// Increment sequence
	segment.sequence++

	// Generate new filename
	segment.filename = b.generateFilename(timestamp, segment.sequence)
	segment.size = 0

	return nil
}

// writeRecord writes a message record to file
func (b *FileBackend) writeRecord(file *os.File, msg Message, segmentTimestamp time.Time) error {
	// Calculate relative timestamp (3 bytes)
	relativeTime := msg.Timestamp.Sub(segmentTimestamp)
	if relativeTime < 0 {
		relativeTime = 0
	}

	// Convert to 3-byte representation (in seconds)
	relativeSeconds := uint32(relativeTime.Seconds())
	if relativeSeconds > 0xFFFFFF { // 3 bytes max
		relativeSeconds = 0xFFFFFF
	}

	// Prepare payload length (2 bytes)
	payloadLen := len(msg.Payload)
	if payloadLen > 0xFFFF { // 2 bytes max
		return fmt.Errorf("payload too large: %d bytes", payloadLen)
	}

	// Calculate CRC32 of payload
	checksum := crc32.ChecksumIEEE(msg.Payload)

	// Write timestamp (3 bytes, big endian)
	timestampBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(timestampBytes, relativeSeconds)
	if _, err := file.Write(timestampBytes[1:]); err != nil {
		return err
	}

	// Write length (2 bytes, big endian)
	lengthBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(lengthBytes, uint16(payloadLen))
	if _, err := file.Write(lengthBytes); err != nil {
		return err
	}

	// Write payload
	if _, err := file.Write(msg.Payload); err != nil {
		return err
	}

	// Write CRC32 (4 bytes, big endian)
	checksumBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(checksumBytes, checksum)
	if _, err := file.Write(checksumBytes); err != nil {
		return err
	}

	return nil
}

// readRecord reads a message record from file
func (b *FileBackend) readRecord(file *os.File, segmentTimestamp time.Time) (Message, error) {
	// Read timestamp (3 bytes)
	timestampBytes := make([]byte, 3)
	if _, err := io.ReadFull(file, timestampBytes); err != nil {
		return Message{}, err
	}

	// Convert to 4 bytes for parsing
	fullTimestampBytes := make([]byte, 4)
	copy(fullTimestampBytes[1:], timestampBytes)
	relativeSeconds := binary.BigEndian.Uint32(fullTimestampBytes)

	// Read length (2 bytes)
	lengthBytes := make([]byte, 2)
	if _, err := io.ReadFull(file, lengthBytes); err != nil {
		return Message{}, err
	}
	payloadLen := binary.BigEndian.Uint16(lengthBytes)

	// Read payload
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(file, payload); err != nil {
		return Message{}, err
	}

	// Read CRC32 (4 bytes)
	checksumBytes := make([]byte, 4)
	if _, err := io.ReadFull(file, checksumBytes); err != nil {
		return Message{}, err
	}
	expectedChecksum := binary.BigEndian.Uint32(checksumBytes)

	// Verify checksum
	actualChecksum := crc32.ChecksumIEEE(payload)
	if actualChecksum != expectedChecksum {
		return Message{}, ErrChecksumMismatch
	}

	// Calculate absolute timestamp
	absoluteTimestamp := segmentTimestamp.Add(time.Duration(relativeSeconds) * time.Second)

	return Message{
		Timestamp: absoluteTimestamp,
		Payload:   payload,
	}, nil
}

// AppendBatch stores multiple messages at once
func (b *FileBackend) AppendBatch(msgs []Message) error {
	if len(msgs) == 0 {
		return nil
	}

	// Validate all messages first
	for _, msg := range msgs {
		if msg.Timestamp.IsZero() {
			return ErrInvalidTimestamp
		}
	}

	// Append messages one by one
	for _, msg := range msgs {
		if err := b.Append(msg); err != nil {
			return err
		}
	}

	return nil
}

// Read returns an iterator for the specified segment starting at the given offset
func (b *FileBackend) Read(segmentTimestamp time.Time, offset int64) (Iterator, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if err := b.checkClosed(); err != nil {
		return nil, err
	}

	segment, exists := b.segments[segmentTimestamp]
	if !exists {
		return nil, ErrSegmentNotFound
	}

	return NewFileIterator(b, segment, offset)
}

// ListSegments returns information about all available segments
func (b *FileBackend) ListSegments() ([]SegmentInfo, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if err := b.checkClosed(); err != nil {
		return nil, err
	}

	segments := make([]SegmentInfo, 0, len(b.segments))
	for _, segment := range b.segments {
		segments = append(segments, segment.info)
	}

	// Sort by timestamp
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].Timestamp.Before(segments[j].Timestamp)
	})

	return segments, nil
}

// GetSegmentInfo returns information about a specific segment
func (b *FileBackend) GetSegmentInfo(timestamp time.Time) (SegmentInfo, error) {
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
func (b *FileBackend) FindSegmentByTime(messageTimestamp time.Time) (SegmentInfo, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if err := b.checkClosed(); err != nil {
		return SegmentInfo{}, err
	}

	if messageTimestamp.IsZero() {
		return SegmentInfo{}, ErrInvalidTimestamp
	}

	for _, segment := range b.segments {
		if !messageTimestamp.Before(segment.info.Timestamp) &&
			!messageTimestamp.After(segment.info.EndTime) {
			return segment.info, nil
		}
	}

	return SegmentInfo{}, ErrSegmentNotFound
}

// CreateNewSegment closes the current segment and creates a new one
func (b *FileBackend) CreateNewSegment(timestamp time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.createNewSegmentInternal(timestamp)
}

// createNewSegmentInternal is the internal implementation
func (b *FileBackend) createNewSegmentInternal(timestamp time.Time) error {
	if err := b.checkClosed(); err != nil {
		return err
	}

	if timestamp.IsZero() {
		return ErrInvalidTimestamp
	}

	// Check if segment already exists
	if _, exists := b.segments[timestamp]; exists {
		return ErrSegmentExists
	}

	// Check chronological order
	if b.currentSegment != nil && timestamp.Before(b.currentSegment.info.Timestamp) {
		return ErrInvalidSegmentOrder
	}

	// Close current segment file
	if b.currentSegment != nil && b.currentSegment.file != nil {
		b.currentSegment.file.Close()
		b.currentSegment.file = nil
	}

	// Create new segment
	filename := b.generateFilename(timestamp, 0)
	segment := &fileSegment{
		info: SegmentInfo{
			Timestamp:    timestamp,
			EndTime:      timestamp,
			MessageCount: 0,
			Size:         0,
		},
		filename: filename,
		sequence: 0,
		size:     0,
	}

	b.segments[timestamp] = segment
	b.currentSegment = segment

	return nil
}

// Sync forces a sync of all pending writes
func (b *FileBackend) Sync() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.checkClosed(); err != nil {
		return err
	}

	// Sync all open files
	for _, segment := range b.segments {
		if segment.file != nil {
			if err := segment.file.Sync(); err != nil {
				return err
			}
		}
	}

	return nil
}

// Close closes the storage backend
func (b *FileBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil
	}

	// Close all open files
	for _, segment := range b.segments {
		if segment.file != nil {
			segment.file.Close()
			segment.file = nil
		}
	}

	b.closed = true
	return nil
}

// FileIterator implements Iterator for file-based segments
type FileIterator struct {
	backend        *FileBackend
	segment        *fileSegment
	file           *os.File
	currentMessage Message
	err            error
	position       int64
	messagesRead   int64
	closed         bool
	mu             sync.RWMutex
}

// NewFileIterator creates a new file iterator
func NewFileIterator(backend *FileBackend, segment *fileSegment, offset int64) (Iterator, error) {
	fullPath := filepath.Join(backend.dir, segment.filename)
	file, err := os.Open(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	iterator := &FileIterator{
		backend:      backend,
		segment:      segment,
		file:         file,
		position:     0,
		messagesRead: 0,
		closed:       false,
	}

	// Skip to offset
	for i := int64(0); i < offset; i++ {
		if !iterator.Next() {
			break
		}
	}

	return iterator, nil
}

// Next advances to the next message
func (it *FileIterator) Next() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed || it.err != nil {
		return false
	}

	if it.messagesRead >= it.segment.info.MessageCount {
		return false
	}

	msg, err := it.backend.readRecord(it.file, it.segment.info.Timestamp)
	if err != nil {
		if err != io.EOF {
			it.err = err
		}
		return false
	}

	it.currentMessage = msg
	it.messagesRead++
	return true
}

// Message returns the current message
func (it *FileIterator) Message() Message {
	it.mu.RLock()
	defer it.mu.RUnlock()

	return it.currentMessage
}

// Err returns any error encountered during iteration
func (it *FileIterator) Err() error {
	it.mu.RLock()
	defer it.mu.RUnlock()

	return it.err
}

// Close releases resources used by the iterator
func (it *FileIterator) Close() error {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed {
		return nil
	}

	it.closed = true
	if it.file != nil {
		return it.file.Close()
	}

	return nil
}
