package appenda

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// MockIterator implements Iterator interface for testing
type MockIterator struct {
	messages []Message
	position int
	err      error
}

func NewMockIterator(messages []Message) *MockIterator {
	return &MockIterator{
		messages: messages,
		position: -1, // Start at -1 so first Next() moves to index 0
		err:      nil,
	}
}

func (it *MockIterator) Next() bool {
	if it.err != nil || it.position >= len(it.messages)-1 {
		return false
	}

	it.position++
	return true
}

func (it *MockIterator) Message() Message {
	if it.position < 0 || it.position >= len(it.messages) {
		return Message{}
	}
	return it.messages[it.position]
}

func (it *MockIterator) Err() error {
	return it.err
}

func (it *MockIterator) Close() error {
	return nil
}

func (it *MockIterator) SetError(err error) {
	it.err = err
}

// TestIterator tests the Iterator interface functionality
func TestIterator(t *testing.T) {
	// Create test messages
	now := time.Now().UTC().Truncate(time.Millisecond)
	messages := []Message{
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

	// Test basic iteration
	t.Run("Basic iteration", func(t *testing.T) {
		iterator := NewMockIterator(messages)

		count := 0
		for iterator.Next() {
			msg := iterator.Message()
			assert.Equal(t, messages[count], msg, "Message should match what was expected")
			count++
		}

		assert.Equal(t, len(messages), count, "Iterator should return all messages")
		assert.NoError(t, iterator.Err(), "Iterator should not have an error")
		assert.NoError(t, iterator.Close(), "Close should not return an error")
	})

	// Test empty iterator
	t.Run("Empty iterator", func(t *testing.T) {
		emptyIterator := NewMockIterator([]Message{})

		assert.False(t, emptyIterator.Next(), "Next should return false for empty iterator")
		assert.Equal(t, Message{}, emptyIterator.Message(), "Message should return empty message for invalid position")
		assert.NoError(t, emptyIterator.Err(), "Iterator should not have an error")
	})

	// Test iterator with error
	t.Run("Iterator with error", func(t *testing.T) {
		errorIterator := NewMockIterator(messages)
		errorIterator.SetError(ErrCorruptData)

		assert.False(t, errorIterator.Next(), "Next should return false when there's an error")
		assert.Equal(t, ErrCorruptData, errorIterator.Err(), "Err should return the set error")
	})

	// Test calling Next after reaching the end
	t.Run("Calling Next after end", func(t *testing.T) {
		iterator := NewMockIterator(messages)

		// Consume all messages
		for i := 0; i < len(messages); i++ {
			assert.True(t, iterator.Next(), "Next should return true for valid positions")
		}

		// This should return false since we're at the end
		assert.False(t, iterator.Next(), "Next should return false after reaching the end")

		// Try calling Next one more time
		assert.False(t, iterator.Next(), "Next should continue to return false")
	})

	// Test Message method edge cases
	t.Run("Message method edge cases", func(t *testing.T) {
		iterator := NewMockIterator(messages)

		// Before first Next call
		assert.Equal(t, Message{}, iterator.Message(), "Message should return empty message before first Next call")

		// Get first message
		assert.True(t, iterator.Next(), "Next should return true")
		assert.Equal(t, messages[0], iterator.Message(), "Message should return first message")

		// Consume all messages
		for i := 1; i < len(messages); i++ {
			iterator.Next()
		}

		// Try Next one more time (should return false)
		assert.False(t, iterator.Next(), "Next should return false after reaching the end")

		// Message should still return the last message
		assert.Equal(t, messages[len(messages)-1], iterator.Message(), "Message should return last message")
	})
}

// TestIteratorWithInMemoryBackend tests the Iterator interface with InMemoryBackend
func TestIteratorWithInMemoryBackend(t *testing.T) {
	backend := NewInMemoryBackend()
	err := backend.Open()
	assert.NoError(t, err, "Opening the backend should not return an error")

	// Create a segment and add messages
	now := time.Now().UTC().Truncate(time.Millisecond)
	err = backend.CreateNewSegment(now)
	assert.NoError(t, err, "Creating a new segment should not return an error")

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
	assert.NoError(t, err, "Appending batch should not return an error")

	// Test reading with valid iterator
	iterator, err := backend.Read(now, 0)
	assert.NoError(t, err, "Reading from segment should not return an error")
	assert.NotNil(t, iterator, "Iterator should not be nil")

	// Test iteration
	count := 0
	var lastMsg Message
	for iterator.Next() {
		msg := iterator.Message()
		assert.Equal(t, msgs[count], msg, "Message should match the expected message")
		lastMsg = msg
		count++
	}

	assert.Equal(t, len(msgs), count, "Iterator should return all messages")
	assert.Equal(t, msgs[len(msgs)-1], lastMsg, "Last message should match the last appended message")
	assert.NoError(t, iterator.Err(), "Iterator should not have an error")
	assert.NoError(t, iterator.Close(), "Closing iterator should not return an error")

	// Test reading with offset
	iterator, err = backend.Read(now, 1)
	assert.NoError(t, err, "Reading with offset should not return an error")

	// Should skip the first message
	assert.True(t, iterator.Next(), "Next should return true")
	assert.Equal(t, msgs[1], iterator.Message(), "Message should match the second message")
}
