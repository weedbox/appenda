package appenda

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Missing import in appenda_impl.go - adding this import here to make tests work
var _ = errors.New("")

// TestAppendaImpl_New tests the NewAppenda function
func TestAppendaImpl_New(t *testing.T) {
	// Test with nil backend
	appenda, err := NewAppenda(nil)
	assert.Error(t, err, "NewAppenda with nil backend should return an error")
	assert.Nil(t, appenda, "NewAppenda with nil backend should return nil")

	// Test with valid backend
	backend := NewInMemoryBackend()
	appenda, err = NewAppenda(backend)
	assert.NoError(t, err, "NewAppenda with valid backend should not return an error")
	assert.NotNil(t, appenda, "NewAppenda with valid backend should return non-nil")

	// Clean up
	err = appenda.Close()
	assert.NoError(t, err, "Closing appenda should not return an error")
}

// TestAppendaImpl_Append tests the Append method
func TestAppendaImpl_Append(t *testing.T) {
	// Setup
	backend := NewInMemoryBackend()
	appenda, err := NewAppenda(backend)
	assert.NoError(t, err, "NewAppenda should not return an error")

	// Test appending a message
	now := time.Now().UTC().Truncate(time.Millisecond)
	msg := Message{
		Timestamp: now,
		Payload:   []byte("test message"),
	}

	err = appenda.Append(msg)
	assert.NoError(t, err, "Append should not return an error")

	// Test appending a message with zero timestamp
	zeroMsg := Message{
		Timestamp: time.Time{},
		Payload:   []byte("zero timestamp"),
	}
	err = appenda.Append(zeroMsg)
	assert.Equal(t, ErrInvalidTimestamp, err, "Append with zero timestamp should return ErrInvalidTimestamp")

	// Test appending to closed appenda
	err = appenda.Close()
	assert.NoError(t, err, "Close should not return an error")

	err = appenda.Append(msg)
	assert.Error(t, err, "Append to closed appenda should return an error")
}

// TestAppendaImpl_AppendBatch tests the AppendBatch method
func TestAppendaImpl_AppendBatch(t *testing.T) {
	// Setup
	backend := NewInMemoryBackend()
	appenda, err := NewAppenda(backend)
	assert.NoError(t, err, "NewAppenda should not return an error")

	// Test appending an empty batch
	err = appenda.AppendBatch([]Message{})
	assert.NoError(t, err, "AppendBatch with empty batch should not return an error")

	// Test appending a batch with valid messages
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

	err = appenda.AppendBatch(msgs)
	assert.NoError(t, err, "AppendBatch with valid messages should not return an error")

	// Test appending a batch with a zero timestamp
	invalidMsgs := []Message{
		{
			Timestamp: now.Add(2 * time.Minute),
			Payload:   []byte("message 3"),
		},
		{
			Timestamp: time.Time{}, // Zero timestamp
			Payload:   []byte("invalid message"),
		},
	}

	err = appenda.AppendBatch(invalidMsgs)
	assert.Equal(t, ErrInvalidTimestamp, err, "AppendBatch with zero timestamp should return ErrInvalidTimestamp")

	// Test appending to closed appenda
	err = appenda.Close()
	assert.NoError(t, err, "Close should not return an error")

	err = appenda.AppendBatch(msgs)
	assert.Error(t, err, "AppendBatch to closed appenda should return an error")
}

// TestAppendaImpl_Iterate tests the Iterate method
func TestAppendaImpl_Iterate(t *testing.T) {
	// Setup
	backend := NewInMemoryBackend()
	appenda, err := NewAppenda(backend)
	assert.NoError(t, err, "NewAppenda should not return an error")

	// Test iterating when no messages exist
	iterator, err := appenda.Iterate()
	assert.NoError(t, err, "Iterate should not return an error when no messages exist")
	assert.NotNil(t, iterator, "Iterator should not be nil")
	assert.False(t, iterator.Next(), "Next should return false when no messages exist")
	assert.NoError(t, iterator.Err(), "Iterator should not have an error")
	assert.NoError(t, iterator.Close(), "Closing iterator should not return an error")

	// Add messages
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
		{
			Timestamp: now.Add(2 * time.Minute),
			Payload:   []byte("message 3"),
		},
	}
	err = appenda.AppendBatch(msgs)
	assert.NoError(t, err, "AppendBatch should not return an error")

	// Test iterating over messages
	iterator, err = appenda.Iterate()
	assert.NoError(t, err, "Iterate should not return an error")
	assert.NotNil(t, iterator, "Iterator should not be nil")

	count := 0
	var lastMsg Message
	for iterator.Next() {
		msg := iterator.Message()
		assert.NotEmpty(t, msg.Payload, "Message payload should not be empty")
		lastMsg = msg
		count++
	}
	assert.Equal(t, len(msgs), count, "Iterator should return all messages")
	assert.Equal(t, msgs[len(msgs)-1].Payload, lastMsg.Payload, "Last message should match the last appended message")
	assert.NoError(t, iterator.Err(), "Iterator should not have an error")
	assert.NoError(t, iterator.Close(), "Closing iterator should not return an error")

	// Test iterating on closed appenda
	err = appenda.Close()
	assert.NoError(t, err, "Close should not return an error")

	iterator, err = appenda.Iterate()
	assert.Error(t, err, "Iterate on closed appenda should return an error")
	assert.Nil(t, iterator, "Iterator should be nil on error")
}

// TestAppendaImpl_IterateFrom tests the IterateFrom method
func TestAppendaImpl_IterateFrom(t *testing.T) {
	// Setup
	backend := NewInMemoryBackend()
	appenda, err := NewAppenda(backend)
	assert.NoError(t, err, "NewAppenda should not return an error")

	// Add messages with different timestamps
	now := time.Now().UTC().Truncate(time.Millisecond)
	msgs := []Message{
		{
			Timestamp: now.Add(-2 * time.Hour),
			Payload:   []byte("message 1"),
		},
		{
			Timestamp: now.Add(-time.Hour),
			Payload:   []byte("message 2"),
		},
		{
			Timestamp: now,
			Payload:   []byte("message 3"),
		},
		{
			Timestamp: now.Add(time.Hour),
			Payload:   []byte("message 4"),
		},
	}
	err = appenda.AppendBatch(msgs)
	assert.NoError(t, err, "AppendBatch should not return an error")

	// Test iterating from a specific timestamp
	iterator, err := appenda.IterateFrom(now, 0)
	assert.NoError(t, err, "IterateFrom should not return an error")
	assert.NotNil(t, iterator, "Iterator should not be nil")

	count := 0
	for iterator.Next() {
		msg := iterator.Message()
		assert.False(t, msg.Timestamp.Before(now), "Messages should not be before the start timestamp")
		count++
	}
	assert.Equal(t, 2, count, "Iterator should return messages from and after the start timestamp")
	assert.NoError(t, iterator.Err(), "Iterator should not have an error")
	assert.NoError(t, iterator.Close(), "Closing iterator should not return an error")

	// Test with zero timestamp
	iterator, err = appenda.IterateFrom(time.Time{}, 0)
	assert.Equal(t, ErrInvalidTimestamp, err, "IterateFrom with zero timestamp should return ErrInvalidTimestamp")
	assert.Nil(t, iterator, "Iterator should be nil on error")

	// Test iterating from a timestamp after all messages
	future := now.Add(2 * time.Hour)
	iterator, err = appenda.IterateFrom(future, 0)
	assert.NoError(t, err, "IterateFrom with future timestamp should not return an error")
	assert.NotNil(t, iterator, "Iterator should not be nil")
	assert.False(t, iterator.Next(), "Next should return false when no messages exist after the timestamp")
	assert.NoError(t, iterator.Err(), "Iterator should not have an error")
	assert.NoError(t, iterator.Close(), "Closing iterator should not return an error")

	// Test iterating from a timestamp with offset
	iterator, err = appenda.IterateFrom(now, 1)
	assert.NoError(t, err, "IterateFrom with offset should not return an error")
	assert.NotNil(t, iterator, "Iterator should not be nil")

	if iterator.Next() {
		msg := iterator.Message()
		assert.Equal(t, msgs[3].Payload, msg.Payload, "Message should match expected with offset")
	} else {
		t.Error("Iterator should have at least one message")
	}

	// Test on closed appenda
	err = appenda.Close()
	assert.NoError(t, err, "Close should not return an error")

	iterator, err = appenda.IterateFrom(now, 0)
	assert.Error(t, err, "IterateFrom on closed appenda should return an error")
	assert.Nil(t, iterator, "Iterator should be nil on error")
}

// TestAppendaImpl_Sync tests the Sync method
func TestAppendaImpl_Sync(t *testing.T) {
	// Setup
	backend := NewInMemoryBackend()
	appenda, err := NewAppenda(backend)
	assert.NoError(t, err, "NewAppenda should not return an error")

	// Test sync
	err = appenda.Sync()
	assert.NoError(t, err, "Sync should not return an error")

	// Test sync on closed appenda
	err = appenda.Close()
	assert.NoError(t, err, "Close should not return an error")

	err = appenda.Sync()
	assert.Error(t, err, "Sync on closed appenda should return an error")
}

// TestAppendaImpl_Close tests the Close method
func TestAppendaImpl_Close(t *testing.T) {
	// Setup
	backend := NewInMemoryBackend()
	appenda, err := NewAppenda(backend)
	assert.NoError(t, err, "NewAppenda should not return an error")

	// Test close
	err = appenda.Close()
	assert.NoError(t, err, "Close should not return an error")

	// Test calling Close again
	err = appenda.Close()
	assert.NoError(t, err, "Calling Close again should not return an error")
}

// TestAppendaImpl_EndToEnd performs an end-to-end test of the Appenda interface
func TestAppendaImpl_EndToEnd(t *testing.T) {
	// Setup
	backend := NewInMemoryBackend()
	appenda, err := NewAppenda(backend)
	assert.NoError(t, err, "NewAppenda should not return an error")

	// Test sequence of operations
	now := time.Now().UTC().Truncate(time.Millisecond)

	// 1. Append individual messages
	for i := 0; i < 5; i++ {
		msg := Message{
			Timestamp: now.Add(time.Duration(i) * time.Minute),
			Payload:   []byte(string(rune('A' + i))),
		}
		err = appenda.Append(msg)
		assert.NoError(t, err, "Append should not return an error")
	}

	// 2. Append batch of messages
	laterMsgs := []Message{
		{
			Timestamp: now.Add(time.Hour),
			Payload:   []byte("later 1"),
		},
		{
			Timestamp: now.Add(time.Hour + time.Minute),
			Payload:   []byte("later 2"),
		},
	}
	err = appenda.AppendBatch(laterMsgs)
	assert.NoError(t, err, "AppendBatch should not return an error")

	// 3. Force sync
	err = appenda.Sync()
	assert.NoError(t, err, "Sync should not return an error")

	// 4. Iterate over all messages
	iterator, err := appenda.Iterate()
	assert.NoError(t, err, "Iterate should not return an error")

	count := 0
	for iterator.Next() {
		count++
	}
	assert.Equal(t, 7, count, "Should have 7 messages total")
	assert.NoError(t, iterator.Err(), "Iterator should not have an error")
	assert.NoError(t, iterator.Close(), "Closing iterator should not return an error")

	// 5. Iterate from specific timestamp
	iterator, err = appenda.IterateFrom(now.Add(30*time.Minute), 0)
	assert.NoError(t, err, "IterateFrom should not return an error")

	count = 0
	for iterator.Next() {
		count++
	}
	assert.Equal(t, 2, count, "Should have 4 messages after the specified timestamp")
	assert.NoError(t, iterator.Err(), "Iterator should not have an error")
	assert.NoError(t, iterator.Close(), "Closing iterator should not return an error")

	// 6. Close
	err = appenda.Close()
	assert.NoError(t, err, "Close should not return an error")
}
