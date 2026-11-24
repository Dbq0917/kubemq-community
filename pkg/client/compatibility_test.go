package client

import (
	"bytes"
	"encoding/binary"
	"testing"

	pb "github.com/kubemq-io/protobuf/go"
	"github.com/stretchr/testify/require"
)

func TestQueueMessageCompatibilityOldReaderSkipsUnknownVersionField(t *testing.T) {
	fullBody := bytes.Repeat([]byte("old-client-compatibility-body-"), 32)
	newMessage := &pb.QueueMessage{
		MessageID: "new-producer-message-id",
		ClientID:  "new-producer-client-id",
		Channel:   "compatibility.header.version",
		Metadata:  "metadata-v1",
		Body:      fullBody,
	}

	knownWire, err := newMessage.Marshal()
	require.NoError(t, err)

	newWire := appendCompatUnknownVarint(nil, 20100, 2)
	newWire = append(newWire, knownWire...)
	newWire = appendCompatUnknownBytes(newWire, 20101, []byte("future-header-value"))

	oldReaderMessage, err := unmarshalToQueueMessage(newWire, 1700000000, 42)
	require.NoError(t, err)
	require.NotNil(t, oldReaderMessage)
	require.Equal(t, newMessage.MessageID, oldReaderMessage.MessageID)
	require.Equal(t, newMessage.ClientID, oldReaderMessage.ClientID)
	require.Equal(t, newMessage.Channel, oldReaderMessage.Channel)
	require.Equal(t, newMessage.Metadata, oldReaderMessage.Metadata)
	require.Equal(t, fullBody, oldReaderMessage.Body)
	require.Len(t, oldReaderMessage.Body, len(fullBody))
	require.Equal(t, int64(1700000000), oldReaderMessage.Attributes.Timestamp)
	require.Equal(t, uint64(42), oldReaderMessage.Attributes.Sequence)

	t.Logf("old reader decoded %d body bytes from a %d-byte new-format message",
		len(oldReaderMessage.Body), len(newWire))
}

func TestQueueMessageCompatibilityTruncatedBodyIsRejected(t *testing.T) {
	fullBody := bytes.Repeat([]byte("truncation-must-not-be-stored-"), 32)
	newMessage := &pb.QueueMessage{
		MessageID: "truncated-message-id",
		ClientID:  "producer-client-id",
		Channel:   "compatibility.truncation",
		Body:      fullBody,
	}

	wire, err := newMessage.Marshal()
	require.NoError(t, err)
	require.Greater(t, len(wire), 16)

	truncatedWire := appendCompatUnknownVarint(nil, 20100, 2)
	truncatedWire = append(truncatedWire, wire[:len(wire)-8]...)

	message, err := unmarshalToQueueMessage(truncatedWire, 1700000000, 43)
	require.Error(t, err)
	require.Nil(t, message)
}

func appendCompatUnknownVarint(dst []byte, fieldNumber uint64, value uint64) []byte {
	var buffer [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buffer[:], fieldNumber<<3)
	dst = append(dst, buffer[:n]...)
	n = binary.PutUvarint(buffer[:], value)
	return append(dst, buffer[:n]...)
}

func appendCompatUnknownBytes(dst []byte, fieldNumber uint64, value []byte) []byte {
	var buffer [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buffer[:], fieldNumber<<3|2)
	dst = append(dst, buffer[:n]...)
	n = binary.PutUvarint(buffer[:], uint64(len(value)))
	dst = append(dst, buffer[:n]...)
	return append(dst, value...)
}
