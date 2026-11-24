package client

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	pb "github.com/kubemq-io/protobuf/go"
	"github.com/nats-io/nuid"
	"github.com/stretchr/testify/require"
)

func sendBroadcastMessages(t *testing.T, ctx context.Context, c *QueueClient, channel string, count int) {
	batch := &pb.QueueMessagesBatchRequest{BatchID: nuid.Next()}
	for i := 1; i <= count; i++ {
		batch.Messages = append(batch.Messages, &pb.QueueMessage{
			MessageID: fmt.Sprintf("bc-%d", i),
			ClientID:  "broadcast-sender",
			Channel:   channel,
			Body:      []byte(fmt.Sprintf("broadcast payload %d", i)),
		})
	}
	for _, msg := range batch.Messages {
		result := c.SendBroadcastMessage(ctx, msg)
		require.False(t, result.IsError, result.Error)
	}
}

func receiveAllBroadcast(t *testing.T, ctx context.Context, c *QueueClient, channel, group string, total int) []*pb.QueueMessage {
	var collected []*pb.QueueMessage
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) && len(collected) < total {
		req := &BroadcastReceiveRequest{
			Channel:             channel,
			Group:               group,
			ClientID:            nuid.Next(),
			MaxNumberOfMessages: 100,
			WaitTimeSeconds:     2,
			AutoAck:             false,
		}
		response, err := c.ReceiveBroadcastMessages(ctx, req)
		require.NoError(t, err)
		messages := response.Messages()
		var sequences []uint64
		for _, msg := range messages {
			collected = append(collected, msg)
			sequences = append(sequences, msg.Attributes.Sequence)
		}
		require.NoError(t, response.AckRange(sequences))
		response.Close()
	}
	require.Len(t, collected, total, "group %s must receive the full stream", group)
	return collected
}

func TestBroadcast_TwoGroupsEachReceiveFullStream(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := getConfig(t)
	brokerServer := setupSingle(ctx, t, cfg)
	defer tearDownSingle(brokerServer, cfg)

	sender, err := NewQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	defer func() { _ = sender.Disconnect() }()

	receiver, err := NewQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	defer func() { _ = receiver.Disconnect() }()

	channel := nuid.Next()
	const messageCount = 20
	sendBroadcastMessages(t, ctx, sender, channel, messageCount)

	groupA := receiveAllBroadcast(t, ctx, receiver, channel, "group-a", messageCount)
	groupB := receiveAllBroadcast(t, ctx, receiver, channel, "group-b", messageCount)

	require.Len(t, groupA, messageCount)
	require.Len(t, groupB, messageCount)
	for i := 0; i < messageCount; i++ {
		require.Equal(t, fmt.Sprintf("bc-%d", i+1), groupA[i].MessageID)
		require.Equal(t, fmt.Sprintf("bc-%d", i+1), groupB[i].MessageID)
	}
}

func TestBroadcast_ThreeGroupsRiskBillingNotification(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := getConfig(t)
	brokerServer := setupSingle(ctx, t, cfg)
	defer tearDownSingle(brokerServer, cfg)

	sender, err := NewQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	defer func() { _ = sender.Disconnect() }()

	receiver, err := NewQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	defer func() { _ = receiver.Disconnect() }()

	channel := nuid.Next()
	const messageCount = 30
	sendBroadcastMessages(t, ctx, sender, channel, messageCount)

	groups := []string{"risk-control", "billing", "notification"}
	results := make(chan []*pb.QueueMessage, len(groups))
	for _, group := range groups {
		group := group
		go func() {
			results <- receiveAllBroadcast(t, ctx, receiver, channel, group, messageCount)
		}()
	}
	for range groups {
		messages := <-results
		require.Len(t, messages, messageCount, "every group must receive the full stream")
	}
}

func TestBroadcast_IndependentOffsetsDoNotStealMessages(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := getConfig(t)
	brokerServer := setupSingle(ctx, t, cfg)
	defer tearDownSingle(brokerServer, cfg)

	sender, err := NewQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	defer func() { _ = sender.Disconnect() }()

	receiver, err := NewQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	defer func() { _ = receiver.Disconnect() }()

	channel := nuid.Next()
	const messageCount = 10
	sendBroadcastMessages(t, ctx, sender, channel, messageCount)

	// group-a consumes and acks only the first half.
	firstReq := &BroadcastReceiveRequest{
		Channel:             channel,
		Group:               "group-a",
		ClientID:            nuid.Next(),
		MaxNumberOfMessages: 5,
		WaitTimeSeconds:     3,
	}
	firstResponse, err := receiver.ReceiveBroadcastMessages(ctx, firstReq)
	require.NoError(t, err)
	require.Len(t, firstResponse.Messages(), 5)
	var sequences []uint64
	for _, msg := range firstResponse.Messages() {
		sequences = append(sequences, msg.Attributes.Sequence)
	}
	require.NoError(t, firstResponse.AckRange(sequences))
	firstResponse.Close()

	// group-b still receives every message from the beginning.
	groupB := receiveAllBroadcast(t, ctx, receiver, channel, "group-b", messageCount)
	require.Len(t, groupB, messageCount)

	// group-a resumes after its own durable offset and only reads the remainder.
	remaining := receiveAllBroadcast(t, ctx, receiver, channel, "group-a", messageCount-5)
	require.Len(t, remaining, messageCount-5)
	require.Equal(t, "bc-6", remaining[0].MessageID)
}

func TestBroadcast_DoesNotAffectPointToPointQueue(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := getConfig(t)
	brokerServer := setupSingle(ctx, t, cfg)
	defer tearDownSingle(brokerServer, cfg)

	c, err := NewQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	defer func() { _ = c.Disconnect() }()

	queueChannel := nuid.Next()
	queueMsg := &pb.QueueMessage{
		MessageID: "p2p-1",
		ClientID:  "p2p-sender",
		Channel:   queueChannel,
		Body:      []byte("point to point payload"),
	}
	result := c.SendQueueMessage(ctx, queueMsg)
	require.False(t, result.IsError, result.Error)

	broadcastChannel := nuid.Next()
	broadcastMsg := &pb.QueueMessage{
		MessageID: "bc-1",
		ClientID:  "bc-sender",
		Channel:   broadcastChannel,
		Body:      []byte("broadcast payload"),
	}
	require.False(t, c.SendBroadcastMessage(ctx, broadcastMsg).IsError)

	// The point-to-point queue still delivers exactly one message to the single
	// durable queue subscriber.
	p2pResponse, err := c.ReceiveQueueMessages(ctx, &pb.ReceiveQueueMessagesRequest{
		ClientID:            nuid.Next(),
		Channel:             queueChannel,
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     3,
	})
	require.NoError(t, err)
	require.Len(t, p2pResponse.Messages, 1)
	require.Equal(t, "p2p-1", p2pResponse.Messages[0].MessageID)

	// Broadcast groups never see point-to-point traffic and vice versa.
	bcResponse, err := c.ReceiveBroadcastMessages(ctx, &BroadcastReceiveRequest{
		Channel:             broadcastChannel,
		Group:               "group-a",
		ClientID:            nuid.Next(),
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     3,
	})
	require.NoError(t, err)
	require.Len(t, bcResponse.Messages(), 1)
	require.Equal(t, "bc-1", bcResponse.Messages()[0].MessageID)
	bcResponse.Close()
}

func TestBroadcast_InvalidGroupName(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := getConfig(t)
	brokerServer := setupSingle(ctx, t, cfg)
	defer tearDownSingle(brokerServer, cfg)

	c, err := NewQueueClient(getClientOptions(t, cfg), cfg.Queue)
	require.NoError(t, err)
	defer func() { _ = c.Disconnect() }()

	invalidGroups := []string{"", "group.", "group name", "group*", "group>"}
	for _, group := range invalidGroups {
		_, err := c.ReceiveBroadcastMessages(ctx, &BroadcastReceiveRequest{
			Channel:             "channel",
			Group:               group,
			ClientID:            nuid.Next(),
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     1,
		})
		require.Error(t, err, "group %q must be rejected", group)
	}
}
