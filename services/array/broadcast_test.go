package array

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/kubemq-io/kubemq-community/pkg/client"
	pb "github.com/kubemq-io/protobuf/go"
	"github.com/nats-io/nuid"
	"github.com/stretchr/testify/require"
)

func sendArrayBroadcastMessages(t *testing.T, ctx context.Context, na *Array, channel string, count int) {
	for i := 1; i <= count; i++ {
		result, err := na.SendBroadcastMessage(ctx, &pb.QueueMessage{
			MessageID: fmt.Sprintf("array-bc-%d", i),
			ClientID:  "broadcast-sender",
			Channel:   channel,
			Body:      []byte(fmt.Sprintf("payload %d", i)),
		})
		require.NoError(t, err)
		require.False(t, result.IsError, result.Error)
	}
}

func receiveAllArrayBroadcast(t *testing.T, ctx context.Context, na *Array, channel, group string, total int) []*pb.QueueMessage {
	var collected []*pb.QueueMessage
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) && len(collected) < total {
		response, err := na.ReceiveBroadcastMessages(ctx, &client.BroadcastReceiveRequest{
			Channel:             channel,
			Group:               group,
			ClientID:            nuid.Next(),
			MaxNumberOfMessages: 100,
			WaitTimeSeconds:     2,
		})
		require.NoError(t, err)
		var sequences []uint64
		for _, msg := range response.Messages() {
			collected = append(collected, msg)
			sequences = append(sequences, msg.Attributes.Sequence)
		}
		require.NoError(t, response.AckRange(sequences))
		response.Close()
		na.ReleaseBroadcastReceiver(channel, group)
	}
	require.Len(t, collected, total)
	return collected
}

func TestArray_Broadcast_RegisterAndListGroups(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)

	channel := nuid.Next()
	require.NoError(t, na.RegisterBroadcastGroup(ctx, channel, "risk-control"))
	require.NoError(t, na.RegisterBroadcastGroup(ctx, channel, "billing"))
	require.NoError(t, na.RegisterBroadcastGroup(ctx, channel, "notification"))
	// Re-registering the same group is idempotent.
	require.NoError(t, na.RegisterBroadcastGroup(ctx, channel, "risk-control"))

	groups := na.ListBroadcastGroups(ctx, channel)
	require.Len(t, groups, 3)
	require.Equal(t, "billing", groups[0].Name)
	require.Equal(t, "notification", groups[1].Name)
	require.Equal(t, "risk-control", groups[2].Name)
}

func TestArray_Broadcast_ThreeGroupsEachGetFullStream(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)

	channel := nuid.Next()
	groups := []string{"risk-control", "billing", "notification"}
	for _, group := range groups {
		require.NoError(t, na.RegisterBroadcastGroup(ctx, channel, group))
	}

	const messageCount = 15
	sendArrayBroadcastMessages(t, ctx, na, channel, messageCount)

	// Warm up each group's pooled client sequentially so concurrent goroutines
	// do not trigger concurrent first-time dials on the in-memory test pipe.
	for _, group := range groups {
		warmUp, err := na.ReceiveBroadcastMessages(ctx, &client.BroadcastReceiveRequest{
			Channel:             channel,
			Group:               group,
			ClientID:            nuid.Next(),
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     1,
		})
		require.NoError(t, err)
		warmUp.Close()
		na.ReleaseBroadcastReceiver(channel, group)
	}

	var wg sync.WaitGroup
	for _, group := range groups {
		wg.Add(1)
		go func(group string) {
			defer wg.Done()
			messages := receiveAllArrayBroadcast(t, ctx, na, channel, group, messageCount)
			require.Len(t, messages, messageCount)
			require.Equal(t, "array-bc-1", messages[0].MessageID)
			require.Equal(t, fmt.Sprintf("array-bc-%d", messageCount), messages[messageCount-1].MessageID)
		}(group)
	}
	wg.Wait()
}

func TestArray_Broadcast_UnregisteredGroupRejected(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)

	channel := nuid.Next()
	_, err := na.ReceiveBroadcastMessages(ctx, &client.BroadcastReceiveRequest{
		Channel:             channel,
		Group:               "not-registered",
		ClientID:            nuid.Next(),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     1,
	})
	require.Error(t, err)
}

func TestArray_Broadcast_ConsumersInSameGroupShareOffset(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)

	channel := nuid.Next()
	group := "risk-control"
	require.NoError(t, na.RegisterBroadcastGroup(ctx, channel, group))

	const messageCount = 12
	sendArrayBroadcastMessages(t, ctx, na, channel, messageCount)

	// Two consumers inside the same group split the stream between themselves
	// and never receive the same message twice.
	// Warm up the pooled client so concurrent consumers reuse one connection;
	// the in-memory test pipe does not support concurrent first-time dials.
	warmUp, err := na.ReceiveBroadcastMessages(ctx, &client.BroadcastReceiveRequest{
		Channel:             channel,
		Group:               group,
		ClientID:            nuid.Next(),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     1,
	})
	require.NoError(t, err)
	warmUp.Close()
	na.ReleaseBroadcastReceiver(channel, group)

	consumerOne := make(chan string, messageCount)
	consumerTwo := make(chan string, messageCount)
	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()
	var wg sync.WaitGroup
	// The existing queue pool tracks LastConnect without a lock, so pool
	// releases for the shared group client are serialized in this test.
	var releaseMu sync.Mutex
	release := func() {
		releaseMu.Lock()
		defer releaseMu.Unlock()
		na.ReleaseBroadcastReceiver(channel, group)
	}
	startSecond := make(chan struct{})
	consume := func(out chan string, waitForStart <-chan struct{}) {
		defer wg.Done()
		if waitForStart != nil {
			<-waitForStart
		}
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) && consumerCtx.Err() == nil {
			response, err := na.ReceiveBroadcastMessages(consumerCtx, &client.BroadcastReceiveRequest{
				Channel:             channel,
				Group:               group,
				ClientID:            nuid.Next(),
				MaxNumberOfMessages: 2,
				WaitTimeSeconds:     1,
			})
			if err != nil {
				if consumerCtx.Err() != nil {
					return
				}
				require.NoError(t, err)
			}
			require.NoError(t, err)
			var sequences []uint64
			for _, msg := range response.Messages() {
				out <- msg.MessageID
				sequences = append(sequences, msg.Attributes.Sequence)
			}
			require.NoError(t, response.AckRange(sequences))
			response.Close()
			release()
		}
	}
	wg.Add(2)
	go consume(consumerOne, nil)
	go consume(consumerTwo, startSecond)
	close(startSecond)

	seen := make(map[string]int)
	for len(seen) < messageCount {
		select {
		case id := <-consumerOne:
			seen[id]++
		case id := <-consumerTwo:
			seen[id]++
		case <-time.After(25 * time.Second):
			t.Fatalf("timed out waiting for messages, got %d of %d", len(seen), messageCount)
		}
	}
	for id, count := range seen {
		require.Equal(t, 1, count, "message %s delivered twice inside the same group", id)
	}
	consumerCancel()
	wg.Wait()
}

func TestArray_Broadcast_ExistingPointToPointQueueUnaffected(t *testing.T) {
	defer leaktest.Check(t)()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	na, ns := setup(ctx, t)
	defer tearDown(na, ns)

	queueChannel := nuid.Next()
	_, err := na.SendQueueMessage(ctx, &pb.QueueMessage{
		MessageID: "legacy-p2p",
		ClientID:  "legacy-sender",
		Channel:   queueChannel,
		Body:      []byte("legacy queue payload"),
	})
	require.NoError(t, err)

	broadcastChannel := nuid.Next()
	require.NoError(t, na.RegisterBroadcastGroup(ctx, broadcastChannel, "risk-control"))
	_, err = na.SendBroadcastMessage(ctx, &pb.QueueMessage{
		MessageID: "new-broadcast",
		ClientID:  "broadcast-sender",
		Channel:   broadcastChannel,
		Body:      []byte("broadcast payload"),
	})
	require.NoError(t, err)

	p2pResponse, err := na.ReceiveQueueMessages(ctx, &pb.ReceiveQueueMessagesRequest{
		ClientID:            nuid.Next(),
		Channel:             queueChannel,
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     2,
	})
	require.NoError(t, err)
	require.Len(t, p2pResponse.Messages, 1)
	require.Equal(t, "legacy-p2p", p2pResponse.Messages[0].MessageID)

	bcResponse, err := na.ReceiveBroadcastMessages(ctx, &client.BroadcastReceiveRequest{
		Channel:             broadcastChannel,
		Group:               "risk-control",
		ClientID:            nuid.Next(),
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     2,
	})
	require.NoError(t, err)
	require.Len(t, bcResponse.Messages(), 1)
	require.Equal(t, "new-broadcast", bcResponse.Messages()[0].MessageID)
	bcResponse.Close()
	na.ReleaseBroadcastReceiver(broadcastChannel, "risk-control")
}
