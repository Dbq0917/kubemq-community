package client

import (
	"context"
	"testing"
	"time"

	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/retention"
	pb "github.com/kubemq-io/protobuf/go"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestPreSendQueueMessageTopicRetentionStampsExpiration(t *testing.T) {
	qc := &QueueClient{
		isUp: atomic.NewBool(true),
		policyCfg: &config.QueueConfig{
			MaxExpirationSeconds: 259200,
			TopicRetentionSeconds: map[string]int32{
				"hot":  259200,
				"cold": 3600,
			},
		},
	}

	tests := []struct {
		topic           string
		expectedSeconds int64
	}{
		{topic: "hot", expectedSeconds: 259200},
		{topic: "cold", expectedSeconds: 3600},
	}

	for _, tt := range tests {
		t.Run(tt.topic, func(t *testing.T) {
			msg := &pb.QueueMessage{
				ClientID: "retention-test-client",
				Channel:  tt.topic,
				Body:     []byte(tt.topic),
				Policy:   &pb.QueueMessagePolicy{},
			}

			before := time.Now()
			result := qc.preSendQueueMessage(msg)
			after := time.Now()

			require.False(t, result.IsError, result.Error)
			require.NotZero(t, result.ExpirationAt)
			require.Equal(t, result.ExpirationAt, msg.Attributes.ExpirationAt)
			expiresAt := time.Unix(0, result.ExpirationAt)
			require.GreaterOrEqual(t, expiresAt.Unix(), before.Add(time.Duration(tt.expectedSeconds)*time.Second).Add(-time.Second).Unix())
			require.LessOrEqual(t, expiresAt.Unix(), after.Add(time.Duration(tt.expectedSeconds)*time.Second).Add(time.Second).Unix())
		})
	}
}

func TestPreSendQueueMessageExplicitRetentionRemainsCompatible(t *testing.T) {
	qc := &QueueClient{
		isUp: atomic.NewBool(true),
		policyCfg: &config.QueueConfig{
			MaxExpirationSeconds: 259200,
			TopicRetentionSeconds: map[string]int32{
				"cold": 3600,
			},
		},
	}
	msg := &pb.QueueMessage{
		ClientID: "retention-test-client",
		Channel:  "cold",
		Body:     []byte("explicit"),
		Policy: &pb.QueueMessagePolicy{
			ExpirationSeconds: 60,
		},
	}

	result := qc.preSendQueueMessage(msg)

	require.False(t, result.IsError, result.Error)
	expected := time.Now().Add(60 * time.Second)
	actual := time.Unix(0, result.ExpirationAt)
	require.WithinDuration(t, expected, actual, 2*time.Second)
}

func TestQueueClientTopicRetentionExpiration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	appConfig := getConfig(t)
	appConfig.Queue.TopicRetentionSeconds = map[string]int32{
		"retention-hot":  259200,
		"retention-cold": 3600,
	}
	server := setupSingle(ctx, t, appConfig)
	defer tearDownSingle(server, appConfig)
	require.Equal(t, time.Hour, server.SnatsOptions.PerChannel[retention.QueueChannel("retention-cold")].MaxAge)

	client, err := NewQueueClient(getClientOptions(t, appConfig), appConfig.Queue)
	require.NoError(t, err)
	defer func() { _ = client.Disconnect() }()

	tests := []struct {
		topic           string
		expectedSeconds int64
	}{
		{topic: "retention-hot", expectedSeconds: 259200},
		{topic: "retention-cold", expectedSeconds: 3600},
	}

	for _, tt := range tests {
		t.Run(tt.topic, func(t *testing.T) {
			sendResult := client.SendQueueMessage(ctx, getQueueMessage(tt.topic, []byte(tt.topic)))
			require.False(t, sendResult.IsError, sendResult.Error)
			require.WithinDuration(t,
				time.Now().Add(time.Duration(tt.expectedSeconds)*time.Second),
				time.Unix(0, sendResult.ExpirationAt),
				5*time.Second,
			)
		})
	}
}
