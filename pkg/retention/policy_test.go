package retention

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPolicyResolveSeconds(t *testing.T) {
	policy := Policy{
		DefaultSeconds: 3 * 24 * 60 * 60,
		TopicSeconds: map[string]int32{
			"hot":       3 * 24 * 60 * 60,
			"cold":      60 * 60,
			"metrics.*": 24 * 60 * 60,
			"events.>":  12 * 60 * 60,
		},
	}

	tests := []struct {
		name           string
		topic          string
		messageSeconds int32
		expected       int32
	}{
		{name: "hot topic", topic: "hot", expected: 259200},
		{name: "cold topic", topic: "cold", expected: 3600},
		{name: "wildcard single segment", topic: "metrics.queue", expected: 86400},
		{name: "wildcard tail", topic: "events.user.created", expected: 43200},
		{name: "default topic", topic: "ordinary", expected: 259200},
		{name: "message ttl overrides topic", topic: "cold", messageSeconds: 60, expected: 60},
		{name: "longer message ttl also overrides topic", topic: "cold", messageSeconds: 7200, expected: 7200},
		{name: "message ttl without topic", topic: "unknown", messageSeconds: 120, expected: 120},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, policy.ResolveSeconds(tt.topic, tt.messageSeconds))
		})
	}
}

func TestPolicyExpirationAtUsesExistingDeadline(t *testing.T) {
	now := time.Unix(100, 0)
	policy := Policy{TopicSeconds: map[string]int32{"cold": 3600}}

	expirationAt := policy.ExpirationAt(now, "cold", 0, 30)

	require.Equal(t, now.Add(3630*time.Second).UnixNano(), expirationAt)
	require.Equal(t, int64(0), policy.ExpirationAt(now, "unconfigured", 0, 0))
}

func TestQueueChannel(t *testing.T) {
	require.Equal(t, "_QUEUES_.cold", QueueChannel("cold"))
	require.Equal(t, "_QUEUES_.cold", QueueChannel("_QUEUES_.cold"))
	require.True(t, MatchTopic("cold", "_QUEUES_.cold"))
}
