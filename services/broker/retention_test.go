package broker

import (
	"testing"
	"time"

	snats "github.com/kubemq-io/broker/server/stan/server"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/stretchr/testify/require"
)

func TestApplyTopicRetentionLimits(t *testing.T) {
	options := snats.GetDefaultOptions()
	queueConfig := &config.QueueConfig{
		DefaultRetentionSeconds: 259200,
		TopicRetentionSeconds: map[string]int32{
			"hot":  259200,
			"cold": 3600,
		},
	}

	applyTopicRetentionLimits(options, queueConfig)
	require.NoError(t, options.StoreLimits.Build())

	require.Equal(t, 259200*time.Second, options.PerChannel["_QUEUES_.hot"].MaxAge)
	require.Equal(t, time.Hour, options.PerChannel["_QUEUES_.cold"].MaxAge)
	require.Equal(t, 259200*time.Second, options.PerChannel["_QUEUES_.>"].MaxAge)
}

func TestApplyTopicRetentionLimitsPreservesExplicitDefaultWildcard(t *testing.T) {
	options := snats.GetDefaultOptions()
	queueConfig := &config.QueueConfig{
		DefaultRetentionSeconds: 259200,
		TopicRetentionSeconds: map[string]int32{
			">": 3600,
		},
	}

	applyTopicRetentionLimits(options, queueConfig)

	require.Equal(t, time.Hour, options.PerChannel["_QUEUES_.>"].MaxAge)
}
