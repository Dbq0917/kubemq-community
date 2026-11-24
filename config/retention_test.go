package config

import (
	"testing"

	"github.com/ghodss/yaml"
	"github.com/stretchr/testify/require"
)

func TestQueueRetentionConfigUnmarshal(t *testing.T) {
	payload := []byte(`
queue:
  defaultRetentionSeconds: 259200
  topicRetentionSeconds:
    hot: 259200
    cold: 3600
`)
	cfg := GetDefaultConfig()
	require.NoError(t, yaml.Unmarshal(payload, cfg))

	require.Equal(t, int32(259200), cfg.Queue.DefaultRetentionSeconds)
	require.Equal(t, int32(259200), cfg.Queue.TopicRetentionSeconds["hot"])
	require.Equal(t, int32(3600), cfg.Queue.TopicRetentionSeconds["cold"])
	require.NoError(t, cfg.Queue.Validate())
}

func TestQueueRetentionConfigValidate(t *testing.T) {
	cfg := defaultQueueConfig()
	cfg.TopicRetentionSeconds = map[string]int32{"cold": -1}
	require.Error(t, cfg.Validate())
}
