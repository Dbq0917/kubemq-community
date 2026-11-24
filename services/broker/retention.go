package broker

import (
	"time"

	snats "github.com/kubemq-io/broker/server/stan/server"
	"github.com/kubemq-io/broker/server/stan/stores"
	"github.com/kubemq-io/kubemq-community/config"
	"github.com/kubemq-io/kubemq-community/pkg/retention"
)

func applyTopicRetentionLimits(options *snats.Options, queueConfig *config.QueueConfig) {
	if options == nil || queueConfig == nil {
		return
	}

	for topic, seconds := range queueConfig.TopicRetentionSeconds {
		if seconds <= 0 {
			continue
		}
		limits := &stores.ChannelLimits{}
		limits.MaxAge = time.Duration(seconds) * time.Second
		options.AddPerChannel(retention.QueueChannel(topic), limits)
	}

	if queueConfig.DefaultRetentionSeconds > 0 {
		defaultChannel := retention.QueueChannel(">")
		if _, exists := options.PerChannel[defaultChannel]; !exists {
			limits := &stores.ChannelLimits{}
			limits.MaxAge = time.Duration(queueConfig.DefaultRetentionSeconds) * time.Second
			options.AddPerChannel(defaultChannel, limits)
		}
	}
}
