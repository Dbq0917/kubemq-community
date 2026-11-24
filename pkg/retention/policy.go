package retention

import (
	"strings"
	"time"
)

const QueueChannelPrefix = "_QUEUES_."

type Policy struct {
	DefaultSeconds int32
	TopicSeconds   map[string]int32
}

func (p Policy) ResolveSeconds(topic string, messageSeconds int32) int32 {
	if messageSeconds > 0 {
		return messageSeconds
	}

	topicSeconds := p.topicSeconds(topic)
	return topicSeconds
}

func (p Policy) ExpirationAt(now time.Time, topic string, messageSeconds, delaySeconds int32) int64 {
	retentionSeconds := p.ResolveSeconds(topic, messageSeconds)
	if retentionSeconds <= 0 {
		return 0
	}

	return now.Add(time.Duration(int64(retentionSeconds)+int64(delaySeconds)) * time.Second).UnixNano()
}

func (p Policy) topicSeconds(topic string) int32 {
	topic = strings.TrimPrefix(topic, QueueChannelPrefix)
	if seconds, ok := p.TopicSeconds[topic]; ok && seconds > 0 {
		return seconds
	}

	bestPattern := ""
	bestSeconds := int32(0)
	for pattern, seconds := range p.TopicSeconds {
		if seconds <= 0 || pattern == topic {
			continue
		}
		if !MatchTopic(pattern, topic) {
			continue
		}

		if bestSeconds == 0 || specificity(pattern) > specificity(bestPattern) ||
			(specificity(pattern) == specificity(bestPattern) && seconds < bestSeconds) {
			bestPattern = pattern
			bestSeconds = seconds
		}
	}

	if bestSeconds > 0 {
		return bestSeconds
	}
	if p.DefaultSeconds > 0 {
		return p.DefaultSeconds
	}
	return 0
}

func MatchTopic(pattern, topic string) bool {
	pattern = strings.TrimPrefix(pattern, QueueChannelPrefix)
	topic = strings.TrimPrefix(topic, QueueChannelPrefix)

	patternParts := strings.Split(pattern, ".")
	topicParts := strings.Split(topic, ".")

	for i, part := range patternParts {
		switch part {
		case ">":
			return i == len(patternParts)-1 && i < len(topicParts)
		case "*":
			if i >= len(topicParts) {
				return false
			}
		default:
			if i >= len(topicParts) || part != topicParts[i] {
				return false
			}
		}
	}

	return len(patternParts) == len(topicParts)
}

func QueueChannel(topic string) string {
	if strings.HasPrefix(topic, QueueChannelPrefix) {
		return topic
	}
	return QueueChannelPrefix + topic
}

func specificity(pattern string) int {
	score := 0
	for _, part := range strings.Split(pattern, ".") {
		switch part {
		case ">":
			score++
		case "*":
			score += 10
		default:
			score += 100
		}
	}
	return score
}
