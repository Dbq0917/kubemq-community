package array

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/kubemq-io/kubemq-community/pkg/client"
	"github.com/kubemq-io/kubemq-community/pkg/entities"
	pb "github.com/kubemq-io/protobuf/go"
)

// BroadcastGroup binds a named subscription group to a logical channel. A
// group is the unit of independent consumption: every group keeps its own
// durable offset and receives the full message stream, while consumers inside
// the same group load-balance messages between themselves.
type BroadcastGroup struct {
	Channel string
	Name    string
}

func (g *BroadcastGroup) key() string {
	return broadcastGroupKey(g.Channel, g.Name)
}

func broadcastGroupKey(channel, group string) string {
	return fmt.Sprintf("%s|%s", channel, group)
}

// broadcastRegistry keeps track of the groups declared for each broadcast
// channel. The streaming server is the source of truth for offsets, the
// registry only makes the groups explicit and guards against reusing a group
// name on a different channel.
type broadcastRegistry struct {
	mu     sync.RWMutex
	groups map[string]*BroadcastGroup
}

func newBroadcastRegistry() *broadcastRegistry {
	return &broadcastRegistry{
		groups: make(map[string]*BroadcastGroup),
	}
}

func (r *broadcastRegistry) register(channel, group string) error {
	if err := client.ValidateBroadcastGroup(group); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := broadcastGroupKey(channel, group)
	if existing, ok := r.groups[key]; ok {
		if existing.Channel != channel || existing.Name != group {
			return entities.ErrBroadcastGroupConflict
		}
		return nil
	}
	r.groups[key] = &BroadcastGroup{Channel: channel, Name: group}
	return nil
}

func (r *broadcastRegistry) list(channel string) []*BroadcastGroup {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var list []*BroadcastGroup
	for _, group := range r.groups {
		if channel == "" || group.Channel == channel {
			list = append(list, group)
		}
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].Channel != list[j].Channel {
			return list[i].Channel < list[j].Channel
		}
		return list[i].Name < list[j].Name
	})
	return list
}

func (r *broadcastRegistry) exists(channel, group string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.groups[broadcastGroupKey(channel, group)]
	return ok
}

// RegisterBroadcastGroup declares a subscription group on a broadcast channel.
// Registering the same group again is idempotent.
func (a *Array) RegisterBroadcastGroup(ctx context.Context, channel, group string) error {
	if a.isShutdown.Load() {
		return entities.ErrShutdownMode
	}
	return a.broadcastGroups.register(channel, group)
}

// ListBroadcastGroups returns the registered groups, optionally filtered by
// channel.
func (a *Array) ListBroadcastGroups(ctx context.Context, channel string) []*BroadcastGroup {
	return a.broadcastGroups.list(channel)
}

// SendBroadcastMessage stores one copy of the message on the broadcast
// subject. Every registered group receives it independently without any
// message duplication.
func (a *Array) SendBroadcastMessage(ctx context.Context, msg *pb.QueueMessage) (*pb.SendQueueMessageResult, error) {
	if a.isShutdown.Load() {
		return nil, entities.ErrShutdownMode
	}
	qc, err := a.NewQueueClientFromPool(broadcastSenderPoolKey(msg.Channel))
	if err != nil {
		return nil, err
	}
	defer a.queueClientsPool.ReleaseClient(broadcastSenderPoolKey(msg.Channel))
	return qc.SendBroadcastMessage(ctx, msg), nil
}

// ReceiveBroadcastMessages pulls messages for one group. The group must be
// registered first. Each group advances its own durable offset, so groups never
// steal messages from each other.
func (a *Array) ReceiveBroadcastMessages(ctx context.Context, req *client.BroadcastReceiveRequest) (*client.BroadcastReceiveResponse, error) {
	if a.isShutdown.Load() {
		return nil, entities.ErrShutdownMode
	}
	if err := client.ValidateBroadcastGroup(req.Group); err != nil {
		return nil, err
	}
	if !a.broadcastGroups.exists(req.Channel, req.Group) {
		return nil, entities.ErrInvalidBroadcastGroup
	}
	qc, err := a.NewQueueClientFromPool(broadcastReceiverPoolKey(req.Channel, req.Group))
	if err != nil {
		return nil, err
	}
	response, err := qc.ReceiveBroadcastMessages(ctx, req)
	if err != nil {
		a.queueClientsPool.ReleaseClient(broadcastReceiverPoolKey(req.Channel, req.Group))
		return nil, err
	}
	return response, nil
}

// ReleaseBroadcastReceiver returns the pooled client used by a group receive
// back to the pool once the response is closed.
func (a *Array) ReleaseBroadcastReceiver(channel, group string) {
	a.queueClientsPool.ReleaseClient(broadcastReceiverPoolKey(channel, group))
}

func broadcastSenderPoolKey(channel string) string {
	return "broadcast-sender-" + channel
}

func broadcastReceiverPoolKey(channel, group string) string {
	return "broadcast-receiver-" + channel + "-" + group
}
