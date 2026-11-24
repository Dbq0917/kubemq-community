package client

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/kubemq-io/broker/client/stan"
	"github.com/kubemq-io/kubemq-community/pkg/entities"
	"github.com/kubemq-io/kubemq-community/services/metrics"
	pb "github.com/kubemq-io/protobuf/go"
	"go.uber.org/atomic"
)

// Broadcast messages are stored once on a dedicated STAN subject that is fully
// isolated from the point-to-point queue subjects. Every subscription group
// attaches its own durable subscription to the same subject, so the streaming
// server tracks one independent ack offset per group while the payload is only
// persisted once.
const broadcastAckWait = 365 * 24 * time.Hour

// broadcastSubjectName returns the STAN subject used to store broadcast
// messages for a given logical channel.
func broadcastSubjectName(channel string) string {
	return prefixBroadcast + channel
}

// broadcastDurableName returns the per-group durable name. Each durable keeps
// its own independent position on the shared subject.
func broadcastDurableName(channel, group string) string {
	return fmt.Sprintf("%s%s.%s", prefixBroadcast, channel, group)
}

// ValidateBroadcastGroup enforces the same naming rules used for channels.
func ValidateBroadcastGroup(group string) error {
	if group == "" {
		return entities.ErrInvalidBroadcastGroup
	}
	if strings.HasSuffix(group, ".") {
		return entities.ErrInvalidBroadcastGroup
	}
	if strings.Contains(group, " ") {
		return entities.ErrInvalidBroadcastGroup
	}
	if strings.Contains(group, "*") || strings.Contains(group, ">") {
		return entities.ErrInvalidBroadcastGroup
	}
	return nil
}

// BroadcastReceiveRequest describes a pull request from a single broadcast
// group. Within a group multiple consumers share the same durable position and
// load-balance messages between themselves.
type BroadcastReceiveRequest struct {
	Channel             string
	Group               string
	ClientID            string
	MaxNumberOfMessages int32
	WaitTimeSeconds     int32
	AutoAck             bool
}

// BroadcastReceiveResponse is the result of a broadcast pull. Closing it
// releases the underlying STAN subscription; the durable position is retained
// server side until the delivered messages are acked.
type BroadcastReceiveResponse struct {
	mu                  sync.Mutex
	qc                  *QueueClient
	request             *BroadcastReceiveRequest
	subscription        stan.Subscription
	messages            []*pb.QueueMessage
	unAckedRawMessages  map[uint64]*stan.Msg
	unAckedSequenceList []uint64
	itemsReady          chan struct{}
	waitCtx             context.Context
	waitCancel          context.CancelFunc
	closed              *atomic.Bool
	acceptMessages      *atomic.Bool
}

func newBroadcastReceiveResponse(qc *QueueClient, request *BroadcastReceiveRequest, waitCtx context.Context, waitCancel context.CancelFunc) *BroadcastReceiveResponse {
	return &BroadcastReceiveResponse{
		qc:                 qc,
		request:            request,
		unAckedRawMessages: make(map[uint64]*stan.Msg),
		itemsReady:         make(chan struct{}, 1),
		waitCtx:            waitCtx,
		waitCancel:         waitCancel,
		closed:             atomic.NewBool(false),
		acceptMessages:     atomic.NewBool(true),
	}
}

func (r *BroadcastReceiveResponse) addRawMessage(rawMsg *stan.Msg) {
	if r.closed.Load() || !r.acceptMessages.Load() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.unAckedRawMessages[rawMsg.Sequence]; exists {
		return
	}
	r.unAckedRawMessages[rawMsg.Sequence] = rawMsg
	r.unAckedSequenceList = append(r.unAckedSequenceList, rawMsg.Sequence)
	if int32(len(r.unAckedRawMessages)) >= r.request.MaxNumberOfMessages {
		r.acceptMessages.Store(false)
		select {
		case r.itemsReady <- struct{}{}:
		default:
		}
	}
}

func (r *BroadcastReceiveResponse) processRawMessages() {
	r.mu.Lock()
	defer r.mu.Unlock()
	var expiredSequences []uint64
	for _, sequence := range r.unAckedSequenceList {
		rawMsg, ok := r.unAckedRawMessages[sequence]
		if !ok {
			continue
		}
		msg, err := unmarshalToQueueMessage(rawMsg.Data, rawMsg.Timestamp, rawMsg.Sequence)
		if err != nil {
			expiredSequences = append(expiredSequences, sequence)
			_ = rawMsg.Ack()
			continue
		}
		addReceiveCount(msg)
		if isExpired(msg) {
			metrics.ReportExpired(msg.Channel, 1)
			expiredSequences = append(expiredSequences, sequence)
			_ = rawMsg.Ack()
			continue
		}
		r.messages = append(r.messages, msg)
	}
	for _, sequence := range expiredSequences {
		delete(r.unAckedRawMessages, sequence)
	}
}

// Messages returns the messages delivered to this broadcast group.
func (r *BroadcastReceiveResponse) Messages() []*pb.QueueMessage {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.messages
}

// AckRange advances the group durable position past the given message
// sequences. Sequences unknown to this response are ignored.
func (r *BroadcastReceiveResponse) AckRange(sequences []uint64) error {
	if r.closed.Load() {
		return fmt.Errorf("cannot ack messages with a closed broadcast response")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, sequence := range sequences {
		rawMsg, ok := r.unAckedRawMessages[sequence]
		if !ok {
			continue
		}
		if err := rawMsg.Ack(); err != nil {
			return err
		}
		delete(r.unAckedRawMessages, sequence)
	}
	return nil
}

// AckAll acks every message delivered by this response.
func (r *BroadcastReceiveResponse) AckAll() error {
	if r.closed.Load() {
		return fmt.Errorf("cannot ack messages with a closed broadcast response")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for sequence, rawMsg := range r.unAckedRawMessages {
		if err := rawMsg.Ack(); err != nil {
			return err
		}
		delete(r.unAckedRawMessages, sequence)
	}
	return nil
}

// ActiveSequences returns the sequences still waiting for an ack by this
// group.
func (r *BroadcastReceiveResponse) ActiveSequences() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	var sequences []uint64
	for sequence := range r.unAckedRawMessages {
		sequences = append(sequences, sequence)
	}
	return sequences
}

// Close releases the subscription while keeping the durable position, so the
// next receive by the same group resumes where it left off.
func (r *BroadcastReceiveResponse) Close() {
	if !r.closed.CompareAndSwap(false, true) {
		return
	}
	r.waitCancel()
	if r.subscription != nil {
		_ = r.subscription.Close()
	}
}

// SendBroadcastMessage stores a single copy of the message on the broadcast
// subject. Existing point-to-point queues are untouched because a different
// subject prefix is used.
func (qc *QueueClient) SendBroadcastMessage(ctx context.Context, msg *pb.QueueMessage) *pb.SendQueueMessageResult {
	result := qc.preSendQueueMessage(msg)
	if result.IsError {
		return result
	}
	channel := broadcastSubjectName(msg.Channel)
	data, _ := msg.Marshal()
	if err := qc.queueConn.Publish(channel, data); err != nil {
		result.IsError = true
		result.Error = err.Error()
		return result
	}
	result.SentAt = time.Now().UnixNano()
	result.ExpirationAt = msg.Attributes.ExpirationAt
	result.DelayedTo = msg.Attributes.DelayedTo
	return result
}

func (qc *QueueClient) checkAndEnforceBroadcastReceiveRequest(request *BroadcastReceiveRequest) error {
	if err := qc.checkChannelNameValidity(request.Channel); err != nil {
		return err
	}
	if err := ValidateBroadcastGroup(request.Group); err != nil {
		return err
	}
	if request.ClientID == "" {
		return entities.ErrInvalidClientID
	}
	if request.MaxNumberOfMessages > qc.policyCfg.MaxNumberOfMessages || request.MaxNumberOfMessages < 1 {
		return entities.ErrInvalidMaxMessages
	}
	if request.WaitTimeSeconds > qc.policyCfg.MaxWaitTimeoutSeconds || request.WaitTimeSeconds < 0 {
		return entities.ErrInvalidWaitTimeout
	}
	if request.WaitTimeSeconds == 0 {
		request.WaitTimeSeconds = qc.policyCfg.DefaultWaitTimeoutSeconds
	}
	return nil
}

// ReceiveBroadcastMessages pulls messages for one group. A queue subscription
// bound to the group durable is used: members of the same group load-balance
// messages while different groups each keep an independent ack offset and all
// receive the full message stream.
func (qc *QueueClient) ReceiveBroadcastMessages(ctx context.Context, request *BroadcastReceiveRequest) (*BroadcastReceiveResponse, error) {
	qc.mu.Lock()
	defer qc.mu.Unlock()

	if !qc.isUp.Load() {
		return nil, entities.ErrConnectionNoAvailable
	}
	if err := qc.checkAndEnforceBroadcastReceiveRequest(request); err != nil {
		return nil, err
	}

	waitTimeout := time.Duration(request.WaitTimeSeconds) * time.Second
	waitCtx, waitCancel := context.WithTimeout(context.Background(), waitTimeout)
	response := newBroadcastReceiveResponse(qc, request, waitCtx, waitCancel)

	subject := broadcastSubjectName(request.Channel)
	durable := broadcastDurableName(request.Channel, request.Group)
	maxInflight := int(request.MaxNumberOfMessages)
	if maxInflight < 1 {
		maxInflight = 1
	}
	sub, err := qc.queueConn.QueueSubscribe(subject, durable, func(rawMsg *stan.Msg) {
		if waitCtx.Err() != nil {
			return
		}
		response.addRawMessage(rawMsg)
	}, stan.StartAt(4), stan.DurableName(durable), stan.MaxInflight(maxInflight), stan.AckWait(broadcastAckWait), stan.SetManualAckMode())
	if err != nil {
		waitCancel()
		return nil, entities.ErrRegisterQueueSubscription
	}
	response.subscription = sub

	select {
	case <-response.itemsReady:
	case <-waitCtx.Done():
	case <-ctx.Done():
		response.Close()
		return nil, fmt.Errorf("context canceled during broadcast receive")
	}

	response.processRawMessages()
	if request.AutoAck {
		if err := response.AckAll(); err != nil {
			response.Close()
			return nil, err
		}
	}
	return response, nil
}
