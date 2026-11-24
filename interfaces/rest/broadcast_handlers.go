package rest

import (
	"context"
	"net/http"

	"github.com/kubemq-io/kubemq-community/pkg/client"
	pb "github.com/kubemq-io/protobuf/go"
	"github.com/labstack/echo/v4"
)

type registerBroadcastGroupRequest struct {
	Channel string `json:"channel"`
	Group   string `json:"group"`
}

type receiveBroadcastRequest struct {
	Channel             string `json:"channel"`
	Group               string `json:"group"`
	ClientID            string `json:"client_id"`
	MaxNumberOfMessages int32  `json:"max_number_of_messages"`
	WaitTimeSeconds     int32  `json:"wait_time_seconds"`
	AutoAck             bool   `json:"auto_ack"`
}

// handlerRegisterBroadcastGroup declares a subscription group on a broadcast
// channel. Every registered group keeps an independent offset.
func (s *Server) handlerRegisterBroadcastGroup(c echo.Context) error {
	ctx, cancel := context.WithCancel(c.Request().Context())
	defer cancel()
	res := NewResponse(c)
	req := &registerBroadcastGroupRequest{}
	if err := c.Bind(req); err != nil {
		return res.SetError(err).SetHttpCode(http.StatusBadRequest).Send()
	}
	if err := s.services.Array.RegisterBroadcastGroup(ctx, req.Channel, req.Group); err != nil {
		return res.SetError(err).SetHttpCode(http.StatusBadRequest).Send()
	}
	return res.SetResponseBody(req).Send()
}

// handlerSendBroadcastMessage stores one copy of a message on the broadcast
// subject. All groups receive it without payload duplication.
func (s *Server) handlerSendBroadcastMessage(c echo.Context) error {
	ctx, cancel := context.WithCancel(c.Request().Context())
	defer cancel()
	res := NewResponse(c)
	msg := &pb.QueueMessage{}
	if err := c.Bind(msg); err != nil {
		return res.SetError(err).SetHttpCode(http.StatusBadRequest).Send()
	}
	result, err := s.services.Array.SendBroadcastMessage(ctx, msg)
	if err != nil {
		return res.SetError(err).SetHttpCode(http.StatusBadRequest).Send()
	}
	return res.SetResponseBody(result).Send()
}

// handlerReceiveBroadcastMessages pulls messages for one broadcast group.
func (s *Server) handlerReceiveBroadcastMessages(c echo.Context) error {
	ctx, cancel := context.WithCancel(c.Request().Context())
	defer cancel()
	res := NewResponse(c)
	req := &receiveBroadcastRequest{}
	if err := c.Bind(req); err != nil {
		return res.SetError(err).SetHttpCode(http.StatusBadRequest).Send()
	}
	response, err := s.services.Array.ReceiveBroadcastMessages(ctx, &client.BroadcastReceiveRequest{
		Channel:             req.Channel,
		Group:               req.Group,
		ClientID:            req.ClientID,
		MaxNumberOfMessages: req.MaxNumberOfMessages,
		WaitTimeSeconds:     req.WaitTimeSeconds,
		AutoAck:             req.AutoAck,
	})
	if err != nil {
		return res.SetError(err).SetHttpCode(http.StatusBadRequest).Send()
	}
	defer func() {
		response.Close()
		s.services.Array.ReleaseBroadcastReceiver(req.Channel, req.Group)
	}()
	return res.SetResponseBody(map[string]interface{}{
		"messages":         response.Messages(),
		"active_sequences": response.ActiveSequences(),
	}).Send()
}
