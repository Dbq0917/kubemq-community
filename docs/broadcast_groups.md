# Multi-Group Broadcast Queues

## Problem

Classic KubeMQ queues are point-to-point: a message stored on a queue channel
is delivered to exactly one consumer. When several independent downstream
systems (for example risk control, billing and notification) all need the full
message stream, the only option was to publish one copied message per system on
separate channels.

Multi-group broadcast keeps a single copy of every message while letting each
subscription group consume the whole stream independently, with its own
progress offset.

## Model

- A **broadcast channel** stores messages once on a dedicated streaming
  subject prefixed with `_BROADCAST_.`.
- A **group** is a named consumer collective on a broadcast channel. Every
  group gets every message.
- Each group binds to its own **durable subscription**, so the streaming
  server maintains one independent ack offset per group. One group consuming
  or acking never advances or blocks another group.
- Multiple consumers inside the same group share that group offset and
  load-balance messages between themselves.

Point-to-point queues keep using the existing `_QUEUES_.` subjects and
`QueueSubscribe` flow untouched. Broadcast traffic uses separate subjects, so
legacy queue subscriptions require no changes and cannot see broadcast
messages.

## Components

- `pkg/client/broadcast.go`
  - `QueueClient.SendBroadcastMessage` publishes one message copy on the
    broadcast subject.
  - `QueueClient.ReceiveBroadcastMessages` pulls messages for one group through
    a durable queue subscription named after the channel and group.
  - `BroadcastReceiveResponse` exposes `AckRange`, `AckAll`,
    `ActiveSequences`, `Messages` and `Close`.
- `services/array/broadcast.go`
  - Group registry (`RegisterBroadcastGroup`, `ListBroadcastGroups`).
  - Array-level send/receive wrappers pooled like the existing queue clients.
- `interfaces/rest/broadcast_handlers.go` exposes the HTTP endpoints.

## HTTP API

Register a group (idempotent for the same channel/group pair):

```bash
curl -X POST http://localhost:8080/broadcast/group \
  -d '{"channel":"orders","group":"risk-control"}'
```

Publish a message once; every registered group receives it:

```bash
curl -X POST http://localhost:8080/broadcast/send \
  -d '{"MessageID":"m1","ClientID":"producer","Channel":"orders","Body":"cGF5bG9hZA=="}'
```

Pull messages for a group:

```bash
curl -X POST http://localhost:8080/broadcast/receive \
  -d '{"channel":"orders","group":"risk-control","client_id":"risk-1",
       "max_number_of_messages":100,"wait_time_seconds":2,"auto_ack":false}'
```

## Worked example: three groups

Register the three required groups, then publish messages:

```bash
for group in risk-control billing notification; do
  curl -X POST http://localhost:8080/broadcast/group \
    -d "{\"channel\":\"orders\",\"group\":\"$group\"}"
done
```

If 100 messages are published to `orders`, each of the three groups receives
all 100 messages in order. Missing one group is harmless to the others: it
simply resumes from its own durable offset the next time it connects.

## Guarantees and limits

- At-least-once delivery per group; consumers must ack handled sequences.
- Independent offsets per group, independent of group availability.
- No payload duplication at publish time; the streaming server fans messages
  out through per-group durable state.
- Retention still follows the broker store configuration
  (`Store.MaxRetention`, `MaxPurgeInactive`). A group that stays inactive
  longer than the configured inactivity window can have its durable state
  reaped by the streaming server, so size retention/windows to the slowest
  group.
