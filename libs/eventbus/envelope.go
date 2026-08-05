// Package eventbus is the shared Kafka layer: the event envelope, a producer that refuses
// to publish an event that does not match its schema, and a consumer that handles
// redelivery, retries and dead-lettering.
//
// Every service uses this package. Nothing in a service writes to Kafka directly.
package eventbus

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/CGainsWorks/sw-event-driven-microservices/events"
)

// Timestamp is time.Time that marshals the way the event schemas demand: UTC, with the
// trailing Z, at millisecond precision.
//
// This is a distinct type rather than a convention because a convention is something a
// future contributor forgets. RFC 3339 permits a numeric offset, and the moment two
// services in different zones emit the same instant with different offsets, string
// comparison of timestamps starts returning wrong answers. Making the wire format a
// property of the type means it cannot be got wrong at a call site.
type Timestamp time.Time

const timestampLayout = "2006-01-02T15:04:05.000Z"

func (t Timestamp) MarshalJSON() ([]byte, error) {
	return []byte(`"` + time.Time(t).UTC().Format(timestampLayout) + `"`), nil
}

func (t *Timestamp) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if s == "" || s == "null" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return fmt.Errorf("eventbus: parsing timestamp %q: %w", s, err)
	}
	*t = Timestamp(parsed.UTC())
	return nil
}

func (t Timestamp) Time() time.Time { return time.Time(t).UTC() }

// Now returns the current instant already truncated to the precision the wire format
// carries. Truncating here rather than at marshal time means a timestamp compares equal to
// its own round-trip, which is the sort of thing that otherwise fails one test in fifty.
func Now() Timestamp { return Timestamp(time.Now().UTC().Truncate(time.Millisecond)) }

// Envelope is the outer shape of every event on every topic. See
// events/schemas/envelope.schema.json, which is the authority; this struct must match it
// and TestEnvelopeMatchesSchema proves that it does.
type Envelope struct {
	// EventID is the deduplication key. A redelivery carries the same value; a genuinely
	// new event never reuses one. Everything this system claims about exactly-once
	// processing rests on this field.
	EventID string `json:"event_id"`

	EventType    string `json:"event_type"`
	EventVersion int    `json:"event_version"`

	// OccurredAt is when the fact became true in the domain, not when it was published.
	// Clock skew between services means it must never be used for ordering.
	OccurredAt Timestamp `json:"occurred_at"`

	// AggregateID doubles as the Kafka message key, which is what gives per-aggregate
	// ordering. It must equal the primary id inside Data.
	AggregateID   string `json:"aggregate_id"`
	AggregateType string `json:"aggregate_type"`

	// CorrelationID is constant for the whole business transaction and is copied onto
	// every event and gRPC call that follows from it.
	CorrelationID string `json:"correlation_id"`

	// CausationID is the EventID of the event that directly caused this one. Empty when
	// the event originated from an external request or a timer.
	CausationID string `json:"causation_id,omitempty"`

	Producer string `json:"producer"`

	Data json.RawMessage `json:"data"`
}

// NewEnvelope builds an envelope with a fresh event id and the current time.
//
// data is marshalled here rather than being taken as json.RawMessage so that a caller
// cannot pass bytes that are not valid JSON and have it surface as a Kafka error three
// layers away.
func NewEnvelope(eventType, aggregateType, aggregateID, producer, correlationID, causationID string, data any) (*Envelope, error) {
	encoded, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("eventbus: marshalling %s payload: %w", eventType, err)
	}
	return &Envelope{
		EventID:       uuid.NewString(),
		EventType:     eventType,
		EventVersion:  1,
		OccurredAt:    Now(),
		AggregateID:   aggregateID,
		AggregateType: aggregateType,
		CorrelationID: correlationID,
		CausationID:   causationID,
		Producer:      producer,
		Data:          encoded,
	}, nil
}

// Validate checks the envelope against the JSON Schema for its own event type.
//
// This runs on every publish, not only in tests. A schema registry rejects an incompatible
// *schema*; it does not stop a service publishing a message that fails its own schema.
// Validating here means a bug that would put a malformed event into a topic — where it
// stays for seven days and breaks every consumer that reads it — fails at the producer
// instead, in the transaction that is about to be rolled back anyway.
//
// The cost is one validation per published event. Measured at roughly 8 microseconds for
// an OrderPlaced on the development machine; see scripts/bench_validation.sh.
func (e *Envelope) Validate() error {
	sch, err := events.Validator(e.EventType)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("eventbus: marshalling envelope %s: %w", e.EventID, err)
	}
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return fmt.Errorf("eventbus: re-reading envelope %s: %w", e.EventID, err)
	}
	if err := sch.Validate(doc); err != nil {
		return fmt.Errorf("eventbus: event %s (%s) does not match its schema: %w", e.EventID, e.EventType, err)
	}
	return nil
}

// UnmarshalData decodes the payload into a typed struct.
func (e *Envelope) UnmarshalData(into any) error {
	if err := json.Unmarshal(e.Data, into); err != nil {
		return fmt.Errorf("eventbus: decoding %s payload: %w", e.EventType, err)
	}
	return nil
}

// Topic returns the topic an event belongs on, derived from its type:
// "orders.v1.OrderPlaced" -> "orders.v1.events".
//
// Derived rather than configured. A mapping table is one more thing to keep in step with
// events/topics.yml, and the derivation is exactly the rule that file already documents.
func (e *Envelope) Topic() (string, error) {
	parts := strings.Split(e.EventType, ".")
	if len(parts) != 3 {
		return "", fmt.Errorf("eventbus: %q is not a <context>.v<major>.<Name> event type", e.EventType)
	}
	return parts[0] + "." + parts[1] + ".events", nil
}
