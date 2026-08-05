// Package core holds the payments domain: the payment aggregate, its ledger, and the
// operations that move it between states.
//
// It knows nothing about gRPC or Kafka. Transport lives in internal/grpcapi and publishing
// goes through libs/outbox, so this package can be tested against a database alone.
package core

import (
	"fmt"
	"time"

	"github.com/CGainsWorks/sw-event-driven-microservices/libs/pgmigrate"
)

// State is the lifecycle of a payment. Mirrors sw_edm.payments.v1.PaymentState.
type State string

const (
	// StatePending means the attempt has been recorded but the provider has not yet
	// answered. It is written and committed BEFORE the provider is called, so that a
	// crash mid-call leaves durable evidence the request may have gone out.
	//
	// Without it, a retry cannot tell "never sent" from "sent, answer lost" — and those
	// need opposite handling. It is also what lets GetPaymentByIdempotencyKey answer a
	// caller whose call timed out, which is the entire ambiguous-outcome mechanism.
	//
	// It is not exposed on the gRPC contract: callers see it as PAYMENT_STATE_UNSPECIFIED
	// with found=true, which tells them "we have your request, we do not yet know".
	StatePending State = "PENDING"

	StateAuthorized State = "AUTHORIZED"
	StateCaptured   State = "CAPTURED"
	StateVoided     State = "VOIDED"
	StateRefunded   State = "REFUNDED"
	StateFailed     State = "FAILED"
)

// EntryType is a row in the ledger. The ledger is append-only: a payment's balances are
// derived from these entries and the denormalised totals on `payments` are a cache of that
// derivation, checked by a constraint rather than trusted.
type EntryType string

const (
	EntryAuthorize EntryType = "AUTHORIZE"
	EntryCapture   EntryType = "CAPTURE"
	EntryVoid      EntryType = "VOID"
	EntryRefund    EntryType = "REFUND"
)

// Money is an amount in minor units. Integer, never float: money arithmetic on floating
// point is wrong in ways that only appear in aggregate, at which point it is a
// reconciliation problem rather than a bug report.
type Money struct {
	AmountMinor int64  `json:"amount_minor"`
	Currency    string `json:"currency"`
}

func (m Money) String() string { return fmt.Sprintf("%d %s", m.AmountMinor, m.Currency) }

// SameCurrency guards every arithmetic operation. Adding GBP to EUR is not an arithmetic
// error the type system catches, so it is caught here instead.
func (m Money) SameCurrency(other Money) bool { return m.Currency == other.Currency }

// Payment is the aggregate.
type Payment struct {
	ID             string
	OrderID        string
	CustomerID     string
	IdempotencyKey string
	State          State
	Currency       string

	AuthorizedMinor int64
	CapturedMinor   int64
	RefundedMinor   int64

	// ExpiresAt is when the authorization lapses at the provider. Nil for a payment that
	// never reached AUTHORIZED.
	ExpiresAt *time.Time
	CreatedAt time.Time

	ProviderRef string

	FailureCode      string
	FailureRetryable bool
	FailureDetail    string
}

func (p *Payment) Authorized() Money { return Money{p.AuthorizedMinor, p.Currency} }
func (p *Payment) Captured() Money   { return Money{p.CapturedMinor, p.Currency} }
func (p *Payment) Refunded() Money   { return Money{p.RefundedMinor, p.Currency} }

// SchemaSQL is the payments database.
//
// The CHECK constraints are the money invariants, enforced by PostgreSQL rather than by
// application code. Application checks have a race between reading and writing; a
// constraint does not. If a bug in this service ever tries to capture more than was
// authorized, the transaction fails rather than the customer being overcharged — which is
// the correct direction for that failure to go.
const SchemaSQL = `
CREATE TABLE IF NOT EXISTS payments (
    id                UUID        PRIMARY KEY,
    order_id          UUID        NOT NULL,
    customer_id       UUID        NOT NULL,

    -- One idempotency key maps to one authorization forever. Enforced by a unique index,
    -- not by an application check, because an application check has a window in which two
    -- concurrent retries both see "no existing payment" and both create one.
    idempotency_key   TEXT        NOT NULL UNIQUE,

    state             TEXT        NOT NULL CHECK (state IN
                                    ('PENDING','AUTHORIZED','CAPTURED','VOIDED','REFUNDED','FAILED')),
    currency          CHAR(3)     NOT NULL,

    authorized_minor  BIGINT      NOT NULL CHECK (authorized_minor >= 0),
    captured_minor    BIGINT      NOT NULL DEFAULT 0 CHECK (captured_minor >= 0),
    refunded_minor    BIGINT      NOT NULL DEFAULT 0 CHECK (refunded_minor >= 0),

    expires_at        TIMESTAMPTZ,
    provider_ref      TEXT        NOT NULL DEFAULT '',

    failure_code      TEXT        NOT NULL DEFAULT '',
    failure_retryable BOOLEAN     NOT NULL DEFAULT false,
    failure_detail    TEXT        NOT NULL DEFAULT '',

    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- The two money invariants, in the only place that can enforce them without a race.
    CONSTRAINT captured_not_over_authorized CHECK (captured_minor <= authorized_minor),
    CONSTRAINT refunded_not_over_captured   CHECK (refunded_minor  <= captured_minor)
);

CREATE INDEX IF NOT EXISTS payments_order_id_idx ON payments (order_id);

CREATE TABLE IF NOT EXISTS ledger_entries (
    id            UUID        PRIMARY KEY,
    payment_id    UUID        NOT NULL REFERENCES payments (id),
    entry_type    TEXT        NOT NULL CHECK (entry_type IN ('AUTHORIZE','CAPTURE','VOID','REFUND')),
    amount_minor  BIGINT      NOT NULL CHECK (amount_minor >= 0),
    currency      CHAR(3)     NOT NULL,

    -- The operation that produced this entry. Together with entry_type it is unique, so a
    -- retried capture cannot write a second ledger row even if every other guard fails.
    -- Defence in depth: the idempotency table should already have stopped it.
    idempotency_key TEXT      NOT NULL,

    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT ledger_entry_once UNIQUE (entry_type, idempotency_key)
);

CREATE INDEX IF NOT EXISTS ledger_entries_payment_id_idx ON ledger_entries (payment_id, created_at);
`

// Migrations returns every migration this service needs, in order. The shared tables come
// from the packages that own their behaviour rather than being copied in.
func Migrations(outbox, idem pgmigrate.Migration) []pgmigrate.Migration {
	return []pgmigrate.Migration{
		outbox,
		idem,
		{Name: "0003_payments", SQL: SchemaSQL},
	}
}
