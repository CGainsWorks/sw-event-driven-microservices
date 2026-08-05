// Package core holds the orders domain: the order aggregate, its lifecycle, and the
// fulfilment saga that drives it.
//
// The saga lives here rather than in a service of its own because the saga's state IS the
// order's state. Splitting them would create an invariant spanning two services and require
// a distributed transaction to keep the order status and the saga status in step — the
// exact thing this architecture exists to avoid. See docs/service_boundaries.md.
package core

import (
	"time"

	"github.com/CGainsWorks/sw-event-driven-microservices/libs/pgmigrate"
)

// State is the order lifecycle. Transitions only ever move forward through this graph:
//
//	PENDING -> AWAITING_PAYMENT -> CONFIRMED
//	                            -> CANCELLED
//
// CONFIRMED and CANCELLED are terminal.
type State string

const (
	StatePending         State = "PENDING"
	StateAwaitingPayment State = "AWAITING_PAYMENT"
	StateConfirmed       State = "CONFIRMED"
	StateCancelled       State = "CANCELLED"
)

// Terminal reports whether the order can still change.
func (s State) Terminal() bool { return s == StateConfirmed || s == StateCancelled }

// Money is an amount in minor units. Duplicated from the payments domain rather than
// shared: a shared money type between two services is a shared library that must be
// versioned and released in lockstep, which is a coupling neither service wants for
// eighteen lines of code.
type Money struct {
	AmountMinor int64  `json:"amount_minor"`
	Currency    string `json:"currency"`
}

// Line is one SKU within an order. Immutable once the order is placed.
type Line struct {
	SKU        string `json:"sku"`
	Quantity   uint32 `json:"quantity"`
	UnitAmount Money  `json:"unit_amount"`
}

// Order is the aggregate.
type Order struct {
	ID            string
	CustomerID    string
	CustomerEmail string
	State         State
	Total         Money
	Lines         []Line

	CorrelationID string

	PaymentID     string
	ReservationID string

	// PaymentMethodToken is an opaque handle for a stored instrument. It is persisted
	// because a saga resumed after a restart must be able to retry the authorize step,
	// and it cannot do that without the token it was given. This system never sees,
	// transmits or stores a card number; anything resembling one arriving here is a bug
	// in the caller.
	PaymentMethodToken string

	CancellationReason string
	FailedStep         string

	CreatedAt time.Time
	UpdatedAt time.Time
}

// SagaState is where a saga instance has got to.
type SagaState string

const (
	SagaRunning      SagaState = "RUNNING"
	SagaCompensating SagaState = "COMPENSATING"
	SagaCompleted    SagaState = "COMPLETED"
	SagaCompensated  SagaState = "COMPENSATED"

	// SagaStuck means a compensation failed. The system cannot go forward and cannot go
	// back, so it stops and asks for a human rather than guessing. This is the one state
	// that is never resolved automatically.
	SagaStuck SagaState = "STUCK"
)

// StepState is the outcome of one saga step.
type StepState string

const (
	StepPending StepState = "PENDING"
	StepDone    StepState = "DONE"
	StepFailed  StepState = "FAILED"

	// StepUnknown is the important one. A step whose call timed out has NOT failed — the
	// callee may well have done the work. Recording it as UNKNOWN rather than FAILED is
	// what stops the saga compensating something that succeeded, or confirming something
	// that did not. It is resolved by asking the callee, never by assuming.
	StepUnknown StepState = "UNKNOWN"

	StepCompensated StepState = "COMPENSATED"
)

// SchemaSQL is the orders database.
//
// Note that saga_instances has a foreign key to orders and a UNIQUE constraint on
// order_id: an order has at most one saga, and both rows are written in the same
// transaction. That is what makes "the saga's state is the order's state" true in the
// schema rather than merely in the documentation.
const SchemaSQL = `
CREATE TABLE IF NOT EXISTS orders (
    id                  UUID        PRIMARY KEY,
    customer_id         UUID        NOT NULL,
    customer_email      TEXT        NOT NULL,
    state               TEXT        NOT NULL CHECK (state IN
                                      ('PENDING','AWAITING_PAYMENT','CONFIRMED','CANCELLED')),
    total_minor         BIGINT      NOT NULL CHECK (total_minor > 0),
    currency            CHAR(3)     NOT NULL,
    correlation_id      UUID        NOT NULL,

    payment_id          UUID,
    reservation_id      UUID,

    -- Opaque instrument handle, never a card number.
    payment_method_token TEXT       NOT NULL DEFAULT '',

    cancellation_reason TEXT        NOT NULL DEFAULT '',
    failed_step         TEXT        NOT NULL DEFAULT '',

    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS orders_customer_id_idx ON orders (customer_id);
CREATE INDEX IF NOT EXISTS orders_correlation_id_idx ON orders (correlation_id);

CREATE TABLE IF NOT EXISTS order_lines (
    order_id     UUID    NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    line_no      INT     NOT NULL,
    sku          TEXT    NOT NULL,
    quantity     INT     NOT NULL CHECK (quantity > 0),
    unit_minor   BIGINT  NOT NULL CHECK (unit_minor >= 0),
    currency     CHAR(3) NOT NULL,
    PRIMARY KEY (order_id, line_no),

    -- One line per SKU. Duplicates are merged at the API boundary; allowing them here
    -- would let one order reserve the same SKU twice and double-count it.
    UNIQUE (order_id, sku)
);

CREATE TABLE IF NOT EXISTS saga_instances (
    id             UUID        PRIMARY KEY,
    order_id       UUID        NOT NULL UNIQUE REFERENCES orders (id) ON DELETE CASCADE,
    state          TEXT        NOT NULL CHECK (state IN
                                 ('RUNNING','COMPENSATING','COMPLETED','COMPENSATED','STUCK')),

    -- Index of the next forward step to attempt, or of the step being compensated when
    -- state is COMPENSATING. Persisted BEFORE each step runs, so a crashed orchestrator
    -- resumes from the log rather than from a process's memory.
    current_step   INT         NOT NULL DEFAULT 0,

    correlation_id UUID        NOT NULL,
    last_error     TEXT        NOT NULL DEFAULT '',

    -- When the saga may next be attempted. Retries after a transient failure are backed
    -- off by moving this forward rather than by sleeping in a goroutine, so a restart
    -- does not reset every backoff in flight.
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts        INT         NOT NULL DEFAULT 0,

    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Drives the recovery loop: find sagas that are due and not terminal.
CREATE INDEX IF NOT EXISTS saga_instances_due_idx
    ON saga_instances (next_attempt_at)
    WHERE state IN ('RUNNING','COMPENSATING');

CREATE TABLE IF NOT EXISTS saga_step_log (
    saga_id        UUID        NOT NULL REFERENCES saga_instances (id) ON DELETE CASCADE,
    step_index     INT         NOT NULL,
    step_name      TEXT        NOT NULL,
    state          TEXT        NOT NULL CHECK (state IN
                                 ('PENDING','DONE','FAILED','UNKNOWN','COMPENSATED')),

    -- The key sent to the callee. Stable across retries of this step, which is what makes
    -- retrying safe and what GetPaymentByIdempotencyKey is looked up by when a step times
    -- out and its outcome has to be resolved rather than guessed.
    idempotency_key TEXT       NOT NULL,

    detail         TEXT        NOT NULL DEFAULT '',
    attempts       INT         NOT NULL DEFAULT 0,
    started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at    TIMESTAMPTZ,
    PRIMARY KEY (saga_id, step_index)
);
`

// Migrations returns every migration this service needs, in order.
func Migrations(outbox, idem pgmigrate.Migration) []pgmigrate.Migration {
	return []pgmigrate.Migration{
		outbox,
		idem,
		{Name: "0003_orders", SQL: SchemaSQL},
	}
}
