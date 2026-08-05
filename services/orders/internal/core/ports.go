package core

import (
	"context"
	"errors"
)

// ErrAmbiguous marks a call whose outcome is genuinely unknown — a timeout, a severed
// connection, a callee that died mid-request.
//
// It is deliberately distinct from a business failure and from a retryable transport
// error, because it needs different handling from both. A step that returns it is recorded
// as UNKNOWN and resolved by asking the callee, never by assuming. Assuming failure risks
// leaking an authorization onto a customer's card; assuming success risks confirming an
// unpaid order. Both are wrong some of the time.
var ErrAmbiguous = errors.New("outcome unknown")

// StepOutcome is what a saga step learned. Business failures are outcomes, not errors: a
// declined card is an answer, and the saga branches on it rather than propagating it.
type StepOutcome struct {
	OK bool

	// Retryable is meaningful only when OK is false. It says whether the same request
	// could succeed later. Supplied by the callee rather than inferred here, so that two
	// services cannot disagree about whether a decline is worth retrying.
	Retryable bool

	Detail string

	// PaymentID and ReservationID are threaded back into the saga so later steps and
	// compensations can address what earlier steps created.
	PaymentID     string
	ReservationID string
}

// PaymentsPort is what the saga needs from payments. An interface rather than a concrete
// client so the saga can be tested against a fake that returns ErrAmbiguous on demand,
// which is the only practical way to exercise the timeout path deterministically.
type PaymentsPort interface {
	Authorize(ctx context.Context, in AuthorizeInput) (StepOutcome, error)
	Capture(ctx context.Context, in CaptureInput) (StepOutcome, error)
	Void(ctx context.Context, in VoidInput) (StepOutcome, error)
	Refund(ctx context.Context, in RefundInput) (StepOutcome, error)

	// ResolveAuthorize answers "did this authorize request ever reach you?" for a step
	// that timed out. A nil outcome with a nil error means no: the request never arrived
	// and it is safe to send again.
	ResolveAuthorize(ctx context.Context, idempotencyKey string) (*StepOutcome, error)
}

// InventoryPort is what the saga needs from inventory. Same shape and same reasoning as
// PaymentsPort: an interface so the saga can be tested against a fake that returns
// ErrAmbiguous on demand.
type InventoryPort interface {
	Reserve(ctx context.Context, in ReserveInput) (StepOutcome, error)
	Release(ctx context.Context, in ReleaseInput) (StepOutcome, error)
	Commit(ctx context.Context, in CommitInput) (StepOutcome, error)

	// ResolveReserve answers "did this reserve request ever reach you?" for a step that
	// timed out. A nil outcome with a nil error means no: it never arrived, and sending it
	// again is safe.
	ResolveReserve(ctx context.Context, idempotencyKey string) (*StepOutcome, error)
}

// StockLine is a SKU and a quantity as inventory sees it. No price — inventory has no
// business knowing what anything costs.
type StockLine struct {
	SKU      string `json:"sku"`
	Quantity uint32 `json:"quantity"`
}

type ReserveInput struct {
	IdempotencyKey string
	OrderID        string
	Lines          []StockLine
	TTLSeconds     uint32
	CorrelationID  string
	CausationID    string
}

type ReleaseInput struct {
	IdempotencyKey string
	ReservationID  string
	CorrelationID  string
	CausationID    string
}

type CommitInput struct {
	IdempotencyKey string
	ReservationID  string
	CorrelationID  string
	CausationID    string
}

type AuthorizeInput struct {
	IdempotencyKey     string
	OrderID            string
	CustomerID         string
	Amount             Money
	PaymentMethodToken string
	CorrelationID      string
	CausationID        string
}

type CaptureInput struct {
	IdempotencyKey string
	PaymentID      string
	Amount         Money
	CorrelationID  string
	CausationID    string
}

type VoidInput struct {
	IdempotencyKey string
	PaymentID      string
	CorrelationID  string
	CausationID    string
}

type RefundInput struct {
	IdempotencyKey string
	PaymentID      string
	Amount         Money
	CorrelationID  string
	CausationID    string
}
