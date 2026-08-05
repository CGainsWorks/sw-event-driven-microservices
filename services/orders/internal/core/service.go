package core

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/CGainsWorks/sw-event-driven-microservices/libs/eventbus"
	"github.com/CGainsWorks/sw-event-driven-microservices/libs/outbox"
)

const producerName = "orders"

// Config holds the saga's time budget.
//
// The relationship between these numbers matters more than any one of them. StepTimeout
// bounds a single call; LeaseDuration must exceed it, or a second orchestrator could claim
// a saga while the first is still mid-call; and the reservation TTL that inventory applies
// must exceed StepTimeout times the number of steps, or reservations expire mid-saga.
// docs/sagas.md carries the arithmetic.
type Config struct {
	StepTimeout   time.Duration
	LeaseDuration time.Duration
	RetryBase     time.Duration
	RetryMax      time.Duration
}

func (c *Config) applyDefaults() {
	if c.StepTimeout <= 0 {
		c.StepTimeout = 10 * time.Second
	}
	if c.LeaseDuration <= 0 {
		// Comfortably longer than a step plus its resolution attempt, so a live
		// orchestrator never has its saga taken from under it.
		c.LeaseDuration = 3 * c.StepTimeout
	}
	if c.RetryBase <= 0 {
		c.RetryBase = 2 * time.Second
	}
	if c.RetryMax <= 0 {
		c.RetryMax = 5 * time.Minute
	}
}

// Service is the orders domain and the saga orchestrator.
type Service struct {
	pool     *pgxpool.Pool
	payments PaymentsPort
	log      *slog.Logger

	stepTimeout   time.Duration
	leaseDuration time.Duration
	retryBase     time.Duration
	retryMax      time.Duration
}

func NewService(pool *pgxpool.Pool, payments PaymentsPort, log *slog.Logger, cfg Config) *Service {
	cfg.applyDefaults()
	return &Service{
		pool:          pool,
		payments:      payments,
		log:           log.With("component", "orders-core"),
		stepTimeout:   cfg.StepTimeout,
		leaseDuration: cfg.LeaseDuration,
		retryBase:     cfg.RetryBase,
		retryMax:      cfg.RetryMax,
	}
}

func (s *Service) paymentMethodToken(o *Order) string { return o.PaymentMethodToken }

// --- placing an order -------------------------------------------------------

// PlaceOrderRequest is what the API accepts.
type PlaceOrderRequest struct {
	CustomerID         string
	CustomerEmail      string
	Lines              []Line
	PaymentMethodToken string

	// CorrelationID may be supplied by the caller to tie this order to something upstream.
	// Generated when empty — every order is the origin of a correlation id if nothing else
	// claims to be.
	CorrelationID string
}

// ErrInvalidOrder is returned for a request the domain refuses. Distinct from an
// infrastructure failure so the API can answer 400 rather than 503.
var ErrInvalidOrder = errors.New("invalid order")

// PlaceOrder writes the order, its saga, and the OrderPlaced event in one transaction.
//
// One transaction is the whole point. The order existing, the saga that will fulfil it
// existing, and the event announcing it existing are three facts that must be true
// together or not at all. Split across two commits, a crash between them leaves an order
// nobody will fulfil or an event about an order that does not exist.
//
// The saga is NOT run here. It is enqueued by virtue of saga_instances existing with
// next_attempt_at = now(), and the caller advances it after the commit. That keeps the
// HTTP request off the critical path of four network calls and means an order is accepted
// even if payments is down.
func (s *Service) PlaceOrder(ctx context.Context, req PlaceOrderRequest) (*Order, error) {
	if len(req.Lines) == 0 {
		return nil, fmt.Errorf("%w: an order needs at least one line", ErrInvalidOrder)
	}
	if req.CustomerID == "" || req.CustomerEmail == "" {
		return nil, fmt.Errorf("%w: customer_id and customer_email are required", ErrInvalidOrder)
	}

	// Merge duplicate SKUs at the boundary rather than storing them. Two lines for one SKU
	// would let inventory reserve it twice and be counted twice in the total.
	merged := map[string]Line{}
	order := make([]string, 0, len(req.Lines))
	currency := req.Lines[0].UnitAmount.Currency
	for _, l := range req.Lines {
		if l.Quantity == 0 {
			return nil, fmt.Errorf("%w: line for %s has zero quantity", ErrInvalidOrder, l.SKU)
		}
		if l.UnitAmount.Currency != currency {
			return nil, fmt.Errorf("%w: mixed currencies (%s and %s) in one order",
				ErrInvalidOrder, currency, l.UnitAmount.Currency)
		}
		if existing, ok := merged[l.SKU]; ok {
			existing.Quantity += l.Quantity
			merged[l.SKU] = existing
			continue
		}
		merged[l.SKU] = l
		order = append(order, l.SKU)
	}

	lines := make([]Line, 0, len(merged))
	var totalMinor int64
	for _, sku := range order {
		l := merged[sku]
		lines = append(lines, l)
		totalMinor += int64(l.Quantity) * l.UnitAmount.AmountMinor
	}
	if totalMinor <= 0 {
		return nil, fmt.Errorf("%w: order total must be positive", ErrInvalidOrder)
	}

	correlationID := req.CorrelationID
	if correlationID == "" {
		correlationID = uuid.NewString()
	}

	o := &Order{
		ID:                 uuid.NewString(),
		CustomerID:         req.CustomerID,
		CustomerEmail:      req.CustomerEmail,
		State:              StatePending,
		Total:              Money{AmountMinor: totalMinor, Currency: currency},
		Lines:              lines,
		CorrelationID:      correlationID,
		PaymentMethodToken: req.PaymentMethodToken,
	}

	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO orders (id, customer_id, customer_email, state, total_minor, currency,
			                    correlation_id, payment_method_token)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
			o.ID, o.CustomerID, o.CustomerEmail, o.State, o.Total.AmountMinor, o.Total.Currency,
			o.CorrelationID, o.PaymentMethodToken); err != nil {
			return fmt.Errorf("orders: inserting order: %w", err)
		}
		for i, l := range o.Lines {
			if _, err := tx.Exec(ctx, `
				INSERT INTO order_lines (order_id, line_no, sku, quantity, unit_minor, currency)
				VALUES ($1,$2,$3,$4,$5,$6)`,
				o.ID, i, l.SKU, l.Quantity, l.UnitAmount.AmountMinor, l.UnitAmount.Currency); err != nil {
				return fmt.Errorf("orders: inserting line %s: %w", l.SKU, err)
			}
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO saga_instances (id, order_id, state, correlation_id)
			VALUES ($1,$2,'RUNNING',$3)`, uuid.NewString(), o.ID, o.CorrelationID); err != nil {
			return fmt.Errorf("orders: creating saga: %w", err)
		}

		env, err := eventbus.NewEnvelope("orders.v1.OrderPlaced", "order", o.ID,
			producerName, o.CorrelationID, "", orderPlacedData{
				OrderID:       o.ID,
				CustomerID:    o.CustomerID,
				CustomerEmail: o.CustomerEmail,
				Lines:         o.Lines,
				TotalAmount:   o.Total,
				PlacedAt:      eventbus.Now(),
			})
		if err != nil {
			return err
		}
		return outbox.Enqueue(ctx, tx, env, nil)
	})
	if err != nil {
		return nil, err
	}

	s.log.Info("order placed",
		"order_id", o.ID, "correlation_id", o.CorrelationID,
		"total_minor", o.Total.AmountMinor, "currency", o.Total.Currency, "lines", len(o.Lines))
	return o, nil
}

// --- saga terminal states ---------------------------------------------------

func (s *Service) complete(ctx context.Context, sc *sagaCtx) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`UPDATE saga_instances SET state = 'COMPLETED', updated_at = now() WHERE id = $1`,
			sc.sagaID); err != nil {
			return fmt.Errorf("orders: completing saga: %w", err)
		}
		if _, err := tx.Exec(ctx,
			`UPDATE orders SET state = 'CONFIRMED', updated_at = now() WHERE id = $1`,
			sc.order.ID); err != nil {
			return fmt.Errorf("orders: confirming order: %w", err)
		}

		env, err := eventbus.NewEnvelope("orders.v1.OrderConfirmed", "order", sc.order.ID,
			producerName, sc.order.CorrelationID, "", orderConfirmedData{
				OrderID:       sc.order.ID,
				CustomerID:    sc.order.CustomerID,
				CustomerEmail: sc.order.CustomerEmail,
				PaymentID:     sc.paymentID,
				ReservationID: sc.reservationID,
				TotalAmount:   sc.order.Total,
				ConfirmedAt:   eventbus.Now(),
			})
		if err != nil {
			return err
		}
		if err := outbox.Enqueue(ctx, tx, env, nil); err != nil {
			return err
		}
		s.log.Info("order confirmed", "order_id", sc.order.ID, "correlation_id", sc.order.CorrelationID)
		return nil
	})
}

func (s *Service) markCompensated(ctx context.Context, sc *sagaCtx) error {
	applied, err := s.appliedCompensations(ctx, sc.sagaID)
	if err != nil {
		return err
	}

	return s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`UPDATE saga_instances SET state = 'COMPENSATED', updated_at = now() WHERE id = $1`,
			sc.sagaID); err != nil {
			return fmt.Errorf("orders: marking saga compensated: %w", err)
		}
		if _, err := tx.Exec(ctx,
			`UPDATE orders SET state = 'CANCELLED', updated_at = now() WHERE id = $1`,
			sc.order.ID); err != nil {
			return fmt.Errorf("orders: cancelling order: %w", err)
		}

		reason := sc.order.CancellationReason
		if reason == "" {
			reason = "INTERNAL_ERROR"
		}
		failedStep := sc.order.FailedStep
		if failedStep == "" {
			failedStep = "NONE"
		}

		// OrderCancelled is published only now, after every compensation has completed.
		// A consumer seeing it can therefore assume the system holds nothing on the
		// customer's behalf — which is a promise the event's schema documentation makes
		// and this ordering is what keeps it.
		env, err := eventbus.NewEnvelope("orders.v1.OrderCancelled", "order", sc.order.ID,
			producerName, sc.order.CorrelationID, "", orderCancelledData{
				OrderID:              sc.order.ID,
				CustomerID:           sc.order.CustomerID,
				CustomerEmail:        sc.order.CustomerEmail,
				Reason:               reason,
				FailedStep:           failedStep,
				CompensationsApplied: applied,
				CancelledAt:          eventbus.Now(),
			})
		if err != nil {
			return err
		}
		if err := outbox.Enqueue(ctx, tx, env, nil); err != nil {
			return err
		}
		s.log.Info("order cancelled",
			"order_id", sc.order.ID, "correlation_id", sc.order.CorrelationID,
			"reason", reason, "failed_step", failedStep, "compensations", applied)
		return nil
	})
}

// appliedCompensations lists which compensations actually ran, in the order they ran.
// Empty when the saga failed on its first step and there was nothing to undo.
func (s *Service) appliedCompensations(ctx context.Context, sagaID string) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT step_name FROM saga_step_log
		 WHERE saga_id = $1 AND state = 'COMPENSATED'
		 ORDER BY finished_at`, sagaID)
	if err != nil {
		return nil, fmt.Errorf("orders: reading compensations: %w", err)
	}
	defer rows.Close()

	applied := []string{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("orders: reading compensations: %w", err)
		}
		if v, ok := compensationEventName[name]; ok {
			applied = append(applied, v)
		}
	}
	return applied, rows.Err()
}

// compensationEventName maps a step to the compensation name used on the wire.
var compensationEventName = map[string]string{
	"authorize_payment":  "VOID_AUTHORIZATION",
	"capture_payment":    "REFUND_PAYMENT",
	"reserve_stock":      "RELEASE_STOCK",
	"commit_reservation": "RELEASE_STOCK",
}

// --- reads ------------------------------------------------------------------

func (s *Service) Get(ctx context.Context, orderID string) (*Order, error) {
	var o *Order
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var err error
		o, err = loadOrder(ctx, tx, orderID)
		return err
	})
	return o, err
}

func loadOrder(ctx context.Context, tx pgx.Tx, orderID string) (*Order, error) {
	var o Order
	var paymentID, reservationID *string
	err := tx.QueryRow(ctx, `
		SELECT id, customer_id, customer_email, state, total_minor, currency, correlation_id,
		       payment_id, reservation_id, payment_method_token,
		       cancellation_reason, failed_step, created_at, updated_at
		  FROM orders WHERE id = $1`, orderID).
		Scan(&o.ID, &o.CustomerID, &o.CustomerEmail, &o.State, &o.Total.AmountMinor, &o.Total.Currency,
			&o.CorrelationID, &paymentID, &reservationID, &o.PaymentMethodToken,
			&o.CancellationReason, &o.FailedStep, &o.CreatedAt, &o.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("orders: reading order %s: %w", orderID, err)
	}
	if paymentID != nil {
		o.PaymentID = *paymentID
	}
	if reservationID != nil {
		o.ReservationID = *reservationID
	}

	rows, err := tx.Query(ctx, `
		SELECT sku, quantity, unit_minor, currency FROM order_lines
		 WHERE order_id = $1 ORDER BY line_no`, orderID)
	if err != nil {
		return nil, fmt.Errorf("orders: reading lines for %s: %w", orderID, err)
	}
	defer rows.Close()
	for rows.Next() {
		var l Line
		if err := rows.Scan(&l.SKU, &l.Quantity, &l.UnitAmount.AmountMinor, &l.UnitAmount.Currency); err != nil {
			return nil, fmt.Errorf("orders: reading lines for %s: %w", orderID, err)
		}
		o.Lines = append(o.Lines, l)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("orders: reading lines for %s: %w", orderID, err)
	}
	return &o, nil
}

// SagaSnapshot is the saga's state, for the API and for tests that need to assert on
// something more precise than the order's status.
type SagaSnapshot struct {
	State       SagaState `json:"state"`
	CurrentStep int       `json:"current_step"`
	Attempts    int       `json:"attempts"`
	LastError   string    `json:"last_error,omitempty"`
	Steps       []StepLog `json:"steps"`
}

type StepLog struct {
	Index    int       `json:"index"`
	Name     string    `json:"name"`
	State    StepState `json:"state"`
	Attempts int       `json:"attempts"`
	Detail   string    `json:"detail,omitempty"`
}

func (s *Service) SagaFor(ctx context.Context, orderID string) (*SagaSnapshot, error) {
	var snap SagaSnapshot
	var sagaID string
	err := s.pool.QueryRow(ctx, `
		SELECT id, state, current_step, attempts, last_error
		  FROM saga_instances WHERE order_id = $1`, orderID).
		Scan(&sagaID, &snap.State, &snap.CurrentStep, &snap.Attempts, &snap.LastError)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("orders: reading saga for %s: %w", orderID, err)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT step_index, step_name, state, attempts, detail
		  FROM saga_step_log WHERE saga_id = $1 ORDER BY step_index`, sagaID)
	if err != nil {
		return nil, fmt.Errorf("orders: reading step log: %w", err)
	}
	defer rows.Close()
	snap.Steps = []StepLog{}
	for rows.Next() {
		var l StepLog
		if err := rows.Scan(&l.Index, &l.Name, &l.State, &l.Attempts, &l.Detail); err != nil {
			return nil, fmt.Errorf("orders: reading step log: %w", err)
		}
		snap.Steps = append(snap.Steps, l)
	}
	return &snap, rows.Err()
}

// --- plumbing ---------------------------------------------------------------

func (s *Service) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("orders: beginning transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("orders: committing transaction: %w", err)
	}
	return nil
}

// --- event payloads ---------------------------------------------------------
//
// These mirror events/schemas/orders.v1.*.schema.json. Every envelope is validated against
// that schema before it reaches the outbox, so a field renamed here without the schema
// being updated fails at publication rather than at a consumer a week later.

type orderPlacedData struct {
	OrderID       string             `json:"order_id"`
	CustomerID    string             `json:"customer_id"`
	CustomerEmail string             `json:"customer_email"`
	Lines         []Line             `json:"lines"`
	TotalAmount   Money              `json:"total_amount"`
	PlacedAt      eventbus.Timestamp `json:"placed_at"`
}

type orderConfirmedData struct {
	OrderID       string `json:"order_id"`
	CustomerID    string `json:"customer_id"`
	CustomerEmail string `json:"customer_email"`
	PaymentID     string `json:"payment_id"`

	// omitempty, not an empty string. An optional field whose schema constrains it to a
	// UUID pattern cannot be sent as "" — an empty string is not an absent field, and the
	// validator is right to reject it. Emitting the key only when there is a value is the
	// difference between "no reservation" and "a malformed reservation id".
	ReservationID string             `json:"reservation_id,omitempty"`
	TotalAmount   Money              `json:"total_amount"`
	ConfirmedAt   eventbus.Timestamp `json:"confirmed_at"`
}

type orderCancelledData struct {
	OrderID              string             `json:"order_id"`
	CustomerID           string             `json:"customer_id"`
	CustomerEmail        string             `json:"customer_email"`
	Reason               string             `json:"reason"`
	FailedStep           string             `json:"failed_step"`
	Detail               string             `json:"detail,omitempty"`
	CompensationsApplied []string           `json:"compensations_applied"`
	CancelledAt          eventbus.Timestamp `json:"cancelled_at"`
}
