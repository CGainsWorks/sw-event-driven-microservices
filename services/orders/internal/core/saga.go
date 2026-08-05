package core

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// The fulfilment saga.
//
// Forward steps run in order. On failure at step n, the compensations for steps n-1 down to
// 0 run in reverse. Every step's state is persisted BEFORE the step is attempted, so a
// crashed orchestrator resumes from the log rather than from a process's memory.
//
// Milestone note: this list currently contains only the payment steps. The inventory steps
// (reserve at the front, commit at the back) are added when the inventory service exists.
// The engine below does not care how many steps there are, which is the point of it being
// a list.
func (s *Service) steps() []step {
	return []step{
		{
			name:          "authorize_payment",
			failureReason: "PAYMENT_DECLINED",

			execute: func(ctx context.Context, sc *sagaCtx, key string) (StepOutcome, error) {
				return s.payments.Authorize(ctx, AuthorizeInput{
					IdempotencyKey:     key,
					OrderID:            sc.order.ID,
					CustomerID:         sc.order.CustomerID,
					Amount:             sc.order.Total,
					PaymentMethodToken: sc.paymentMethodToken,
					CorrelationID:      sc.order.CorrelationID,
				})
			},

			// A timed-out authorize is the canonical ambiguous outcome. It is resolved by
			// asking payments whether that idempotency key ever produced an authorization,
			// which works only because the key is stable across retries.
			resolve: func(ctx context.Context, sc *sagaCtx, key string) (*StepOutcome, error) {
				return s.payments.ResolveAuthorize(ctx, key)
			},

			compensate: func(ctx context.Context, sc *sagaCtx, key string) (StepOutcome, error) {
				return s.payments.Void(ctx, VoidInput{
					IdempotencyKey: key,
					PaymentID:      sc.paymentID,
					CorrelationID:  sc.order.CorrelationID,
				})
			},
		},
		{
			name:          "capture_payment",
			failureReason: "PAYMENT_ERROR",

			execute: func(ctx context.Context, sc *sagaCtx, key string) (StepOutcome, error) {
				return s.payments.Capture(ctx, CaptureInput{
					IdempotencyKey: key,
					PaymentID:      sc.paymentID,
					Amount:         sc.order.Total,
					CorrelationID:  sc.order.CorrelationID,
				})
			},

			compensate: func(ctx context.Context, sc *sagaCtx, key string) (StepOutcome, error) {
				return s.payments.Refund(ctx, RefundInput{
					IdempotencyKey: key,
					PaymentID:      sc.paymentID,
					Amount:         sc.order.Total,
					CorrelationID:  sc.order.CorrelationID,
				})
			},
		},
	}
}

type step struct {
	name string

	// failureReason is the cancellation reason recorded on the order when this step fails
	// terminally. Part of the step definition rather than derived at the failure site, so
	// that adding a step forces a decision about what its failure means to a customer.
	failureReason string

	execute    func(ctx context.Context, sc *sagaCtx, key string) (StepOutcome, error)
	compensate func(ctx context.Context, sc *sagaCtx, key string) (StepOutcome, error)

	// resolve answers "did this actually happen?" after an ambiguous outcome. Optional:
	// a step without one is simply retried, which is safe because every call is keyed.
	resolve func(ctx context.Context, sc *sagaCtx, key string) (*StepOutcome, error)
}

// sagaCtx is the state threaded through one run.
type sagaCtx struct {
	sagaID             string
	order              *Order
	paymentID          string
	reservationID      string
	paymentMethodToken string
	currentStep        int
	state              SagaState
}

// stepKey is the idempotency key sent to the callee for one step.
//
// Stable across every retry of that step — that stability is the entire mechanism. A key
// that varied per attempt would make each retry a new request, and a timed-out authorize
// followed by a retry would charge the customer twice.
func stepKey(orderID, stepName string) string {
	return fmt.Sprintf("%s:%s:1", orderID, stepName)
}

func compensationKey(orderID, stepName string) string {
	return fmt.Sprintf("%s:%s:compensate:1", orderID, stepName)
}

// Advance drives one saga as far as it can go in a single pass.
//
// It claims the saga with a short lease before doing anything, so two orchestrator
// replicas cannot run the same saga concurrently. A pass ends when the saga reaches a
// terminal state, or when it needs to wait — for a backoff, or for an outcome it could not
// resolve. In the waiting case the next attempt time is persisted and the recovery loop
// picks it up later, rather than a goroutine sleeping and losing its place on restart.
func (s *Service) Advance(ctx context.Context, orderID string) error {
	sc, ok, err := s.claim(ctx, orderID)
	if err != nil || !ok {
		return err
	}

	log := s.log.With("order_id", orderID, "saga_id", sc.sagaID, "correlation_id", sc.order.CorrelationID)
	steps := s.steps()

	if sc.state == SagaCompensating {
		return s.compensate(ctx, sc, steps, log)
	}

	for sc.currentStep < len(steps) {
		st := steps[sc.currentStep]
		key := stepKey(orderID, st.name)

		if err := s.markStep(ctx, sc.sagaID, sc.currentStep, st.name, StepPending, key, ""); err != nil {
			return err
		}

		callCtx, cancel := context.WithTimeout(ctx, s.stepTimeout)
		outcome, err := st.execute(callCtx, sc, key)
		cancel()

		if err != nil {
			if !errors.Is(err, ErrAmbiguous) {
				// A plain transport failure. The callee was never reached, or told us so.
				// Retry with backoff; nothing has changed anywhere.
				log.Warn("saga step failed in transport, will retry",
					"step", st.name, "error", err)
				return s.scheduleRetry(ctx, sc, fmt.Sprintf("%s: %v", st.name, err))
			}

			// Ambiguous. Record it as UNKNOWN — never as FAILED — and try to find out.
			log.Warn("saga step outcome unknown, attempting to resolve", "step", st.name, "error", err)
			if err := s.markStep(ctx, sc.sagaID, sc.currentStep, st.name, StepUnknown, key, err.Error()); err != nil {
				return err
			}
			if st.resolve == nil {
				return s.scheduleRetry(ctx, sc, fmt.Sprintf("%s: unknown outcome, no resolver", st.name))
			}

			resolveCtx, cancel := context.WithTimeout(ctx, s.stepTimeout)
			resolved, rerr := st.resolve(resolveCtx, sc, key)
			cancel()
			switch {
			case rerr != nil:
				// Could not even ask. Stay UNKNOWN and try again later. Guessing here is
				// the one thing that must not happen.
				log.Warn("could not resolve unknown outcome, staying unresolved",
					"step", st.name, "error", rerr)
				return s.scheduleRetry(ctx, sc, fmt.Sprintf("%s: unresolved: %v", st.name, rerr))
			case resolved == nil:
				// The callee has no record of the request, so it never arrived. Safe to
				// send again.
				log.Info("unknown outcome resolved: request never arrived, retrying", "step", st.name)
				return s.scheduleRetry(ctx, sc, fmt.Sprintf("%s: never arrived", st.name))
			default:
				log.Info("unknown outcome resolved", "step", st.name, "ok", resolved.OK)
				outcome = *resolved
			}
		}

		if !outcome.OK {
			if outcome.Retryable {
				log.Warn("saga step failed but is retryable", "step", st.name, "detail", outcome.Detail)
				return s.scheduleRetry(ctx, sc, fmt.Sprintf("%s: %s", st.name, outcome.Detail))
			}
			log.Info("saga step failed terminally, compensating",
				"step", st.name, "detail", outcome.Detail)
			if err := s.markStep(ctx, sc.sagaID, sc.currentStep, st.name, StepFailed, key, outcome.Detail); err != nil {
				return err
			}
			if err := s.beginCompensation(ctx, sc, st, outcome.Detail); err != nil {
				return err
			}
			return s.compensate(ctx, sc, steps, log)
		}

		// Forward progress. The ids the step produced are persisted on the order in the
		// same transaction as the step being marked done, so a compensation after a
		// restart knows what to address.
		if outcome.PaymentID != "" {
			sc.paymentID = outcome.PaymentID
		}
		if outcome.ReservationID != "" {
			sc.reservationID = outcome.ReservationID
		}
		if err := s.completeStep(ctx, sc, st.name, key, outcome); err != nil {
			return err
		}
		sc.currentStep++
		log.Info("saga step done", "step", st.name)
	}

	return s.complete(ctx, sc)
}

// compensate runs the compensations for every completed step, in reverse.
func (s *Service) compensate(ctx context.Context, sc *sagaCtx, steps []step, log logger) error {
	done, err := s.completedSteps(ctx, sc.sagaID)
	if err != nil {
		return err
	}

	for i := len(steps) - 1; i >= 0; i-- {
		if !done[i] {
			continue
		}
		st := steps[i]
		if st.compensate == nil {
			continue
		}
		key := compensationKey(sc.order.ID, st.name)

		callCtx, cancel := context.WithTimeout(ctx, s.stepTimeout)
		outcome, err := st.compensate(callCtx, sc, key)
		cancel()

		if err != nil || !outcome.OK {
			// A failed compensation. The saga cannot go forward and cannot go back, so it
			// stops and asks for a human. This is the one state the system never resolves
			// automatically, and pretending otherwise would mean silently losing either a
			// customer's money or the shop's stock.
			detail := "compensation failed"
			if err != nil {
				detail = err.Error()
			} else if outcome.Detail != "" {
				detail = outcome.Detail
			}
			log.Error("compensation failed, saga is stuck and needs a human",
				"step", st.name, "detail", detail)
			return s.markStuck(ctx, sc, fmt.Sprintf("%s compensation: %s", st.name, detail))
		}

		if err := s.markStep(ctx, sc.sagaID, i, st.name, StepCompensated, key, ""); err != nil {
			return err
		}
		log.Info("compensation done", "step", st.name)
	}

	return s.markCompensated(ctx, sc)
}

// logger is the small slice of *slog.Logger this file uses, so that compensate can be
// handed a pre-decorated logger without importing slog here.
type logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// --- persistence ------------------------------------------------------------

// claim takes a lease on the saga so that only one orchestrator advances it at a time.
//
// A lease rather than a held row lock: a lock would have to survive the network calls the
// saga makes, holding a database connection open for the duration of a third-party round
// trip. The lease is just a timestamp, so a crashed orchestrator releases it by expiry.
func (s *Service) claim(ctx context.Context, orderID string) (*sagaCtx, bool, error) {
	var sc sagaCtx
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var state string
		err := tx.QueryRow(ctx, `
			UPDATE saga_instances
			   SET next_attempt_at = now() + $2::interval,
			       attempts        = attempts + 1,
			       updated_at      = now()
			 WHERE order_id = $1
			   AND state IN ('RUNNING','COMPENSATING')
			   AND next_attempt_at <= now()
			RETURNING id, state, current_step`,
			orderID, s.leaseDuration.String()).Scan(&sc.sagaID, &state, &sc.currentStep)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil // terminal, or leased by another orchestrator
		}
		if err != nil {
			return fmt.Errorf("orders: claiming saga for %s: %w", orderID, err)
		}
		sc.state = SagaState(state)

		order, err := loadOrder(ctx, tx, orderID)
		if err != nil {
			return err
		}
		if order == nil {
			return fmt.Errorf("orders: saga %s references missing order %s", sc.sagaID, orderID)
		}
		sc.order = order
		sc.paymentID = order.PaymentID
		sc.reservationID = order.ReservationID
		sc.paymentMethodToken = s.paymentMethodToken(order)
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return &sc, sc.sagaID != "", nil
}

func (s *Service) markStep(ctx context.Context, sagaID string, index int, name string, state StepState, key, detail string) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO saga_step_log (saga_id, step_index, step_name, state, idempotency_key, detail, attempts)
			VALUES ($1, $2, $3, $4, $5, $6, 1)
			ON CONFLICT (saga_id, step_index) DO UPDATE
			   SET state       = EXCLUDED.state,
			       detail      = EXCLUDED.detail,
			       attempts    = saga_step_log.attempts + 1,
			       finished_at = CASE WHEN EXCLUDED.state IN ('DONE','FAILED','COMPENSATED')
			                          THEN now() ELSE NULL END`,
			sagaID, index, name, state, key, detail)
		if err != nil {
			return fmt.Errorf("orders: recording step %s: %w", name, err)
		}
		return nil
	})
}

// completeStep marks a step done and advances the saga, together with the ids the step
// produced. One transaction, so the saga can never believe a step succeeded while having
// forgotten what it created.
func (s *Service) completeStep(ctx context.Context, sc *sagaCtx, name, key string, outcome StepOutcome) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			UPDATE saga_step_log SET state = 'DONE', detail = '', finished_at = now()
			 WHERE saga_id = $1 AND step_index = $2`, sc.sagaID, sc.currentStep); err != nil {
			return fmt.Errorf("orders: completing step %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE saga_instances
			   SET current_step = $2, next_attempt_at = now(), attempts = 0, last_error = '', updated_at = now()
			 WHERE id = $1`, sc.sagaID, sc.currentStep+1); err != nil {
			return fmt.Errorf("orders: advancing saga: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE orders
			   SET payment_id     = COALESCE(NULLIF($2,'')::uuid, payment_id),
			       reservation_id = COALESCE(NULLIF($3,'')::uuid, reservation_id),
			       state          = CASE WHEN state = 'PENDING' THEN 'AWAITING_PAYMENT' ELSE state END,
			       updated_at     = now()
			 WHERE id = $1`, sc.order.ID, sc.paymentID, sc.reservationID); err != nil {
			return fmt.Errorf("orders: recording step ids: %w", err)
		}
		return nil
	})
}

func (s *Service) scheduleRetry(ctx context.Context, sc *sagaCtx, reason string) error {
	// Exponential backoff derived from the persisted attempt count, so a restart does not
	// reset a backoff that was already several minutes long.
	return s.inTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			UPDATE saga_instances
			   SET next_attempt_at = now() + least(
			         ($2::interval) * power(2, least(attempts, 8)),
			         $3::interval),
			       last_error = $4,
			       updated_at = now()
			 WHERE id = $1`,
			sc.sagaID, s.retryBase.String(), s.retryMax.String(), reason)
		if err != nil {
			return fmt.Errorf("orders: scheduling saga retry: %w", err)
		}
		return nil
	})
}

func (s *Service) beginCompensation(ctx context.Context, sc *sagaCtx, failed step, detail string) error {
	sc.state = SagaCompensating
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			UPDATE saga_instances SET state = 'COMPENSATING', last_error = $2, updated_at = now()
			 WHERE id = $1`, sc.sagaID, detail); err != nil {
			return fmt.Errorf("orders: entering compensation: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE orders SET cancellation_reason = $2, failed_step = $3, updated_at = now()
			 WHERE id = $1`, sc.order.ID, failed.failureReason, stepConstantName(failed.name)); err != nil {
			return fmt.Errorf("orders: recording failure reason: %w", err)
		}
		sc.order.CancellationReason = failed.failureReason
		sc.order.FailedStep = stepConstantName(failed.name)
		return nil
	})
}

func (s *Service) completedSteps(ctx context.Context, sagaID string) (map[int]bool, error) {
	done := map[int]bool{}
	rows, err := s.pool.Query(ctx,
		`SELECT step_index FROM saga_step_log WHERE saga_id = $1 AND state = 'DONE'`, sagaID)
	if err != nil {
		return nil, fmt.Errorf("orders: reading step log: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var i int
		if err := rows.Scan(&i); err != nil {
			return nil, fmt.Errorf("orders: reading step log: %w", err)
		}
		done[i] = true
	}
	return done, rows.Err()
}

func (s *Service) markStuck(ctx context.Context, sc *sagaCtx, detail string) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			UPDATE saga_instances SET state = 'STUCK', last_error = $2, updated_at = now()
			 WHERE id = $1`, sc.sagaID, detail)
		if err != nil {
			return fmt.Errorf("orders: marking saga stuck: %w", err)
		}
		return nil
	})
}

// stepConstantName maps an internal step name to the enum member used on the wire.
func stepConstantName(name string) string {
	switch name {
	case "authorize_payment":
		return "AUTHORIZE_PAYMENT"
	case "capture_payment":
		return "CAPTURE_PAYMENT"
	case "reserve_stock":
		return "RESERVE_STOCK"
	case "commit_reservation":
		return "COMMIT_RESERVATION"
	default:
		return "NONE"
	}
}

// RecoveryLoop resumes sagas that are due.
//
// This is what makes a crashed orchestrator harmless: the saga's progress is in the
// database, and any replica can pick it up once the lease expires. Without it, an order
// whose orchestrator died would sit in AWAITING_PAYMENT forever with a hold on the
// customer's card.
func (s *Service) RecoveryLoop(ctx context.Context, interval time.Duration) {
	s.log.Info("saga recovery loop started", "interval", interval)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			s.log.Info("saga recovery loop stopped")
			return
		case <-t.C:
			ids, err := s.dueSagas(ctx, 50)
			if err != nil {
				s.log.Error("finding due sagas", "error", err)
				continue
			}
			for _, orderID := range ids {
				if err := s.Advance(ctx, orderID); err != nil {
					s.log.Error("advancing saga", "order_id", orderID, "error", err)
				}
			}
		}
	}
}

func (s *Service) dueSagas(ctx context.Context, limit int) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT order_id FROM saga_instances
		 WHERE state IN ('RUNNING','COMPENSATING') AND next_attempt_at <= now()
		 ORDER BY next_attempt_at
		 LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("orders: finding due sagas: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("orders: finding due sagas: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
