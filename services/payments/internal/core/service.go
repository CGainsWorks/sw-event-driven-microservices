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
	"github.com/CGainsWorks/sw-event-driven-microservices/libs/idempotency"
	"github.com/CGainsWorks/sw-event-driven-microservices/libs/outbox"
	"github.com/CGainsWorks/sw-event-driven-microservices/services/payments/internal/psp"
)

const producerName = "payments"

// Outcome mirrors the outcome enums on the gRPC responses. Business results are values,
// not errors: a declined card is an answer the domain records, not an exception.
type Outcome string

const (
	OutcomeAuthorized        Outcome = "AUTHORIZED"
	OutcomeCaptured          Outcome = "CAPTURED"
	OutcomeVoided            Outcome = "VOIDED"
	OutcomeRefunded          Outcome = "REFUNDED"
	OutcomeFailed            Outcome = "FAILED"
	OutcomeNotFound          Outcome = "NOT_FOUND"
	OutcomeAlreadyCaptured   Outcome = "ALREADY_CAPTURED"
	OutcomeAuthExpired       Outcome = "AUTHORIZATION_EXPIRED"
	OutcomeExceedsAuthorized Outcome = "EXCEEDS_AUTHORIZED"
	OutcomeExceedsCaptured   Outcome = "EXCEEDS_CAPTURED"
	OutcomeNothingCaptured   Outcome = "NOTHING_CAPTURED"
)

// FailureInfo is the serialisable form of a provider refusal.
type FailureInfo struct {
	Code      string `json:"code"`
	Retryable bool   `json:"retryable"`
	Detail    string `json:"detail"`
}

func failureFrom(f *psp.Failure) *FailureInfo {
	if f == nil {
		return nil
	}
	return &FailureInfo{Code: string(f.Code), Retryable: f.Code.Retryable(), Detail: f.Detail}
}

// Service is the payments domain.
type Service struct {
	pool *pgxpool.Pool
	psp  psp.Provider
	log  *slog.Logger

	// authTTL is how long an authorization is held before it lapses at the provider. The
	// saga must reach capture within it; docs/sagas.md carries the time budget.
	authTTL time.Duration
}

func NewService(pool *pgxpool.Pool, provider psp.Provider, log *slog.Logger, authTTL time.Duration) *Service {
	if authTTL <= 0 {
		authTTL = 7 * 24 * time.Hour
	}
	return &Service{pool: pool, psp: provider, log: log.With("component", "payments-core"), authTTL: authTTL}
}

// --- Authorize --------------------------------------------------------------

type AuthorizeRequest struct {
	IdempotencyKey     string
	OrderID            string
	CustomerID         string
	Amount             Money
	PaymentMethodToken string
	CorrelationID      string
	CausationID        string
}

// fingerprint is what the idempotency key is checked against.
//
// Correlation and causation ids are excluded on purpose: two genuine retries of one
// logical request may carry different causation ids, and including them would make the
// retry look like a different request and defeat the whole mechanism.
func (r AuthorizeRequest) fingerprint() any {
	return struct {
		OrderID    string `json:"order_id"`
		CustomerID string `json:"customer_id"`
		Amount     Money  `json:"amount"`
		Token      string `json:"token"`
	}{r.OrderID, r.CustomerID, r.Amount, r.PaymentMethodToken}
}

type AuthorizeResult struct {
	Outcome      Outcome      `json:"outcome"`
	PaymentID    string       `json:"payment_id"`
	State        State        `json:"state"`
	ExpiresAt    *time.Time   `json:"expires_at,omitempty"`
	Failure      *FailureInfo `json:"failure,omitempty"`
	Deduplicated bool         `json:"-"`
}

// Authorize places a hold on funds.
//
// Three phases, and the split matters:
//
//	tx1  claim the idempotency key and write a PENDING payment row, then COMMIT.
//	     This commit is the durable evidence that the request may have reached the
//	     provider. Without it, a crash during the provider call leaves no trace and a
//	     retry cannot tell "never sent" from "sent, answer lost".
//	psp  call the provider, outside any transaction. A provider call inside a transaction
//	     holds row locks for the duration of a network round trip to a third party.
//	tx2  record the answer, write the ledger entry, enqueue the event and complete the
//	     idempotency record — all together, so state and the telling of it cannot diverge.
func (s *Service) Authorize(ctx context.Context, req AuthorizeRequest) (*AuthorizeResult, error) {
	if req.Amount.AmountMinor <= 0 {
		return nil, fmt.Errorf("payments: authorize amount must be positive, got %s", req.Amount)
	}

	// --- tx1: claim and record the attempt ---
	var (
		paymentID string
		replay    *AuthorizeResult
	)
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		status, stored, err := idempotency.Claim(ctx, tx,
			idempotency.ScopeAuthorizePayment, req.IdempotencyKey, req.fingerprint(), 0)
		if err != nil {
			return err
		}
		if status == idempotency.StatusCompleted {
			var prev AuthorizeResult
			if err := (&idempotency.Result{Response: stored}).Unmarshal(&prev); err != nil {
				return err
			}
			prev.Deduplicated = true
			replay = &prev
			return nil
		}

		// StatusResume means a previous attempt died mid-call; its PENDING row already
		// exists and is reused rather than duplicated.
		existing, err := loadPaymentByIdempotencyKey(ctx, tx, req.IdempotencyKey)
		if err != nil {
			return err
		}
		if existing != nil {
			paymentID = existing.ID
			return nil
		}

		paymentID = uuid.NewString()
		_, err = tx.Exec(ctx, `
			INSERT INTO payments (id, order_id, customer_id, idempotency_key, state,
			                      currency, authorized_minor)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			paymentID, req.OrderID, req.CustomerID, req.IdempotencyKey, StatePending,
			req.Amount.Currency, req.Amount.AmountMinor)
		if err != nil {
			return fmt.Errorf("payments: recording authorize attempt: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if replay != nil {
		return replay, nil
	}

	// --- provider call, outside any transaction ---
	//
	// The idempotency key is passed through as the provider reference. A real PSP
	// deduplicates on it, which is what makes the resume path safe: replaying a call the
	// provider already saw returns the original answer rather than charging twice.
	pspResult, pspErr := s.psp.Authorize(ctx, req.PaymentMethodToken,
		req.Amount.AmountMinor, req.Amount.Currency, req.IdempotencyKey)
	if pspErr != nil {
		// A transport failure, not a decision. The PENDING row stays and the idempotency
		// claim stays, so the caller can resolve this through
		// GetPaymentByIdempotencyKey rather than guessing. Returning an error here is
		// correct: the caller must NOT treat it as a decline.
		return nil, fmt.Errorf("payments: provider call failed for %s: %w", req.IdempotencyKey, pspErr)
	}

	// --- tx2: record the answer atomically with the event ---
	result := &AuthorizeResult{PaymentID: paymentID}
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		now := time.Now().UTC()

		if !pspResult.OK() {
			f := failureFrom(pspResult.Failure)

			// A retryable failure is not an outcome. The provider was unreachable or
			// busy; nothing was decided and nothing happened. Memoising it under the
			// idempotency key would replay "unavailable" forever and turn a thirty-second
			// outage into a permanently failed order — a bug that looks exactly like a
			// working retry loop from the outside.
			//
			// So: keep the payment PENDING, record the reason for diagnostics, publish
			// nothing (there is no fact to report), and release the claim so the caller's
			// next attempt genuinely reaches the provider again.
			if f.Retryable {
				if err := s.updateState(ctx, tx, paymentID, StatePending, func(q *stateUpdate) {
					q.failure = f
				}); err != nil {
					return err
				}
				if err := idempotency.Release(ctx, tx, idempotency.ScopeAuthorizePayment, req.IdempotencyKey); err != nil {
					return err
				}
				result.Outcome = OutcomeFailed
				result.State = StatePending
				result.Failure = f
				s.log.Warn("authorize failed transiently, claim released for retry",
					"payment_id", paymentID, "code", f.Code, "correlation_id", req.CorrelationID)
				return nil
			}

			if err := s.updateState(ctx, tx, paymentID, StateFailed, func(q *stateUpdate) {
				q.failure = f
			}); err != nil {
				return err
			}
			result.Outcome = OutcomeFailed
			result.State = StateFailed
			result.Failure = f

			env, err := eventbus.NewEnvelope("payments.v1.PaymentFailed", "payment", paymentID,
				producerName, req.CorrelationID, req.CausationID, paymentFailedData{
					PaymentID:      paymentID,
					OrderID:        req.OrderID,
					CustomerID:     req.CustomerID,
					Amount:         req.Amount,
					Operation:      "AUTHORIZE",
					FailureCode:    f.Code,
					Retryable:      f.Retryable,
					Detail:         f.Detail,
					IdempotencyKey: req.IdempotencyKey,
					FailedAt:       eventbus.Timestamp(now),
				})
			if err != nil {
				return err
			}
			if err := outbox.Enqueue(ctx, tx, env, nil); err != nil {
				return err
			}
			return idempotency.Complete(ctx, tx, idempotency.ScopeAuthorizePayment, req.IdempotencyKey, result)
		}

		expiresAt := now.Add(s.authTTL)
		if err := s.updateState(ctx, tx, paymentID, StateAuthorized, func(q *stateUpdate) {
			q.expiresAt = &expiresAt
			q.providerRef = pspResult.Reference
		}); err != nil {
			return err
		}
		if err := insertLedgerEntry(ctx, tx, paymentID, EntryAuthorize,
			req.Amount, req.IdempotencyKey); err != nil {
			return err
		}

		result.Outcome = OutcomeAuthorized
		result.State = StateAuthorized
		result.ExpiresAt = &expiresAt

		env, err := eventbus.NewEnvelope("payments.v1.PaymentAuthorized", "payment", paymentID,
			producerName, req.CorrelationID, req.CausationID, paymentAuthorizedData{
				PaymentID:      paymentID,
				OrderID:        req.OrderID,
				CustomerID:     req.CustomerID,
				Amount:         req.Amount,
				IdempotencyKey: req.IdempotencyKey,
				AuthorizedAt:   eventbus.Timestamp(now),
				ExpiresAt:      eventbus.Timestamp(expiresAt),
			})
		if err != nil {
			return err
		}
		if err := outbox.Enqueue(ctx, tx, env, nil); err != nil {
			return err
		}
		return idempotency.Complete(ctx, tx, idempotency.ScopeAuthorizePayment, req.IdempotencyKey, result)
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// --- Capture ----------------------------------------------------------------

type CaptureRequest struct {
	IdempotencyKey string
	PaymentID      string
	Amount         Money
	CorrelationID  string
	CausationID    string
}

func (r CaptureRequest) fingerprint() any {
	return struct {
		PaymentID string `json:"payment_id"`
		Amount    Money  `json:"amount"`
	}{r.PaymentID, r.Amount}
}

type CaptureResult struct {
	Outcome       Outcome      `json:"outcome"`
	PaymentID     string       `json:"payment_id"`
	State         State        `json:"state"`
	CapturedTotal Money        `json:"captured_total"`
	LedgerEntryID string       `json:"ledger_entry_id,omitempty"`
	Failure       *FailureInfo `json:"failure,omitempty"`
	Deduplicated  bool         `json:"-"`
}

// Capture takes the money. Same three-phase shape as Authorize, with one addition: the
// preconditions are checked in tx1, before the provider is called, so a capture that the
// ledger already forbids never leaves this process.
func (s *Service) Capture(ctx context.Context, req CaptureRequest) (*CaptureResult, error) {
	var (
		replay  *CaptureResult
		refused *CaptureResult
		payment *Payment
	)

	err := s.inTx(ctx, func(tx pgx.Tx) error {
		status, stored, err := idempotency.Claim(ctx, tx,
			idempotency.ScopeCapturePayment, req.IdempotencyKey, req.fingerprint(), 0)
		if err != nil {
			return err
		}
		if status == idempotency.StatusCompleted {
			var prev CaptureResult
			if err := (&idempotency.Result{Response: stored}).Unmarshal(&prev); err != nil {
				return err
			}
			prev.Deduplicated = true
			replay = &prev
			return nil
		}

		payment, err = loadPaymentForUpdate(ctx, tx, req.PaymentID)
		if err != nil {
			return err
		}

		// Refusals decided from local state alone. Completing them here means a retry
		// gets the same refusal without another provider round trip.
		if refusal := s.refuseCapture(payment, req); refusal != nil {
			refused = refusal
			return idempotency.Complete(ctx, tx, idempotency.ScopeCapturePayment, req.IdempotencyKey, refusal)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if replay != nil {
		return replay, nil
	}
	if refused != nil {
		return refused, nil
	}

	pspResult, pspErr := s.psp.Capture(ctx, payment.ProviderRef, req.Amount.AmountMinor, req.Amount.Currency)
	if pspErr != nil {
		return nil, fmt.Errorf("payments: provider capture failed for %s: %w", req.IdempotencyKey, pspErr)
	}

	result := &CaptureResult{PaymentID: req.PaymentID}
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		now := time.Now().UTC()
		current, err := loadPaymentForUpdate(ctx, tx, req.PaymentID)
		if err != nil {
			return err
		}
		// The row lock was released when tx1 committed, so the payment may have moved.
		// Re-checking here rather than trusting the earlier read is what stops a
		// concurrent void from being overwritten by this capture.
		if refusal := s.refuseCapture(current, req); refusal != nil {
			*result = *refusal
			return idempotency.Complete(ctx, tx, idempotency.ScopeCapturePayment, req.IdempotencyKey, result)
		}

		if !pspResult.OK() {
			f := failureFrom(pspResult.Failure)

			// Same rule as Authorize: only terminal outcomes are memoised. See the
			// comment there for why replaying a transient failure is so damaging.
			if f.Retryable {
				if err := idempotency.Release(ctx, tx, idempotency.ScopeCapturePayment, req.IdempotencyKey); err != nil {
					return err
				}
				result.Outcome = OutcomeFailed
				result.State = current.State
				result.CapturedTotal = current.Captured()
				result.Failure = f
				s.log.Warn("capture failed transiently, claim released for retry",
					"payment_id", req.PaymentID, "code", f.Code, "correlation_id", req.CorrelationID)
				return nil
			}

			result.Outcome = OutcomeFailed
			result.State = current.State
			result.CapturedTotal = current.Captured()
			result.Failure = f

			env, err := eventbus.NewEnvelope("payments.v1.PaymentFailed", "payment", req.PaymentID,
				producerName, req.CorrelationID, req.CausationID, paymentFailedData{
					PaymentID: req.PaymentID, OrderID: current.OrderID, CustomerID: current.CustomerID,
					Amount: req.Amount, Operation: "CAPTURE", FailureCode: f.Code,
					Retryable: f.Retryable, Detail: f.Detail,
					IdempotencyKey: req.IdempotencyKey, FailedAt: eventbus.Timestamp(now),
				})
			if err != nil {
				return err
			}
			if err := outbox.Enqueue(ctx, tx, env, nil); err != nil {
				return err
			}
			return idempotency.Complete(ctx, tx, idempotency.ScopeCapturePayment, req.IdempotencyKey, result)
		}

		newTotal := current.CapturedMinor + req.Amount.AmountMinor
		if _, err := tx.Exec(ctx, `
			UPDATE payments SET state = $2, captured_minor = $3, updated_at = now()
			 WHERE id = $1`, req.PaymentID, StateCaptured, newTotal); err != nil {
			return fmt.Errorf("payments: recording capture: %w", err)
		}
		entryID, err := insertLedgerEntryReturningID(ctx, tx, req.PaymentID, EntryCapture,
			req.Amount, req.IdempotencyKey)
		if err != nil {
			return err
		}

		result.Outcome = OutcomeCaptured
		result.State = StateCaptured
		result.CapturedTotal = Money{newTotal, current.Currency}
		result.LedgerEntryID = entryID

		env, err := eventbus.NewEnvelope("payments.v1.PaymentCaptured", "payment", req.PaymentID,
			producerName, req.CorrelationID, req.CausationID, paymentCapturedData{
				PaymentID: req.PaymentID, OrderID: current.OrderID, CustomerID: current.CustomerID,
				Amount: req.Amount, CapturedTotal: result.CapturedTotal,
				LedgerEntryID: entryID, CapturedAt: eventbus.Timestamp(now),
			})
		if err != nil {
			return err
		}
		if err := outbox.Enqueue(ctx, tx, env, nil); err != nil {
			return err
		}
		return idempotency.Complete(ctx, tx, idempotency.ScopeCapturePayment, req.IdempotencyKey, result)
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// refuseCapture returns a refusal decided from local state, or nil to proceed.
func (s *Service) refuseCapture(p *Payment, req CaptureRequest) *CaptureResult {
	switch {
	case p == nil:
		return &CaptureResult{Outcome: OutcomeNotFound, PaymentID: req.PaymentID}
	case p.State == StateVoided:
		return &CaptureResult{Outcome: OutcomeVoided, PaymentID: p.ID, State: p.State, CapturedTotal: p.Captured()}
	case p.State == StateFailed || p.State == StatePending:
		return &CaptureResult{Outcome: OutcomeNotFound, PaymentID: p.ID, State: p.State, CapturedTotal: p.Captured()}
	case p.ExpiresAt != nil && p.ExpiresAt.Before(time.Now().UTC()):
		return &CaptureResult{Outcome: OutcomeAuthExpired, PaymentID: p.ID, State: p.State, CapturedTotal: p.Captured()}
	case !p.Captured().SameCurrency(req.Amount):
		return &CaptureResult{Outcome: OutcomeExceedsAuthorized, PaymentID: p.ID, State: p.State, CapturedTotal: p.Captured()}
	case p.CapturedMinor+req.Amount.AmountMinor > p.AuthorizedMinor:
		// The ledger invariant, checked before the provider is called so that money never
		// moves for a request the database would reject afterwards.
		return &CaptureResult{Outcome: OutcomeExceedsAuthorized, PaymentID: p.ID, State: p.State, CapturedTotal: p.Captured()}
	}
	return nil
}

// --- Void and Refund --------------------------------------------------------

type VoidRequest struct {
	IdempotencyKey string
	PaymentID      string
	CorrelationID  string
	CausationID    string
}

type VoidResult struct {
	Outcome      Outcome      `json:"outcome"`
	State        State        `json:"state"`
	Failure      *FailureInfo `json:"failure,omitempty"`
	Deduplicated bool         `json:"-"`
}

// Void releases a hold. Compensation for Authorize.
//
// "Already voided" and "authorization already expired" are both success: the hold is gone,
// which is what the caller wanted. A compensation that finds its work already done has
// succeeded, and treating that as an error would make retrying compensation unsafe.
func (s *Service) Void(ctx context.Context, req VoidRequest) (*VoidResult, error) {
	var replay, decided *VoidResult
	var payment *Payment

	err := s.inTx(ctx, func(tx pgx.Tx) error {
		status, stored, err := idempotency.Claim(ctx, tx,
			idempotency.ScopeVoidAuthorization, req.IdempotencyKey,
			struct {
				P string `json:"payment_id"`
			}{req.PaymentID}, 0)
		if err != nil {
			return err
		}
		if status == idempotency.StatusCompleted {
			var prev VoidResult
			if err := (&idempotency.Result{Response: stored}).Unmarshal(&prev); err != nil {
				return err
			}
			prev.Deduplicated = true
			replay = &prev
			return nil
		}

		payment, err = loadPaymentForUpdate(ctx, tx, req.PaymentID)
		if err != nil {
			return err
		}
		switch {
		case payment == nil:
			decided = &VoidResult{Outcome: OutcomeNotFound}
		case payment.State == StateVoided:
			decided = &VoidResult{Outcome: OutcomeVoided, State: StateVoided}
		case payment.CapturedMinor > 0:
			// A void is impossible; the caller must refund. Reported rather than silently
			// converted, because it means the saga compensated the wrong step and that is
			// worth failing loudly on.
			decided = &VoidResult{Outcome: OutcomeAlreadyCaptured, State: payment.State}
		case payment.State != StateAuthorized:
			decided = &VoidResult{Outcome: OutcomeNotFound, State: payment.State}
		}
		if decided != nil {
			return idempotency.Complete(ctx, tx, idempotency.ScopeVoidAuthorization, req.IdempotencyKey, decided)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if replay != nil {
		return replay, nil
	}
	if decided != nil {
		return decided, nil
	}

	pspResult, pspErr := s.psp.Void(ctx, payment.ProviderRef)
	if pspErr != nil {
		return nil, fmt.Errorf("payments: provider void failed for %s: %w", req.IdempotencyKey, pspErr)
	}

	result := &VoidResult{}
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		if !pspResult.OK() {
			// A failed compensation. The saga cannot proceed and cannot undo; this is the
			// one case the system escalates to a human rather than resolving itself.
			result.Outcome = OutcomeFailed
			result.State = payment.State
			result.Failure = failureFrom(pspResult.Failure)
			s.log.Error("compensation failed: could not void authorization",
				"payment_id", req.PaymentID, "correlation_id", req.CorrelationID,
				"failure", result.Failure.Code, "detail", result.Failure.Detail)
			return idempotency.Complete(ctx, tx, idempotency.ScopeVoidAuthorization, req.IdempotencyKey, result)
		}
		if err := s.updateState(ctx, tx, req.PaymentID, StateVoided, nil); err != nil {
			return err
		}
		if err := insertLedgerEntry(ctx, tx, req.PaymentID, EntryVoid,
			payment.Authorized(), req.IdempotencyKey); err != nil {
			return err
		}
		result.Outcome = OutcomeVoided
		result.State = StateVoided
		return idempotency.Complete(ctx, tx, idempotency.ScopeVoidAuthorization, req.IdempotencyKey, result)
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

type RefundRequest struct {
	IdempotencyKey string
	PaymentID      string
	Amount         Money
	Reason         string
	CorrelationID  string
	CausationID    string
}

type RefundResult struct {
	Outcome       Outcome      `json:"outcome"`
	State         State        `json:"state"`
	RefundedTotal Money        `json:"refunded_total"`
	LedgerEntryID string       `json:"ledger_entry_id,omitempty"`
	Failure       *FailureInfo `json:"failure,omitempty"`
	Deduplicated  bool         `json:"-"`
}

// Refund returns captured money. Compensation for Capture — but not a rollback: the
// capture still happened and both entries stay in the ledger forever.
func (s *Service) Refund(ctx context.Context, req RefundRequest) (*RefundResult, error) {
	var replay, decided *RefundResult
	var payment *Payment

	err := s.inTx(ctx, func(tx pgx.Tx) error {
		status, stored, err := idempotency.Claim(ctx, tx,
			idempotency.ScopeRefundPayment, req.IdempotencyKey,
			struct {
				P string `json:"payment_id"`
				A Money  `json:"amount"`
			}{req.PaymentID, req.Amount}, 0)
		if err != nil {
			return err
		}
		if status == idempotency.StatusCompleted {
			var prev RefundResult
			if err := (&idempotency.Result{Response: stored}).Unmarshal(&prev); err != nil {
				return err
			}
			prev.Deduplicated = true
			replay = &prev
			return nil
		}

		payment, err = loadPaymentForUpdate(ctx, tx, req.PaymentID)
		if err != nil {
			return err
		}
		switch {
		case payment == nil:
			decided = &RefundResult{Outcome: OutcomeNotFound}
		case payment.CapturedMinor == 0:
			decided = &RefundResult{Outcome: OutcomeNothingCaptured, State: payment.State}
		case !payment.Captured().SameCurrency(req.Amount),
			payment.RefundedMinor+req.Amount.AmountMinor > payment.CapturedMinor:
			decided = &RefundResult{Outcome: OutcomeExceedsCaptured, State: payment.State, RefundedTotal: payment.Refunded()}
		}
		if decided != nil {
			return idempotency.Complete(ctx, tx, idempotency.ScopeRefundPayment, req.IdempotencyKey, decided)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if replay != nil {
		return replay, nil
	}
	if decided != nil {
		return decided, nil
	}

	pspResult, pspErr := s.psp.Refund(ctx, payment.ProviderRef, req.Amount.AmountMinor, req.Amount.Currency)
	if pspErr != nil {
		return nil, fmt.Errorf("payments: provider refund failed for %s: %w", req.IdempotencyKey, pspErr)
	}

	result := &RefundResult{}
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		now := time.Now().UTC()
		if !pspResult.OK() {
			result.Outcome = OutcomeFailed
			result.State = payment.State
			result.RefundedTotal = payment.Refunded()
			result.Failure = failureFrom(pspResult.Failure)
			s.log.Error("compensation failed: could not refund payment",
				"payment_id", req.PaymentID, "correlation_id", req.CorrelationID,
				"failure", result.Failure.Code)
			return idempotency.Complete(ctx, tx, idempotency.ScopeRefundPayment, req.IdempotencyKey, result)
		}

		newTotal := payment.RefundedMinor + req.Amount.AmountMinor
		if _, err := tx.Exec(ctx, `
			UPDATE payments SET state = $2, refunded_minor = $3, updated_at = now()
			 WHERE id = $1`, req.PaymentID, StateRefunded, newTotal); err != nil {
			return fmt.Errorf("payments: recording refund: %w", err)
		}
		entryID, err := insertLedgerEntryReturningID(ctx, tx, req.PaymentID, EntryRefund,
			req.Amount, req.IdempotencyKey)
		if err != nil {
			return err
		}

		result.Outcome = OutcomeRefunded
		result.State = StateRefunded
		result.RefundedTotal = Money{newTotal, payment.Currency}
		result.LedgerEntryID = entryID

		reason := req.Reason
		if reason == "" {
			reason = "SAGA_COMPENSATION"
		}
		env, err := eventbus.NewEnvelope("payments.v1.PaymentRefunded", "payment", req.PaymentID,
			producerName, req.CorrelationID, req.CausationID, paymentRefundedData{
				PaymentID: req.PaymentID, OrderID: payment.OrderID, CustomerID: payment.CustomerID,
				Amount: req.Amount, RefundedTotal: result.RefundedTotal,
				LedgerEntryID: entryID, Reason: reason, RefundedAt: eventbus.Timestamp(now),
			})
		if err != nil {
			return err
		}
		if err := outbox.Enqueue(ctx, tx, env, nil); err != nil {
			return err
		}
		return idempotency.Complete(ctx, tx, idempotency.ScopeRefundPayment, req.IdempotencyKey, result)
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// --- Read -------------------------------------------------------------------

// GetByIdempotencyKey resolves an ambiguous outcome.
//
// A caller whose AuthorizePayment timed out does not know whether a hold was placed.
// Assuming failure risks leaking an authorization on a customer's card; assuming success
// risks confirming an unpaid order. Both are wrong some of the time, so it asks the
// service that owns the truth instead.
//
// A nil result means no payment matches — which is the answer that means "the request
// never arrived, it is safe to send again".
func (s *Service) GetByIdempotencyKey(ctx context.Context, key string) (*Payment, error) {
	var p *Payment
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var err error
		p, err = loadPaymentByIdempotencyKey(ctx, tx, key)
		return err
	})
	return p, err
}

func (s *Service) Get(ctx context.Context, paymentID string) (*Payment, error) {
	var p *Payment
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var err error
		p, err = loadPayment(ctx, tx, paymentID, false)
		return err
	})
	return p, err
}

// --- plumbing ---------------------------------------------------------------

func (s *Service) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("payments: beginning transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("payments: committing transaction: %w", err)
	}
	return nil
}

type stateUpdate struct {
	expiresAt   *time.Time
	providerRef string
	failure     *FailureInfo
}

func (s *Service) updateState(ctx context.Context, tx pgx.Tx, paymentID string, state State, mutate func(*stateUpdate)) error {
	u := stateUpdate{}
	if mutate != nil {
		mutate(&u)
	}
	var code, detail string
	var retryable bool
	if u.failure != nil {
		code, retryable, detail = u.failure.Code, u.failure.Retryable, u.failure.Detail
	}
	_, err := tx.Exec(ctx, `
		UPDATE payments
		   SET state = $2,
		       expires_at        = COALESCE($3, expires_at),
		       provider_ref      = CASE WHEN $4 = '' THEN provider_ref ELSE $4 END,
		       failure_code      = $5,
		       failure_retryable = $6,
		       failure_detail    = $7,
		       updated_at        = now()
		 WHERE id = $1`, paymentID, state, u.expiresAt, u.providerRef, code, retryable, detail)
	if err != nil {
		return fmt.Errorf("payments: updating payment %s to %s: %w", paymentID, state, err)
	}
	return nil
}

func insertLedgerEntry(ctx context.Context, tx pgx.Tx, paymentID string, entryType EntryType, amount Money, key string) error {
	_, err := insertLedgerEntryReturningID(ctx, tx, paymentID, entryType, amount, key)
	return err
}

func insertLedgerEntryReturningID(ctx context.Context, tx pgx.Tx, paymentID string, entryType EntryType, amount Money, key string) (string, error) {
	id := uuid.NewString()
	_, err := tx.Exec(ctx, `
		INSERT INTO ledger_entries (id, payment_id, entry_type, amount_minor, currency, idempotency_key)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		id, paymentID, entryType, amount.AmountMinor, amount.Currency, key)
	if err != nil {
		return "", fmt.Errorf("payments: writing %s ledger entry for %s: %w", entryType, paymentID, err)
	}
	return id, nil
}

const paymentColumns = `id, order_id, customer_id, idempotency_key, state, currency,
	authorized_minor, captured_minor, refunded_minor, expires_at, provider_ref,
	failure_code, failure_retryable, failure_detail, created_at`

func scanPayment(row pgx.Row) (*Payment, error) {
	var p Payment
	err := row.Scan(&p.ID, &p.OrderID, &p.CustomerID, &p.IdempotencyKey, &p.State, &p.Currency,
		&p.AuthorizedMinor, &p.CapturedMinor, &p.RefundedMinor, &p.ExpiresAt, &p.ProviderRef,
		&p.FailureCode, &p.FailureRetryable, &p.FailureDetail, &p.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("payments: reading payment: %w", err)
	}
	return &p, nil
}

func loadPayment(ctx context.Context, tx pgx.Tx, paymentID string, forUpdate bool) (*Payment, error) {
	q := `SELECT ` + paymentColumns + ` FROM payments WHERE id = $1`
	if forUpdate {
		q += ` FOR UPDATE`
	}
	return scanPayment(tx.QueryRow(ctx, q, paymentID))
}

func loadPaymentForUpdate(ctx context.Context, tx pgx.Tx, paymentID string) (*Payment, error) {
	return loadPayment(ctx, tx, paymentID, true)
}

func loadPaymentByIdempotencyKey(ctx context.Context, tx pgx.Tx, key string) (*Payment, error) {
	return scanPayment(tx.QueryRow(ctx,
		`SELECT `+paymentColumns+` FROM payments WHERE idempotency_key = $1`, key))
}

// --- event payloads ---------------------------------------------------------
//
// These mirror the `data` objects in events/schemas/payments.v1.*.schema.json. Every
// envelope is validated against that schema before it reaches the outbox, so a field
// renamed here without the schema being updated fails at the point of publication rather
// than at a consumer a week later.

type paymentAuthorizedData struct {
	PaymentID      string             `json:"payment_id"`
	OrderID        string             `json:"order_id"`
	CustomerID     string             `json:"customer_id"`
	Amount         Money              `json:"amount"`
	IdempotencyKey string             `json:"idempotency_key"`
	AuthorizedAt   eventbus.Timestamp `json:"authorized_at"`
	ExpiresAt      eventbus.Timestamp `json:"expires_at"`
}

type paymentCapturedData struct {
	PaymentID     string             `json:"payment_id"`
	OrderID       string             `json:"order_id"`
	CustomerID    string             `json:"customer_id"`
	Amount        Money              `json:"amount"`
	CapturedTotal Money              `json:"captured_total"`
	LedgerEntryID string             `json:"ledger_entry_id"`
	CapturedAt    eventbus.Timestamp `json:"captured_at"`
}

type paymentRefundedData struct {
	PaymentID     string             `json:"payment_id"`
	OrderID       string             `json:"order_id"`
	CustomerID    string             `json:"customer_id"`
	Amount        Money              `json:"amount"`
	RefundedTotal Money              `json:"refunded_total"`
	LedgerEntryID string             `json:"ledger_entry_id"`
	Reason        string             `json:"reason"`
	RefundedAt    eventbus.Timestamp `json:"refunded_at"`
}

type paymentFailedData struct {
	PaymentID      string             `json:"payment_id"`
	OrderID        string             `json:"order_id"`
	CustomerID     string             `json:"customer_id"`
	Amount         Money              `json:"amount"`
	Operation      string             `json:"operation"`
	FailureCode    string             `json:"failure_code"`
	Retryable      bool               `json:"retryable"`
	Detail         string             `json:"detail,omitempty"`
	IdempotencyKey string             `json:"idempotency_key"`
	FailedAt       eventbus.Timestamp `json:"failed_at"`
}
