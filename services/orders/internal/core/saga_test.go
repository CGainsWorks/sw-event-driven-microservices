package core

// The compensation matrix: force the saga to fail at each step index and assert exactly
// which compensations run.
//
// These are unit tests of the saga ENGINE with scripted ports, not integration tests. That
// is deliberate. Failure at `capture_payment` and `commit_reservation` is very hard to
// provoke through the real system — you would have to make a payment provider fail only on
// capture, or expire a reservation inside the window between capture and commit — and any
// test that tried would be timing-dependent and flaky. With scripted ports the failure is
// exact and the assertion is about the engine's logic, which is what is being tested.
//
// They need a real PostgreSQL, because the saga's state machine IS database behaviour: the
// step log, the lease, the ordering of writes. They skip when SW_EDM_TEST_POSTGRES_DSN is
// unset, so `go test ./...` stays hermetic.
//
//	SW_EDM_TEST_POSTGRES_DSN='postgres://orders:orders@localhost:55432/orders?sslmode=disable' \
//	  go test ./services/orders/...

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/CGainsWorks/sw-event-driven-microservices/libs/idempotency"
	"github.com/CGainsWorks/sw-event-driven-microservices/libs/outbox"
	"github.com/CGainsWorks/sw-event-driven-microservices/libs/pgmigrate"
)

// --- an isolated schema -----------------------------------------------------

// testPool builds a pool pinned to a throwaway schema.
//
// A private schema, not the shared one. The orders service running against the same
// database has a recovery loop that polls saga_instances for anything due — it would find
// these test sagas and advance them with the REAL ports, which is a race that would make
// every assertion here meaningless and intermittent.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("SW_EDM_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("SW_EDM_TEST_POSTGRES_DSN is not set; skipping database-backed saga tests")
	}
	ctx := context.Background()
	schema := "saga_test_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")

	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}
	defer admin.Close()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("creating schema: %v", err)
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parsing dsn: %v", err)
	}
	// Every unqualified table in the migrations and the queries resolves here.
	cfg.ConnConfig.RuntimeParams["search_path"] = schema

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connecting to schema: %v", err)
	}

	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := pgmigrate.Apply(ctx, pool, quiet,
		Migrations(outbox.Migration(), idempotency.Migration())); err != nil {
		t.Fatalf("migrating: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		cleanup, err := pgxpool.New(context.Background(), dsn)
		if err != nil {
			return
		}
		defer cleanup.Close()
		_, _ = cleanup.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
	})
	return pool
}

// --- scripted ports ---------------------------------------------------------

// scripted records every call and returns whatever the test told it to.
//
// Recording the call ORDER is the point: the compensation matrix is a claim about which
// compensations run and in what sequence, and only a call log can check the sequence.
type scripted struct {
	mu    sync.Mutex
	calls []string

	reserve   func() (StepOutcome, error)
	release   func() (StepOutcome, error)
	commit    func() (StepOutcome, error)
	authorize func() (StepOutcome, error)
	capture   func() (StepOutcome, error)
	void      func() (StepOutcome, error)
	refund    func() (StepOutcome, error)
}

func (s *scripted) record(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, name)
}

func (s *scripted) log() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

// compensations filters the log down to the undo calls, which is what the matrix is about.
func (s *scripted) compensations() []string {
	var out []string
	for _, c := range s.log() {
		switch c {
		case "release", "void", "refund":
			out = append(out, c)
		}
	}
	return out
}

func ok(id string) func() (StepOutcome, error) {
	return func() (StepOutcome, error) {
		return StepOutcome{OK: true, PaymentID: id, ReservationID: id}, nil
	}
}

// terminalFailure is a business refusal: the callee answered, and the answer was no.
// Retrying cannot change it, so the saga must compensate rather than back off.
func terminalFailure(detail string) func() (StepOutcome, error) {
	return func() (StepOutcome, error) {
		return StepOutcome{OK: false, Retryable: false, Detail: detail}, nil
	}
}

func (s *scripted) Reserve(context.Context, ReserveInput) (StepOutcome, error) {
	s.record("reserve")
	return s.reserve()
}
func (s *scripted) Release(context.Context, ReleaseInput) (StepOutcome, error) {
	s.record("release")
	return s.release()
}
func (s *scripted) Commit(context.Context, CommitInput) (StepOutcome, error) {
	s.record("commit")
	return s.commit()
}
func (s *scripted) ResolveReserve(context.Context, string) (*StepOutcome, error) {
	s.record("resolve_reserve")
	return nil, nil
}
func (s *scripted) Authorize(context.Context, AuthorizeInput) (StepOutcome, error) {
	s.record("authorize")
	return s.authorize()
}
func (s *scripted) Capture(context.Context, CaptureInput) (StepOutcome, error) {
	s.record("capture")
	return s.capture()
}
func (s *scripted) Void(context.Context, VoidInput) (StepOutcome, error) {
	s.record("void")
	return s.void()
}
func (s *scripted) Refund(context.Context, RefundInput) (StepOutcome, error) {
	s.record("refund")
	return s.refund()
}
func (s *scripted) ResolveAuthorize(context.Context, string) (*StepOutcome, error) {
	s.record("resolve_authorize")
	return nil, nil
}

// allSucceed is the baseline every scenario starts from; the test then breaks one step.
func allSucceed() *scripted {
	res := uuid.NewString()
	pay := uuid.NewString()
	return &scripted{
		reserve:   ok(res),
		release:   ok(res),
		commit:    ok(res),
		authorize: ok(pay),
		capture:   ok(pay),
		void:      ok(pay),
		refund:    ok(pay),
	}
}

func newTestService(t *testing.T, pool *pgxpool.Pool, ports *scripted) *Service {
	t.Helper()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewService(pool, ports, ports, quiet, Config{
		StepTimeout:   2 * time.Second,
		LeaseDuration: 10 * time.Second,
		RetryBase:     50 * time.Millisecond,
		RetryMax:      time.Second,
	})
}

func placeAndAdvance(t *testing.T, svc *Service) *Order {
	t.Helper()
	ctx := context.Background()
	order, err := svc.PlaceOrder(ctx, PlaceOrderRequest{
		CustomerID:         uuid.NewString(),
		CustomerEmail:      "saga@example.com",
		PaymentMethodToken: "pm_ok",
		Lines: []Line{{
			SKU: "SKU-SAGA-TEST", Quantity: 2,
			UnitAmount: Money{AmountMinor: 1250, Currency: "GBP"},
		}},
	})
	if err != nil {
		t.Fatalf("placing order: %v", err)
	}
	if err := svc.Advance(ctx, order.ID); err != nil {
		t.Fatalf("advancing saga: %v", err)
	}
	final, err := svc.Get(ctx, order.ID)
	if err != nil {
		t.Fatalf("reading order: %v", err)
	}
	return final
}

// --- the matrix -------------------------------------------------------------

// TestCompensationMatrix is the claim docs/sagas.md makes, checked.
//
// Compensations run in REVERSE, and stock is released last — so that a compensation which
// gets stuck partway leaves the system holding stock it does not need, rather than having
// released the stock while still holding the customer's money.
func TestCompensationMatrix(t *testing.T) {
	pool := testPool(t)

	cases := []struct {
		name string
		// breakStep installs the failure.
		breakStep func(*scripted)

		wantOrderState    State
		wantSagaState     SagaState
		wantReason        string
		wantFailedStep    string
		wantCompensations []string
	}{
		{
			name:              "every step succeeds",
			breakStep:         func(*scripted) {},
			wantOrderState:    StateConfirmed,
			wantSagaState:     SagaCompleted,
			wantCompensations: nil,
		},
		{
			name:           "step 0 reserve_stock fails",
			breakStep:      func(s *scripted) { s.reserve = terminalFailure("OUTCOME_INSUFFICIENT_STOCK") },
			wantOrderState: StateCancelled,
			wantSagaState:  SagaCompensated,
			wantReason:     "INSUFFICIENT_STOCK",
			wantFailedStep: "RESERVE_STOCK",
			// Nothing had succeeded, so there is nothing to undo. A compensation here
			// would mean releasing a reservation that was never made.
			wantCompensations: nil,
		},
		{
			name:              "step 1 authorize_payment fails",
			breakStep:         func(s *scripted) { s.authorize = terminalFailure("FAILURE_CODE_CARD_DECLINED") },
			wantOrderState:    StateCancelled,
			wantSagaState:     SagaCompensated,
			wantReason:        "PAYMENT_DECLINED",
			wantFailedStep:    "AUTHORIZE_PAYMENT",
			wantCompensations: []string{"release"},
		},
		{
			name:           "step 2 capture_payment fails",
			breakStep:      func(s *scripted) { s.capture = terminalFailure("FAILURE_CODE_PSP_DECLINED") },
			wantOrderState: StateCancelled,
			wantSagaState:  SagaCompensated,
			wantReason:     "PAYMENT_ERROR",
			wantFailedStep: "CAPTURE_PAYMENT",
			// The authorization must be voided BEFORE the stock is released.
			wantCompensations: []string{"void", "release"},
		},
		{
			name:           "step 3 commit_reservation fails",
			breakStep:      func(s *scripted) { s.commit = terminalFailure("OUTCOME_EXPIRED") },
			wantOrderState: StateCancelled,
			wantSagaState:  SagaCompensated,
			wantReason:     "RESERVATION_EXPIRED",
			wantFailedStep: "COMMIT_RESERVATION",
			// The expensive path, and the only one with a customer-visible trace: money was
			// already captured, so undoing it is a refund on their statement.
			wantCompensations: []string{"refund", "void", "release"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ports := allSucceed()
			tc.breakStep(ports)
			svc := newTestService(t, pool, ports)

			final := placeAndAdvance(t, svc)

			if final.State != tc.wantOrderState {
				t.Errorf("order state = %s, want %s (reason %q)", final.State, tc.wantOrderState, final.CancellationReason)
			}
			if tc.wantReason != "" && final.CancellationReason != tc.wantReason {
				t.Errorf("cancellation_reason = %q, want %q", final.CancellationReason, tc.wantReason)
			}
			if tc.wantFailedStep != "" && final.FailedStep != tc.wantFailedStep {
				t.Errorf("failed_step = %q, want %q", final.FailedStep, tc.wantFailedStep)
			}

			saga, err := svc.SagaFor(context.Background(), final.ID)
			if err != nil {
				t.Fatalf("reading saga: %v", err)
			}
			if saga.State != tc.wantSagaState {
				t.Errorf("saga state = %s, want %s (last error %q)", saga.State, tc.wantSagaState, saga.LastError)
			}

			got := ports.compensations()
			if !equal(got, tc.wantCompensations) {
				t.Errorf("compensations ran %v, want %v\nfull call log: %v",
					got, tc.wantCompensations, ports.log())
			}
			t.Logf("calls: %v", ports.log())
		})
	}
}

// TestCompensationsRunInReverse checks the ordering claim directly, rather than inferring it
// from the matrix. Release LAST is the property that matters if compensation gets stuck.
func TestCompensationsRunInReverse(t *testing.T) {
	pool := testPool(t)
	ports := allSucceed()
	ports.commit = terminalFailure("OUTCOME_EXPIRED")
	svc := newTestService(t, pool, ports)

	placeAndAdvance(t, svc)

	comps := ports.compensations()
	want := []string{"refund", "void", "release"}
	if !equal(comps, want) {
		t.Fatalf("compensations ran %v, want %v", comps, want)
	}
	if comps[len(comps)-1] != "release" {
		t.Error("stock was not released last; a stuck compensation would leave the customer's " +
			"money held with the stock already given away")
	}
}

// TestFailedCompensationLeavesTheSagaStuck.
//
// STUCK is the one state the system never resolves automatically. Pretending otherwise
// would mean silently losing either a customer's money or the shop's stock, so it stops and
// asks for a human — and the order must NOT be reported as cancelled, because the system is
// still holding something on the customer's behalf.
func TestFailedCompensationLeavesTheSagaStuck(t *testing.T) {
	pool := testPool(t)
	ports := allSucceed()
	ports.authorize = terminalFailure("FAILURE_CODE_CARD_DECLINED")
	ports.release = func() (StepOutcome, error) {
		return StepOutcome{OK: false, Detail: "inventory refused the release"}, nil
	}
	svc := newTestService(t, pool, ports)

	final := placeAndAdvance(t, svc)

	saga, err := svc.SagaFor(context.Background(), final.ID)
	if err != nil {
		t.Fatalf("reading saga: %v", err)
	}
	if saga.State != SagaStuck {
		t.Errorf("saga state = %s, want STUCK", saga.State)
	}
	if final.State == StateCancelled {
		t.Error("the order was reported CANCELLED even though compensation failed; " +
			"OrderCancelled promises the system holds nothing on the customer's behalf")
	}
	if saga.LastError == "" {
		t.Error("a stuck saga recorded no reason, so a human has nothing to act on")
	}
}

// TestRetryableFailureDoesNotCompensate.
//
// A retryable failure means the callee could not answer, not that it said no. Compensating
// on it would cancel an order that was about to succeed — which is the same mistake as
// treating a timeout as a decline, one layer up.
func TestRetryableFailureDoesNotCompensate(t *testing.T) {
	pool := testPool(t)
	ports := allSucceed()
	ports.authorize = func() (StepOutcome, error) {
		return StepOutcome{OK: false, Retryable: true, Detail: "FAILURE_CODE_PSP_UNAVAILABLE"}, nil
	}
	svc := newTestService(t, pool, ports)

	final := placeAndAdvance(t, svc)

	if final.State == StateCancelled {
		t.Error("the order was cancelled on a RETRYABLE failure; nothing was decided")
	}
	saga, err := svc.SagaFor(context.Background(), final.ID)
	if err != nil {
		t.Fatalf("reading saga: %v", err)
	}
	if saga.State != SagaRunning {
		t.Errorf("saga state = %s, want RUNNING — it should be waiting to try again", saga.State)
	}
	if comps := ports.compensations(); len(comps) != 0 {
		t.Errorf("compensations ran on a retryable failure: %v", comps)
	}
}

// TestAmbiguousOutcomeIsNotTreatedAsFailure.
//
// The single most important behaviour in the system: a call whose outcome is unknown must
// never become a decline. Here the resolver also cannot answer, so the step must stay
// unresolved and the saga must wait — not compensate, and certainly not confirm.
func TestAmbiguousOutcomeIsNotTreatedAsFailure(t *testing.T) {
	pool := testPool(t)
	ports := allSucceed()
	ports.authorize = func() (StepOutcome, error) {
		return StepOutcome{}, fmt.Errorf("%w: deadline exceeded", ErrAmbiguous)
	}
	svc := newTestService(t, pool, ports)

	final := placeAndAdvance(t, svc)

	if final.State == StateCancelled {
		t.Fatal("the order was CANCELLED on an ambiguous outcome; the payment may have " +
			"succeeded and this would confirm a cancellation that is not true")
	}
	if final.State == StateConfirmed {
		t.Fatal("the order was CONFIRMED on an ambiguous outcome; the payment may never " +
			"have happened")
	}

	saga, err := svc.SagaFor(context.Background(), final.ID)
	if err != nil {
		t.Fatalf("reading saga: %v", err)
	}
	if saga.State != SagaRunning {
		t.Errorf("saga state = %s, want RUNNING", saga.State)
	}

	var authorize *StepLog
	for i := range saga.Steps {
		if saga.Steps[i].Name == "authorize_payment" {
			authorize = &saga.Steps[i]
		}
	}
	if authorize == nil {
		t.Fatal("authorize_payment is missing from the step log")
	}
	if authorize.State == StepFailed {
		t.Error("the step was marked FAILED; a timeout is not an outcome")
	}
	if authorize.State != StepUnknown {
		t.Errorf("step state = %s, want UNKNOWN", authorize.State)
	}

	// It must have TRIED to resolve rather than assuming.
	tried := false
	for _, c := range ports.log() {
		if c == "resolve_authorize" {
			tried = true
		}
	}
	if !tried {
		t.Error("the saga did not attempt to resolve the unknown outcome; it guessed")
	}
}

// TestSagaIsIdempotentAcrossAdvances: advancing an already-terminal saga must do nothing.
// The recovery loop and the nudge queue can both reach the same saga, so this happens.
func TestSagaIsIdempotentAcrossAdvances(t *testing.T) {
	pool := testPool(t)
	ports := allSucceed()
	svc := newTestService(t, pool, ports)

	final := placeAndAdvance(t, svc)
	if final.State != StateConfirmed {
		t.Fatalf("order state = %s, want CONFIRMED", final.State)
	}
	before := len(ports.log())

	for i := 0; i < 3; i++ {
		if err := svc.Advance(context.Background(), final.ID); err != nil {
			t.Fatalf("re-advancing: %v", err)
		}
	}
	if after := len(ports.log()); after != before {
		t.Errorf("re-advancing a completed saga made %d extra calls", after-before)
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
