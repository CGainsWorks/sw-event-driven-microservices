// Package psp is a stand-in for a payment service provider.
//
// It is a simulator, not a real integration, and nothing in this repository pretends
// otherwise. Its behaviour is chosen entirely by the payment_method_token on the request,
// which makes every failure path in the system reachable from a test without stopping a
// container or patching a mock:
//
//	pm_ok                success
//	pm_declined          CARD_DECLINED        terminal
//	pm_insufficient      INSUFFICIENT_FUNDS   terminal
//	pm_expired           CARD_EXPIRED         terminal
//	pm_fraud             FRAUD_SUSPECTED      terminal
//	pm_unavailable       PSP_UNAVAILABLE      retryable
//	pm_slow_<ms>         succeeds after <ms>  used to drive deadline and timeout tests
//	pm_flaky_<n>         fails <n> times with PSP_UNAVAILABLE, then succeeds
//	anything else        success
//
// A real adapter would go here unchanged in shape: same interface, same error taxonomy.
// What it would not have is deterministic control, which is why the tests target this one.
package psp

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// FailureCode mirrors sw_edm.payments.v1.FailureCode. It is redefined rather than imported
// so that the PSP adapter has no dependency on the gRPC contract — a provider integration
// should not know what shape this system's API happens to be.
type FailureCode string

const (
	CodeInsufficientFunds FailureCode = "INSUFFICIENT_FUNDS"
	CodeCardDeclined      FailureCode = "CARD_DECLINED"
	CodeCardExpired       FailureCode = "CARD_EXPIRED"
	CodeFraudSuspected    FailureCode = "FRAUD_SUSPECTED"
	CodeInvalidRequest    FailureCode = "INVALID_REQUEST"
	CodeUnavailable       FailureCode = "PSP_UNAVAILABLE"
	CodeInternalError     FailureCode = "INTERNAL_ERROR"
)

// Retryable reports whether the same request could succeed later.
//
// This mapping lives in one place. Publishing a `retryable` flag derived from it, rather
// than letting each caller keep its own copy, is why callers cannot disagree about whether
// a decline is worth retrying.
func (c FailureCode) Retryable() bool {
	switch c {
	case CodeUnavailable, CodeInternalError:
		return true
	default:
		return false
	}
}

// Failure is a refusal from the provider. It is a value, not an error: a declined card is
// an answer the domain must record, not an exception to propagate.
type Failure struct {
	Code   FailureCode
	Detail string
}

// Result of an operation. Exactly one of Reference or Failure is meaningful.
type Result struct {
	// Reference is the provider's handle for the transaction, stored for support queries.
	Reference string
	Failure   *Failure
}

func (r Result) OK() bool { return r.Failure == nil }

// Provider is the interface a real PSP adapter would implement.
type Provider interface {
	Authorize(ctx context.Context, token string, amountMinor int64, currency, reference string) (Result, error)
	Capture(ctx context.Context, providerRef string, amountMinor int64, currency string) (Result, error)
	Void(ctx context.Context, providerRef string) (Result, error)
	Refund(ctx context.Context, providerRef string, amountMinor int64, currency string) (Result, error)
}

// Simulator implements Provider with behaviour driven by the token.
type Simulator struct {
	mu sync.Mutex

	// flakyAttempts counts calls for a pm_flaky_<n> token so it can fail a fixed number of
	// times and then succeed.
	//
	// Keyed by token AND reference, not by token alone. The reference is derived from the
	// caller's idempotency key, so retries of one logical request share a counter while a
	// different order starts a fresh one. Keying by token alone makes the simulator
	// stateful across orders: the first test to use pm_flaky_2 exhausts it and every later
	// one sees an immediate success, which is a test that passes for the wrong reason and
	// then stops testing anything at all.
	flakyAttempts map[string]int
}

func NewSimulator() *Simulator {
	return &Simulator{flakyAttempts: map[string]int{}}
}

func (s *Simulator) Authorize(ctx context.Context, token string, amountMinor int64, currency, reference string) (Result, error) {
	if amountMinor <= 0 {
		return Result{Failure: &Failure{Code: CodeInvalidRequest, Detail: "amount must be positive"}}, nil
	}
	if len(currency) != 3 {
		return Result{Failure: &Failure{Code: CodeInvalidRequest, Detail: "currency must be a three-letter code"}}, nil
	}
	return s.decide(ctx, token, "auth_"+reference)
}

// Capture, Void and Refund succeed for any reference the simulator issued. A real provider
// would reject an unknown or already-settled reference; this one leaves those checks to
// payments, which enforces them against its own ledger before ever calling out. That is
// deliberate — the ledger invariants must hold whether or not the provider helps.
func (s *Simulator) Capture(ctx context.Context, providerRef string, amountMinor int64, currency string) (Result, error) {
	return s.decide(ctx, providerRef, "cap_"+providerRef)
}

func (s *Simulator) Void(ctx context.Context, providerRef string) (Result, error) {
	return Result{Reference: "void_" + providerRef}, nil
}

func (s *Simulator) Refund(ctx context.Context, providerRef string, amountMinor int64, currency string) (Result, error) {
	return Result{Reference: "ref_" + providerRef}, nil
}

func (s *Simulator) decide(ctx context.Context, token, reference string) (Result, error) {
	switch {
	case strings.HasPrefix(token, "pm_slow_"):
		d, err := strconv.Atoi(strings.TrimPrefix(token, "pm_slow_"))
		if err != nil {
			return Result{Failure: &Failure{Code: CodeInvalidRequest, Detail: "malformed pm_slow_ token"}}, nil
		}
		select {
		case <-ctx.Done():
			// The caller gave up. This is the ambiguous outcome the saga has to resolve:
			// from the caller's side it is indistinguishable from the provider never
			// having received the request.
			return Result{}, ctx.Err()
		case <-time.After(time.Duration(d) * time.Millisecond):
			return Result{Reference: reference}, nil
		}

	case strings.HasPrefix(token, "pm_flaky_"):
		n, err := strconv.Atoi(strings.TrimPrefix(token, "pm_flaky_"))
		if err != nil {
			return Result{Failure: &Failure{Code: CodeInvalidRequest, Detail: "malformed pm_flaky_ token"}}, nil
		}
		counterKey := token + "|" + reference
		s.mu.Lock()
		s.flakyAttempts[counterKey]++
		attempt := s.flakyAttempts[counterKey]
		s.mu.Unlock()
		if attempt <= n {
			return Result{Failure: &Failure{
				Code:   CodeUnavailable,
				Detail: fmt.Sprintf("simulated transient failure %d of %d", attempt, n),
			}}, nil
		}
		return Result{Reference: reference}, nil
	}

	switch token {
	case "pm_declined":
		return Result{Failure: &Failure{Code: CodeCardDeclined, Detail: "psp returned do_not_honour"}}, nil
	case "pm_insufficient":
		return Result{Failure: &Failure{Code: CodeInsufficientFunds, Detail: "psp returned insufficient_funds"}}, nil
	case "pm_expired":
		return Result{Failure: &Failure{Code: CodeCardExpired, Detail: "psp returned expired_card"}}, nil
	case "pm_fraud":
		return Result{Failure: &Failure{Code: CodeFraudSuspected, Detail: "psp returned suspected_fraud"}}, nil
	case "pm_unavailable":
		return Result{Failure: &Failure{Code: CodeUnavailable, Detail: "psp connection refused"}}, nil
	default:
		return Result{Reference: reference}, nil
	}
}

// Reset clears the flaky-token counters. Tests call it so that one test's retry history
// does not leak into the next.
func (s *Simulator) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flakyAttempts = map[string]int{}
}
