//go:build integration

// Integration tests against a running compose stack.
//
//	docker compose up -d --build
//	go test -tags=integration ./tests/integration/... -v
//
// Build-tagged so `go test ./...` stays fast and hermetic. These tests need real
// PostgreSQL, real Kafka and two real services, and they assert on observable behaviour
// through the public API rather than reaching into databases — a test that reads the
// service's tables passes when the service is broken in exactly the way that matters.
package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"
)

var ordersBase = envOr("ORDERS_BASE_URL", "http://localhost:18080")

func envOr(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}

type money struct {
	AmountMinor int64  `json:"amount_minor"`
	Currency    string `json:"currency"`
}

type line struct {
	SKU        string `json:"sku"`
	Quantity   uint32 `json:"quantity"`
	UnitAmount money  `json:"unit_amount"`
}

type placeRequest struct {
	CustomerID         string `json:"customer_id"`
	CustomerEmail      string `json:"customer_email"`
	PaymentMethodToken string `json:"payment_method_token"`
	Lines              []line `json:"lines"`
}

type orderResponse struct {
	OrderID       string `json:"order_id"`
	State         string `json:"state"`
	Total         money  `json:"total_amount"`
	CorrelationID string `json:"correlation_id"`
	PaymentID     string `json:"payment_id"`
	ReservationID string `json:"reservation_id"`
	Reason        string `json:"cancellation_reason"`
	FailedStep    string `json:"failed_step"`
}

type stepLog struct {
	Index    int    `json:"index"`
	Name     string `json:"name"`
	State    string `json:"state"`
	Attempts int    `json:"attempts"`
	Detail   string `json:"detail"`
}

type sagaSnapshot struct {
	State       string    `json:"state"`
	CurrentStep int       `json:"current_step"`
	Attempts    int       `json:"attempts"`
	LastError   string    `json:"last_error"`
	Steps       []stepLog `json:"steps"`
}

func TestMain(m *testing.M) {
	// Fail fast and clearly when the stack is not up, rather than producing a wall of
	// connection-refused errors that look like test failures.
	if err := waitReady(60 * time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "orders is not ready at %s: %v\nrun `docker compose up -d --build` first\n", ordersBase, err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func waitReady(within time.Duration) error {
	deadline := time.Now().Add(within)
	var last error
	for time.Now().Before(deadline) {
		resp, err := http.Get(ordersBase + "/health/ready")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
			last = fmt.Errorf("status %d", resp.StatusCode)
		} else {
			last = err
		}
		time.Sleep(time.Second)
	}
	return last
}

func place(t *testing.T, token string, lines ...line) orderResponse {
	t.Helper()
	if len(lines) == 0 {
		lines = []line{{SKU: "SKU-WIDGET-01", Quantity: 1, UnitAmount: money{1250, "GBP"}}}
	}
	body, err := json.Marshal(placeRequest{
		CustomerID:         "1d5a8c3f-7b2e-4d9a-8f6c-3e5b7d9f1a2c",
		CustomerEmail:      "integration@example.com",
		PaymentMethodToken: token,
		Lines:              lines,
	})
	if err != nil {
		t.Fatalf("marshalling request: %v", err)
	}

	resp, err := http.Post(ordersBase+"/orders", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("placing order: %v", err)
	}
	defer resp.Body.Close()

	// 202, not 201. The order is accepted but not fulfilled: no stock is checked and no
	// money has moved. Asserting on this is asserting that the API does not overclaim.
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d", resp.StatusCode)
	}

	var out orderResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if out.OrderID == "" {
		t.Fatal("response carried no order_id")
	}
	return out
}

func getOrder(t *testing.T, id string) orderResponse {
	t.Helper()
	resp, err := http.Get(ordersBase + "/orders/" + id)
	if err != nil {
		t.Fatalf("reading order: %v", err)
	}
	defer resp.Body.Close()
	var out orderResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decoding order: %v", err)
	}
	return out
}

func getSaga(t *testing.T, id string) sagaSnapshot {
	t.Helper()
	resp, err := http.Get(ordersBase + "/orders/" + id + "/saga")
	if err != nil {
		t.Fatalf("reading saga: %v", err)
	}
	defer resp.Body.Close()
	var out sagaSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decoding saga: %v", err)
	}
	return out
}

// awaitTerminal polls until the order reaches CONFIRMED or CANCELLED.
//
// Polling rather than sleeping a fixed duration: the saga is asynchronous by design, and a
// fixed sleep is either flaky or slow. On timeout it dumps the saga log, because "the
// order never confirmed" is not a useful failure message on its own.
func awaitTerminal(t *testing.T, id string, within time.Duration) orderResponse {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		o := getOrder(t, id)
		if o.State == "CONFIRMED" || o.State == "CANCELLED" {
			return o
		}
		time.Sleep(250 * time.Millisecond)
	}
	saga := getSaga(t, id)
	dump, _ := json.MarshalIndent(saga, "", "  ")
	t.Fatalf("order %s did not reach a terminal state within %s\nsaga:\n%s", id, within, dump)
	return orderResponse{}
}

// TestHappyPath is the end-to-end proof that two services are genuinely talking: orders
// accepts over HTTP, drives payments over gRPC through two saga steps, and confirms.
func TestHappyPath(t *testing.T) {
	placed := place(t, "pm_ok",
		line{SKU: "SKU-WIDGET-01", Quantity: 2, UnitAmount: money{1250, "GBP"}},
		line{SKU: "SKU-GIZMO-07", Quantity: 1, UnitAmount: money{4999, "GBP"}},
	)

	if placed.State != "PENDING" {
		t.Errorf("a freshly placed order should be PENDING, got %s", placed.State)
	}
	if got, want := placed.Total.AmountMinor, int64(2*1250+4999); got != want {
		t.Errorf("total = %d, want %d (the server must compute it, not trust the client)", got, want)
	}

	final := awaitTerminal(t, placed.OrderID, 30*time.Second)
	if final.State != "CONFIRMED" {
		t.Fatalf("state = %s, want CONFIRMED (reason %q)", final.State, final.Reason)
	}
	if final.PaymentID == "" {
		t.Error("a confirmed order must carry the payment_id that paid for it")
	}
	if final.CorrelationID != placed.CorrelationID {
		t.Error("correlation_id changed between placement and confirmation")
	}

	saga := getSaga(t, placed.OrderID)
	if saga.State != "COMPLETED" {
		t.Errorf("saga state = %s, want COMPLETED", saga.State)
	}
	for _, s := range saga.Steps {
		if s.State != "DONE" {
			t.Errorf("step %s = %s, want DONE", s.Name, s.State)
		}
	}
}

// TestTerminalDeclineCompensates proves the saga branches on a business outcome rather
// than retrying it, and that the order ends CANCELLED with a reason a human can act on.
func TestTerminalDeclineCompensates(t *testing.T) {
	placed := place(t, "pm_declined")
	final := awaitTerminal(t, placed.OrderID, 30*time.Second)

	if final.State != "CANCELLED" {
		t.Fatalf("state = %s, want CANCELLED", final.State)
	}
	if final.Reason != "PAYMENT_DECLINED" {
		t.Errorf("cancellation_reason = %q, want PAYMENT_DECLINED", final.Reason)
	}
	if final.FailedStep != "AUTHORIZE_PAYMENT" {
		t.Errorf("failed_step = %q, want AUTHORIZE_PAYMENT", final.FailedStep)
	}

	saga := getSaga(t, placed.OrderID)
	if saga.State != "COMPENSATED" {
		t.Errorf("saga state = %s, want COMPENSATED", saga.State)
	}

	// The shape that must hold for any terminal failure: every step BEFORE the failure is
	// compensated, the failing step is FAILED, and nothing after it ran at all.
	//
	// Compensating the failed step itself would be a bug — there is nothing to undo, and
	// for authorize_payment specifically it would mean voiding an authorization that was
	// never granted.
	failedAt := -1
	for i, s := range saga.Steps {
		if s.State == "FAILED" {
			failedAt = i
			break
		}
	}
	if failedAt < 0 {
		t.Fatalf("no step is marked FAILED: %+v", saga.Steps)
	}
	for i, s := range saga.Steps {
		switch {
		case i < failedAt && s.State != "COMPENSATED":
			t.Errorf("step %s ran before the failure but is %s, want COMPENSATED", s.Name, s.State)
		case i > failedAt:
			t.Errorf("step %s ran after the failure; the saga must stop at the first one", s.Name)
		}
	}

	// A terminal decline must not be retried. Two attempts is the observed cost of the
	// step being marked PENDING and then FAILED; anything much higher would mean the saga
	// is backing off against a card that will never be approved.
	if a := saga.Steps[failedAt].Attempts; a > 2 {
		t.Errorf("failed step was attempted %d times; a terminal decline must not be retried", a)
	}
}

// TestTransientFailureIsRetriedNotMemoised is a regression test for a bug that is easy to
// write and hard to see: recording a retryable provider failure under the idempotency key.
//
// When that happens every retry replays the stored "unavailable" answer, the saga appears
// to be retrying correctly, and the order never completes. The provider simulator is told
// to fail twice and then succeed, so a correct implementation reaches CONFIRMED and an
// incorrect one spins until this test times out.
func TestTransientFailureIsRetriedNotMemoised(t *testing.T) {
	placed := place(t, "pm_flaky_2")
	final := awaitTerminal(t, placed.OrderID, 90*time.Second)

	if final.State != "CONFIRMED" {
		t.Fatalf("state = %s, want CONFIRMED: a transient provider failure must not become permanent", final.State)
	}

	saga := getSaga(t, placed.OrderID)
	var authorize *stepLog
	for i := range saga.Steps {
		if saga.Steps[i].Name == "authorize_payment" {
			authorize = &saga.Steps[i]
		}
	}
	if authorize == nil {
		t.Fatal("no authorize_payment step in the saga log")
	}
	// Three attempts: two simulated failures and the success. Fewer would mean the
	// failures never reached the provider.
	if authorize.Attempts < 3 {
		t.Errorf("authorize_payment attempts = %d, want at least 3 "+
			"(two transient failures then success)", authorize.Attempts)
	}
}

// TestServerComputesTotals rejects a client-supplied total. Trusting the client here would
// let a caller pay whatever it liked.
func TestOrderValidation(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"no lines", `{"customer_id":"1d5a8c3f-7b2e-4d9a-8f6c-3e5b7d9f1a2c","customer_email":"a@b.c","payment_method_token":"pm_ok","lines":[]}`},
		{"zero quantity", `{"customer_id":"1d5a8c3f-7b2e-4d9a-8f6c-3e5b7d9f1a2c","customer_email":"a@b.c","payment_method_token":"pm_ok","lines":[{"sku":"SKU-WIDGET-01","quantity":0,"unit_amount":{"amount_minor":1250,"currency":"GBP"}}]}`},
		{"missing customer", `{"customer_email":"a@b.c","payment_method_token":"pm_ok","lines":[{"sku":"SKU-WIDGET-01","quantity":1,"unit_amount":{"amount_minor":1250,"currency":"GBP"}}]}`},
		{"mixed currencies", `{"customer_id":"1d5a8c3f-7b2e-4d9a-8f6c-3e5b7d9f1a2c","customer_email":"a@b.c","payment_method_token":"pm_ok","lines":[{"sku":"SKU-WIDGET-01","quantity":1,"unit_amount":{"amount_minor":1250,"currency":"GBP"}},{"sku":"SKU-GIZMO-07","quantity":1,"unit_amount":{"amount_minor":1250,"currency":"EUR"}}]}`},
		{"unknown field", `{"customer_id":"1d5a8c3f-7b2e-4d9a-8f6c-3e5b7d9f1a2c","customer_email":"a@b.c","payment_method_token":"pm_ok","total_amount":1,"lines":[{"sku":"SKU-WIDGET-01","quantity":1,"unit_amount":{"amount_minor":1250,"currency":"GBP"}}]}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := http.Post(ordersBase+"/orders", "application/json", bytes.NewReader([]byte(tc.body)))
			if err != nil {
				t.Fatalf("posting: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", resp.StatusCode)
			}
		})
	}
}

// TestUnknownOrderIs404 confirms a missing order is distinguishable from a broken service.
func TestUnknownOrderIs404(t *testing.T) {
	resp, err := http.Get(ordersBase + "/orders/00000000-0000-4000-8000-000000000000")
	if err != nil {
		t.Fatalf("reading order: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}
