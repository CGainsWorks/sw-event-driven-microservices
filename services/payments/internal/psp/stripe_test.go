package psp

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
)

// fakeStripe answers with a fixed status code and body and records the last request.
type fakeStripe struct {
	status int
	body   string
	path   string
	form   url.Values
	idem   string
	user   string
}

func (f *fakeStripe) server(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		f.path = r.URL.Path
		f.form, _ = url.ParseQuery(string(raw))
		f.idem = r.Header.Get("Idempotency-Key")
		f.user = strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(f.status)
		_, _ = io.WriteString(w, f.body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newTestStripe(t *testing.T, f *fakeStripe) *Stripe {
	t.Helper()
	s, err := NewStripe(f.server(t).URL, "sk_test_unit", nil)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestStripeRefusesLiveKeys(t *testing.T) {
	for _, key := range []string{"sk_live_abc", "", "pk_test_abc"} {
		if _, err := NewStripe("", key, nil); err == nil {
			t.Errorf("key %q was accepted", key)
		}
	}
}

func TestStripeAuthorizeSendsAManualCaptureIntent(t *testing.T) {
	f := &fakeStripe{status: 200, body: `{"id":"pi_1","object":"payment_intent","status":"requires_capture"}`}
	res, err := newTestStripe(t, f).Authorize(context.Background(), "pm_card_visa", 2599, "GBP", "order-7")
	if err != nil || !res.OK() || res.Reference != "pi_1" {
		t.Fatalf("got %+v, %v", res, err)
	}
	want := map[string]string{
		"amount": "2599", "currency": "gbp", "payment_method": "pm_card_visa",
		"confirm": "true", "capture_method": "manual", "metadata[reference]": "order-7",
	}
	for k, v := range want {
		if got := f.form.Get(k); got != v {
			t.Errorf("%s = %q, want %q", k, got, v)
		}
	}
	if f.path != "/v1/payment_intents" || f.idem != "auth_order-7" || f.user != "sk_test_unit" {
		t.Errorf("path %q idempotency %q user %q", f.path, f.idem, f.user)
	}
}

func TestStripeStatusOtherThanRequiresCaptureIsADecline(t *testing.T) {
	f := &fakeStripe{status: 200, body: `{"id":"pi_2","object":"payment_intent","status":"requires_action"}`}
	res, err := newTestStripe(t, f).Authorize(context.Background(), "pm_card_threeDSecure2Required", 100, "gbp", "r")
	if err != nil || res.OK() || res.Failure.Code != CodeCardDeclined || !strings.Contains(res.Failure.Detail, "requires_action") {
		t.Fatalf("got %+v, %v", res, err)
	}
}

func TestStripeErrorMapping(t *testing.T) {
	cases := []struct {
		status int
		body   string
		want   FailureCode
	}{
		{402, `{"error":{"type":"card_error","code":"card_declined","decline_code":"insufficient_funds"}}`, CodeInsufficientFunds},
		{402, `{"error":{"type":"card_error","code":"expired_card"}}`, CodeCardExpired},
		{402, `{"error":{"type":"card_error","code":"card_declined","decline_code":"fraudulent"}}`, CodeFraudSuspected},
		{402, `{"error":{"type":"card_error","code":"card_declined","decline_code":"generic_decline"}}`, CodeCardDeclined},
		{400, `{"error":{"type":"invalid_request_error","message":"bad amount"}}`, CodeInvalidRequest},
		{429, `{"error":{"type":"rate_limit_error"}}`, CodeUnavailable},
		{503, `{}`, CodeUnavailable},
	}
	for _, c := range cases {
		f := &fakeStripe{status: c.status, body: c.body}
		res, err := newTestStripe(t, f).Authorize(context.Background(), "pm", 100, "gbp", "r")
		if err != nil || res.OK() || res.Failure.Code != c.want {
			t.Errorf("HTTP %d %s: got %+v, %v; want %s", c.status, c.body, res, err, c.want)
		}
		if res.Failure != nil && res.Failure.Code.Retryable() != (c.want == CodeUnavailable) {
			t.Errorf("HTTP %d: retryable mismatch", c.status)
		}
	}
}

func TestStripeTransportFailureIsAmbiguousNotADecline(t *testing.T) {
	s, _ := NewStripe("http://127.0.0.1:1", "sk_test_unit", nil)
	res, err := s.Authorize(context.Background(), "pm", 100, "gbp", "r")
	if err == nil || res.Failure != nil {
		t.Fatalf("a transport error must be returned as an error, got %+v, %v", res, err)
	}
}

func TestStripeCaptureVoidRefund(t *testing.T) {
	ctx := context.Background()
	f := &fakeStripe{status: 200, body: `{"id":"pi_3","object":"payment_intent","status":"succeeded"}`}
	s := newTestStripe(t, f)
	if res, err := s.Capture(ctx, "pi_3", 500, "gbp"); err != nil || !res.OK() || f.path != "/v1/payment_intents/pi_3/capture" || f.form.Get("amount_to_capture") != "500" {
		t.Errorf("capture: %+v %v %s", res, err, f.path)
	}
	f.body = `{"id":"pi_3","object":"payment_intent","status":"canceled"}`
	if res, err := s.Void(ctx, "pi_3"); err != nil || !res.OK() || f.path != "/v1/payment_intents/pi_3/cancel" {
		t.Errorf("void: %+v %v %s", res, err, f.path)
	}
	f.body = `{"id":"re_1","object":"refund","status":"pending"}`
	if res, err := s.Refund(ctx, "pi_3", 200, "gbp"); err != nil || !res.OK() || f.path != "/v1/refunds" || f.form.Get("payment_intent") != "pi_3" {
		t.Errorf("refund: %+v %v %s", res, err, f.path)
	}
}

func TestFromEnv(t *testing.T) {
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	if p, _, err := FromEnv(env(nil)); err != nil {
		t.Fatal(err)
	} else if _, ok := p.(*Simulator); !ok {
		t.Errorf("default provider is %T, want the simulator", p)
	}
	if _, _, err := FromEnv(env(map[string]string{"PSP_PROVIDER": "stripe", "STRIPE_SECRET_KEY": "sk_live_x"})); err == nil {
		t.Error("a live key was accepted")
	}
	if _, _, err := FromEnv(env(map[string]string{"PSP_PROVIDER": "acme"})); err == nil {
		t.Error("an unknown provider was accepted")
	}
}

// TestStripeAgainstStripeMock sends every request the adapter makes to stripe-mock, which
// validates it against Stripe's OpenAPI spec. stripe-mock is stateless and answers with
// canned objects, so this checks request shape and response parsing, not outcomes. CI
// runs it with a stripe-mock service container; locally:
//
//	docker run --rm -p 12111:12111 stripe/stripe-mock:v0.197.0
//	SW_EDM_STRIPE_MOCK_URL=http://localhost:12111 go test ./services/payments/internal/psp/
func TestStripeAgainstStripeMock(t *testing.T) {
	base := os.Getenv("SW_EDM_STRIPE_MOCK_URL")
	if base == "" {
		t.Skip("SW_EDM_STRIPE_MOCK_URL not set")
	}
	s, err := NewStripe(base, "sk_test_123", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	auth, err := s.Authorize(ctx, "pm_card_visa", 2599, "GBP", "order-mock-1")
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}
	// A request stripe-mock rejected (bad shape or bad credentials) would come back as
	// INVALID_REQUEST or INTERNAL_ERROR; a canned PaymentIntent maps to a status decline.
	if auth.Failure != nil && auth.Failure.Code != CodeCardDeclined {
		t.Fatalf("stripe-mock rejected the authorize request: %s", auth.Failure.Detail)
	}

	for name, call := range map[string]func() (Result, error){
		"capture": func() (Result, error) { return s.Capture(ctx, "pi_mock", 2599, "GBP") },
		"void":    func() (Result, error) { return s.Void(ctx, "pi_mock") },
		"refund":  func() (Result, error) { return s.Refund(ctx, "pi_mock", 2599, "GBP") },
	} {
		res, err := call()
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if res.Failure != nil && res.Failure.Code != CodeCardDeclined {
			t.Errorf("stripe-mock rejected the %s request: %s", name, res.Failure.Detail)
		}
	}

	// And a request that is invalid by Stripe's spec is rejected, so the check above is
	// not vacuous.
	bad, err := s.call(ctx, "/v1/payment_intents",
		url.Values{"amount": {"not-a-number"}, "currency": {"gbp"}}, "bad-1", "requires_capture")
	if err != nil || bad.Failure == nil || bad.Failure.Code != CodeInvalidRequest {
		t.Errorf("a non-numeric amount should be rejected by stripe-mock, got %+v, %v", bad.Failure, err)
	}
}
