package psp

// Stripe is a Provider backed by Stripe's PaymentIntents API, in test mode only.
//
// It exists to show the Provider interface holds for a real provider, not to take money:
// the constructor refuses anything but a test-mode key, and the service only selects it
// when PSP_PROVIDER=stripe. CI exercises it against stripe-mock, Stripe's official mock
// server, which validates every request against Stripe's published OpenAPI spec but
// answers with canned objects. The status and error mapping is therefore tested
// separately, against recorded response shapes, in stripe_test.go.
//
// The mapping onto the Provider contract:
//
//	Authorize  POST /v1/payment_intents (confirm=true, capture_method=manual)
//	           requires_capture -> authorised; anything else -> a decline with the status
//	Capture    POST /v1/payment_intents/{id}/capture      succeeded -> captured
//	Void       POST /v1/payment_intents/{id}/cancel       canceled  -> voided
//	Refund     POST /v1/refunds                           succeeded or pending -> refunded
//
// Card errors (HTTP 402) become terminal failures, 429 and 5xx become PSP_UNAVAILABLE
// (retryable), and a transport error is returned as an error: like a timeout, it is an
// ambiguous outcome the saga must resolve by asking rather than by guessing. Every write
// carries an Idempotency-Key derived from the caller's reference, so a retried call cannot
// create a second charge.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// StripeDefaultBase is Stripe's API. Tests and CI point the adapter at stripe-mock instead.
const StripeDefaultBase = "https://api.stripe.com"

// Stripe implements Provider over Stripe's REST API.
type Stripe struct {
	base   string
	key    string
	client *http.Client
}

// NewStripe returns a test-mode Stripe adapter. A live key is refused outright.
func NewStripe(base, key string, client *http.Client) (*Stripe, error) {
	if !strings.HasPrefix(key, "sk_test_") && !strings.HasPrefix(key, "rk_test_") {
		return nil, errors.New("psp: the Stripe adapter accepts test-mode keys only (sk_test_ or rk_test_)")
	}
	if base == "" {
		base = StripeDefaultBase
	}
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	return &Stripe{base: strings.TrimRight(base, "/"), key: key, client: client}, nil
}

type stripeObject struct {
	ID     string `json:"id"`
	Object string `json:"object"`
	Status string `json:"status"`
}

type stripeError struct {
	Error struct {
		Type        string `json:"type"`
		Code        string `json:"code"`
		DeclineCode string `json:"decline_code"`
		Message     string `json:"message"`
	} `json:"error"`
}

func (s *Stripe) Authorize(ctx context.Context, token string, amountMinor int64, currency, reference string) (Result, error) {
	if amountMinor <= 0 {
		return Result{Failure: &Failure{Code: CodeInvalidRequest, Detail: "amount must be positive"}}, nil
	}
	if len(currency) != 3 {
		return Result{Failure: &Failure{Code: CodeInvalidRequest, Detail: "currency must be a three-letter code"}}, nil
	}
	form := url.Values{
		"amount":              {strconv.FormatInt(amountMinor, 10)},
		"currency":            {strings.ToLower(currency)},
		"payment_method":      {token},
		"confirm":             {"true"},
		"capture_method":      {"manual"},
		"metadata[reference]": {reference},
	}
	return s.call(ctx, "/v1/payment_intents", form, "auth_"+reference, "requires_capture")
}

func (s *Stripe) Capture(ctx context.Context, providerRef string, amountMinor int64, currency string) (Result, error) {
	form := url.Values{"amount_to_capture": {strconv.FormatInt(amountMinor, 10)}}
	return s.call(ctx, "/v1/payment_intents/"+url.PathEscape(providerRef)+"/capture", form,
		"cap_"+providerRef, "succeeded")
}

func (s *Stripe) Void(ctx context.Context, providerRef string) (Result, error) {
	return s.call(ctx, "/v1/payment_intents/"+url.PathEscape(providerRef)+"/cancel", url.Values{},
		"void_"+providerRef, "canceled")
}

func (s *Stripe) Refund(ctx context.Context, providerRef string, amountMinor int64, currency string) (Result, error) {
	form := url.Values{
		"payment_intent": {providerRef},
		"amount":         {strconv.FormatInt(amountMinor, 10)},
	}
	return s.call(ctx, "/v1/refunds", form, "ref_"+providerRef, "succeeded", "pending")
}

func (s *Stripe) call(ctx context.Context, path string, form url.Values, idemKey string, okStatuses ...string) (Result, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.base+path, strings.NewReader(form.Encode()))
	if err != nil {
		return Result{}, err
	}
	req.Header.Set("Authorization", "Bearer "+s.key)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Idempotency-Key", idemKey)

	resp, err := s.client.Do(req)
	if err != nil {
		// No answer at all: the request may or may not have reached Stripe.
		return Result{}, fmt.Errorf("psp: stripe %s: %w", path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return Result{}, fmt.Errorf("psp: stripe %s: reading response: %w", path, err)
	}

	if resp.StatusCode >= 300 {
		return Result{Failure: mapStripeError(resp.StatusCode, body)}, nil
	}

	var obj stripeObject
	if err := json.Unmarshal(body, &obj); err != nil || obj.ID == "" {
		return Result{}, fmt.Errorf("psp: stripe %s: unexpected response body", path)
	}
	for _, ok := range okStatuses {
		if obj.Status == ok {
			return Result{Reference: obj.ID}, nil
		}
	}
	return Result{Failure: &Failure{
		Code:   CodeCardDeclined,
		Detail: fmt.Sprintf("stripe %s %s is %q", obj.Object, obj.ID, obj.Status),
	}}, nil
}

func mapStripeError(status int, body []byte) *Failure {
	var e stripeError
	_ = json.Unmarshal(body, &e)
	detail := fmt.Sprintf("stripe HTTP %d", status)
	if e.Error.Message != "" {
		detail += ": " + e.Error.Message
	}

	switch {
	case status == http.StatusTooManyRequests || status >= 500:
		return &Failure{Code: CodeUnavailable, Detail: detail}
	case e.Error.Type == "card_error":
		code := e.Error.DeclineCode
		if code == "" {
			code = e.Error.Code
		}
		switch code {
		case "insufficient_funds":
			return &Failure{Code: CodeInsufficientFunds, Detail: detail}
		case "expired_card":
			return &Failure{Code: CodeCardExpired, Detail: detail}
		case "fraudulent", "stolen_card", "lost_card", "pickup_card":
			return &Failure{Code: CodeFraudSuspected, Detail: detail}
		default:
			return &Failure{Code: CodeCardDeclined, Detail: detail}
		}
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return &Failure{Code: CodeInternalError, Detail: detail}
	default:
		return &Failure{Code: CodeInvalidRequest, Detail: detail}
	}
}

// FromEnv picks the provider for the payments service.
//
//	PSP_PROVIDER=simulator   (default) the deterministic Simulator; no network, no account
//	PSP_PROVIDER=stripe      Stripe in test mode: STRIPE_SECRET_KEY must be a test key,
//	                         STRIPE_API_BASE overrides the endpoint (for stripe-mock)
//
// The second return value names the choice for the startup log.
func FromEnv(getenv func(string) string) (Provider, string, error) {
	switch p := getenv("PSP_PROVIDER"); p {
	case "", "simulator":
		return NewSimulator(), "simulator (simulated PSP, no real provider)", nil
	case "stripe":
		s, err := NewStripe(getenv("STRIPE_API_BASE"), getenv("STRIPE_SECRET_KEY"), nil)
		if err != nil {
			return nil, "", err
		}
		return s, "stripe test mode at " + s.base, nil
	default:
		return nil, "", fmt.Errorf("psp: unknown PSP_PROVIDER %q (simulator or stripe)", p)
	}
}
