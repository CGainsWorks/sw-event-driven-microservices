// Package paymentsclient is the orders side of the payments gRPC contract.
//
// Its one interesting job is deciding which transport failures are ambiguous. Everything
// else is mechanical translation.
package paymentsclient

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	commonv1 "github.com/CGainsWorks/sw-event-driven-microservices/proto/gen/go/sw_edm/common/v1"
	paymentsv1 "github.com/CGainsWorks/sw-event-driven-microservices/proto/gen/go/sw_edm/payments/v1"
	"github.com/CGainsWorks/sw-event-driven-microservices/services/orders/internal/core"
)

// Client implements core.PaymentsPort.
type Client struct {
	conn *grpc.ClientConn
	api  paymentsv1.PaymentsServiceClient
	log  *slog.Logger
}

// Dial connects lazily. gRPC establishes the connection on first use, so this does not
// fail when payments is not yet up — which matters because compose starts everything at
// once and an order should be accepted even while payments is still booting.
func Dial(target string, log *slog.Logger) (*Client, error) {
	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// Injects traceparent into gRPC metadata, so the callee's spans become children of
		// the saga step that called it. This is the whole of the cross-service trace
		// plumbing for gRPC — neither side reads a header by hand.
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		// No retry policy configured on purpose. Retries are the saga's business: it owns
		// the idempotency keys, the backoff schedule and the persisted attempt count, and
		// a transport-level retry underneath it would retry without any of that context.
		grpc.WithDefaultCallOptions(grpc.WaitForReady(false)),
	)
	if err != nil {
		return nil, fmt.Errorf("paymentsclient: dialling %s: %w", target, err)
	}
	return &Client{conn: conn, api: paymentsv1.NewPaymentsServiceClient(conn), log: log.With("component", "payments-client")}, nil
}

func (c *Client) Close() error { return c.conn.Close() }

// classify decides what a transport error means to the saga.
//
// The distinction that matters is whether the callee might have done the work.
//
//	Unavailable, DeadlineExceeded, Unknown, Aborted  -> ambiguous. The request may have
//	    arrived and been processed. The saga must resolve it by asking, not by assuming.
//	FailedPrecondition -> a live duplicate holds the key; a plain retry is correct.
//	everything else -> a genuine, unambiguous error.
//
// Unavailable is treated as ambiguous even though it usually means the connection was
// refused, because it can also be returned after a request was written and the peer died.
// Resolving costs one extra call and answers correctly in both cases; assuming costs a
// double charge in one of them.
func classify(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return fmt.Errorf("%w: %v", core.ErrAmbiguous, err)
	}
	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Unknown, codes.Aborted:
		return fmt.Errorf("%w: %s: %s", core.ErrAmbiguous, st.Code(), st.Message())
	default:
		return fmt.Errorf("paymentsclient: %s: %s", st.Code(), st.Message())
	}
}

func money(m core.Money) *commonv1.Money {
	return &commonv1.Money{AmountMinor: m.AmountMinor, Currency: m.Currency}
}

func reqCtx(correlationID, causationID string) *commonv1.RequestContext {
	return &commonv1.RequestContext{CorrelationId: correlationID, CausationEventId: causationID}
}

func (c *Client) Authorize(ctx context.Context, in core.AuthorizeInput) (core.StepOutcome, error) {
	res, err := c.api.AuthorizePayment(ctx, &paymentsv1.AuthorizePaymentRequest{
		Context:            reqCtx(in.CorrelationID, in.CausationID),
		IdempotencyKey:     in.IdempotencyKey,
		OrderId:            in.OrderID,
		CustomerId:         in.CustomerID,
		Amount:             money(in.Amount),
		PaymentMethodToken: in.PaymentMethodToken,
	})
	if err != nil {
		return core.StepOutcome{}, classify(err)
	}
	if res.GetOutcome() == paymentsv1.AuthorizePaymentResponse_OUTCOME_AUTHORIZED {
		return core.StepOutcome{OK: true, PaymentID: res.GetPaymentId()}, nil
	}
	return core.StepOutcome{
		OK:        false,
		Retryable: res.GetFailure().GetRetryable(),
		Detail:    failureDetail(res.GetFailure()),
		PaymentID: res.GetPaymentId(),
	}, nil
}

func (c *Client) Capture(ctx context.Context, in core.CaptureInput) (core.StepOutcome, error) {
	res, err := c.api.CapturePayment(ctx, &paymentsv1.CapturePaymentRequest{
		Context:        reqCtx(in.CorrelationID, in.CausationID),
		IdempotencyKey: in.IdempotencyKey,
		PaymentId:      in.PaymentID,
		Amount:         money(in.Amount),
	})
	if err != nil {
		return core.StepOutcome{}, classify(err)
	}
	switch res.GetOutcome() {
	case paymentsv1.CapturePaymentResponse_OUTCOME_CAPTURED:
		return core.StepOutcome{OK: true, PaymentID: res.GetPaymentId()}, nil
	case paymentsv1.CapturePaymentResponse_OUTCOME_FAILED:
		return core.StepOutcome{OK: false, Retryable: res.GetFailure().GetRetryable(),
			Detail: failureDetail(res.GetFailure())}, nil
	default:
		// AUTHORIZATION_EXPIRED, VOIDED, EXCEEDS_AUTHORIZED, NOT_FOUND. All terminal:
		// retrying cannot change any of them, so the saga compensates rather than backing
		// off against a wall.
		return core.StepOutcome{OK: false, Retryable: false,
			Detail: res.GetOutcome().String()}, nil
	}
}

func (c *Client) Void(ctx context.Context, in core.VoidInput) (core.StepOutcome, error) {
	res, err := c.api.VoidAuthorization(ctx, &paymentsv1.VoidAuthorizationRequest{
		Context:        reqCtx(in.CorrelationID, in.CausationID),
		IdempotencyKey: in.IdempotencyKey,
		PaymentId:      in.PaymentID,
	})
	if err != nil {
		return core.StepOutcome{}, classify(err)
	}
	switch res.GetOutcome() {
	case paymentsv1.VoidAuthorizationResponse_OUTCOME_VOIDED,
		paymentsv1.VoidAuthorizationResponse_OUTCOME_NOT_FOUND:
		// NOT_FOUND counts as success for a compensation: there is nothing holding the
		// customer's funds, which is what the compensation was for. A compensation that
		// finds its work already done, or never needed, has succeeded.
		return core.StepOutcome{OK: true}, nil
	default:
		return core.StepOutcome{OK: false, Detail: res.GetOutcome().String()}, nil
	}
}

func (c *Client) Refund(ctx context.Context, in core.RefundInput) (core.StepOutcome, error) {
	res, err := c.api.RefundPayment(ctx, &paymentsv1.RefundPaymentRequest{
		Context:        reqCtx(in.CorrelationID, in.CausationID),
		IdempotencyKey: in.IdempotencyKey,
		PaymentId:      in.PaymentID,
		Amount:         money(in.Amount),
		Reason:         paymentsv1.RefundReason_REFUND_REASON_SAGA_COMPENSATION,
	})
	if err != nil {
		return core.StepOutcome{}, classify(err)
	}
	switch res.GetOutcome() {
	case paymentsv1.RefundPaymentResponse_OUTCOME_REFUNDED,
		paymentsv1.RefundPaymentResponse_OUTCOME_NOTHING_CAPTURED:
		return core.StepOutcome{OK: true}, nil
	default:
		return core.StepOutcome{OK: false, Detail: res.GetOutcome().String()}, nil
	}
}

// ResolveAuthorize answers whether a timed-out authorize request ever reached payments.
//
// A nil outcome with a nil error means no record exists, so the request never arrived and
// the saga may safely send it again. That is a real answer, and it is the reason this call
// exists rather than the saga guessing.
func (c *Client) ResolveAuthorize(ctx context.Context, idempotencyKey string) (*core.StepOutcome, error) {
	res, err := c.api.GetPaymentByIdempotencyKey(ctx, &paymentsv1.GetPaymentByIdempotencyKeyRequest{
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		// Could not ask. Explicitly NOT an answer: the caller must keep the step UNKNOWN
		// and try again rather than treat an unreachable payments service as evidence of
		// anything.
		return nil, classify(err)
	}
	if !res.GetFound() {
		return nil, nil
	}

	switch res.GetState() {
	case paymentsv1.PaymentState_PAYMENT_STATE_AUTHORIZED,
		paymentsv1.PaymentState_PAYMENT_STATE_CAPTURED:
		return &core.StepOutcome{OK: true, PaymentID: res.GetPaymentId()}, nil

	case paymentsv1.PaymentState_PAYMENT_STATE_FAILED:
		return &core.StepOutcome{
			OK:        false,
			Retryable: res.GetFailure().GetRetryable(),
			Detail:    failureDetail(res.GetFailure()),
			PaymentID: res.GetPaymentId(),
		}, nil

	default:
		// UNSPECIFIED means payments has the request recorded but the provider has not
		// answered. Still unknown — report it as unresolved rather than inventing an
		// outcome, so the saga waits and asks again.
		return nil, fmt.Errorf("%w: payments has the request but no outcome yet", core.ErrAmbiguous)
	}
}

func failureDetail(f *paymentsv1.Failure) string {
	if f == nil {
		return "unspecified failure"
	}
	if f.GetDetail() == "" {
		return f.GetCode().String()
	}
	return f.GetCode().String() + ": " + f.GetDetail()
}

// DialTimeout is how long main waits for payments to become reachable before reporting
// itself degraded. Not a hard failure: orders accepts orders whether or not payments is
// up, which is the availability posture docs/service_boundaries.md argues for.
const DialTimeout = 5 * time.Second
