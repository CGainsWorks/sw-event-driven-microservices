// Package grpcapi is the transport layer for payments. It translates protobuf messages to
// and from the domain types in internal/core and does nothing else — no business rules
// live here, so that the domain can be tested without a server.
package grpcapi

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/CGainsWorks/sw-event-driven-microservices/libs/idempotency"
	commonv1 "github.com/CGainsWorks/sw-event-driven-microservices/proto/gen/go/sw_edm/common/v1"
	paymentsv1 "github.com/CGainsWorks/sw-event-driven-microservices/proto/gen/go/sw_edm/payments/v1"
	"github.com/CGainsWorks/sw-event-driven-microservices/services/payments/internal/core"
)

// Server implements sw_edm.payments.v1.PaymentsService.
type Server struct {
	paymentsv1.UnimplementedPaymentsServiceServer
	svc *core.Service
	log *slog.Logger
}

func New(svc *core.Service, log *slog.Logger) *Server {
	return &Server{svc: svc, log: log.With("component", "payments-grpc")}
}

func (s *Server) AuthorizePayment(ctx context.Context, req *paymentsv1.AuthorizePaymentRequest) (*paymentsv1.AuthorizePaymentResponse, error) {
	if err := requireKey(req.GetIdempotencyKey()); err != nil {
		return nil, err
	}
	amount, err := fromProtoMoney(req.GetAmount())
	if err != nil {
		return nil, err
	}

	res, err := s.svc.Authorize(ctx, core.AuthorizeRequest{
		IdempotencyKey:     req.GetIdempotencyKey(),
		OrderID:            req.GetOrderId(),
		CustomerID:         req.GetCustomerId(),
		Amount:             amount,
		PaymentMethodToken: req.GetPaymentMethodToken(),
		CorrelationID:      req.GetContext().GetCorrelationId(),
		CausationID:        req.GetContext().GetCausationEventId(),
	})
	if err != nil {
		return nil, toStatus(err)
	}

	out := &paymentsv1.AuthorizePaymentResponse{
		PaymentId:    res.PaymentID,
		State:        toProtoState(res.State),
		Failure:      toProtoFailure(res.Failure),
		Deduplicated: res.Deduplicated,
	}
	switch res.Outcome {
	case core.OutcomeAuthorized:
		out.Outcome = paymentsv1.AuthorizePaymentResponse_OUTCOME_AUTHORIZED
	default:
		out.Outcome = paymentsv1.AuthorizePaymentResponse_OUTCOME_FAILED
	}
	if res.ExpiresAt != nil {
		out.ExpiresAt = timestamppb.New(*res.ExpiresAt)
	}
	return out, nil
}

func (s *Server) CapturePayment(ctx context.Context, req *paymentsv1.CapturePaymentRequest) (*paymentsv1.CapturePaymentResponse, error) {
	if err := requireKey(req.GetIdempotencyKey()); err != nil {
		return nil, err
	}
	amount, err := fromProtoMoney(req.GetAmount())
	if err != nil {
		return nil, err
	}

	res, err := s.svc.Capture(ctx, core.CaptureRequest{
		IdempotencyKey: req.GetIdempotencyKey(),
		PaymentID:      req.GetPaymentId(),
		Amount:         amount,
		CorrelationID:  req.GetContext().GetCorrelationId(),
		CausationID:    req.GetContext().GetCausationEventId(),
	})
	if err != nil {
		return nil, toStatus(err)
	}

	outcome := map[core.Outcome]paymentsv1.CapturePaymentResponse_Outcome{
		core.OutcomeCaptured:          paymentsv1.CapturePaymentResponse_OUTCOME_CAPTURED,
		core.OutcomeAuthExpired:       paymentsv1.CapturePaymentResponse_OUTCOME_AUTHORIZATION_EXPIRED,
		core.OutcomeVoided:            paymentsv1.CapturePaymentResponse_OUTCOME_VOIDED,
		core.OutcomeExceedsAuthorized: paymentsv1.CapturePaymentResponse_OUTCOME_EXCEEDS_AUTHORIZED,
		core.OutcomeFailed:            paymentsv1.CapturePaymentResponse_OUTCOME_FAILED,
		core.OutcomeNotFound:          paymentsv1.CapturePaymentResponse_OUTCOME_NOT_FOUND,
	}[res.Outcome]

	return &paymentsv1.CapturePaymentResponse{
		Outcome:       outcome,
		PaymentId:     res.PaymentID,
		State:         toProtoState(res.State),
		CapturedTotal: toProtoMoney(res.CapturedTotal),
		LedgerEntryId: res.LedgerEntryID,
		Failure:       toProtoFailure(res.Failure),
		Deduplicated:  res.Deduplicated,
	}, nil
}

func (s *Server) VoidAuthorization(ctx context.Context, req *paymentsv1.VoidAuthorizationRequest) (*paymentsv1.VoidAuthorizationResponse, error) {
	if err := requireKey(req.GetIdempotencyKey()); err != nil {
		return nil, err
	}
	res, err := s.svc.Void(ctx, core.VoidRequest{
		IdempotencyKey: req.GetIdempotencyKey(),
		PaymentID:      req.GetPaymentId(),
		CorrelationID:  req.GetContext().GetCorrelationId(),
		CausationID:    req.GetContext().GetCausationEventId(),
	})
	if err != nil {
		return nil, toStatus(err)
	}

	outcome := map[core.Outcome]paymentsv1.VoidAuthorizationResponse_Outcome{
		core.OutcomeVoided:          paymentsv1.VoidAuthorizationResponse_OUTCOME_VOIDED,
		core.OutcomeAlreadyCaptured: paymentsv1.VoidAuthorizationResponse_OUTCOME_ALREADY_CAPTURED,
		core.OutcomeNotFound:        paymentsv1.VoidAuthorizationResponse_OUTCOME_NOT_FOUND,
		core.OutcomeFailed:          paymentsv1.VoidAuthorizationResponse_OUTCOME_FAILED,
	}[res.Outcome]

	return &paymentsv1.VoidAuthorizationResponse{
		Outcome:      outcome,
		State:        toProtoState(res.State),
		Failure:      toProtoFailure(res.Failure),
		Deduplicated: res.Deduplicated,
	}, nil
}

func (s *Server) RefundPayment(ctx context.Context, req *paymentsv1.RefundPaymentRequest) (*paymentsv1.RefundPaymentResponse, error) {
	if err := requireKey(req.GetIdempotencyKey()); err != nil {
		return nil, err
	}
	amount, err := fromProtoMoney(req.GetAmount())
	if err != nil {
		return nil, err
	}

	res, err := s.svc.Refund(ctx, core.RefundRequest{
		IdempotencyKey: req.GetIdempotencyKey(),
		PaymentID:      req.GetPaymentId(),
		Amount:         amount,
		Reason:         refundReasonName(req.GetReason()),
		CorrelationID:  req.GetContext().GetCorrelationId(),
		CausationID:    req.GetContext().GetCausationEventId(),
	})
	if err != nil {
		return nil, toStatus(err)
	}

	outcome := map[core.Outcome]paymentsv1.RefundPaymentResponse_Outcome{
		core.OutcomeRefunded:        paymentsv1.RefundPaymentResponse_OUTCOME_REFUNDED,
		core.OutcomeExceedsCaptured: paymentsv1.RefundPaymentResponse_OUTCOME_EXCEEDS_CAPTURED,
		core.OutcomeNothingCaptured: paymentsv1.RefundPaymentResponse_OUTCOME_NOTHING_CAPTURED,
		core.OutcomeNotFound:        paymentsv1.RefundPaymentResponse_OUTCOME_NOT_FOUND,
		core.OutcomeFailed:          paymentsv1.RefundPaymentResponse_OUTCOME_FAILED,
	}[res.Outcome]

	return &paymentsv1.RefundPaymentResponse{
		Outcome:       outcome,
		State:         toProtoState(res.State),
		RefundedTotal: toProtoMoney(res.RefundedTotal),
		LedgerEntryId: res.LedgerEntryID,
		Failure:       toProtoFailure(res.Failure),
		Deduplicated:  res.Deduplicated,
	}, nil
}

// GetPaymentByIdempotencyKey answers the question a timed-out caller has to ask.
//
// found=false is a real answer, not an error: it means no payment matches that key, so the
// request never arrived and it is safe to send again. Reporting it as NOT_FOUND would make
// it indistinguishable from "I could not reach payments", which is the exact ambiguity
// this RPC exists to remove.
func (s *Server) GetPaymentByIdempotencyKey(ctx context.Context, req *paymentsv1.GetPaymentByIdempotencyKeyRequest) (*paymentsv1.GetPaymentByIdempotencyKeyResponse, error) {
	if err := requireKey(req.GetIdempotencyKey()); err != nil {
		return nil, err
	}
	p, err := s.svc.GetByIdempotencyKey(ctx, req.GetIdempotencyKey())
	if err != nil {
		return nil, toStatus(err)
	}
	if p == nil {
		return &paymentsv1.GetPaymentByIdempotencyKeyResponse{Found: false}, nil
	}

	out := &paymentsv1.GetPaymentByIdempotencyKeyResponse{
		Found:            true,
		PaymentId:        p.ID,
		OrderId:          p.OrderID,
		State:            toProtoState(p.State),
		AuthorizedAmount: toProtoMoney(p.Authorized()),
		CapturedTotal:    toProtoMoney(p.Captured()),
		RefundedTotal:    toProtoMoney(p.Refunded()),
		CreatedAt:        timestamppb.New(p.CreatedAt),
	}
	if p.ExpiresAt != nil {
		out.ExpiresAt = timestamppb.New(*p.ExpiresAt)
	}
	if p.State == core.StateFailed && p.FailureCode != "" {
		out.Failure = &paymentsv1.Failure{
			Code:      failureCodeFromName(p.FailureCode),
			Retryable: p.FailureRetryable,
			Detail:    p.FailureDetail,
		}
	}
	return out, nil
}

// --- mapping ----------------------------------------------------------------

func requireKey(key string) error {
	if key == "" {
		return status.Error(codes.InvalidArgument, "idempotency_key is required")
	}
	return nil
}

func fromProtoMoney(m *commonv1.Money) (core.Money, error) {
	if m == nil {
		return core.Money{}, status.Error(codes.InvalidArgument, "amount is required")
	}
	if len(m.GetCurrency()) != 3 {
		return core.Money{}, status.Error(codes.InvalidArgument, "currency must be a three-letter ISO 4217 code")
	}
	if m.GetAmountMinor() < 0 {
		return core.Money{}, status.Error(codes.InvalidArgument, "amount_minor must not be negative")
	}
	return core.Money{AmountMinor: m.GetAmountMinor(), Currency: m.GetCurrency()}, nil
}

func toProtoMoney(m core.Money) *commonv1.Money {
	return &commonv1.Money{AmountMinor: m.AmountMinor, Currency: m.Currency}
}

// toProtoState maps the domain state to the contract.
//
// StatePending deliberately becomes PAYMENT_STATE_UNSPECIFIED. It is an internal state
// meaning "we have your request and the provider has not answered", and callers that see
// it alongside found=true know to ask again rather than to conclude anything.
func toProtoState(s core.State) paymentsv1.PaymentState {
	switch s {
	case core.StateAuthorized:
		return paymentsv1.PaymentState_PAYMENT_STATE_AUTHORIZED
	case core.StateCaptured:
		return paymentsv1.PaymentState_PAYMENT_STATE_CAPTURED
	case core.StateVoided:
		return paymentsv1.PaymentState_PAYMENT_STATE_VOIDED
	case core.StateRefunded:
		return paymentsv1.PaymentState_PAYMENT_STATE_REFUNDED
	case core.StateFailed:
		return paymentsv1.PaymentState_PAYMENT_STATE_FAILED
	default:
		return paymentsv1.PaymentState_PAYMENT_STATE_UNSPECIFIED
	}
}

func toProtoFailure(f *core.FailureInfo) *paymentsv1.Failure {
	if f == nil {
		return nil
	}
	return &paymentsv1.Failure{
		Code:      failureCodeFromName(f.Code),
		Retryable: f.Retryable,
		Detail:    f.Detail,
	}
}

func failureCodeFromName(name string) paymentsv1.FailureCode {
	if v, ok := paymentsv1.FailureCode_value["FAILURE_CODE_"+name]; ok {
		return paymentsv1.FailureCode(v)
	}
	return paymentsv1.FailureCode_FAILURE_CODE_UNSPECIFIED
}

func refundReasonName(r paymentsv1.RefundReason) string {
	name := paymentsv1.RefundReason_name[int32(r)]
	if name == "" || r == paymentsv1.RefundReason_REFUND_REASON_UNSPECIFIED {
		return "SAGA_COMPENSATION"
	}
	return name[len("REFUND_REASON_"):]
}

// toStatus turns a domain error into a gRPC status.
//
// Only genuine faults reach here — business outcomes are values, not errors. The
// distinction the caller needs is between "retrying might work" and "it will not", so
// that is what the code carries:
//
//	FailedPrecondition  the key is held by a live attempt; try again shortly
//	InvalidArgument     the caller reused a key for a different request; a bug, never retry
//	Unavailable         the provider or database could not be reached; retryable
//	DeadlineExceeded    passed through, because it is the ambiguous outcome and the caller
//	                    must resolve it with GetPaymentByIdempotencyKey rather than assume
func toStatus(err error) error {
	switch {
	case errors.Is(err, idempotency.ErrInProgress):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, idempotency.ErrKeyReuse):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	default:
		return status.Error(codes.Unavailable, err.Error())
	}
}

// AuthorizationTTL is exported so main can log the configured value at startup; a hold
// whose lifetime nobody knows is a hold nobody can reason about.
const AuthorizationTTL = 7 * 24 * time.Hour
