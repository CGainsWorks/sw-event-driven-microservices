// Package inventoryclient is the orders side of the inventory gRPC contract.
//
// Same shape as paymentsclient, and the same one interesting decision: which transport
// failures are ambiguous. Everything else is mechanical translation.
package inventoryclient

import (
	"context"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	commonv1 "github.com/CGainsWorks/sw-event-driven-microservices/proto/gen/go/sw_edm/common/v1"
	inventoryv1 "github.com/CGainsWorks/sw-event-driven-microservices/proto/gen/go/sw_edm/inventory/v1"
	"github.com/CGainsWorks/sw-event-driven-microservices/services/orders/internal/core"
)

// Client implements core.InventoryPort.
type Client struct {
	conn *grpc.ClientConn
	api  inventoryv1.InventoryServiceClient
	log  *slog.Logger
}

func Dial(target string, log *slog.Logger) (*Client, error) {
	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// See paymentsclient: this is what carries traceparent over gRPC metadata.
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		// No transport-level retry policy. Retries belong to the saga, which owns the
		// idempotency keys, the backoff schedule and the persisted attempt count; a retry
		// underneath it would repeat the call without any of that.
		grpc.WithDefaultCallOptions(grpc.WaitForReady(false)),
	)
	if err != nil {
		return nil, fmt.Errorf("inventoryclient: dialling %s: %w", target, err)
	}
	return &Client{
		conn: conn,
		api:  inventoryv1.NewInventoryServiceClient(conn),
		log:  log.With("component", "inventory-client"),
	}, nil
}

func (c *Client) Close() error { return c.conn.Close() }

// classify decides what a transport error means to the saga. See the equivalent in
// paymentsclient for why Unavailable counts as ambiguous rather than as a clean miss.
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
		return fmt.Errorf("inventoryclient: %s: %s", st.Code(), st.Message())
	}
}

func reqCtx(correlationID, causationID string) *commonv1.RequestContext {
	return &commonv1.RequestContext{CorrelationId: correlationID, CausationEventId: causationID}
}

func protoLines(lines []core.StockLine) []*commonv1.StockLine {
	out := make([]*commonv1.StockLine, 0, len(lines))
	for _, l := range lines {
		out = append(out, &commonv1.StockLine{Sku: l.SKU, Quantity: l.Quantity})
	}
	return out
}

func (c *Client) Reserve(ctx context.Context, in core.ReserveInput) (core.StepOutcome, error) {
	res, err := c.api.ReserveStock(ctx, &inventoryv1.ReserveStockRequest{
		Context:        reqCtx(in.CorrelationID, in.CausationID),
		IdempotencyKey: in.IdempotencyKey,
		OrderId:        in.OrderID,
		Lines:          protoLines(in.Lines),
		TtlSeconds:     in.TTLSeconds,
	})
	if err != nil {
		return core.StepOutcome{}, classify(err)
	}

	switch res.GetOutcome() {
	case inventoryv1.ReserveStockResponse_OUTCOME_RESERVED:
		return core.StepOutcome{OK: true, ReservationID: res.GetReservationId()}, nil
	default:
		// INSUFFICIENT_STOCK and UNKNOWN_SKU are both terminal. Retrying an out-of-stock
		// SKU is a bet that someone else will cancel an order in the next few seconds, and
		// making a customer wait on that bet is worse than telling them no now.
		return core.StepOutcome{
			OK:            false,
			Retryable:     false,
			Detail:        shortfallDetail(res),
			ReservationID: res.GetReservationId(),
		}, nil
	}
}

func (c *Client) Release(ctx context.Context, in core.ReleaseInput) (core.StepOutcome, error) {
	res, err := c.api.ReleaseStock(ctx, &inventoryv1.ReleaseStockRequest{
		Context:        reqCtx(in.CorrelationID, in.CausationID),
		IdempotencyKey: in.IdempotencyKey,
		ReservationId:  in.ReservationID,
		Reason:         inventoryv1.ReleaseReason_RELEASE_REASON_COMPENSATION,
	})
	if err != nil {
		return core.StepOutcome{}, classify(err)
	}
	switch res.GetOutcome() {
	case inventoryv1.ReleaseStockResponse_OUTCOME_RELEASED,
		inventoryv1.ReleaseStockResponse_OUTCOME_NOT_FOUND:
		// NOT_FOUND counts as success for a compensation: no stock is held on this order's
		// behalf, which is what the compensation was for. A compensation that finds its
		// work already done, or never needed, has succeeded.
		return core.StepOutcome{OK: true}, nil
	default:
		// ALREADY_COMMITTED. The units are gone and cannot be released; the saga tried to
		// compensate something it should not have. Reported as a failure so it becomes a
		// stuck saga a human looks at, rather than being silently swallowed.
		return core.StepOutcome{OK: false, Detail: res.GetOutcome().String()}, nil
	}
}

func (c *Client) Commit(ctx context.Context, in core.CommitInput) (core.StepOutcome, error) {
	res, err := c.api.CommitReservation(ctx, &inventoryv1.CommitReservationRequest{
		Context:        reqCtx(in.CorrelationID, in.CausationID),
		IdempotencyKey: in.IdempotencyKey,
		ReservationId:  in.ReservationID,
	})
	if err != nil {
		return core.StepOutcome{}, classify(err)
	}
	switch res.GetOutcome() {
	case inventoryv1.CommitReservationResponse_OUTCOME_COMMITTED:
		return core.StepOutcome{OK: true, ReservationID: in.ReservationID}, nil
	default:
		// EXPIRED, RELEASED, NOT_FOUND. All terminal: no amount of retrying revives a
		// lapsed hold, so the saga compensates rather than backing off against a wall.
		return core.StepOutcome{OK: false, Retryable: false, Detail: res.GetOutcome().String()}, nil
	}
}

// ResolveReserve answers whether a timed-out reserve request ever reached inventory.
//
// A nil outcome with a nil error means no record exists, so the request never arrived and
// the saga may safely send it again.
func (c *Client) ResolveReserve(ctx context.Context, idempotencyKey string) (*core.StepOutcome, error) {
	res, err := c.api.GetReservation(ctx, &inventoryv1.GetReservationRequest{
		Lookup: &inventoryv1.GetReservationRequest_IdempotencyKey{IdempotencyKey: idempotencyKey},
	})
	if err != nil {
		// Could not ask. Explicitly not an answer: the caller keeps the step UNKNOWN and
		// tries again rather than treating an unreachable inventory as evidence.
		return nil, classify(err)
	}
	if !res.GetFound() {
		return nil, nil
	}

	switch res.GetState() {
	case inventoryv1.ReservationState_RESERVATION_STATE_HELD,
		inventoryv1.ReservationState_RESERVATION_STATE_COMMITTED:
		return &core.StepOutcome{OK: true, ReservationID: res.GetReservationId()}, nil

	case inventoryv1.ReservationState_RESERVATION_STATE_FAILED:
		return &core.StepOutcome{
			OK:            false,
			Retryable:     false,
			Detail:        "reservation was refused",
			ReservationID: res.GetReservationId(),
		}, nil

	case inventoryv1.ReservationState_RESERVATION_STATE_RELEASED,
		inventoryv1.ReservationState_RESERVATION_STATE_EXPIRED:
		// The reservation existed and is gone. Treated as a terminal failure of this step:
		// the saga cannot proceed on a hold that no longer exists, and re-reserving under
		// the same key would return this same released record rather than new stock.
		return &core.StepOutcome{
			OK:            false,
			Retryable:     false,
			Detail:        "reservation " + res.GetState().String(),
			ReservationID: res.GetReservationId(),
		}, nil

	default:
		return nil, fmt.Errorf("%w: inventory has the reservation in an unspecified state", core.ErrAmbiguous)
	}
}

func shortfallDetail(res *inventoryv1.ReserveStockResponse) string {
	detail := res.GetOutcome().String()
	for _, s := range res.GetShortfalls() {
		detail += fmt.Sprintf(" [%s requested=%d available=%d]", s.GetSku(), s.GetRequested(), s.GetAvailable())
	}
	return detail
}
