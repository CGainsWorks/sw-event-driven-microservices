// Package httpapi is the customer-facing surface of orders. It validates input, calls the
// domain and renders JSON. No business rules live here.
package httpapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/CGainsWorks/sw-event-driven-microservices/libs/outbox"
	"github.com/CGainsWorks/sw-event-driven-microservices/services/orders/internal/core"
)

// API renders the orders HTTP surface.
type API struct {
	svc   *core.Service
	relay *outbox.Relay
	log   *slog.Logger

	// nudge asks the orchestrator to advance a saga now rather than waiting for the
	// recovery loop's next tick. Best effort: if it is dropped, the recovery loop picks
	// the saga up anyway, because the saga's readiness lives in the database and not in
	// this channel.
	nudge func(orderID string)
}

func New(svc *core.Service, relay *outbox.Relay, nudge func(string), log *slog.Logger) *API {
	return &API{svc: svc, relay: relay, nudge: nudge, log: log.With("component", "orders-http")}
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", a.placeOrder)
	mux.HandleFunc("GET /orders/{id}", a.getOrder)
	mux.HandleFunc("GET /orders/{id}/saga", a.getSaga)
	mux.HandleFunc("GET /health/live", a.live)
	mux.HandleFunc("GET /health/ready", a.ready)
	return mux
}

type placeOrderRequest struct {
	CustomerID         string `json:"customer_id"`
	CustomerEmail      string `json:"customer_email"`
	PaymentMethodToken string `json:"payment_method_token"`
	CorrelationID      string `json:"correlation_id,omitempty"`
	Lines              []struct {
		SKU        string `json:"sku"`
		Quantity   uint32 `json:"quantity"`
		UnitAmount struct {
			AmountMinor int64  `json:"amount_minor"`
			Currency    string `json:"currency"`
		} `json:"unit_amount"`
	} `json:"lines"`
}

type orderResponse struct {
	OrderID       string      `json:"order_id"`
	State         core.State  `json:"state"`
	CustomerID    string      `json:"customer_id"`
	Total         core.Money  `json:"total_amount"`
	Lines         []core.Line `json:"lines"`
	CorrelationID string      `json:"correlation_id"`
	PaymentID     string      `json:"payment_id,omitempty"`
	ReservationID string      `json:"reservation_id,omitempty"`
	Reason        string      `json:"cancellation_reason,omitempty"`
	FailedStep    string      `json:"failed_step,omitempty"`
	CreatedAt     time.Time   `json:"created_at"`
	UpdatedAt     time.Time   `json:"updated_at"`
}

// placeOrder accepts an order and returns 202, not 201.
//
// 202 because the order is accepted but not yet fulfilled — stock is unchecked and no money
// has moved. The eventual consistency between placing and confirming is real and
// customer-visible, so the API states it rather than hiding it behind a status code that
// implies more than happened. The client polls GET /orders/{id} or waits for the event.
func (a *API) placeOrder(w http.ResponseWriter, r *http.Request) {
	var req placeOrderRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "malformed_request", err.Error())
		return
	}

	lines := make([]core.Line, 0, len(req.Lines))
	for _, l := range req.Lines {
		lines = append(lines, core.Line{
			SKU:        l.SKU,
			Quantity:   l.Quantity,
			UnitAmount: core.Money{AmountMinor: l.UnitAmount.AmountMinor, Currency: l.UnitAmount.Currency},
		})
	}

	order, err := a.svc.PlaceOrder(r.Context(), core.PlaceOrderRequest{
		CustomerID:         req.CustomerID,
		CustomerEmail:      req.CustomerEmail,
		Lines:              lines,
		PaymentMethodToken: req.PaymentMethodToken,
		CorrelationID:      req.CorrelationID,
	})
	if err != nil {
		if errors.Is(err, core.ErrInvalidOrder) {
			writeError(w, http.StatusBadRequest, "invalid_order", err.Error())
			return
		}
		a.log.Error("placing order", "error", err)
		writeError(w, http.StatusServiceUnavailable, "unavailable", "could not accept the order")
		return
	}

	if a.nudge != nil {
		a.nudge(order.ID)
	}

	w.Header().Set("Location", "/orders/"+order.ID)
	writeJSON(w, http.StatusAccepted, toResponse(order))
}

func (a *API) getOrder(w http.ResponseWriter, r *http.Request) {
	order, err := a.svc.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		a.log.Error("reading order", "error", err)
		writeError(w, http.StatusServiceUnavailable, "unavailable", "could not read the order")
		return
	}
	if order == nil {
		writeError(w, http.StatusNotFound, "not_found", "no such order")
		return
	}
	writeJSON(w, http.StatusOK, toResponse(order))
}

// getSaga exposes the saga's step log.
//
// Deliberately part of the API rather than hidden. When an order sits in
// AWAITING_PAYMENT, the only useful question is "which step, and what did it say" — and
// answering it from a log aggregator is far slower than answering it from the service that
// owns the state. The integration and chaos tests assert against this endpoint for the
// same reason.
func (a *API) getSaga(w http.ResponseWriter, r *http.Request) {
	snap, err := a.svc.SagaFor(r.Context(), r.PathValue("id"))
	if err != nil {
		a.log.Error("reading saga", "error", err)
		writeError(w, http.StatusServiceUnavailable, "unavailable", "could not read the saga")
		return
	}
	if snap == nil {
		writeError(w, http.StatusNotFound, "not_found", "no saga for that order")
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (a *API) live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ready reports the outbox backlog without failing on it.
//
// A relay that cannot reach Kafka must not take orders out of service: the outbox is
// precisely what makes accepting orders during a broker outage safe. The number is exposed
// so the chaos tests can assert the backlog drains once the broker returns, rather than
// assuming that it did.
func (a *API) ready(w http.ResponseWriter, r *http.Request) {
	pending, err := a.relay.PendingCount(r.Context())
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status": "degraded", "error": err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "outbox_pending": pending})
}

func toResponse(o *core.Order) orderResponse {
	return orderResponse{
		OrderID:       o.ID,
		State:         o.State,
		CustomerID:    o.CustomerID,
		Total:         o.Total,
		Lines:         o.Lines,
		CorrelationID: o.CorrelationID,
		PaymentID:     o.PaymentID,
		ReservationID: o.ReservationID,
		Reason:        o.CancellationReason,
		FailedStep:    o.FailedStep,
		CreatedAt:     o.CreatedAt,
		UpdatedAt:     o.UpdatedAt,
	}
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, code int, kind, message string) {
	writeJSON(w, code, map[string]string{"error": kind, "message": message})
}
